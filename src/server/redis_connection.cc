/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#include <event2/buffer.h>
#include <glog/logging.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>

#include <chrono>
#include <mutex>
#include <ratio>
#include <shared_mutex>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "commands/commander.h"
#include "commands/scan_util.h"
#include "fmt/format.h"
#include "lock/lock.h"
#include "lock/lock_defines.h"
#include "stats/stats.h"
#include "status.h"
#include "string_util.h"
#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#endif
#include <event2/bufferevent_struct.h>
#include <event2/event.h>

#include "cluster/cluster.h"
#include "commands/blocking_commander.h"
#include "commands/cmd_scan.h"
#include "redis_connection.h"
#include "scope_exit.h"
#include "server.h"
#include "time_util.h"
#include "tls_util.h"
#include "worker.h"

namespace redis {

Connection::Connection(bufferevent *bev, Worker *owner)
    : need_free_bev_(true), bev_(bev), req_(owner->srv), owner_(owner), srv_(owner->srv) {
  int64_t now = util::GetTimeStamp();
  create_time_ = now;
  last_interaction_ = now;
}

Connection::~Connection() {
  if (bev_) {
    if (need_free_bev_) {
      bufferevent_free(bev_);
    } else {
      // cleanup event callbacks here to prevent using Connection's resource
      bufferevent_setcb(bev_, nullptr, nullptr, nullptr, nullptr);
    }
  }
  // unsubscribe all channels and patterns if exists
  UnsubscribeAll();
  PUnsubscribeAll();
}

std::string Connection::ToString() {
  return fmt::format("id={} addr={} fd={} name={} age={} idle={} flags={} namespace={} qbuf={} obuf={} cmd={}\n", id_,
                     addr_, bufferevent_getfd(bev_), name_, GetAge(), GetIdleTime(), GetFlags(), ns_,
                     evbuffer_get_length(Input()), evbuffer_get_length(Output()), last_cmd_);
}

void Connection::Close() {
  if (close_cb) close_cb(GetFD());
  owner_->FreeConnection(this);
}

void Connection::Detach() { owner_->DetachConnection(this); }

void Connection::OnRead(struct bufferevent *bev) {
  if (hasBufferExceedLimit(0)) {
    if (IsFlagEnabled(kCloseAsync)) {
      Close();
    }
    return;
  }
  if (is_blocked_) return;

  is_running_ = true;
  auto exit = MakeScopeExit([this] {
    thread_local_metric_array.Record(MetricType::CLIENT_OUT_BUFFER_SIZE, {}, evbuffer_get_length(Output()));
    is_running_ = false;
  });

  SetLastInteraction();
  start_processing_time_ = std::chrono::steady_clock::now();

  auto s = req_.Tokenize(Input(), GetCommandsPtr());
  if (!s.IsOK()) {
    EnableFlag(redis::Connection::kCloseAfterReply);
    Reply(redis::Error("ERR " + s.Msg()));
    LOG(INFO) << "[connection] Failed to tokenize the request. Error: " << s.Msg();
    if (IsFlagEnabled(kCloseAsync)) {
      Close();
    }
    return;
  }

  ExecuteCommands();
  if (IsFlagEnabled(kCloseAsync)) {
    Close();
  }
}

void Connection::OnWrite(bufferevent *bev) {
  int current_priority = event_get_priority(&bev->ev_write);
  int target_priority = owner_->srv->GetConfig()->bufferevent_write_priority;

  if (current_priority != target_priority) {
    if (event_priority_set(&bev->ev_write, target_priority) == -1) {
      LOG(WARNING) << "[connection] Failed to set write priority for fd=" << bufferevent_getfd(bev)
                   << ", current=" << current_priority << ", target=" << target_priority;
    } else {
      LOG(INFO) << "[connection] Set write priority for fd=" << bufferevent_getfd(bev) << ", from=" << current_priority
                << " to=" << target_priority;
    }
  }

  if (IsFlagEnabled(kCloseAfterReply) || IsFlagEnabled(kCloseAsync)) {
    Close();
    return;
  }

  if (IsFlagEnabled(kReadDisabled)) {
    if (auto ret = bufferevent_enable(bev, EV_READ); ret != 0) {
      LOG(WARNING) << "[connection] Enable read event failed, client:" << GetAddr() << ", code:" << ret;
      Close();
      return;
    }
    DisableFlag(kReadDisabled);
    bufferevent_trigger(bev, EV_READ, 0);
  }
}

void Connection::OnEvent(bufferevent *bev, int16_t events) {
  if (events & BEV_EVENT_ERROR) {
    LOG(ERROR) << "[connection] Going to remove the client: " << GetAddr()
               << ", while encounter error: " << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())
#ifdef ENABLE_OPENSSL
               << ", SSL Error: " << SSLError(bufferevent_get_openssl_error(bev))  // NOLINT
#endif
        ;  // NOLINT
    Close();
    return;
  }

  if (events & BEV_EVENT_EOF) {
    LOG(INFO) << "[connection] Going to remove the client: " << GetAddr() << ", while closed by client";
    Close();
    return;
  }

  if (events & BEV_EVENT_TIMEOUT) {
    LOG(INFO) << "[connection] The client: " << GetAddr() << "] reached timeout";
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

bool Connection::hasBufferExceedLimit(size_t msg_size) {
  if (size_t limit = srv_->GetConfig()->client_buffer_limit_mb * MiB; limit > 0) {
    size_t input_size = evbuffer_get_length(Input());
    size_t output_size = evbuffer_get_length(Output());
    if (input_size + output_size + msg_size > limit) {
      LOG(WARNING) << "[connection] Client buffer exceed limit, client:" << GetAddr() << ", input:" << input_size
                   << ", output:" << output_size << ", msg:" << msg_size << ", limit:" << limit;
      static const std::string kBufferExceedLimitErr = redis::Error("ERR connection buffer exceed limit");
      GlobalStatsInstance().IncrClientBufferExceedLimitCount();
      if (disableReadEvent()) {
        // close after send kBufferExceedLimitErr reply
        if (reply(kBufferExceedLimitErr)) EnableFlag(kCloseAfterReply);
      } else {
        // close ASAP when disable read event failed
        EnableFlag(kCloseAsync);
      }
      return true;
    }
    if ((output_size + msg_size) > limit / 2) {
      // try disable read event when output buffer congested
      GlobalStatsInstance().IncrClientBufferCongestedCount();
      disableReadEvent();
    }
  }
  return false;
}

bool Connection::disableReadEvent() {
  if (IsFlagEnabled(kReadDisabled)) return true;

  if (auto ret = bufferevent_disable(bev_, EV_READ); ret != 0) {
    LOG(WARNING) << "[connection] Disable read event failed, client:" << GetAddr() << ", code:" << ret;
    return false;
  }
  EnableFlag(kReadDisabled);
  return true;
}

void Connection::Reply(const std::string &msg) {
  if (!hasBufferExceedLimit(msg.size())) reply(msg);
}

bool Connection::reply(const std::string &msg) {
  if (auto ret = redis::Reply(Output(), msg); ret != 0) {
    LOG(WARNING) << "[connection] Add resp to output buffer failed, client:" << GetAddr() << ", code:" << ret;
    EnableFlag(kCloseAsync);
    return false;
  }
  GlobalStatsInstance().IncrOutbondBytes(msg.size());
  return true;
}

void Connection::SendFile(int fd) {
  // NOTE: we don't need to close the fd, the libevent will do that
  auto output = bufferevent_get_output(bev_);
  evbuffer_add_file(output, fd, 0, -1);
}

void Connection::SetAddr(std::string ip, uint32_t port) {
  ip_ = std::move(ip);
  port_ = port;
  addr_ = ip_ + ":" + std::to_string(port_);
}

uint64_t Connection::GetAge() const { return static_cast<uint64_t>(util::GetTimeStamp() - create_time_); }

uint64_t Connection::GetIdleTime() const { return static_cast<uint64_t>(util::GetTimeStamp() - last_interaction_); }

// Currently, master connection is not handled in connection
// but in replication thread.
//
// The function will return one of the following:
//  kTypeSlave  -> Slave
//  kTypeNormal -> Normal client
//  kTypePubsub -> Client subscribed to Pub/Sub channels
uint64_t Connection::GetClientType() const {
  if (IsFlagEnabled(kSlave)) return kTypeSlave;

  if (!subscribe_channels_.empty() || !subscribe_patterns_.empty()) return kTypePubsub;

  return kTypeNormal;
}

std::string Connection::GetFlags() const {
  std::string flags;
  if (IsFlagEnabled(kSlave)) flags.append("S");
  if (IsFlagEnabled(kCloseAfterReply)) flags.append("c");
  if (IsFlagEnabled(kMonitor)) flags.append("M");
  if (!subscribe_channels_.empty() || !subscribe_patterns_.empty()) flags.append("P");
  if (flags.empty()) flags = "N";
  return flags;
}

void Connection::EnableFlag(Flag flag) { flags_ |= flag; }

void Connection::DisableFlag(Flag flag) { flags_ &= (~flag); }

bool Connection::IsFlagEnabled(Flag flag) const { return (flags_ & flag) > 0; }

bool Connection::CanMigrate() const {
  return !is_running_                                                    // reading or writing
         && !IsFlagEnabled(redis::Connection::kCloseAfterReply)          // close after reply
         && saved_current_command_ == nullptr                            // not executing blocking command like BLPOP
         && subscribe_channels_.empty() && subscribe_patterns_.empty();  // not subscribing any channel
}

void Connection::SubscribeChannel(const std::string &channel) {
  for (const auto &chan : subscribe_channels_) {
    if (channel == chan) return;
  }

  subscribe_channels_.emplace_back(channel);
  owner_->srv->SubscribeChannel(channel, this);
}

void Connection::UnsubscribeChannel(const std::string &channel) {
  for (auto iter = subscribe_channels_.begin(); iter != subscribe_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_channels_.erase(iter);
      owner_->srv->UnsubscribeChannel(channel, this);
      return;
    }
  }
}

void Connection::UnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_channels_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_patterns_.size()));
    return;
  }

  int removed = 0;
  for (const auto &chan : subscribe_channels_) {
    owner_->srv->UnsubscribeChannel(chan, this);
    removed++;
    if (reply) {
      reply(chan, static_cast<int>(subscribe_channels_.size() - removed + subscribe_patterns_.size()));
    }
  }
  subscribe_channels_.clear();
}

int Connection::SubscriptionsCount() { return static_cast<int>(subscribe_channels_.size()); }

void Connection::PSubscribeChannel(const std::string &pattern) {
  for (const auto &p : subscribe_patterns_) {
    if (pattern == p) return;
  }
  subscribe_patterns_.emplace_back(pattern);
  owner_->srv->PSubscribeChannel(pattern, this);
}

void Connection::PUnsubscribeChannel(const std::string &pattern) {
  for (auto iter = subscribe_patterns_.begin(); iter != subscribe_patterns_.end(); iter++) {
    if (*iter == pattern) {
      subscribe_patterns_.erase(iter);
      owner_->srv->PUnsubscribeChannel(pattern, this);
      return;
    }
  }
}

void Connection::PUnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_patterns_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_channels_.size()));
    return;
  }

  int removed = 0;
  for (const auto &pattern : subscribe_patterns_) {
    owner_->srv->PUnsubscribeChannel(pattern, this);
    removed++;
    if (reply) {
      reply(pattern, static_cast<int>(subscribe_patterns_.size() - removed + subscribe_channels_.size()));
    }
  }
  subscribe_patterns_.clear();
}

int Connection::PSubscriptionsCount() { return static_cast<int>(subscribe_patterns_.size()); }

bool Connection::IsProfilingEnabled(const std::string &cmd) {
  auto config = srv_->GetConfig();
  if (config->profiling_sample_ratio == 0) return false;

  if (!config->profiling_sample_all_commands &&
      config->profiling_sample_commands.find(cmd) == config->profiling_sample_commands.end()) {
    return false;
  }

  if (config->profiling_sample_ratio == 100 || std::rand() % 100 <= config->profiling_sample_ratio) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    return true;
  }

  return false;
}

void Connection::RecordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration,
                                             std::optional<std::pair<std::string, std::string>> &&perf_io_context,
                                             int64_t prepare_duration, int64_t command_queue_latency_on_connection) {
  int64_t threshold = srv_->GetConfig()->profiling_sample_record_threshold_us;
  if (threshold < 0 || static_cast<int64_t>(duration) < threshold || srv_->GetPerfLog()->GetMaxEntries() <= 0) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    return;
  }

  if (!perf_io_context.has_value()) {
    perf_io_context =
        std::make_pair(rocksdb::get_perf_context()->ToString(true), rocksdb::get_iostats_context()->ToString(true));
  }
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  // request without db operation
  if (perf_io_context->first.empty()) return;

  auto entry = std::make_unique<PerfEntry>();
  entry->cmd_name = cmd;
  entry->duration = duration;
  entry->prepare_duration = prepare_duration;
  entry->command_queue_latency_on_connection = command_queue_latency_on_connection;
  entry->perf_context = std::move(perf_io_context->first);
  entry->iostats_context = std::move(perf_io_context->second);
  srv_->GetPerfLog()->PushEntry(std::move(entry));
}

Status Connection::CheckKeysInSameSlot(const CommandAttributes *attr, const CommandTokens &cmd_token) {
  // get key indexes
  std::vector<int> key_indexes;
  auto s = CommandTable::GetKeysFromCommand(attr, cmd_token, &key_indexes);
  if (!s.IsOK()) return s;

  // check keys in same slot
  int16_t slot = -1;
  for (auto idx : key_indexes) {
    if (idx >= static_cast<int>(cmd_token.size())) break;
    auto cur_slot = GetSlotIdFromKey(cmd_token[idx]);
    if (slot == -1) slot = static_cast<int16_t>(cur_slot);
    if (cur_slot != slot) {
      return Status{Status::NotOK, "CROSSSLOT Attempted to access keys that don't hash to the same slot"};
    }
  }

  return Status::OK();
}

Status Connection::HandleCmdScript(Context *ctx, std::vector<std::unique_ptr<SlotRangeLock>> *sr_locks) {
  // lock all slotrange with LOCK_X
  auto s = lockAllSlotRanges(ctx, mgl::LockMode::LOCK_X, sr_locks);
  if (!s.IsOK()) {
    return s;
  }

  // check script executable
  s = srv_->cluster->CanScriptExecbyMyself();
  if (!s.IsOK()) {
    return s;
  }
  return Status::OK();
}

StatusOr<std::shared_ptr<engine::Storage>> Connection::HandleCmdNodeScan(
    uint64_t cmd_flags, const std::unique_ptr<Commander> &cmd, Context *ctx,
    std::vector<std::unique_ptr<SlotRangeLock>> *sr_locks) {
  // get slotrange name of starting slot
  auto slot = static_cast<CommandNodeScan *>(cmd.get())->GetStartSlotId();
  auto slotrange_name = srv_->cluster->GetSlotRangeNameBySlotId(slot);

  // lock slotrange
  auto res = SlotRangeLock::AcquireSlotRangeLock(slotrange_name, mgl::LockMode::LOCK_IS, ctx, srv_->GetMGLockMgr());
  if (!res.IsOK()) {
    return Status{res.Is<Status::LockTimeOut>() ? Status::LockTimeOutSlotRange : Status::NotOK, res.Msg()};
  }
  sr_locks->emplace_back(std::move(res.GetValue()));

  // return storage of locked slotrange
  return srv_->cluster->CanExecByMySelf(cmd_flags, slotrange_name, std::string(), slot, true);
}

Status Connection::getExecLocks(uint64_t cmd_flags, const std::string &slot_range_name, const CommandAttributes *attr,
                                const CommandTokens &cmd_token, Context *ctx,
                                std::vector<std::unique_ptr<KeyLock>> *key_locks,
                                std::vector<std::unique_ptr<SlotRangeLock>> *sr_locks) {
  if (cmd_flags & kCmdWrite) {
    auto &range = attr->GetKeyRange(cmd_token);
    for (size_t idx = range.first_key;
         range.last_key > 0 ? idx <= size_t(range.last_key) : idx <= cmd_token.size() + range.last_key;
         idx += range.key_step) {
      // check multi key are in same slotrange
      auto s = srv_->cluster->CheckKeyInSlotRange(slot_range_name, cmd_token[idx]);
      if (!s.IsOK()) {
        LOG(WARNING) << "cmd:" << attr->name << ", keys not in same slotrange, " << s.Msg();
        return Status{Status::CrossSlotRange, "CROSSSLOTRANGE Keys in request don't hash to the same slot range"};
      }
      // get key's mgl
      auto res =
          KeyLock::AcquireKeyLock(slot_range_name, cmd_token[idx], mgl::LockMode::LOCK_X, ctx, srv_->GetMGLockMgr());
      if (!res.IsOK()) {
        LOG(ERROR) << "Failed to get key lock, cmd:" << attr->name << ", key:" << cmd_token[idx]
                   << ", Err: " << res.Msg();
        return Status{Status::NotOK, res.Msg()};
      }
      // Lock same key will get nullptr if lock has been taken
      if (res.GetValue() != nullptr) {
        key_locks->emplace_back(std::move(res.GetValue()));
      }
    }
  } else {
    // get SlotRange LOCK_IS for read cmd
    auto res = SlotRangeLock::AcquireSlotRangeLock(slot_range_name, mgl::LockMode::LOCK_IS, ctx, srv_->GetMGLockMgr());
    if (!res.IsOK()) {
      LOG(ERROR) << "Failed to lock slot range: " << slot_range_name << ", cmd:" << attr->name
                 << ", Err: " << res.Msg();
      return Status{res.Is<Status::LockTimeOut>() ? Status::LockTimeOutSlotRange : Status::NotOK, res.Msg()};
    }
    // check multi keys are in same slotrange, if cmd with multikey
    if (attr->key_range.first_key != attr->key_range.last_key) {
      auto &range = attr->GetKeyRange(cmd_token);
      for (size_t idx = range.first_key;
           range.last_key > 0 ? idx <= size_t(range.last_key) : idx <= cmd_token.size() + range.last_key;
           idx += range.key_step) {
        auto s = srv_->cluster->CheckKeyInSlotRange(slot_range_name, cmd_token[idx]);
        if (!s.IsOK()) {
          LOG(WARNING) << "cmd:" << attr->name << ", keys not in same slotrange, " << s.Msg();
          return Status{Status::CrossSlotRange, "CROSSSLOTRANGE Keys in request don't hash to the same slot range"};
        }
      }
    }
    sr_locks->emplace_back(std::move(res.GetValue()));
  }
  return Status::OK();
}

Status Connection::lockAllSlotRanges(Context *ctx, mgl::LockMode mode,
                                     std::vector<std::unique_ptr<SlotRangeLock>> *sr_locks) {
  auto sr_names = srv_->cluster->GetAllLocalSlotRangeNames();
  for (const auto &name : sr_names) {
    auto res = SlotRangeLock::AcquireSlotRangeLock(name, mode, ctx, srv_->GetMGLockMgr());
    if (!res.IsOK()) {
      sr_locks->clear();
      LOG(INFO) << "Failed to lock slotrange: " << name << ", Err: " << res.Msg();
      return res.ToStatus();
    }
    LOG(INFO) << "Locked slotrange: " << name;
    sr_locks->emplace_back(std::move(res.GetValue()));
  }
  // make sure slotranges same as locked
  if (sr_names != srv_->cluster->GetAllLocalSlotRangeNames()) {
    LOG(WARNING) << "Slotranges changed during locking all slotranges";
    sr_locks->clear();
    return Status{Status::NotOK, "Failed to lock current slotranges"};
  }
  return Status::OK();
}

void Connection::RecordFailAndReply(const std::string &msg, GlobalStats::RequestResult type) {
  Reply(msg);
  GlobalStatsInstance().IncrRequests(type);
}

void Connection::RecordFailAndReplyWithCmd(const std::string &msg, GlobalStats::RequestResult type,
                                           const std::string &cmd_name) {
  RecordFailAndReply(msg, type);
  GlobalStatsInstance().IncrCalls(cmd_name, false);
}

void Connection::SetUnblocked() {
  is_blocked_ = false;
  bufferevent_trigger(bev_, EV_READ, 0);  // Trigger read event on connection
}

void Connection::ExecuteCommands() {
  Config *config = srv_->GetConfig();
  std::string reply, password = config->requirepass;
  thread_local_metric_array.Record(MetricType::REQUESTS_BATCH_SIZE,
                                   {{"worker_id", std::to_string(thread_local_metric_array.thread_id)}},
                                   to_process_cmds_.size());

  while (!to_process_cmds_.empty()) {
    if (IsFlagEnabled(kCloseAsync) || IsFlagEnabled(kCloseAfterReply) || IsFlagEnabled(kReadDisabled)) {
      return;
    }
    auto start = std::chrono::high_resolution_clock::now();
    owner_->blocked_worker_start_block_time.store(std::chrono::steady_clock::now().time_since_epoch());
    absl::Cleanup cleanup = [&block_time = owner_->blocked_worker_start_block_time] {
      block_time.store(std::chrono::nanoseconds(0));
    };

    auto cmd_tokens = to_process_cmds_.front();
    to_process_cmds_.pop_front();
    if (cmd_tokens.empty()) continue;

    std::unique_ptr<Commander> current_cmd;
    auto s = srv_->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd);
    if (!s.IsOK()) {
      RecordFailAndReply(redis::Error("ERR unknown command " + cmd_tokens.front()), GlobalStats::FAIL_USAGE_ERROR);
      continue;
    }
    const auto attributes = current_cmd->GetAttributes();
    auto cmd_name = attributes->name;
    auto cmd_flags = attributes->GenerateFlags(cmd_tokens);
    int arity = attributes->arity;
    int tokens = static_cast<int>(cmd_tokens.size());

    if (GetNamespace().empty()) {
      if (!password.empty() && util::ToLower(cmd_tokens.front()) != "auth" &&
          util::ToLower(cmd_tokens.front()) != "hello") {
        RecordFailAndReplyWithCmd(redis::Error("NOAUTH Authentication required."), GlobalStats::FAIL_UNCONCERNED_ERROR,
                                  cmd_name);
        continue;
      }

      if (password.empty()) {
        BecomeAdmin();
        SetNamespace(kDefaultNamespace);
      }
    }

    if ((arity > 0 && tokens != arity) || (arity < 0 && tokens < -arity)) {
      RecordFailAndReplyWithCmd(redis::Error("ERR wrong number of arguments"), GlobalStats::FAIL_USAGE_ERROR, cmd_name);
      continue;
    }

    current_cmd->SetArgs(cmd_tokens);
    s = current_cmd->Parse();
    if (!s.IsOK()) {
      if (s.GetCode() == Status::ExpireTSExceedRedisLimit) {
        RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_KKV_EXPIRE_EXCEED_REDIS, cmd_name);
      } else if (s.GetCode() == Status::CmdDisabled) {
        RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_DISABLED_CMD, cmd_name);
      } else {
        RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_USAGE_ERROR, cmd_name);
      }
      continue;
    }

    // check cluster topo has inited
    if (!srv_->cluster->TopoHasInited()) {
      RecordFailAndReplyWithCmd(redis::Error("ERR cluster is not running"), GlobalStats::FAIL_UNCONCERNED_ERROR,
                                cmd_name);
      continue;
    }

    engine::Storage *storage = nullptr;
    Context ctx;
    std::vector<std::unique_ptr<KeyLock>> key_locks;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    auto key_range = attributes->GetKeyRange(cmd_tokens);
    std::string slot_range_name = "unknown";
    // NOTE(mingfo): We determine whether a command is a data operation based on if it includes a key.
    if (key_range.first_key != 0) {
      // check multi keys in same slot of lua cmd
      bool need_lock = true;
      if (cmd_flags & kCmdScript) {
        // NOTE(mingfo): 'numkeys >= 0' which is varified in 'cmd->Parse()'.
        auto numkeys = ParseInt<int64_t>(cmd_tokens[2], 10).GetValue();
        if (numkeys != 0) {
          s = CheckKeysInSameSlot(attributes, cmd_tokens);
          if (!s.IsOK()) {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_USAGE_ERROR, cmd_name);
            continue;
          }
        } else {
          // For eval/evalsha with 'numkeys == 0'
          need_lock = false;
        }
      }

      if (need_lock) {
        // get slotrange by key
        slot_range_name = srv_->cluster->GetSlotRangeNameByKey(cmd_tokens[key_range.first_key]);

        // get shared or exclusive lock for cmd
        s = getExecLocks(cmd_flags, slot_range_name, attributes, cmd_tokens, &ctx, &key_locks, &sr_locks);
        if (!s.IsOK()) {
          if (s.Is<Status::LockTimeOutSlotRange>()) {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_SLOTRANGE_LOCK_TIMEOUT,
                                      cmd_name);
          } else if (s.Is<Status::CrossSlotRange>()) {
            RecordFailAndReplyWithCmd(redis::Error(s.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
          } else {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
          }
          continue;
        }

        // check slotrange
        auto ret = srv_->cluster->CanExecByMySelf(cmd_flags, slot_range_name, cmd_tokens[key_range.first_key]);
        if (!ret.IsOK()) {
          if (ret.IsRetry()) {
            if (ret.Is<Status::ClusterRetryWriteStopped>()) {
              is_blocked_ = true;
              to_process_cmds_.push_front(std::move(cmd_tokens));
              return;
            } else {
              RecordFailAndReplyWithCmd(redis::Error(ret.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
            }
          } else {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + ret.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
          }
          continue;
        }
        // get storage of target slotrange
        storage = ret.GetValue().get();  // TODO: use shared_ptr
      }
    } else {
      if (cmd_flags & CommandFlags::kCmdScript) {
        auto s = HandleCmdScript(&ctx, &sr_locks);
        if (!s.IsOK()) {
          LOG(WARNING) << "Failed to handle cmd:" << cmd_name << ", Err: " << s.Msg();
          if (s.Is<Status::LockTimeOutSlotRange>()) {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_SLOTRANGE_LOCK_TIMEOUT,
                                      cmd_name);
          } else {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
          }
          continue;
        }
      } else if (attributes->name == "nodescan") {
        auto ret = HandleCmdNodeScan(cmd_flags, current_cmd, &ctx, &sr_locks);
        if (!ret.IsOK()) {
          if (ret.IsRetry()) {
            RecordFailAndReplyWithCmd(redis::Error(ret.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
          } else if (s.Is<Status::LockTimeOutSlotRange>()) {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_SLOTRANGE_LOCK_TIMEOUT,
                                      cmd_name);
          } else {
            RecordFailAndReplyWithCmd(redis::Error("ERR " + ret.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
          }
          continue;
        }
        // get storage of target slotrange
        storage = ret.GetValue().get();  // TODO: use shared_ptr
      }
    }

    SetLastCmd(cmd_name);

    bool is_profiling = IsProfilingEnabled(cmd_name);
    auto end = std::chrono::high_resolution_clock::now();
    int64_t prepare_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    int64_t command_queue_latency =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_processing_time_)
            .count();

    if (command_queue_latency >= 0) {
      thread_local_metric_array.Record(MetricType::COMMAND_QUEUE_LATENCY_ON_CONNECTION,
                                       {{"worker_id", std::to_string(thread_local_metric_array.thread_id)}},
                                       static_cast<uint64_t>(command_queue_latency));
    }
    s = current_cmd->Execute(srv_, this, &reply, storage);  // TODO: use shared_ptr
    end = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    std::optional<std::pair<std::string, std::string>> perf_io_context;
    srv_->SlowlogPushEntryIfNeeded(&cmd_tokens, duration, this, is_profiling, perf_io_context, prepare_duration,
                                   command_queue_latency, static_cast<int64_t>(current_cmd->GetEstimatedSubkeyNum()));
    if (is_profiling)
      RecordProfilingSampleIfNeed(cmd_name, duration, std::move(perf_io_context), prepare_duration,
                                  command_queue_latency);
    GlobalStatsInstance().IncrSlowQueryCallsIfNeeded(static_cast<uint64_t>(duration));
    srv_->FeedMonitorConns(this, cmd_tokens);

    // Reply for MULTI
    if (!s.IsOK()) {
      if (!(cmd_flags & CommandFlags::kCmdScript)) {
        auto idx = s.Msg().find(':');
        RecordFailAndReplyWithCmd(
            redis::Error("ERR " + ((idx == std::string::npos) ? s.Msg() : s.Msg().substr(idx + 2))),
            GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);  // TODO(chris): refactor
      } else {
        RecordFailAndReplyWithCmd(redis::Error("ERR " + s.Msg()), GlobalStats::FAIL_UNCONCERNED_ERROR, cmd_name);
      }
      continue;
    }

    GlobalStatsInstance().IncrRequests(GlobalStats::SUCCEED);
    GlobalStatsInstance().IncrCalls(cmd_name, true);
    // only record latency of success requests.
    thread_local_metric_array.RecordCommadLatency(cmd_name, slot_range_name, duration);
    if (!reply.empty()) {
      thread_local_metric_array.RecordReplySize(cmd_name, slot_range_name, reply.size());
      Reply(reply);
    }
    thread_local_metric_array.RecordCommadSize(cmd_name, slot_range_name, current_cmd->GetEstimatedSubkeyNum(),
                                               current_cmd->GetEstimatedSubkeySize());
    reply.clear();
  }
}

void Connection::ResetMultiExec() {
  in_exec_ = false;
  multi_error_ = false;
  multi_cmds_.clear();
  DisableFlag(Connection::kMultiExec);
}

}  // namespace redis
