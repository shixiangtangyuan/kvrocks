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

#include "server.h"

#include <absl/base/internal/endian.h>
#include <glog/logging.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <kv/datanode/v1/common.pb.h>
#include <rocksdb/convenience.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/statistics.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iterator>
#include <jsoncons/json.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <utility>

#include "cdc/cdc_sender.h"
#include "cluster/cluster.h"
#include "commands/commander.h"
#include "common/pb_util.h"
#include "common/scope_exit.h"
#include "common/sync_status.h"
#include "config.h"
#include "fmt/format.h"
#include "grpcpp/support/status.h"
#include "ingest/ingest.h"
#include "ingest/ingest_stats.h"
#include "kv/datanode/v1/ingest.pb.h"
#include "redis_connection.h"
#include "server/grpc_interceptor.h"
#include "stats/stats.h"
#include "status.h"
#include "storage/compaction_checker.h"
#include "storage/redis_db.h"
#include "storage/storage.h"
#include "string_util.h"
#include "sync/dts_sender.h"
#include "sync/repl_sender.h"
#include "sync/sync_receiver.h"
#include "thread_util.h"
#include "time_util.h"
#include "version.h"
#include "worker.h"

#define FAILOVER_INFO LOG(INFO) << "[server][failover] "
#define FAILOVER_WARNING LOG(WARNING) << "[server][failover] "
#define FAILOVER_ERROR LOG(ERROR) << "[server][failover] "

#define MIGRATE_INFO LOG(INFO) << "[server][migrate] "
#define MIGRATE_WARNING LOG(WARNING) << "[server][migrate] "
#define MIGRATE_ERROR LOG(ERROR) << "[server][migrate] "

constexpr const char *REDIS_VERSION = "4.0.0";

// legacyslots compaction info
std::atomic<int16_t> global_legacyslots_compacting_count{0};
std::atomic<uint64_t> global_legacyslots_last_compact_time{0};
std::atomic<uint64_t> global_lagacyslots_last_compact_duration{0};

Server::Server(Config *config) : start_time_(util::GetTimeStamp()), config_(config) {
  // create mgr
  mgl_mgr_ = std::make_unique<redis::mgl::MGLockMgr>();
  // create syncmanager
  sync_manager = std::make_unique<redis::SyncManager>();
  // create cdc manager
  cdc_manager = std::make_unique<redis::CDCManager>();

  // init storage manager
  storage_mgr = std::make_shared<engine::StorageManager>();
  auto s = storage_mgr->CreateStorages(config);
  if (!s.IsOK()) {
    LOG(ERROR) << "failed to create storage manager, Err: " << s.Msg();
    exit(1);
  }
  // create scripting manager
  script_mgr = std::make_unique<lua::ScriptManager>(this);

  // scan LRU cache
  scan_lru_cache = std::make_unique<redis::CursorLRUcache>(redis::kMaxSessionCount, redis::kSessionTTL);
  // xscan lru cache
  xscan_lru_cache = std::make_unique<redis::CursorLRUcache>(redis::kMaxSessionCount, redis::kSessionTTL);

  {
    migration = std::make_unique<redis::Migration>(this);
    if (migration == nullptr) {
      LOG(ERROR) << "create migration task fail";
      exit(1);
    }

    timeout_mgr = std::make_unique<util::TimeoutManager>();
    timeout_mgr->Run();
  }

  {
    auto local_addr = redis::Cluster::GetLocalIPAddr("POD_IP");
    if (local_addr.empty()) {
      LOG(ERROR) << "get local ip addr fail. ";
      exit(1);
    }
    auto local_host = redis::Cluster::GetLocalIPAddr("HOST_IP");
    // std::string local_host = "localhost";
    if (local_host.empty()) {
      LOG(ERROR) << "get local host fail. ";
    }
    cluster = std::make_unique<redis::Cluster>(this, std::move(local_addr), std::move(local_host));
  }

  // init controller rpc client and start
  {
    if (config_->cluster_enabled) {
      ctrl_rpc_client = ControllerApiClient::Create(this, GetConfig()->controller_addr);
      if (ctrl_rpc_client == nullptr) {
        LOG(ERROR) << "create controller api client fail";
        exit(1);
      }
    }
  }
  // init commands stats here to prevent concurrent insert, and cause core
  auto commands = redis::CommandTable::GetOriginal();
  for (const auto &iter : *commands) {
    GlobalStatsInstance().commands_stats[iter.first].success_calls = 0;
    GlobalStatsInstance().commands_stats[iter.first].fail_calls = 0;
  }

  // init cursor_dict_
  // cursor_dict_ = std::make_unique<CursorDictType>();

#ifdef ENABLE_OPENSSL
  // init ssl context
  if (config->tls_port || config->tls_replication) {
    ssl_ctx = CreateSSLContext(config);
    if (!ssl_ctx) {
      exit(1);
    }
  }
#endif

  for (int i = 0; i < config->workers; i++) {
    auto worker = std::make_unique<Worker>(this, config);
    worker_threads_.emplace_back(std::make_unique<WorkerThread>(std::move(worker)));
  }

  AdjustOpenFilesLimit();
  slow_log_.SetMaxEntries(config->slow_req_record_max_len);
  perf_log_.SetMaxEntries(config->profiling_sample_record_max_len);
}

Server::~Server() {
  // Wait for all fetch file threads stop and exit and force destroy the server after 60s.
  int counter = 0;
  while (GetFetchFileThreadNum() != 0) {
    usleep(100000);
    if (++counter == 600) {
      LOG(WARNING) << "[server] Will force destroy the server after waiting 60s, leave " << GetFetchFileThreadNum()
                   << " fetch file threads are still running";
      break;
    }
  }

  for (auto &worker_thread : worker_threads_) {
    worker_thread.reset();
  }
  cleanupExitedWorkerThreads(true /* force */);
}

// Kvrocks threads list:
// - Work-thread: process client's connections and requests
// - Task-runner: one thread pool, handle some jobs that may freeze server if run directly
// - Cron-thread: server's crontab, clean backups, resize sst and memtable size
// - Compaction-checker: active compaction according to collected statistics
// - Replication-thread: replicate incremental stream from master if in slave role, there
//   are some dynamic threads to fetch files when full sync.
//     - fetch-file-thread: fetch SST files from master
// - Feed-slave-thread: feed data to slaves if having slaves, but there also are some dynamic
//   threads when full sync, TODO(@shooterit) we should manage this threads uniformly.
//     - feed-replica-data-info: generate checkpoint and send files list when full sync
//     - feed-replica-file: send SST files when slaves ask for full sync
Status Server::Start() {
  for (const auto &worker : worker_threads_) {
    worker->Start();
  }

  if (auto s = task_runner_.Start(); !s) {
    LOG(WARNING) << "Failed to start task runner: " << s.Msg();
  }

  cron_thread_ = GET_OR_RET(util::CreateThread("server-cron", [this] { this->cron(); }));

  // start storages task threads
  {
    auto storages = storage_mgr->GetAllStorage();
    for (const auto &[id, storage] : storages) {
      auto s = storage->StartTaskThreads(id);
      if (!s.IsOK()) {
        LOG(ERROR) << "start storage:" << id << " task thead failed, err: " << s.Msg();
        return s;
      }
    }
  }

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;
  auto grpc_port_str = std::to_string(config_->GetGrpcPort());
  for (const auto &bind : config_->binds) {
    auto addr = bind + ":" + grpc_port_str;
    // TODO(ying.qiu): use SslServerCredentials when ENABLE_OPENSSL
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  }
  builder.RegisterService(this);
  // set grpc keepalive configs
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, config_->grpc_server_keepalive_time_ms);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, config_->grpc_server_keepalive_timeout_ms);
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS,
                             config_->grpc_server_min_recv_ping_interval_ms);
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, config_->grpc_server_max_ping_strikes);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,
                             config_->grpc_server_keepalive_permit_without_calls);
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, config_->grpc_server_max_pings_without_data);
  // set grpc max send and receive message length
  builder.AddChannelArgument(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, config_->grpc_datanode_service_max_message_length);
  builder.AddChannelArgument(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, config_->grpc_datanode_service_max_message_length);
  std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> interceptor_creators;
  interceptor_creators.push_back(std::make_unique<redis::ServerStatsInterceptorFactory>());
  builder.experimental().SetInterceptorCreators(std::move(interceptor_creators));
  grpc_server_ = builder.BuildAndStart();
  if (!grpc_server_) {
    return {Status::NotOK, "failed to start grpc server"};
  }
  grpc_server_->GetHealthCheckService()->SetServingStatus(kv::datanode::v1::DataNodeService::service_full_name(), true);
  LOG(INFO) << "[grpc-server] Ready to serve";

  auto storages = storage_mgr->GetAllStorage();

  memory_startup_use_.store(GlobalStats::GetMemoryRSS(), std::memory_order_relaxed);
  LOG(INFO) << "[server] Ready to accept connections";

  return Status::OK();
}

void Server::Stop() {
  stop_ = true;

  if (grpc_server_) {
    if (sync_manager) {
      sync_manager->ClearAll(UnknownSyncError("server stopped"));
    }
    if (cdc_manager) {
      cdc_manager->ClearAll(UnknownSyncError("server stopped"));
    }
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    grpc_server_->Shutdown(deadline);
    grpc_server_->Wait();
  }

  for (const auto &worker : worker_threads_) {
    worker->Stop(0 /* immediately terminate  */);
  }

  // rocksdb::CancelAllBackgroundWork(storage->GetDB(), true);
  task_runner_.Cancel();

  ctrl_rpc_client->Stop();

  timeout_mgr->Stop();

  // TODO(chenghua): failover->StopAndJoin()
}

void Server::Join() {
  if (auto s = util::ThreadJoin(cron_thread_); !s) {
    LOG(WARNING) << "Cron thread operation failed: " << s.Msg();
  }

  if (auto s = task_runner_.Join(); !s) {
    LOG(WARNING) << s.Msg();
  }
  for (const auto &worker : worker_threads_) {
    worker->Join();
  }
}

void Server::FeedMonitorConns(redis::Connection *conn, const std::vector<std::string> &tokens) {
  if (monitor_clients_ <= 0) return;

  auto now = util::GetTimeStampUS();
  std::string output =
      fmt::format("{}.{} [{} {}]", now / 1000000, now % 1000000, conn->GetNamespace(), conn->GetAddr());
  for (const auto &token : tokens) {
    output += " \"";
    output += util::EscapeString(token);
    output += "\"";
  }

  for (const auto &worker_thread : worker_threads_) {
    auto worker = worker_thread->GetWorker();
    worker->FeedMonitorConns(conn, redis::SimpleString(output));
  }
}

void Server::SubscribeChannel(const std::string &channel, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD());
  if (auto iter = pubsub_channels_.find(channel); iter == pubsub_channels_.end()) {
    pubsub_channels_.emplace(channel, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::UnsubscribeChannel(const std::string &channel, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto iter = pubsub_channels_.find(channel);
  if (iter == pubsub_channels_.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_channels_.erase(iter);
      }
      break;
    }
  }
}

void Server::GetChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  for (const auto &iter : pubsub_channels_) {
    if (pattern.empty() || util::StringMatch(pattern, iter.first, 0)) {
      channels->emplace_back(iter.first);
    }
  }
}

void Server::ListChannelSubscribeNum(const std::vector<std::string> &channels,
                                     std::vector<ChannelSubscribeNum> *channel_subscribe_nums) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  for (const auto &chan : channels) {
    if (auto iter = pubsub_channels_.find(chan); iter != pubsub_channels_.end()) {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{iter->first, iter->second.size()});
    } else {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{chan, 0});
    }
  }
}

void Server::PSubscribeChannel(const std::string &pattern, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD());
  if (auto iter = pubsub_patterns_.find(pattern); iter == pubsub_patterns_.end()) {
    pubsub_patterns_.emplace(pattern, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::PUnsubscribeChannel(const std::string &pattern, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto iter = pubsub_patterns_.find(pattern);
  if (iter == pubsub_patterns_.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_patterns_.erase(iter);
      }
      break;
    }
  }
}

void Server::BlockOnKey(const std::string &key, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD());

  if (auto iter = blocking_keys_.find(key); iter == blocking_keys_.end()) {
    blocking_keys_.emplace(key, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }

  IncrBlockedClientNum();
}

void Server::UnblockOnKey(const std::string &key, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);

  auto iter = blocking_keys_.find(key);
  if (iter == blocking_keys_.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        blocking_keys_.erase(iter);
      }
      break;
    }
  }

  DecrBlockedClientNum();
}

void Server::BlockOnStreams(const std::vector<std::string> &keys, const std::vector<redis::StreamEntryID> &entry_ids,
                            redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocked_stream_consumers_mu_);

  IncrBlockedClientNum();

  for (size_t i = 0; i < keys.size(); ++i) {
    auto consumer = std::make_shared<StreamConsumer>(conn->Owner(), conn->GetFD(), conn->GetNamespace(), entry_ids[i]);
    if (auto iter = blocked_stream_consumers_.find(keys[i]); iter == blocked_stream_consumers_.end()) {
      std::set<std::shared_ptr<StreamConsumer>> consumers;
      consumers.insert(consumer);
      blocked_stream_consumers_.emplace(keys[i], consumers);
    } else {
      iter->second.insert(consumer);
    }
  }
}

void Server::UnblockOnStreams(const std::vector<std::string> &keys, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocked_stream_consumers_mu_);

  DecrBlockedClientNum();

  for (const auto &key : keys) {
    auto iter = blocked_stream_consumers_.find(key);
    if (iter == blocked_stream_consumers_.end()) {
      continue;
    }

    for (auto it = iter->second.begin(); it != iter->second.end();) {
      const auto &consumer = *it;
      if (conn->GetFD() == consumer->fd && conn->Owner() == consumer->owner) {
        iter->second.erase(it);
        if (iter->second.empty()) {
          blocked_stream_consumers_.erase(iter);
        }
        break;
      }
      ++it;
    }
  }
}

void Server::WakeupBlockingConns(const std::string &key, size_t n_conns) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);

  auto iter = blocking_keys_.find(key);
  if (iter == blocking_keys_.end() || iter->second.empty()) {
    return;
  }

  while (n_conns-- && !iter->second.empty()) {
    auto conn_ctx = iter->second.front();
    auto s = conn_ctx.owner->EnableWriteEvent(conn_ctx.fd);
    if (!s.IsOK()) {
      LOG(ERROR) << "[server] Failed to enable write event on blocked client " << conn_ctx.fd << ": " << s.Msg();
    }
    iter->second.pop_front();
  }
}

void Server::OnEntryAddedToStream(const std::string &ns, const std::string &key, const redis::StreamEntryID &entry_id) {
  std::lock_guard<std::mutex> guard(blocked_stream_consumers_mu_);

  auto iter = blocked_stream_consumers_.find(key);
  if (iter == blocked_stream_consumers_.end() || iter->second.empty()) {
    return;
  }

  for (auto it = iter->second.begin(); it != iter->second.end();) {
    auto consumer = *it;
    if (consumer->ns == ns && entry_id > consumer->last_consumed_id) {
      auto s = consumer->owner->EnableWriteEvent(consumer->fd);
      if (!s.IsOK()) {
        LOG(ERROR) << "[server] Failed to enable write event on blocked stream consumer " << consumer->fd << ": "
                   << s.Msg();
      }
      it = iter->second.erase(it);
    } else {
      ++it;
    }
  }
}

void Server::updateCachedTime() { unix_time.store(util::GetTimeStamp()); }

int Server::IncrClientNum(const std::thread::id &tid) {
  ++(this->number_of_worker_connections_[tid]);
  total_clients_.fetch_add(1, std::memory_order::memory_order_relaxed);
  return connected_clients_.fetch_add(1, std::memory_order_relaxed);
}

int Server::DecrClientNum(const std::thread::id &tid) {
  --(this->number_of_worker_connections_[tid]);
  return connected_clients_.fetch_sub(1, std::memory_order_relaxed);
}

int Server::IncrMonitorClientNum() { return monitor_clients_.fetch_add(1, std::memory_order_relaxed); }

int Server::DecrMonitorClientNum() { return monitor_clients_.fetch_sub(1, std::memory_order_relaxed); }

int Server::IncrBlockedClientNum() { return blocked_clients_.fetch_add(1, std::memory_order_relaxed); }

int Server::DecrBlockedClientNum() { return blocked_clients_.fetch_sub(1, std::memory_order_relaxed); }

uint64_t Server::GetClientID() { return client_id_.fetch_add(1, std::memory_order_relaxed); }

void Server::cron() {
  uint64_t counter = 0;
  while (!stop_) {
    // Sleep first
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    updateCachedTime();
    counter++;

    if (is_loading_) {
      // We need to skip the cron operations since `is_loading_` means the db is restoring,
      // and the db pointer will be modified after that. It will panic if we use the db pointer
      // before the new db was reopened.
      continue;
    }

    // check if we need to clean up exited worker threads every 5s
    if (counter != 0 && counter % 50 == 0) {
      cleanupExitedWorkerThreads(false);
    }

    recordInstantaneousMetrics();
  }
}

void Server::GetServerInfo(std::string *info) {
  static int call_uname = 1;
  static utsname name;
  if (call_uname) {
    /* Uname can be slow and is always the same output. Cache it. */
    uname(&name);
    call_uname = 0;
  }

  std::ostringstream string_stream;
  string_stream << "# Server\r\n";
  string_stream << "version:" << VERSION << "\r\n";
  string_stream << "datanode_version:" << VERSION << "\r\n";
  string_stream << "redis_version:" << REDIS_VERSION << "\r\n";
  string_stream << "git_sha1:" << GIT_COMMIT << "\r\n";
  string_stream << "datanode_git_sha1:" << GIT_COMMIT << "\r\n";
  string_stream << "redis_mode:" << (config_->cluster_enabled ? "cluster" : "standalone") << "\r\n";
  string_stream << "datanode_mode:" << (config_->cluster_enabled ? "cluster" : "standalone") << "\r\n";
  string_stream << "os:" << name.sysname << " " << name.release << " " << name.machine << "\r\n";
#ifdef __GNUC__
  string_stream << "gcc_version:" << __GNUC__ << "." << __GNUC_MINOR__ << "." << __GNUC_PATCHLEVEL__ << "\r\n";
#endif
#ifdef __clang__
  string_stream << "clang_version:" << __clang_major__ << "." << __clang_minor__ << "." << __clang_patchlevel__
                << "\r\n";
#endif
  string_stream << "arch_bits:" << sizeof(void *) * 8 << "\r\n";
  string_stream << "process_id:" << getpid() << "\r\n";
  string_stream << "tcp_port:" << config_->port << "\r\n";
  int64_t now = util::GetTimeStamp();
  string_stream << "uptime_in_seconds:" << now - start_time_ << "\r\n";
  string_stream << "uptime_in_days:" << (now - start_time_) / 86400 << "\r\n";

  double seconds = std::chrono::duration<double>(GetWorkersBlockedDuration()).count();
  string_stream << "worker_blocked_seconds:" << std::fixed << std::setprecision(9) << seconds << "\r\n";
  string_stream << "is_blocked:" << GlobalStatsInstance().is_blocked.load() << "\r\n";
  *info = string_stream.str();
}

void Server::GetClientsInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# Clients\r\n";
  string_stream << "connected_clients:" << connected_clients_ << "\r\n";
  string_stream << "total_connections_received:" << total_clients_ << "\r\n";
  tbb::concurrent_unordered_map<std::thread::id, uint32_t>::const_iterator it;
  for (it = this->number_of_worker_connections_.cbegin(); it != this->number_of_worker_connections_.cend(); ++it) {
    if (it->second != 0) {
      string_stream << "worker_connections." << it->first << ":" << it->second << "\r\n";
    }
  }
  GlobalStatsInstance().GetClientsInfo(string_stream);
  *info = string_stream.str();
}

void Server::GetMemoryInfo(std::string *info) {
  int64_t rss = GlobalStats::GetMemoryRSS();
  std::string used_memory_rss_human = util::BytesToHuman(rss);

  std::ostringstream string_stream;
  string_stream << "# Memory\r\n";
  string_stream << "used_memory_rss:" << rss << "\r\n";
  string_stream << "used_memory_rss_human:" << used_memory_rss_human << "\r\n";
  *info = string_stream.str();
}

void Server::GetCounterMetric(std::string *info) {
  for (int i = 0; i <= static_cast<int>(MetricType::METRIC_TYPE_MAX); ++i) {
    auto type = static_cast<MetricType>(i);
    if (!MetricIsCounter(type)) {
      continue;
    }
    GlobalStatsInstance().MetricInfo(type, info);
  }
}

void Server::GetHistogramMetric(std::string *info) {
  for (int i = 0; i <= static_cast<int>(MetricType::METRIC_TYPE_MAX); ++i) {
    auto type = static_cast<MetricType>(i);
    if (!MetricIsHistogram(type)) {
      continue;
    }
    GlobalStatsInstance().MetricInfo(type, info);
  }
}

std::string Server::GetLastRandomKeyCursor() {
  std::string cursor;
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  cursor = last_random_key_cursor_;
  return cursor;
}

void Server::SetLastRandomKeyCursor(const std::string &cursor) {
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  last_random_key_cursor_ = cursor;
}

int64_t Server::GetCachedUnixTime() {
  if (unix_time.load() == 0) {
    updateCachedTime();
  }
  return unix_time.load();
}

void Server::GetStatsInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# Stats\r\n";
  string_stream << "total_commands_processed:" << GlobalStatsInstance().total_calls << "\r\n";
  string_stream << "instantaneous_ops_per_sec:"
                << GlobalStatsInstance().GetInstantaneousMetric(GlobalStats::STATS_METRIC_COMMAND) << "\r\n";
  string_stream << "slow_query_count:" << GlobalStatsInstance().slow_query_calls << "\r\n";
  string_stream << "slow_query_qps:"
                << GlobalStatsInstance().GetInstantaneousMetric(GlobalStats::STATS_METRIC_SLOW_QUERY) << "\r\n";

  GlobalStatsInstance().GetRequestStatsInfo(string_stream);

  *info = string_stream.str();
}

void Server::GetNetInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# Net\r\n";
  string_stream << "total_net_input_bytes:" << GlobalStatsInstance().in_bytes << "\r\n";
  string_stream << "total_net_output_bytes:" << GlobalStatsInstance().out_bytes << "\r\n";
  string_stream << "instantaneous_input_kbps:"
                << static_cast<float>(
                       GlobalStatsInstance().GetInstantaneousMetric(GlobalStats::STATS_METRIC_NET_INPUT) / 1024)
                << "\r\n";
  string_stream << "instantaneous_output_kbps:"
                << static_cast<float>(
                       GlobalStatsInstance().GetInstantaneousMetric(GlobalStats::STATS_METRIC_NET_OUTPUT) / 1024)
                << "\r\n";

  *info = string_stream.str();
}

void Server::GetCommandsStatsInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# Commandstats\r\n";

  for (const auto &cmd_stat : GlobalStatsInstance().commands_stats) {
    auto success_calls = cmd_stat.second.success_calls.load();
    auto fail_calls = cmd_stat.second.fail_calls.load();
    if (success_calls == 0 && fail_calls == 0) continue;

    auto calls = success_calls + fail_calls;
    string_stream << "cmdstat_" << cmd_stat.first << ":calls=" << calls << ",success_calls=" << success_calls
                  << ",fail_calls=" << fail_calls << "\r\n";
  }

  *info = string_stream.str();
}

void Server::GetClusterInfo(std::string *info) {
  std::ostringstream string_stream;

  string_stream << "# Cluster\r\n";
  string_stream << "topo_version:" << cluster->Version() << "\r\n";
  string_stream << "set_topo_ok_count:" << cluster->SetTopoOkCount() << "\r\n";
  string_stream << "set_topo_err_count:" << cluster->SetTopoErrCount() << "\r\n";

  *info = string_stream.str();
}

void Server::GetCtrlClientInfo(std::string *info) const {
  std::ostringstream ss;

  ss << "# CtrlClient\r\n";
  ss << "send_heartbeat_ok_count:" << ctrl_rpc_client->SendHeartbeatOKCount() << "\r\n";
  ss << "send_heartbeat_err_count:" << ctrl_rpc_client->SendHeartbeatErrCount() << "\r\n";
  ss << "receive_topology_ok_count:" << ctrl_rpc_client->ReceiveTopoOKCount() << "\r\n";
  ss << "receive_topology_err_count:" << ctrl_rpc_client->ReceiveTopoErrCount() << "\r\n";

  *info = ss.str();
}

std::string Server::IngestJobIsRunning() {
  std::ostringstream ss;
  try {
    auto slot_ranges = cluster->LocalSlotRanges();
    for (const auto &[name, slot_range] : slot_ranges) {
      ss << "Ingest." + slot_range->GetName() + ".ingest_running:"
         << (slot_range->GetStorage()->GetIngester()->IsRunning() ? 1 : 0) << "\r\n";
    }
  } catch (const std::system_error &e) {
    LOG(ERROR) << "get Ingest job status error, reason: " << e.what();
  }

  return ss.str();
}

// WARNING: we must not access DB(i.e. RocksDB) when server is loading since
// DB is closed and the pointer is invalid. Server may crash if we access DB during loading.
// If you add new fields which access DB into INFO command output, make sure
// this section can't be shown when loading(i.e. !is_loading_).
void Server::GetInfo(const std::string &ns, const std::string &section, std::string *info) {
  info->clear();

  std::ostringstream string_stream;
  bool all = section == "all";
  int section_cnt = 0;

  if (all || section == "server") {
    std::string server_info;
    GetServerInfo(&server_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << server_info;
  }

  if (all || section == "clients") {
    std::string clients_info;
    GetClientsInfo(&clients_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << clients_info;
  }

  if (all || section == "memory") {
    std::string memory_info;
    GetMemoryInfo(&memory_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << memory_info;
  }

  if (all || section == "persistence") {
    if (section_cnt++) string_stream << "\r\n";
    string_stream << "# Persistence\r\n";
    string_stream << "loading:" << is_loading_ << "\r\n";
  }

  if (all || section == "stats") {
    std::string stats_info;
    GetStatsInfo(&stats_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << stats_info;
  }

  if (all || section == "net") {
    std::string stats_info;
    GetNetInfo(&stats_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << stats_info;
  }

  if (all || section == "cpu") {
    rusage self_ru;
    getrusage(RUSAGE_SELF, &self_ru);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << "# CPU\r\n";
    string_stream << "used_cpu_sys:"
                  << static_cast<float>(self_ru.ru_stime.tv_sec) +
                         static_cast<float>(self_ru.ru_stime.tv_usec / 1000000)
                  << "\r\n";
    string_stream << "used_cpu_user:"
                  << static_cast<float>(self_ru.ru_utime.tv_sec) +
                         static_cast<float>(self_ru.ru_utime.tv_usec / 1000000)
                  << "\r\n";
  }

  if (all || section == "commandstats") {
    std::string commands_stats_info;
    GetCommandsStatsInfo(&commands_stats_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << commands_stats_info;
  }

  if (config_->cluster_enabled) {
    if (all || section == "cluster") {
      std::string cluster_info;
      GetClusterInfo(&cluster_info);
      if (section_cnt++) string_stream << "\r\n";
      string_stream << cluster_info;
    }
  }

  // In rocksdb section, we access DB, so we can't do that when loading
  if (!is_loading_ && (all || section == "rocksdb")) {
    std::string rocksdb_info;
    GetRocksDBInfo(&rocksdb_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << rocksdb_info;
  }

  if (!is_loading_ && (all || section == "legacyslots")) {
    std::string rocksdb_info;
    GetLegacyslotsCompactInfo(&rocksdb_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << rocksdb_info;
  }

  if (all || section == "syncstats") {
    if (section_cnt++) string_stream << "\r\n";
    string_stream << "# SyncStats\r\n";
    GlobalStatsInstance().GetSyncStatsInfo(string_stream);
  }

  if (all || section == "cdcstats") {
    if (section_cnt++) string_stream << "\r\n";
    string_stream << "# CDCStats\r\n";
    GlobalStatsInstance().GetCDCStatsInfo(string_stream);
  }

  if (all || section == "ctrlclient") {
    std::string ctrl_client_info;
    GetCtrlClientInfo(&ctrl_client_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << ctrl_client_info;
  }

  if (all || section == "rocksdbstats") {
    std::string stats = GetRocksDBStats();
    if (section_cnt++) string_stream << "\r\n";
    string_stream << "# RocksDBStats\r\n";
    string_stream << stats;
  }

  if (all || section == "ingeststats") {
    if (section_cnt++) string_stream << "\r\n";
    string_stream << "# IngestStats\r\n";
    auto res = IngestJobIsRunning();
    string_stream << res << "\r\n";
    GlobalStatsInstance().GetIngestStatsInfo(string_stream);
  }

  *info = string_stream.str();
}

std::string Server::GetRocksDBStatsJson() const {
  jsoncons::json stats_json;

  auto slot_ranges = cluster->LocalSlotRanges();

  for (const auto &[name, slot_range] : slot_ranges) {
    auto storage = slot_range->GetStorage();
    jsoncons::json db_stats_json;

    auto stats = storage->GetDB()->GetDBOptions().statistics;
    for (const auto &iter : rocksdb::TickersNameMap) {
      db_stats_json[iter.second] = stats->getTickerCount(iter.first);
    }

    for (const auto &iter : rocksdb::HistogramsNameMap) {
      rocksdb::HistogramData hist_data;
      stats->histogramData(iter.first, &hist_data);
      /* P50 P95 P99 P100 COUNT SUM */
      db_stats_json[iter.second] =
          jsoncons::json(jsoncons::json_array_arg, {hist_data.median, hist_data.percentile95, hist_data.percentile99,
                                                    hist_data.max, hist_data.count, hist_data.sum});
    }

    std::string db_name = "";
    db_name.append("[");
    db_name.append(std::to_string(slot_range->GetRangeStart()));
    db_name.append("-");
    db_name.append(std::to_string(slot_range->GetRangeEnd()));
    db_name.append("].");

    stats_json[db_name] = db_stats_json;
  }

  return stats_json.to_string();
}

std::string Server::GetRocksDBStats() const {
  std::ostringstream string_stream;

  auto slot_ranges = cluster->LocalSlotRanges();

  for (const auto &[name, slot_range] : slot_ranges) {
    std::string db_prefix{"rocksdb."};
    db_prefix.append("[");
    db_prefix.append(std::to_string(slot_range->GetRangeStart()));
    db_prefix.append("-");
    db_prefix.append(std::to_string(slot_range->GetRangeEnd()));
    db_prefix.append("].");

    auto storage = slot_range->GetStorage();
    auto stats = storage->GetDB()->GetDBOptions().statistics;
    for (const auto &iter : rocksdb::TickersNameMap) {
      string_stream << db_prefix << iter.second << ":" << stats->getTickerCount(iter.first) << "\r\n";
    }

    for (const auto &iter : rocksdb::HistogramsNameMap) {
      rocksdb::HistogramData hist_data;
      stats->histogramData(iter.first, &hist_data);
      /* P50 P95 P99 P100 COUNT SUM */
      string_stream << db_prefix << iter.second << ".p50:" << hist_data.median << "\r\n";
      string_stream << db_prefix << iter.second << ".p95:" << hist_data.percentile95 << "\r\n";
      string_stream << db_prefix << iter.second << ".p99:" << hist_data.percentile99 << "\r\n";
      string_stream << db_prefix << iter.second << ".max:" << hist_data.max << "\r\n";
      string_stream << db_prefix << iter.second << ".count:" << hist_data.count << "\r\n";
      string_stream << db_prefix << iter.second << ".sum:" << hist_data.sum << "\r\n";
    }
  }

  return string_stream.str();
}

void Server::GetLatestKeyNumStats(const std::string &ns, KeyNumStats *stats) {
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    *stats = iter->second.key_num_stats;
  }
}

time_t Server::GetLastScanTime(const std::string &ns) {
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    return iter->second.last_scan_time;
  }
  return 0;
}

void Server::GetMetricInfo(const std::string &ns, const std::string &section, std::string *info) {
  info->clear();

  std::ostringstream string_stream;
  bool all = section == "all";
  int section_cnt = 0;

  if (all || section == "histogram") {
    std::string hist_info;
    GetHistogramMetric(&hist_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << hist_info;
  }

  if (all || section == "counter") {
    std::string counter_info;
    GetCounterMetric(&counter_info);
    if (section_cnt++) string_stream << "\r\n";
    string_stream << counter_info;
  }

  *info = string_stream.str();
}

void Server::SlowlogPushEntryIfNeeded(const std::vector<std::string> *args, uint64_t duration,
                                      const redis::Connection *conn, bool is_profiling,
                                      std::optional<std::pair<std::string, std::string>> &perf_io_context,
                                      int64_t prepare_duration, int64_t command_queue_latency_on_connection,
                                      int64_t estimated_subkey_count) {
  int64_t log_threshold = config_->slow_req_log_threshold_us;
  bool need_log = (log_threshold >= 0 && static_cast<int64_t>(duration) >= log_threshold);
  int64_t record_threshold = config_->slow_req_record_threshold_us;
  bool need_record =
      (record_threshold >= 0 && static_cast<int64_t>(duration) >= record_threshold) && slow_log_.GetMaxEntries() > 0;

  if (need_log) {
    auto log_period_in_us = config_->slow_req_log_period_us;
    need_log = log_period_in_us == 0;
    if (log_period_in_us > 0) {
      static std::atomic<std::chrono::nanoseconds> prev_time{std::chrono::nanoseconds{0}};
      auto prev_nanos = prev_time.load();
      auto curr_time = std::chrono::steady_clock::now().time_since_epoch();
      auto curr_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(curr_time);
      if (curr_nanos - prev_nanos >= std::chrono::microseconds(log_period_in_us)) {
        need_log = prev_time.compare_exchange_strong(prev_nanos, curr_nanos);
      }
    }
  }

  if (need_log || need_record) {
    auto entry = std::make_unique<SlowEntry>();
    entry->duration = duration;
    entry->client_name = conn->GetName();
    entry->ip = conn->GetIP();
    entry->port = conn->GetPort();
    entry->prepare_duration = prepare_duration;
    entry->command_queue_latency_on_connection = command_queue_latency_on_connection;
    entry->estimated_subkey_count = estimated_subkey_count;
    size_t argc = args->size() > kSlowLogMaxArgc ? kSlowLogMaxArgc : args->size();
    for (size_t i = 0; i < argc; i++) {
      if (argc != args->size() && i == argc - 1) {
        entry->args.emplace_back(fmt::format("... ({} more arguments)", args->size() - argc + 1));
        break;
      }
      if ((*args)[i].length() <= kSlowLogMaxString) {
        entry->args.emplace_back((*args)[i]);
      } else {
        entry->args.emplace_back(fmt::format("{}... ({} more bytes)", (*args)[i].substr(0, kSlowLogMaxString),
                                             (*args)[i].length() - kSlowLogMaxString));
      }
    }
    if (need_log) {
      if (is_profiling) {
        perf_io_context =
            std::make_pair(rocksdb::get_perf_context()->ToString(true), rocksdb::get_iostats_context()->ToString(true));
        LOG(WARNING) << "[slow log] " << *entry.get() << ", perf:" << perf_io_context->first
                     << ", iostats:" << perf_io_context->second;
      } else {
        LOG(WARNING) << "[slow log] " << *entry.get();
      }
    }
    if (need_record) {
      slow_log_.PushEntry(std::move(entry));
    }
  }
}

std::string Server::GetClientsStr() {
  std::string clients;
  for (const auto &t : worker_threads_) {
    clients.append(t->GetWorker()->GetClientsStr());
  }

  return clients;
}

void Server::KillClient(int64_t *killed, const std::string &addr, uint64_t id, uint64_t type, bool skipme,
                        redis::Connection *conn) {
  *killed = 0;

  // Normal clients and pubsub clients
  for (const auto &t : worker_threads_) {
    int64_t killed_in_worker = 0;
    t->GetWorker()->KillClient(conn, id, addr, type, skipme, &killed_in_worker);
    *killed += killed_in_worker;
  }
}

Status Server::LookupAndCreateCommand(const std::string &cmd_name, std::unique_ptr<redis::Commander> *cmd) {
  if (cmd_name.empty()) return {Status::RedisUnknownCmd};

  auto commands = redis::CommandTable::Get();
  auto cmd_iter = commands->find(util::ToLower(cmd_name));
  if (cmd_iter == commands->end()) {
    return {Status::RedisUnknownCmd};
  }

  auto redis_cmd = cmd_iter->second;
  *cmd = redis_cmd->factory();
  (*cmd)->SetAttributes(redis_cmd);

  return Status::OK();
}

// AdjustOpenFilesLimit only try best to raise the max open files according to
// the max clients and RocksDB open file configuration. It also reserves a number
// of file descriptors(128) for extra operations of persistence, listening sockets,
// log files and so forth.
void Server::AdjustOpenFilesLimit() {
  const int min_reserved_fds = 128;
  auto rocksdb_max_open_file = static_cast<rlim_t>(config_->rocks_db.max_open_files);
  auto max_clients = static_cast<rlim_t>(config_->maxclients);
  auto max_files = max_clients + rocksdb_max_open_file + min_reserved_fds;

  rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    return;
  }

  rlim_t old_limit = limit.rlim_cur;
  // Set the max number of files only if the current limit is not enough
  if (old_limit >= max_files) {
    return;
  }

  int setrlimit_error = 0;
  rlim_t best_limit = max_files;

  while (best_limit > old_limit) {
    limit.rlim_cur = best_limit;
    limit.rlim_max = best_limit;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) break;

    setrlimit_error = errno;

    rlim_t decr_step = 16;
    if (best_limit < decr_step) {
      best_limit = old_limit;
      break;
    }

    best_limit -= decr_step;
  }

  if (best_limit < old_limit) best_limit = old_limit;

  if (best_limit < max_files) {
    if (best_limit <= static_cast<int>(min_reserved_fds)) {
      LOG(WARNING) << "[server] Your current 'ulimit -n' of " << old_limit << " is not enough for the server to start."
                   << "Please increase your open file limit to at least " << max_files << ". Exiting.";
      exit(1);
    }

    LOG(WARNING) << "[server] You requested max clients of " << max_clients << " and RocksDB max open files of "
                 << rocksdb_max_open_file << " requiring at least " << max_files << " max file descriptors.";
    LOG(WARNING) << "[server] Server can't set maximum open files to " << max_files
                 << " because of OS error: " << strerror(setrlimit_error);
  } else {
    LOG(WARNING) << "[server] Increased maximum number of open files to " << max_files << " (it's originally set to "
                 << old_limit << ")";
  }
}

void Server::AdjustWorkerThreads() {
  auto new_worker_threads = static_cast<size_t>(config_->workers);
  auto old_worker_threads = worker_threads_.size();
  if (new_worker_threads == old_worker_threads) {
    return;
  }
  size_t delta = 0;
  if (new_worker_threads > old_worker_threads) {
    delta = new_worker_threads - old_worker_threads;
    increaseWorkerThreads(delta);
    LOG(INFO) << "[server] Increase worker threads from " << old_worker_threads << " to " << new_worker_threads;
    return;
  }

  delta = old_worker_threads - new_worker_threads;
  LOG(INFO) << "[server] Decrease worker threads from " << old_worker_threads << " to " << new_worker_threads;
  decreaseWorkerThreads(delta);
}

void Server::increaseWorkerThreads(size_t delta) {
  for (size_t i = 0; i < delta; i++) {
    auto worker = std::make_unique<Worker>(this, config_);
    auto worker_thread = std::make_unique<WorkerThread>(std::move(worker));
    worker_thread->Start();
    worker_threads_.emplace_back(std::move(worker_thread));
  }
}

void Server::decreaseWorkerThreads(size_t delta) {
  auto current_worker_threads = worker_threads_.size();
  DCHECK(current_worker_threads > delta);
  auto remain_worker_threads = current_worker_threads - delta;
  for (size_t i = remain_worker_threads; i < current_worker_threads; i++) {
    // Unix socket will be listening on the first worker,
    // so it MUST remove workers from the end of the vector.
    // Otherwise, the unix socket will be closed.
    auto worker_thread = std::move(worker_threads_.back());
    worker_threads_.pop_back();
    // Migrate connections to other workers before stopping the worker,
    // we use round-robin to choose the target worker here.
    auto connections = worker_thread->GetWorker()->GetConnections();
    for (const auto &iter : connections) {
      auto target_worker = worker_threads_[iter.first % remain_worker_threads]->GetWorker();
      worker_thread->GetWorker()->MigrateConnection(target_worker, iter.second);
    }
    worker_thread->Stop(10 /* graceful timeout */);
    // Don't join the worker thread here, because it may join itself.
    recycle_worker_threads_.push(std::move(worker_thread));
  }
}

void Server::cleanupExitedWorkerThreads(bool force) {
  std::unique_ptr<WorkerThread> worker_thread = nullptr;
  auto total = recycle_worker_threads_.unsafe_size();
  for (size_t i = 0; i < total; i++) {
    if (!recycle_worker_threads_.try_pop(worker_thread)) {
      break;
    }
    if (worker_thread->IsTerminated() || force) {
      worker_thread->Join();
      worker_thread.reset();
    } else {
      // Push the worker thread back to the queue if it's still running.
      recycle_worker_threads_.push(std::move(worker_thread));
    }
  }
}

std::string ServerLogData::Encode() const {
  if (type_ == kReplIdLog) {
    std::string encoded(kPrefixSize + content_.size(), '\0');
    encoded[0] = kReplIdTag;
    absl::little_endian::Store64(encoded.data() + sizeof(kReplIdTag), time_nanos_);
    memcpy(encoded.data() + kPrefixSize, content_.data(), content_.size());
    return encoded;
  }
  return content_;
}

Status ServerLogData::Decode(const rocksdb::Slice &blob) {
  if (blob.size() == 0) {
    return {Status::NotOK, "empty server log data"};
  }

  const char *header = blob.data();
  // Only support `kReplIdTag` now
  if (*header == kReplIdTag && blob.size() == kPrefixSize + kReplIdLength) {
    type_ = kReplIdLog;
    time_nanos_ = absl::little_endian::Load64(blob.data() + sizeof(kReplIdTag));
    content_ = std::string(blob.data() + kPrefixSize, blob.size() - kPrefixSize);
    return Status::OK();
  }
  return {Status::NotOK, "invalid server log data"};
}

void Server::updateWatchedKeysFromRange(const std::vector<std::string> &args, const redis::CommandKeyRange &range) {
  std::shared_lock lock(watched_key_mutex_);

  for (size_t i = range.first_key; range.last_key > 0 ? i <= size_t(range.last_key) : i <= args.size() + range.last_key;
       i += range.key_step) {
    if (auto iter = watched_key_map_.find(args[i]); iter != watched_key_map_.end()) {
      for (auto *conn : iter->second) {
        conn->watched_keys_modified = true;
      }
    }
  }
}

void Server::updateAllWatchedKeys() {
  std::shared_lock lock(watched_key_mutex_);

  for (auto &[_, conn_map] : watched_key_map_) {
    for (auto *conn : conn_map) {
      conn->watched_keys_modified = true;
    }
  }
}

void Server::UpdateWatchedKeysFromArgs(const std::vector<std::string> &args, const redis::CommandAttributes &attr) {
  if ((attr.flags & redis::kCmdWrite) && watched_key_size_ > 0) {
    if (attr.key_range.first_key > 0) {
      updateWatchedKeysFromRange(args, attr.key_range);
    } else if (attr.key_range.first_key == -1) {
      redis::CommandKeyRange range = attr.key_range_gen(args);

      if (range.first_key > 0) {
        updateWatchedKeysFromRange(args, range);
      }
    } else if (attr.key_range.first_key == -2) {
      std::vector<redis::CommandKeyRange> vec_range = attr.key_range_vec_gen(args);

      for (const auto &range : vec_range) {
        if (range.first_key > 0) {
          updateWatchedKeysFromRange(args, range);
        }
      }

    } else {
      // support commands like flushdb (write flag && key range {0,0,0})
      updateAllWatchedKeys();
    }
  }
}

void Server::UpdateWatchedKeysManually(const std::vector<std::string> &keys) {
  std::shared_lock lock(watched_key_mutex_);

  for (const auto &key : keys) {
    if (auto iter = watched_key_map_.find(key); iter != watched_key_map_.end()) {
      for (auto *conn : iter->second) {
        conn->watched_keys_modified = true;
      }
    }
  }
}

void Server::WatchKey(redis::Connection *conn, const std::vector<std::string> &keys) {
  std::unique_lock lock(watched_key_mutex_);

  for (const auto &key : keys) {
    if (auto iter = watched_key_map_.find(key); iter != watched_key_map_.end()) {
      iter->second.emplace(conn);
    } else {
      watched_key_map_.emplace(key, std::set<redis::Connection *>{conn});
    }

    conn->watched_keys.insert(key);
  }

  watched_key_size_ = watched_key_map_.size();
}

bool Server::IsWatchedKeysModified(redis::Connection *conn) { return conn->watched_keys_modified; }

void Server::ResetWatchedKeys(redis::Connection *conn) {
  if (watched_key_size_ != 0) {
    std::unique_lock lock(watched_key_mutex_);

    for (const auto &key : conn->watched_keys) {
      if (auto iter = watched_key_map_.find(key); iter != watched_key_map_.end()) {
        iter->second.erase(conn);

        if (iter->second.empty()) {
          watched_key_map_.erase(iter);
        }
      }
    }

    conn->watched_keys.clear();
    conn->watched_keys_modified = false;
    watched_key_size_ = watched_key_map_.size();
  }
}

/*
// The numeric cursor consists of a 16-bit counter, a 16-bit time stamp, a 29-bit hash,and a 3-bit cursor type. The
// hash is used to prevent information leakage. The time_stamp is used to prevent the generation of the same cursor in
// the extremely short period before and after a restart.
NumberCursor::NumberCursor(CursorType cursor_type, uint16_t counter, const std::string &key_name) {
  auto hash = static_cast<uint32_t>(std::hash<std::string>{}(key_name));
  auto time_stamp = static_cast<uint16_t>(util::GetTimeStamp());
  // make hash top 3-bit zero
  constexpr uint64_t hash_mask = 0x1FFFFFFFFFFFFFFF;
  cursor_ = static_cast<uint64_t>(counter) | static_cast<uint64_t>(time_stamp) << 16 |
            (static_cast<uint64_t>(hash) << 32 & hash_mask) | static_cast<uint64_t>(cursor_type) << 61;
}

bool NumberCursor::IsMatch(const CursorDictElement &element, CursorType cursor_type) const {
  return cursor_ == element.cursor.cursor_ && cursor_type == getCursorType();
}

std::string Server::GenerateCursorFromKeyName(const std::string &key_name, CursorType cursor_type, const char *prefix) {
  if (!config_->redis_cursor_compatible) {
    // add prefix for SCAN
    return prefix + key_name;
  }
  auto counter = cursor_counter_.fetch_add(1);
  auto number_cursor = NumberCursor(cursor_type, counter, key_name);
  cursor_dict_->at(number_cursor.GetIndex()) = {number_cursor, key_name};
  return number_cursor.ToString();
}

std::string Server::GetKeyNameFromCursor(const std::string &cursor, CursorType cursor_type) {
  // When cursor is 0, cursor string is empty
  if (cursor.empty() || !config_->redis_cursor_compatible) {
    return cursor;
  }

  auto s = ParseInt<uint64_t>(cursor, 10);
  // When Cursor 0 or not a Integer return empty string.
  // Although the parameter 'cursor' is not expected to be 0, we still added a check for 0 to increase the robustness of
  // the code.
  if (!s.IsOK() || *s == 0) {
    return {};
  }
  auto number_cursor = NumberCursor(*s);
  // Because the index information is fully stored in the cursor, we can directly obtain the index from the cursor.
  auto item = cursor_dict_->at(number_cursor.GetIndex());
  if (number_cursor.IsMatch(item, cursor_type)) {
    return item.key_name;
  }

  return {};
}
*/

grpc::ServerUnaryReactor *Server::GetSyncPoint(grpc::CallbackServerContext *ctx,
                                               const kv::datanode::v1::GetSyncPointRequest *req,
                                               kv::datanode::v1::GetSyncPointResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slot_range();
  auto slot_range_idx_name = redis::SlotRangeIndexToString(slor_range_idx);
  if (!CheckClusterId(req->cluster_id())) {
    GlobalStatsInstance().IncrGetSyncPointCount(slot_range_idx_name, kClusterIdMismatchError);
    reactor->Finish(kClusterIdMismatchStatus);
    return reactor;
  }
  const auto &slot_range = GetSlotRangeByIndex(slor_range_idx);
  if (!slot_range) {
    GlobalStatsInstance().IncrGetSyncPointCount(slot_range_idx_name, kSlotRangeNotFoundError);
    reactor->Finish(kSlotRangeNotFoundStatus);
    return reactor;
  }
  const auto ret = slot_range->GetSyncPoint();
  if (!ret.IsOK()) {
    GlobalStatsInstance().IncrGetSyncPointCount(slot_range_idx_name, kUnknownError);
    reactor->Finish(UnknownSyncStatus(ret.Msg()));
    return reactor;
  }
  GlobalStatsInstance().IncrGetSyncPointCount(slot_range_idx_name, std::nullopt);
  resp->mutable_sync_point()->CopyFrom(ret.GetValue());
  reactor->Finish(kOkStatus);
  return reactor;
}

grpc::ServerWriteReactor<kv::datanode::v1::PullSyncDataResponse> *Server::PullSyncData(
    grpc::CallbackServerContext *ctx, const kv::datanode::v1::PullSyncDataRequest *req) {
  return new redis::DtsSender(shared_from_this(), ctx, req);
}

grpc::ServerReadReactor<kv::datanode::v1::PushSyncDataRequest> *Server::PushSyncData(
    grpc::CallbackServerContext *ctx, kv::datanode::v1::PushSyncDataResponse *resp) {
  return new redis::SyncReceiver(shared_from_this(), ctx, resp);
}

grpc::ServerUnaryReactor *Server::ReportSyncError(grpc::CallbackServerContext *ctx,
                                                  const kv::datanode::v1::ReportSyncErrorRequest *req,
                                                  kv::datanode::v1::ReportSyncErrorResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slot_range();
  auto slot_range_idx_name = redis::SlotRangeIndexToString(slor_range_idx);
  auto report_error = req->sync_error();
  if (!CheckClusterId(req->cluster_id())) {
    GlobalStatsInstance().IncrReportSyncErrorCount(slot_range_idx_name, kClusterIdMismatchError, report_error);
    reactor->Finish(kClusterIdMismatchStatus);
    return reactor;
  }
  auto error = cluster->CanSyncReceiveDataCrossPool(req->pusher_node_id());
  if (error.has_value()) {
    GlobalStatsInstance().IncrReportSyncErrorCount(slot_range_idx_name, error, report_error);
    reactor->Finish(SyncStatus(error.value()));
    return reactor;
  }
  const auto &slot_range = GetSlotRangeByIndex(req->slot_range());
  if (!slot_range) {
    GlobalStatsInstance().IncrReportSyncErrorCount(slot_range_idx_name, kSlotRangeNotFoundError, report_error);
    reactor->Finish(kSlotRangeNotFoundStatus);
    return reactor;
  }
  auto ret = slot_range->GetSyncPoint();
  if (!ret.IsOK()) {
    GlobalStatsInstance().IncrReportSyncErrorCount(slot_range_idx_name, kUnknownError, report_error);
    reactor->Finish(UnknownSyncStatus(ret.Msg()));
    return reactor;
  }
  error = redis::SyncReceiver::CheckSyncPoint(req->sync_point(), ret.GetValue());
  if (error.has_value()) {
    LOG(ERROR) << "Report sync error failed:sync point mismatch, req:" << *req << ", local:" << ret.GetValue()
               << ", error:" << error;
    GlobalStatsInstance().IncrReportSyncErrorCount(slot_range_idx_name, error, report_error);
    reactor->Finish(SyncStatus(error.value()));
    return reactor;
  }
  auto repl_status = SyncErrorToReplStatus(req->sync_error().code());
  if (repl_status.has_value()) {
    slot_range->SetReplicationStauts(repl_status.value());
  }
  LOG(INFO) << "Report sync error success, slot range:" << slot_range->GetName()
            << ", replication status:" << repl_status;
  GlobalStatsInstance().IncrReportSyncErrorCount(slot_range_idx_name, std::nullopt, report_error);
  reactor->Finish(kOkStatus);
  return reactor;
}

grpc::ServerUnaryReactor *Server::Ingest(grpc::CallbackServerContext *ctx, const kv::datanode::v1::IngestRequest *req,
                                         kv::datanode::v1::IngestResponse *) {
  auto reactor = ctx->DefaultReactor();
  auto &slot_range_idx = req->slot_range();
  auto slot_range_idx_name = redis::SlotRangeIndexToString(slot_range_idx);
  ingest::IngestMonitor::IngestStatsRecord record;
  if (!CheckClusterId(req->cluster_id())) {
    reactor->Finish(kClusterIdMismatchStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestClusterIdMisMatchError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  const auto &slot_range = GetSlotRangeByIndex(req->slot_range());
  if (!slot_range) {
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestSlotRangeNotFindError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    LOG(ERROR) << "Ingest error not find slot_range : " << slot_range_idx_name;
    return reactor;
  }

  auto storage = slot_range->GetStorage();
  auto guard = storage->ReadLockGuard();
  if (storage->IsClosing()) {
    LOG(ERROR) << "slot range : " << slot_range_idx_name << " is closing!";
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestClusterIdMisMatchError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  auto &task_id = req->ingestion_task_name();
  LOG(INFO) << "Ingest Task Id is :" << task_id;
  std::vector<ingest::IngestArg> req_args;

  auto &cfs = req->cfs_info();
  for (auto &cf : cfs) {
    req_args.emplace_back(cf.cf_name(), cf.sst_files_path(), cf.check_sum());
  }

  LOG(INFO) << "Ingest task id:" << task_id << ", slot_range: " << slot_range_idx_name;

  ingest::IngestTaskInfo task_info(slot_range_idx_name, req_args, task_id);
  auto ingester = storage->GetIngester();
  auto s = ingester->Run(task_info);

  if (!s.IsOK()) {
    task_info.ingest_sucessed = false;
    LOG(ERROR) << "Ingest Failed" << s.Msg();
    if (s.GetCode() == Status::IngestJobRunning) {
      reactor->Finish(kIngestTaskRunningStatus);
    } else {
      reactor->Finish(kIngestTaskFailedStatus);
    }

    ingester->IngestDone(task_info);
    {
      record.success = task_info.ingest_sucessed;
      record.reason = task_info.failed_reason;
      record.sst_count = task_info.sst_file_number;
      record.sst_size = task_info.sst_file_size;
      record.duration = task_info.ingest_duration_time_ms;
      record.ingest_finish_time = util::GetTimeStampMS();
      GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    }
    return reactor;
  }

  if (storage->GetConfig()->duration_ingest_close_auto_compact) {
    s = storage->AsyncCompactDB(std::string(), std::string(), false);
    if (!s.IsOK()) {
      LOG(ERROR) << "execute async compact faild, reason:" << s.Msg();
    }
  }

  ingester->IngestDone(task_info);
  {
    record.success = task_info.ingest_sucessed;
    record.reason = task_info.failed_reason;
    record.sst_count = task_info.sst_file_number;
    record.sst_size = task_info.sst_file_size;
    record.duration = task_info.ingest_duration_time_ms;
    record.ingest_finish_time = util::GetTimeStampMS();
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
  }
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor *Server::GetIngestInfo(grpc::CallbackServerContext *ctx,
                                                const kv::datanode::v1::GetIngestInfoRequest *req,
                                                kv::datanode::v1::GetIngestInfoResponse *reqs) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slot_range();
  auto slot_range_idx_name = redis::SlotRangeIndexToString(slor_range_idx);
  ingest::IngestMonitor::IngestStatsRecord record;
  if (!CheckClusterId(req->cluster_id())) {
    reactor->Finish(kClusterIdMismatchStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestClusterIdMisMatchError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }
  const auto &slot_range = GetSlotRangeByIndex(req->slot_range());
  if (!slot_range) {
    LOG(ERROR) << "Ingest error not find slot_range : " << slot_range_idx_name;
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestSlotRangeNotFindError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  auto storage = slot_range->GetStorage();
  auto gurad = storage->ReadLockGuard();
  if (storage->IsClosing()) {
    LOG(ERROR) << "slot range : " << slot_range_idx_name << " is closing!";
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestSlotRangeNotFindError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  auto &task_name = req->ingestion_task_name();

  std::string ingest_args;
  auto ingester = storage->GetIngester();

  auto code = ingester->GetResult(task_name);
  reqs->mutable_result()->CopyFrom(code);
  LOG(INFO) << "get ingest Task name is :" << task_name << ", result is: " << code.message();

  reactor->Finish(grpc::Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor *Server::StopDatanodeDts(grpc::CallbackServerContext *ctx,
                                                  const kv::datanode::v1::StopDatanodeDtsRequest *req,
                                                  kv::datanode::v1::StopDatanodeDtsResponse *) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slot_range();
  auto slot_range_idx_name = redis::SlotRangeIndexToString(slor_range_idx);
  ingest::IngestMonitor::IngestStatsRecord record;
  if (!CheckClusterId(req->cluster_id())) {
    reactor->Finish(kClusterIdMismatchStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestClusterIdMisMatchError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }
  const auto &slot_range = GetSlotRangeByIndex(req->slot_range());
  if (!slot_range) {
    LOG(ERROR) << "Ingest error not find slot_range : " << slot_range_idx_name;
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestSlotRangeNotFindError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  auto storage = slot_range->GetStorage();
  auto guard = storage->ReadLockGuard();
  if (storage->IsClosing()) {
    LOG(ERROR) << "slot range : " << slot_range_idx_name << " is closing!";
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestSlotRangeNotFindError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  slot_range->SetDtsWriteRunningStatus(redis::WriteStatus::WR_PROHIBITED);
  GlobalStatsInstance().StartDatanodeDtsWrProthibited(slot_range_idx_name);
  sync_manager->StopAndJoinDtsReceiver(
      slot_range_idx_name, SlotRangeStatusMismatchSyncError("dts running status read and write prohibited"));
  sync_manager->StopAndJoinDtsSender(slot_range_idx_name,
                                     SlotRangeStatusMismatchSyncError("dts running status read and write prohibited"));

  LOG(INFO) << "Stop datanode dts receive request sucessed";
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor *Server::StartDatanodeDts(grpc::CallbackServerContext *ctx,
                                                   const kv::datanode::v1::StartDatanodeDtsRequest *req,
                                                   kv::datanode::v1::StartDatanodeDtsResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slot_range();
  auto slot_range_idx_name = redis::SlotRangeIndexToString(slor_range_idx);
  ingest::IngestMonitor::IngestStatsRecord record;
  if (!CheckClusterId(req->cluster_id())) {
    reactor->Finish(kClusterIdMismatchStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestClusterIdMisMatchError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  const auto &slot_range = GetSlotRangeByIndex(req->slot_range());
  if (!slot_range) {
    LOG(ERROR) << "Ingest error not find slot_range : " << slot_range_idx_name;
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestSlotRangeNotFindError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  auto storage = slot_range->GetStorage();
  auto guard = storage->ReadLockGuard();
  if (storage->IsClosing()) {
    LOG(ERROR) << "slot range : " << slot_range_idx_name << " is closing!";
    reactor->Finish(kSlotRangeNotFoundStatus);
    record.success = false;
    record.reason = ingest::IngestMonitor::Reason::IngestSlotRangeNotFindError;
    GlobalStatsInstance().RecordIngestStats(slot_range_idx_name, record);
    return reactor;
  }

  slot_range->SetDtsWriteRunningStatus(redis::WriteStatus::UNSPECIFIED);
  GlobalStatsInstance().FinishDatanodeDtsWrProthibited(slot_range_idx_name);
  reactor->Finish(grpc::Status::OK);
  LOG(INFO) << "Start datanode dts receive request sucessed";
  return reactor;
}

grpc::ServerUnaryReactor *Server::GetSyncConfig(grpc::CallbackServerContext *ctx,
                                                const ::kv::datanode::v1::GetSyncConfigRequest *req,
                                                kv::datanode::v1::GetSyncConfigResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  reactor->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "unimplemented method in datanode v2"));
  return reactor;
}

grpc::ServerUnaryReactor *Server::UpdateSyncConfig(grpc::CallbackServerContext *ctx,
                                                   const kv::datanode::v1::UpdateSyncConfigRequest *req,
                                                   kv::datanode::v1::UpdateSyncConfigResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  reactor->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "unimplemented method in datanode v2"));
  return reactor;
}

void Server::GetRocksDBInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# RocksDB\r\n";

  auto slot_ranges = cluster->LocalSlotRanges();
  for (const auto &[name, slot_range] : slot_ranges) {
    std::string db_prefix{"rocksdb."};
    db_prefix.append("[");
    db_prefix.append(std::to_string(slot_range->GetRangeStart()));
    db_prefix.append("-");
    db_prefix.append(std::to_string(slot_range->GetRangeEnd()));
    db_prefix.append("].");

    auto storage = slot_range->GetStorage();
    rocksdb::DB *db = storage->GetDB();

    uint64_t memtable_sizes = 0, cur_memtable_sizes = 0, num_snapshots = 0, num_running_flushes = 0;
    uint64_t num_immutable_tables = 0, memtable_flush_pending = 0, compaction_pending = 0;
    uint64_t num_running_compaction = 0, num_live_versions = 0, num_super_version = 0, num_background_errors = 0;
    uint64_t delayed_write_rate = 0, is_write_stopped = 0;
    uint64_t total_sst_files_size = 0, live_sst_files_size = 0;
    uint64_t total_blob_file_size = 0, live_blob_file_size = 0, num_blob_files = 0;

    db->GetAggregatedIntProperty("rocksdb.num-snapshots", &num_snapshots);
    db->GetAggregatedIntProperty("rocksdb.size-all-mem-tables", &memtable_sizes);
    db->GetAggregatedIntProperty("rocksdb.cur-size-all-mem-tables", &cur_memtable_sizes);
    db->GetAggregatedIntProperty("rocksdb.num-running-flushes", &num_running_flushes);
    db->GetAggregatedIntProperty("rocksdb.num-immutable-mem-table", &num_immutable_tables);
    db->GetAggregatedIntProperty("rocksdb.mem-table-flush-pending", &memtable_flush_pending);
    db->GetAggregatedIntProperty("rocksdb.num-running-compactions", &num_running_compaction);
    db->GetAggregatedIntProperty("rocksdb.current-super-version-number", &num_super_version);
    db->GetAggregatedIntProperty("rocksdb.background-errors", &num_background_errors);
    db->GetAggregatedIntProperty("rocksdb.compaction-pending", &compaction_pending);
    db->GetAggregatedIntProperty("rocksdb.num-live-versions", &num_live_versions);
    db->GetAggregatedIntProperty("rocksdb.total-sst-files-size", &total_sst_files_size);
    db->GetAggregatedIntProperty("rocksdb.live-sst-files-size", &live_sst_files_size);
    db->GetAggregatedIntProperty("rocksdb.total-blob-file-size", &total_blob_file_size);
    db->GetAggregatedIntProperty("rocksdb.live-blob-file-size", &live_blob_file_size);
    db->GetAggregatedIntProperty("rocksdb.num-blob-files", &num_blob_files);
    db->GetIntProperty("rocksdb.actual-delayed-write-rate", &delayed_write_rate);
    db->GetIntProperty("rocksdb.is-write-stopped", &is_write_stopped);

    for (const auto &cf_handle : *storage->GetCFHandles()) {
      uint64_t estimate_keys = 0, block_cache_usage = 0, block_cache_pinned_usage = 0, index_and_filter_cache_usage = 0;
      std::map<std::string, std::string> cf_stats_map;
      db->GetIntProperty(cf_handle, "rocksdb.estimate-num-keys", &estimate_keys);
      string_stream << db_prefix << "estimate_keys[" << cf_handle->GetName() << "]:" << estimate_keys << "\r\n";
      db->GetIntProperty(cf_handle, "rocksdb.block-cache-usage", &block_cache_usage);
      string_stream << db_prefix << "block_cache_usage[" << cf_handle->GetName() << "]:" << block_cache_usage << "\r\n";
      db->GetIntProperty(cf_handle, "rocksdb.block-cache-pinned-usage", &block_cache_pinned_usage);
      string_stream << db_prefix << "block_cache_pinned_usage[" << cf_handle->GetName()
                    << "]:" << block_cache_pinned_usage << "\r\n";
      db->GetIntProperty(cf_handle, "rocksdb.estimate-table-readers-mem", &index_and_filter_cache_usage);
      string_stream << db_prefix << "index_and_filter_cache_usage[" << cf_handle->GetName()
                    << "]:" << index_and_filter_cache_usage << "\r\n";

      std::string level0_files, level1_files, level2_files, level3_files, level4_files, level5_files, level6_files;
      db->GetProperty(cf_handle, "rocksdb.num-files-at-level0", &level0_files);
      db->GetProperty(cf_handle, "rocksdb.num-files-at-level1", &level1_files);
      db->GetProperty(cf_handle, "rocksdb.num-files-at-level2", &level2_files);
      db->GetProperty(cf_handle, "rocksdb.num-files-at-level3", &level3_files);
      db->GetProperty(cf_handle, "rocksdb.num-files-at-level4", &level4_files);
      db->GetProperty(cf_handle, "rocksdb.num-files-at-level5", &level5_files);
      db->GetProperty(cf_handle, "rocksdb.num-files-at-level6", &level6_files);

      std::string cf_total_sst_files_size, cf_live_sst_files_size, cf_total_blob_file_size, cf_live_blob_file_size,
          cf_num_blob_files;
      db->GetProperty(cf_handle, "rocksdb.total-sst-files-size", &cf_total_sst_files_size);
      db->GetProperty(cf_handle, "rocksdb.live-sst-files-size", &cf_live_sst_files_size);
      db->GetProperty(cf_handle, "rocksdb.total-blob-file-size", &cf_total_blob_file_size);
      db->GetProperty(cf_handle, "rocksdb.live-blob-file-size", &cf_live_blob_file_size);
      db->GetProperty(cf_handle, "rocksdb.num-blob-files", &cf_num_blob_files);
      string_stream << db_prefix << "cf_total_sst_files_size[" << cf_handle->GetName()
                    << "]:" << cf_total_sst_files_size << "\r\n";
      string_stream << db_prefix << "cf_live_sst_files_size[" << cf_handle->GetName() << "]:" << cf_live_sst_files_size
                    << "\r\n";
      string_stream << db_prefix << "cf_total_blob_file_size[" << cf_handle->GetName()
                    << "]:" << cf_total_blob_file_size << "\r\n";
      string_stream << db_prefix << "cf_live_blob_file_size[" << cf_handle->GetName() << "]:" << cf_live_blob_file_size
                    << "\r\n";
      string_stream << db_prefix << "cf_num_blob_files[" << cf_handle->GetName() << "]:" << cf_num_blob_files << "\r\n";

      db->GetMapProperty(cf_handle, rocksdb::DB::Properties::kCFStats, &cf_stats_map);
      string_stream << db_prefix << "level0_file_limit_slowdown[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["l0-file-count-limit-delays"] << "\r\n";
      string_stream << db_prefix << "level0_file_limit_stop[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["l0-file-count-limit-stops"] << "\r\n";
      string_stream << db_prefix << "pending_compaction_bytes_slowdown[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["pending-compaction-bytes-delays"] << "\r\n";
      string_stream << db_prefix << "pending_compaction_bytes_stop[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["pending-compaction-bytes-stops"] << "\r\n";
      string_stream << db_prefix << "level0_file_limit_stop_with_ongoing_compaction[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["cf-l0-file-count-limit-stops-with-ongoing-compaction"] << "\r\n";
      string_stream << db_prefix << "level0_file_limit_slowdown_with_ongoing_compaction[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["cf-l0-file-count-limit-delays-with-ongoing-compaction"] << "\r\n";
      string_stream << db_prefix << "memtable_count_limit_slowdown[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["memtable-limit-delays"] << "\r\n";
      string_stream << db_prefix << "memtable_count_limit_stop[" << cf_handle->GetName()
                    << "]:" << cf_stats_map["memtable-limit-stops"] << "\r\n";
      string_stream << db_prefix << "num_files_at_level0[" << cf_handle->GetName() << "]:" << level0_files << "\r\n";
      string_stream << db_prefix << "num_files_at_level1[" << cf_handle->GetName() << "]:" << level1_files << "\r\n";
      string_stream << db_prefix << "num_files_at_level2[" << cf_handle->GetName() << "]:" << level2_files << "\r\n";
      string_stream << db_prefix << "num_files_at_level3[" << cf_handle->GetName() << "]:" << level3_files << "\r\n";
      string_stream << db_prefix << "num_files_at_level4[" << cf_handle->GetName() << "]:" << level4_files << "\r\n";
      string_stream << db_prefix << "num_files_at_level5[" << cf_handle->GetName() << "]:" << level5_files << "\r\n";
      string_stream << db_prefix << "num_files_at_level6[" << cf_handle->GetName() << "]:" << level6_files << "\r\n";
    }
    string_stream << db_prefix << "used_db_size:" << storage->GetTotalSize() << "\r\n";
    string_stream << db_prefix << "all_mem_tables:" << memtable_sizes << "\r\n";
    string_stream << db_prefix << "cur_mem_tables:" << cur_memtable_sizes << "\r\n";
    string_stream << db_prefix << "snapshots:" << num_snapshots << "\r\n";
    string_stream << db_prefix << "num_immutable_tables:" << num_immutable_tables << "\r\n";
    string_stream << db_prefix << "num_running_flushes:" << num_running_flushes << "\r\n";
    string_stream << db_prefix << "memtable_flush_pending:" << memtable_flush_pending << "\r\n";
    string_stream << db_prefix << "compaction_pending:" << compaction_pending << "\r\n";
    string_stream << db_prefix << "num_running_compactions:" << num_running_compaction << "\r\n";
    string_stream << db_prefix << "num_live_versions:" << num_live_versions << "\r\n";
    string_stream << db_prefix << "num_super_version:" << num_super_version << "\r\n";
    string_stream << db_prefix << "num_background_errors:" << num_background_errors << "\r\n";
    string_stream << db_prefix << "delayed_write_rate:" << delayed_write_rate << "\r\n";
    string_stream << db_prefix << "is_write_stopped:" << is_write_stopped << "\r\n";
    string_stream << db_prefix << "total_sst_files_size:" << total_sst_files_size << "\r\n";
    string_stream << db_prefix << "live_sst_files_size:" << live_sst_files_size << "\r\n";
    string_stream << db_prefix << "total_blob_file_size:" << total_blob_file_size << "\r\n";
    string_stream << db_prefix << "live_blob_file_size:" << live_blob_file_size << "\r\n";
    string_stream << db_prefix << "num_blob_files:" << num_blob_files << "\r\n";
    string_stream << db_prefix << "flush_count:" << storage->GetFlushCount() << "\r\n";
    string_stream << db_prefix << "compaction_count:" << storage->GetCompactionCount() << "\r\n";
    string_stream << db_prefix
                  << "put_per_sec:" << storage->stats.GetInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_PUT)
                  << "\r\n";
    string_stream << db_prefix << "get_per_sec:"
                  << storage->stats.GetInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_GET) +
                         storage->stats.GetInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_MULTIGET)
                  << "\r\n";
    string_stream << db_prefix
                  << "seek_per_sec:" << storage->stats.GetInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_SEEK)
                  << "\r\n";
    string_stream << db_prefix
                  << "next_per_sec:" << storage->stats.GetInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_NEXT)
                  << "\r\n";
    string_stream << db_prefix
                  << "prev_per_sec:" << storage->stats.GetInstantaneousMetric(StorageStats::STATS_METRIC_ROCKSDB_PREV)
                  << "\r\n";

    string_stream << storage->GetCronInfo(db_prefix);
    string_stream << storage->GetJobInfo(db_prefix);
    string_stream << storage->GetWriteStall(db_prefix);
    string_stream << storage->GetOpsLatency(db_prefix);
    string_stream << storage->GetCompressionInfo(db_prefix);

    storage->stats.GetFailedWriteInfo(string_stream, db_prefix);
  }

  *info = string_stream.str();
}

void Server::GetLegacyslotsCompactInfo(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# LegacyslotsCompact\r\n";
  string_stream << "legacyslots.is_compacting:" << (global_legacyslots_compacting_count != 0 ? 1 : 0) << "\r\n";
  string_stream << "legacyslots.last_compact_time:" << global_legacyslots_last_compact_time.load() << "\r\n";
  string_stream << "legacyslots.last_compact_spent_secs:" << global_lagacyslots_last_compact_duration.load() << "\r\n";
  *info = string_stream.str();
}

Status Server::SetDBOption(const std::string &key, const std::string &value) {
  auto slot_ranges = cluster->LocalSlotRanges();
  for (const auto &[name, slot_range] : slot_ranges) {
    auto storage = slot_range->GetStorage();
    auto s = storage->SetDBOption(key, value);
    if (!s.IsOK()) {
      std::string err_msg{"rocksdb.["};
      err_msg.append(std::to_string(slot_range->GetRangeStart()))
          .append("-")
          .append(std::to_string(slot_range->GetRangeEnd()))
          .append("]")
          .append(s.Msg());
      return {Status::NotOK, err_msg};
    }
  }

  return Status::OK();
}

Status Server::SetDBOptionForAllColumnFamilies(const std::string &key, const std::string &value) {
  auto slot_ranges = cluster->LocalSlotRanges();
  for (const auto &[name, slot_range] : slot_ranges) {
    auto storage = slot_range->GetStorage();
    auto s = storage->SetOptionForAllColumnFamilies(key, value);
    if (!s.IsOK()) {
      std::string err_msg{"rocksdb.["};
      err_msg.append(std::to_string(slot_range->GetRangeStart()))
          .append("-")
          .append(std::to_string(slot_range->GetRangeEnd()))
          .append("]")
          .append(s.Msg());
      return {Status::NotOK, err_msg};
    }
  }

  return Status::OK();
}

Status Server::SetDBOptionForColumnFamily(const std::string &db_name, const std::string &cf_name,
                                          const std::string &key, const std::string &value) {
  auto slot_ranges = cluster->LocalSlotRanges();
  auto it = slot_ranges.find(db_name);
  if (it == slot_ranges.end()) {
    return {Status::NotOK, "Database not found: " + db_name};
  }

  auto storage = it->second->GetStorage();
  auto s = storage->SetOptionForColumnFamily(cf_name, key, value);
  if (!s.IsOK()) {
    std::string err_msg{"rocksdb.["};
    err_msg.append(std::to_string(it->second->GetRangeStart()))
        .append("-")
        .append(std::to_string(it->second->GetRangeEnd()))
        .append("]")
        .append(s.Msg());
    return {Status::NotOK, err_msg};
  }
  return Status::OK();
}

grpc::ServerUnaryReactor *Server::Failover(grpc::CallbackServerContext *ctx,
                                           const kv::datanode::v1::FailoverRequest *req,
                                           kv::datanode::v1::FailoverResponse *resp) {
  FAILOVER_INFO << "receive failover. task_id: " << req->task_id() << ", time point ms:" << req->timeout_point_ms();

  auto reactor = ctx->DefaultReactor();

  if (!CheckDataNodeId(req->dst_datanode_id())) {
    FAILOVER_ERROR << "failover failed. task_id: " << req->task_id()
                   << ", failover dst datanode id: " << req->dst_datanode_id()
                   << " not expected. local id:" << cluster->DatanodeId();
    reactor->Finish(kDataNodeIdMismatchStatus);
    return reactor;
  }

  auto now = int64_t(util::GetTimeStampMS());
  if (req->timeout_point_ms() <= now) {
    FAILOVER_ERROR << "failover failed. task_id: " << req->task_id() << ", timeout point: " << req->timeout_point_ms()
                   << " less or equal than now:" << now;
    reactor->Finish(kTimeoutPointExceededStatus);
    return reactor;
  }

  redis::Migration::TaskInfo task_info;
  task_info.task_id = req->task_id();
  task_info.dst_datanode_id = req->dst_datanode_id();
  task_info.timeout_point_ms = req->timeout_point_ms();
  task_info.src_datanodes.emplace(req->src_datanode_id(), std::move(cluster->GetAllLocalSlotRangeNames()));

  Status status;
  if (req->is_force()) {
    FAILOVER_INFO << "run force failover. task_id: " << req->task_id() << ", time point ms:" << req->timeout_point_ms();
    task_info.need_replicate_data = false;
    status = migration->StartMigrateWithoutReplData(task_info);
  } else {
    FAILOVER_INFO << "run failover. task_id: " << req->task_id() << ", time point ms:" << req->timeout_point_ms();
    task_info.need_replicate_data = true;
    status = migration->StartMigrateWithReplData(task_info);
  }

  if (!status.IsOK() && status.GetCode() != Status::MigrationReentrant) {
    if (status.GetCode() == Status::AnotherMigrationDoing) {
      reactor->Finish(kMigrationTaskDoingStatus);
    } else {
      reactor->Finish(kMigrationTaskFailedStatus);
    }
    FAILOVER_ERROR << "failover failed. task_id: " << req->task_id() << ", " << status.Msg();
    return reactor;
  }

  reactor->Finish(kOkStatus);
  FAILOVER_INFO << "failover OK. task_id: " << req->task_id() << ", time point ms:" << req->timeout_point_ms()
                << ", status:" << status.Msg();
  return reactor;
}

grpc::ServerUnaryReactor *Server::Migrate(grpc::CallbackServerContext *ctx, const kv::datanode::v1::MigrateRequest *req,
                                          kv::datanode::v1::MigrateResponse *resp) {
  MIGRATE_INFO << "receive migrate. task_id: " << req->task_id() << ", time point ms:" << req->timeout_point_ms();

  auto reactor = ctx->DefaultReactor();

  if (!CheckDataNodeId(req->dst_datanode_id())) {
    MIGRATE_ERROR << "migrate failed. task_id: " << req->task_id()
                  << ", migrate dst datanode id: " << req->dst_datanode_id()
                  << " not expected. local id:" << cluster->DatanodeId();
    reactor->Finish(kDataNodeIdMismatchStatus);
    return reactor;
  }

  auto now = int64_t(util::GetTimeStampMS());
  if (req->timeout_point_ms() <= now) {
    MIGRATE_ERROR << "migrate failed. task_id: " << req->task_id() << ", timeout point: " << req->timeout_point_ms()
                  << " less or equal than now:" << now;
    reactor->Finish(kTimeoutPointExceededStatus);
    return reactor;
  }

  redis::Migration::TaskInfo task_info;
  task_info.task_id = req->task_id();
  task_info.dst_datanode_id = req->dst_datanode_id();
  task_info.timeout_point_ms = req->timeout_point_ms();
  task_info.need_replicate_data = true;
  size_t count = 0;
  for (const auto &src : req->src_datanodes()) {
    std::set<std::string> slot_range_names;
    for (const auto &sr_idx : src.second.slot_range_index()) {
      slot_range_names.emplace(redis::CreateSlotRangeName(sr_idx.start(), sr_idx.end()));
    }
    auto s = cluster->CheckAndGetSlotRangesServedByMySelf(slot_range_names);
    if (!s.IsOK()) {
      MIGRATE_ERROR << "migrate failed. task_id: " << req->task_id() << ", " << s.Msg();
      reactor->Finish(kSlotRangeNotFoundStatus);
      return reactor;
    }
    count += slot_range_names.size();
    task_info.src_datanodes.emplace(src.first, std::move(slot_range_names));
  }

  // slot_ranges between src and dst datanodes should match
  if (count != cluster->LocalSlotRanges().size()) {
    std::ostringstream stream;
    stream << "slot_ranges are not matched between src and dst datanode. task_id: " << req->task_id()
           << "; dst_datanode_id: " << req->dst_datanode_id() << ", dst_slot_ragens: [";
    auto slot_range_names = cluster->GetAllLocalSlotRangeNames();
    for (const auto &name : slot_range_names) {
      stream << name << ",";
    }
    stream << "]";
    for (const auto &src : task_info.src_datanodes) {
      stream << "; src_datanode_id: " << src.first << ", src_slot_ranges: [";
      for (const auto &name : src.second) {
        stream << name << ",";
      }
      stream << "]";
    }

    MIGRATE_ERROR << "migrate failed. task_id: " << req->task_id() << ", " << stream.str();
    reactor->Finish(kMigrationTaskFailedStatus);
    return reactor;
  }

  Status status = migration->StartMigrateWithReplData(task_info);
  if (!status.IsOK() && status.GetCode() != Status::MigrationReentrant) {
    if (status.GetCode() == Status::AnotherMigrationDoing) {
      reactor->Finish(kMigrationTaskDoingStatus);
    } else {
      reactor->Finish(kMigrationTaskFailedStatus);
    }
    MIGRATE_ERROR << "migrate failed. task_id: " << req->task_id() << ", " << status.Msg();
    return reactor;
  }

  MIGRATE_INFO << "migrate succ. task_id: " << req->task_id() << ", time point ms:" << req->timeout_point_ms()
               << ", status:" << status.Msg();
  reactor->Finish(kOkStatus);
  return reactor;
}

grpc::ServerUnaryReactor *Server::GetLatestPoint(grpc::CallbackServerContext *ctx,
                                                 const kv::datanode::v1::GetLatestPointRequest *req,
                                                 kv::datanode::v1::GetLatestPointResponse *rsp) {
  auto reactor = ctx->DefaultReactor();

  // check cluster id
  if (config_->cluster_id != req->cluster_id()) {
    kv::datanode::v1::Error error(kUnknownError);
    error.set_message(fmt::format("wrong cluster_id '{}', local_id '{}'", req->cluster_id(), config_->cluster_id));
    reactor->Finish(SyncStatus(error));
    return reactor;
  }
  // get latest points
  std::vector<uint64_t> db_ids;
  for (auto &elem : req->db_ids()) db_ids.emplace_back(elem);
  std::vector<kv::datanode::v1::LatestPoint> latest_points;
  auto s = storage_mgr->GetLatestPoints(db_ids, &latest_points);
  if (!s.IsOK()) {
    kv::datanode::v1::Error error(kUnknownError);
    error.set_message(s.Msg());
    reactor->Finish(SyncStatus(error));
    return reactor;
  }
  // set response
  for (auto &point : latest_points) {
    rsp->add_db_points()->CopyFrom(point);
  }

  std::stringstream stream;
  stream << "[server] GetLatestPointResponse: \n";
  for (auto &point : rsp->db_points()) {
    stream << "db_id,seq_id,repl_id: [" << point.db_id() << "," << point.seq_id() << "," << point.repl_id() << "]\n";
  }
  LOG(INFO) << stream.str();

  reactor->Finish(kOkStatus);
  return reactor;
}

grpc::ServerUnaryReactor *Server::GetDataWithCmd(grpc::CallbackServerContext *ctx,
                                                 const kv::datanode::v1::GetDataWithCmdRequest *req,
                                                 kv::datanode::v1::GetDataWithCmdResponse *rsp) {
  auto reactor = ctx->DefaultReactor();

  // get db_id,seq_id
  uint64_t db_id = req->db_id();
  uint64_t next_seq = req->seq();

  // get writebatch
  std::vector<std::vector<std::string>> results;
  bool is_finished = false;
  auto s = storage_mgr->GetWalDataWithCmd(db_id, &next_seq, &results, &is_finished);
  if (!s.IsOK()) {
    std::stringstream stream;
    if (!results.empty()) {
      stream << "[server] GetDataWithCmdResponse: Got parsed results size: " << results.size() << " \n";
      for (auto &res : results) {
        for (auto &token : res) {
          stream << token << " ";
        }
        stream << "\n";
      }
    } else {
      stream << "[server] GetDataWithCmdResponse: None cmds parsed";
    }
    LOG(INFO) << stream.str();

    kv::datanode::v1::Error error(kUnknownError);
    error.set_message(s.Msg());
    reactor->Finish(SyncStatus(error));
    return reactor;
  }
  // set response
  rsp->set_next_seq(next_seq);
  if (is_finished) rsp->set_finished(true);
  for (auto &tokens : results) {
    rsp->add_resp_cmds(redis::MultiBulkString(tokens, false));
  }

  std::stringstream stream;
  stream << "[server] GetDataWithCmdResponse next_seq: " << rsp->next_seq()
         << ", finished: " << (rsp->finished() ? "YES" : "NO") << ", cmds_size: " << rsp->resp_cmds_size()
         << ", got cmds: \n";
  for (auto &cmd : rsp->resp_cmds()) {
    stream << cmd << "\n";
  }
  LOG(INFO) << stream.str();

  reactor->Finish(kOkStatus);
  return reactor;
}

void Server::recordInstantaneousMetrics() {
  GlobalStatsInstance().TrackInstantaneousMetric(GlobalStats::STATS_METRIC_COMMAND, GlobalStatsInstance().total_calls);
  GlobalStatsInstance().TrackInstantaneousMetric(GlobalStats::STATS_METRIC_NET_INPUT, GlobalStatsInstance().in_bytes);
  GlobalStatsInstance().TrackInstantaneousMetric(GlobalStats::STATS_METRIC_NET_OUTPUT, GlobalStatsInstance().out_bytes);
  GlobalStatsInstance().TrackInstantaneousMetric(GlobalStats::STATS_METRIC_SLOW_QUERY,
                                                 GlobalStatsInstance().slow_query_calls);

  auto slot_ranges = cluster->LocalSlotRanges();
  for (const auto &[name, slot_range] : slot_ranges) {
    auto storage = slot_range->GetStorage();
    auto gurad = storage->ReadLockGuard();
    if (storage->IsClosing()) {
      continue;
    }
    storage->RecordInstantaneousDBMetrics();
  }
}

grpc::ServerBidiReactor<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse> *Server::SyncData(
    grpc::CallbackServerContext *ctx) {
  return new redis::ReplSender(shared_from_this(), ctx);
}

Status Server::SubCompactJob(const std::string &begin_key, std::string &end_key, const std::string &name,
                             bool is_compact, bool with_filter) {
  if (name.empty()) {
    return {Status::NotOK, "slot range cannot be empty"};
  }

  auto slot_ranges = cluster->LocalSlotRanges();
  for (const auto &[slot_range_name, slot_range] : slot_ranges) {
    if (name == slot_range_name) {
      auto storage = slot_range->GetStorage();
      auto guard = storage->ReadLockGuard();
      if (storage->IsClosing()) {
        return {Status::NotOK, fmt::format("Slotrange {} is closing", name)};
      }

      if (is_compact) {
        LOG(WARNING) << fmt::format("Trigger compaction for slotrange {}", name);
        return storage->AsyncCompactDB(begin_key, end_key, with_filter);
      }
      LOG(WARNING) << fmt::format("Cancel compaction of slotrange {}", name);
      return storage->CancleCompactDB();
    }
  }

  return {Status::NotOK, fmt::format("Slotrange {} is not found", name)};
}

Status Server::SubCompactLegacyJob() const {
  auto slot_ranges = cluster->LocalSlotRanges();
  for (const auto &[slot_range_name, slot_range] : slot_ranges) {
    std::vector<std::pair<int16_t, int16_t>> range_pairs;
    auto start = slot_range->GetRangeStart();
    auto end = slot_range->GetRangeEnd();
    if (start > 0) range_pairs.emplace_back(std::pair{0, start});
    if (end < kClusterSlots - 1) range_pairs.emplace_back(std::pair{end + 1, kClusterSlots});

    auto storage = slot_range->GetStorage();
    auto guard = storage->ReadLockGuard();
    if (storage->IsClosing()) {
      LOG(WARNING) << fmt::format("Slotrange {} is closing", slot_range_name);
      continue;
    }
    if (range_pairs.empty()) {
      LOG(WARNING) << fmt::format("No legacy compaction triggered for slotrange [{},{}]", start, end);
      continue;
    }
    // trigger legacy slotrange compaction
    for (const auto &it : range_pairs) {
      std::string start_key, end_key;
      PutFixed16(&start_key, it.first);
      PutFixed16(&end_key, it.second);
      global_legacyslots_compacting_count.fetch_add(1);
      auto s = storage->AsyncCompactDB(start_key, end_key, false, true);
      if (!s.IsOK()) {
        global_legacyslots_compacting_count.fetch_sub(1);
        return s;
      }
      LOG(WARNING) << fmt::format("Trigger compaction legacy for slotrange [{},{}]", it.first, it.second);
    }
  }

  return Status::OK();
}

void Server::GetAllSlotRangeName(std::string *output) const {
  std::ostringstream string_stream;
  auto slot_ranges = cluster->LocalSlotRanges();
  for (const auto &[name, slot_range] : slot_ranges) {
    string_stream << "DBname:{" << name << "}; ";
  }

  *output = redis::BulkString(string_stream.str());
}

void Server::GetDBAndCFList(std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# DB and CF List\r\n";

  auto slot_ranges = cluster->LocalSlotRanges();
  std::vector<std::string> db_names;
  size_t total_cf_count = 0;

  for (const auto &[name, slot_range] : slot_ranges) {
    db_names.push_back(name);

    // Calculate actual CF count for each database
    auto storage = slot_range->GetStorage();
    if (storage) {
      const auto &cf_handles = *storage->GetCFHandles();
      total_cf_count += cf_handles.size();
    }
  }

  string_stream << "total_databases:" << db_names.size() << "\r\n";
  string_stream << "total_column_families:" << total_cf_count << "\r\n";

  // List all database names
  string_stream << "databases:";
  for (size_t i = 0; i < db_names.size(); ++i) {
    if (i > 0) string_stream << ",";
    string_stream << db_names[i];
  }
  string_stream << "\r\n";

  // List column families for each database
  for (const auto &[name, slot_range] : slot_ranges) {
    auto storage = slot_range->GetStorage();
    if (!storage) continue;

    string_stream << name << ".column_families:";
    const auto &cf_handles = *storage->GetCFHandles();
    for (size_t i = 0; i < cf_handles.size(); ++i) {
      if (i > 0) string_stream << ",";
      string_stream << cf_handles[i]->GetName();
    }
    string_stream << "\r\n";
  }

  *info = string_stream.str();
}

void Server::GetDBPropertyInfo(const std::string &query_params, std::string *info) {
  std::ostringstream string_stream;
  string_stream << "# DB Property Query Results\r\n";
  string_stream << "query:" << query_params << "\r\n";

  // Parse query parameters: action:db_name:cf_name:property_type:property_name:map_key
  std::vector<std::string> params = util::Split(query_params, ":");

  if (params.empty()) {
    string_stream << "error:invalid_query_format\r\n";
    string_stream << "format:action[:db_name[:cf_name[:property_type[:property_name[:map_key]]]]]\r\n";
    string_stream << "example:get:all:all:int:rocksdb.estimate-num-keys\r\n";
    *info = string_stream.str();
    return;
  }

  std::string action = util::ToLower(params[0]);
  std::string db_name = params.size() > 1 ? params[1] : "all";
  std::string cf_name = params.size() > 2 ? params[2] : "all";
  std::string property_type = params.size() > 3 ? util::ToLower(params[3]) : "";
  std::string property_name = params.size() > 4 ? params[4] : "";
  std::string map_key = params.size() > 5 ? params[5] : "";

  if (action != "get") {
    string_stream << "error:unsupported_action:" << action << "\r\n";
    string_stream << "supported_actions:get\r\n";
    *info = string_stream.str();
    return;
  }

  if (property_type.empty() || property_name.empty()) {
    string_stream << "error:missing_property_info\r\n";
    string_stream << "required:property_type and property_name\r\n";
    string_stream << "property_types:int,string,aggint,map\r\n";
    *info = string_stream.str();
    return;
  }

  // Validate property type
  if (property_type != "int" && property_type != "string" && property_type != "aggint" && property_type != "map") {
    string_stream << "error:invalid_property_type:" << property_type << "\r\n";
    string_stream << "supported_types:int,string,aggint,map\r\n";
    *info = string_stream.str();
    return;
  }

  // Execute property query
  int result_count = 0;
  auto slot_ranges = cluster->LocalSlotRanges();

  // Select target databases
  std::vector<std::pair<std::string, std::shared_ptr<redis::SlotRange>>> selected_dbs;
  if (db_name == "all") {
    for (const auto &[name, slot_range] : slot_ranges) {
      selected_dbs.emplace_back(name, slot_range);
    }
  } else {
    auto it = slot_ranges.find(db_name);
    if (it != slot_ranges.end()) {
      selected_dbs.emplace_back(it->first, it->second);
    } else {
      string_stream << "error:db_not_found:" << db_name << "\r\n";
      *info = string_stream.str();
      return;
    }
  }

  // Process each selected database
  for (const auto &[db_name_actual, slot_range] : selected_dbs) {
    auto storage = slot_range->GetStorage();
    if (!storage) continue;

    rocksdb::DB *db = storage->GetDB();
    if (!db) continue;

    // Select target column families
    std::vector<rocksdb::ColumnFamilyHandle *> selected_cfs;
    if (cf_name == "all") {
      for (const auto &cf_handle : *storage->GetCFHandles()) {
        selected_cfs.push_back(cf_handle);
      }
    } else {
      auto cf_handle = storage->GetCFHandle(cf_name);
      if (cf_handle) {
        selected_cfs.push_back(cf_handle);
      } else {
        string_stream << "error:cf_not_found:" << cf_name << ":db:" << db_name_actual << "\r\n";
        continue;
      }
    }

    // Execute property query for each CF
    for (const auto &cf_handle : selected_cfs) {
      std::string cf_name_actual = cf_handle->GetName();
      std::string result_key = db_name_actual + "." + cf_name_actual + "." + property_name;

      bool success = false;

      if (property_type == "int") {
        uint64_t value = 0;
        if (db->GetIntProperty(cf_handle, property_name, &value)) {
          string_stream << result_key << ":" << value << "\r\n";
          success = true;
        }
      } else if (property_type == "string") {
        std::string value;
        if (db->GetProperty(cf_handle, property_name, &value)) {
          string_stream << result_key << ":" << value << "\r\n";
          success = true;
        }
      } else if (property_type == "aggint") {
        uint64_t value = 0;
        if (db->GetAggregatedIntProperty(property_name, &value)) {
          string_stream << db_name_actual << "." << property_name << ":" << value << "\r\n";
          success = true;
        }
      } else if (property_type == "map") {
        std::map<std::string, std::string> value_map;
        if (db->GetMapProperty(cf_handle, property_name, &value_map)) {
          if (map_key.empty()) {
            // Return all map entries
            for (const auto &[key, value] : value_map) {
              string_stream << result_key << "." << key << ":" << value << "\r\n";
            }
          } else {
            // Return specific map entry
            auto it = value_map.find(map_key);
            if (it != value_map.end()) {
              string_stream << result_key << "." << map_key << ":" << it->second << "\r\n";
            } else {
              string_stream << "error:map_key_not_found:" << map_key << ":property:" << result_key << "\r\n";
            }
          }
          success = true;
        }
      }

      if (success) {
        result_count++;
      } else {
        string_stream << "error:property_query_failed:" << result_key << "\r\n";
      }
    }
  }

  string_stream << "total_results:" << result_count << "\r\n";
  *info = string_stream.str();
}

// Helper function to get CF option values
static std::string GetCFOptionValue(engine::Storage *storage, rocksdb::ColumnFamilyHandle *cf_handle,
                                    const std::string &db_name, const std::string &cf_name,
                                    const std::string &option_name) {
  std::ostringstream string_stream;

  auto db = storage->GetDB();
  auto cf_options = db->GetOptions(cf_handle);

  if (option_name == "all") {
    // Return all important CF options
    string_stream << "# CF Options for " << db_name << "." << cf_name << "\r\n";
    string_stream << "write_buffer_size:" << cf_options.write_buffer_size << "\r\n";
    string_stream << "max_write_buffer_number:" << cf_options.max_write_buffer_number << "\r\n";
    string_stream << "min_write_buffer_number_to_merge:" << cf_options.min_write_buffer_number_to_merge << "\r\n";
    string_stream << "level0_file_num_compaction_trigger:" << cf_options.level0_file_num_compaction_trigger << "\r\n";
    string_stream << "level0_slowdown_writes_trigger:" << cf_options.level0_slowdown_writes_trigger << "\r\n";
    string_stream << "level0_stop_writes_trigger:" << cf_options.level0_stop_writes_trigger << "\r\n";
    string_stream << "max_bytes_for_level_base:" << cf_options.max_bytes_for_level_base << "\r\n";
    string_stream << "max_bytes_for_level_multiplier:" << cf_options.max_bytes_for_level_multiplier << "\r\n";
    string_stream << "target_file_size_base:" << cf_options.target_file_size_base << "\r\n";
    string_stream << "target_file_size_multiplier:" << cf_options.target_file_size_multiplier << "\r\n";
    string_stream << "disable_auto_compactions:" << (cf_options.disable_auto_compactions ? "true" : "false") << "\r\n";
    string_stream << "compression:" << static_cast<int>(cf_options.compression) << "\r\n";
    string_stream << "num_levels:" << cf_options.num_levels << "\r\n";
  } else if (option_name == "max_write_buffer_number") {
    string_stream << "success:get:" << db_name << "." << cf_name << "." << option_name << ":"
                  << cf_options.max_write_buffer_number << "\r\n";
  } else if (option_name == "write_buffer_size") {
    string_stream << "success:get:" << db_name << "." << cf_name << "." << option_name << ":"
                  << cf_options.write_buffer_size << "\r\n";
  } else if (option_name == "level0_file_num_compaction_trigger") {
    string_stream << "success:get:" << db_name << "." << cf_name << "." << option_name << ":"
                  << cf_options.level0_file_num_compaction_trigger << "\r\n";
  } else if (option_name == "level0_slowdown_writes_trigger") {
    string_stream << "success:get:" << db_name << "." << cf_name << "." << option_name << ":"
                  << cf_options.level0_slowdown_writes_trigger << "\r\n";
  } else if (option_name == "level0_stop_writes_trigger") {
    string_stream << "success:get:" << db_name << "." << cf_name << "." << option_name << ":"
                  << cf_options.level0_stop_writes_trigger << "\r\n";
  } else {
    string_stream << "error:unsupported_option:" << option_name << "\r\n";
    string_stream
        << "supported_options: all, max_write_buffer_number, write_buffer_size, level0_file_num_compaction_trigger, "
           "level0_slowdown_writes_trigger, level0_stop_writes_trigger\r\n";
  }

  return string_stream.str();
}

// Helper function to set CF option value
static std::string SetCFOptionValue(Server *server, const std::string &db_name, const std::string &cf_name,
                                    const std::string &option_name, const std::string &value) {
  std::ostringstream string_stream;

  auto status = server->SetDBOptionForColumnFamily(db_name, cf_name, option_name, value);
  if (status.IsOK()) {
    string_stream << "success:set:" << db_name << "." << cf_name << "." << option_name << ":" << value << "\r\n";
  } else {
    string_stream << "error:set_failed:" << status.Msg() << "\r\n";
  }

  return string_stream.str();
}

void Server::HandleCFOptionCommand(const std::string &query_params, std::string *result) {
  std::ostringstream string_stream;

  // Parse format: action:db_name:cf_name:option_name[:value]
  std::vector<std::string> parts;
  std::stringstream ss(query_params);
  std::string item;
  while (std::getline(ss, item, ':')) {
    parts.push_back(item);
  }

  if (parts.size() < 3) {
    string_stream << "error:invalid_format:expected action:db_name:cf_name[:option_name[:value]]\r\n";
    string_stream << "actions: get, set, list\r\n";
    string_stream << "example: get:slot_0_8191:metadata:max_write_buffer_number\r\n";
    string_stream << "example: set:slot_0_8191:metadata:max_write_buffer_number:8\r\n";
    string_stream << "example: list:slot_0_8191:metadata\r\n";
    *result = string_stream.str();
    return;
  }

  std::string action = parts[0];   // "get", "set", "list"
  std::string db_name = parts[1];  // DB name
  std::string cf_name = parts[2];  // CF name

  // Validate DB exists
  auto slot_ranges = cluster->LocalSlotRanges();
  auto it = slot_ranges.find(db_name);
  if (it == slot_ranges.end()) {
    string_stream << "error:db_not_found:" << db_name << "\r\n";
    *result = string_stream.str();
    return;
  }

  auto storage = it->second->GetStorage();
  auto cf_handle = storage->GetCFHandle(cf_name);
  if (!cf_handle) {
    string_stream << "error:cf_not_found:" << cf_name << ":db:" << db_name << "\r\n";
    *result = string_stream.str();
    return;
  }

  if (action == "get") {
    if (parts.size() < 4) {
      string_stream << "error:invalid_format:get requires option_name\r\n";
      string_stream << "example: get:db_name:cf_name:option_name\r\n";
      *result = string_stream.str();
      return;
    }
    std::string option_name = parts[3];
    *result = GetCFOptionValue(storage.get(), cf_handle, db_name, cf_name, option_name);
  } else if (action == "set") {
    if (parts.size() != 5) {
      string_stream << "error:invalid_format:set requires option_name and value\r\n";
      string_stream << "example: set:db_name:cf_name:option_name:value\r\n";
      *result = string_stream.str();
      return;
    }
    std::string option_name = parts[3];
    std::string value = parts[4];
    *result = SetCFOptionValue(this, db_name, cf_name, option_name, value);
  } else {
    string_stream << "error:unknown_action:" << action << "\r\n";
    string_stream << "supported_actions: get, set\r\n";
    string_stream
        << "get_options: all, max_write_buffer_number, write_buffer_size, level0_file_num_compaction_trigger, "
           "level0_slowdown_writes_trigger, level0_stop_writes_trigger\r\n";
    string_stream
        << "set_options: any valid RocksDB CF option (e.g., max_write_buffer_number, write_buffer_size, etc.)\r\n";
    *result = string_stream.str();
  }
}

// return worker id and worker start time
std::chrono::nanoseconds Server::GetWorkersBlockedDuration() const {
  auto it =
      std::min_element(worker_threads_.begin(), worker_threads_.end(), [=](auto &worker_thread1, auto &worker_thread2) {
        auto worker1 = worker_thread1->GetWorker();
        auto worker2 = worker_thread2->GetWorker();
        auto blocked_start_time1 = worker1->blocked_worker_start_block_time.load();
        auto blocked_start_time2 = worker2->blocked_worker_start_block_time.load();

        if (blocked_start_time1.count() == 0 && blocked_start_time2.count() == 0) {
          return false;
        }

        if (blocked_start_time1.count() == 0) return false;
        if (blocked_start_time2.count() == 0) return true;

        return blocked_start_time1 < blocked_start_time2;
      });
  if (it != worker_threads_.end()) {
    auto blocked_start_time = (*it)->GetWorker()->blocked_worker_start_block_time.load();
    if (blocked_start_time.count() > 0) {
      auto now = std::chrono::steady_clock::now();
      auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
      return now_ns - blocked_start_time;
    }
  }

  return std::chrono::nanoseconds(0);
}

grpc::ServerUnaryReactor *Server::CDCGetLatestPoint(grpc::CallbackServerContext *ctx,
                                                    const kv::datanode::v1::CDCGetLatestPointRequest *req,
                                                    kv::datanode::v1::CDCGetLatestPointResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slotrange_idx();
  auto slot_range_index_name = redis::SlotRangeIndexToString(slor_range_idx);
  if (!CheckClusterId(req->cluster_id())) {
    GlobalStatsInstance().IncrGetCDCPointCount(slot_range_index_name, kClusterIdMismatchError);
    reactor->Finish(kClusterIdMismatchStatus);
    return reactor;
  }
  const auto &slot_range = GetSlotRangeByIndex(slor_range_idx);
  if (!slot_range) {
    GlobalStatsInstance().IncrGetCDCPointCount(slot_range_index_name, kSlotRangeNotFoundError);
    reactor->Finish(kSlotRangeNotFoundStatus);
    return reactor;
  }
  auto ret = slot_range->GetCDCPoint();
  if (!ret.IsOK()) {
    GlobalStatsInstance().IncrGetCDCPointCount(slot_range_index_name, kUnknownError);
    reactor->Finish(UnknownSyncStatus(ret.Msg()));
    return reactor;
  }
  resp->mutable_db_point()->CopyFrom(ret.GetValue());
  GlobalStatsInstance().IncrGetCDCPointCount(slot_range_index_name, std::nullopt);
  reactor->Finish(kOkStatus);
  return reactor;
}

grpc::ServerWriteReactor<kv::datanode::v1::CDCGetEventsResponse> *Server::CDCGetEvents(
    grpc::CallbackServerContext *ctx, const kv::datanode::v1::CDCGetEventsRequest *req) {
  return new redis::CDCSender(shared_from_this(), ctx, req);
}

grpc::ServerUnaryReactor *Server::CDCGetRestartPoint(grpc::CallbackServerContext *ctx,
                                                     const kv::datanode::v1::CDCGetRestartPointRequest *req,
                                                     kv::datanode::v1::CDCGetRestartPointResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slotrange_idx();
  auto slot_range_index_name = redis::SlotRangeIndexToString(slor_range_idx);
  if (!CheckClusterId(req->cluster_id())) {
    GlobalStatsInstance().IncrGetCDCRestartPointCount(slot_range_index_name, kClusterIdMismatchError);
    reactor->Finish(kClusterIdMismatchStatus);
    return reactor;
  }
  const auto &slot_range = GetSlotRangeByIndex(slor_range_idx);
  if (!slot_range) {
    GlobalStatsInstance().IncrGetCDCRestartPointCount(slot_range_index_name, kSlotRangeNotFoundError);
    reactor->Finish(kSlotRangeNotFoundStatus);
    return reactor;
  }
  auto ret = slot_range->GetCDCRestartPoint();
  if (!ret.IsOK()) {
    GlobalStatsInstance().IncrGetCDCRestartPointCount(slot_range_index_name, kUnknownError);
    reactor->Finish(UnknownSyncStatus(ret.Msg()));
    return reactor;
  }
  resp->mutable_db_point()->CopyFrom(ret.GetValue());
  GlobalStatsInstance().IncrGetCDCRestartPointCount(slot_range_index_name, std::nullopt);
  reactor->Finish(kOkStatus);
  return reactor;
}

grpc::ServerUnaryReactor *Server::CDCGetOldestPoint(grpc::CallbackServerContext *ctx,
                                                    const kv::datanode::v1::CDCGetOldestPointRequest *req,
                                                    kv::datanode::v1::CDCGetOldestPointResponse *resp) {
  auto reactor = ctx->DefaultReactor();
  auto &slor_range_idx = req->slotrange_idx();
  auto slot_range_index_name = redis::SlotRangeIndexToString(slor_range_idx);
  if (!CheckClusterId(req->cluster_id())) {
    GlobalStatsInstance().IncrGetCDCOldestPointCount(slot_range_index_name, kClusterIdMismatchError);
    reactor->Finish(kClusterIdMismatchStatus);
    return reactor;
  }
  const auto &slot_range = GetSlotRangeByIndex(slor_range_idx);
  if (!slot_range) {
    GlobalStatsInstance().IncrGetCDCOldestPointCount(slot_range_index_name, kSlotRangeNotFoundError);
    reactor->Finish(kSlotRangeNotFoundStatus);
    return reactor;
  }
  auto ret = slot_range->GetCDCOldestPoint();
  if (!ret.IsOK()) {
    LOG(WARNING) << "GetCDCOldestPoint slot range:" << slot_range_index_name << ", error:" << ret.Msg();
    GlobalStatsInstance().IncrGetCDCOldestPointCount(slot_range_index_name, kUnknownError);
    reactor->Finish(UnknownSyncStatus(ret.Msg()));
    return reactor;
  }
  resp->mutable_db_point()->CopyFrom(ret.GetValue());
  GlobalStatsInstance().IncrGetCDCOldestPointCount(slot_range_index_name, std::nullopt);
  reactor->Finish(kOkStatus);
  return reactor;
}

grpc::ServerUnaryReactor *Server::MonitorCmd(grpc::CallbackServerContext *ctx,
                                             const kv::datanode::v1::MonitorCmdRequest *request,
                                             kv::datanode::v1::MonitorCmdResponse *response) {
  auto reactor = ctx->DefaultReactor();
  auto command = util::ToLower(request->cmd().command());
  auto sub_command = util::ToLower(request->cmd().sub_command());
  std::string result;
  std::string ns = "default";
  if (command == "info") {
    GetInfo(ns, sub_command, &result);
  } else if (command == "stats") {
    result = GetRocksDBStatsJson();
  } else if (command == "metric") {
    GetMetricInfo(ns, sub_command, &result);
  } else if (command == "dbproperty") {
    if (!is_loading_) {
      if (sub_command.empty() || sub_command == "list") {
        GetDBAndCFList(&result);
      } else {
        GetDBPropertyInfo(sub_command, &result);
      }
    } else {
      result = "# DB Property\r\nserver is loading, db property info not available\r\n";
    }
  } else if (command == "cfoption") {
    if (!is_loading_) {
      if (sub_command.empty()) {
        result = "# CF Option Command\r\n";
        result += "usage: cfoption <action:db_name:cf_name:option_name[:value]>\r\n";
        result += "actions: get, set\r\n";
        result += "examples:\r\n";
        result += "  get:slot_0_8191:metadata:all\r\n";
        result += "  get:slot_0_8191:metadata:max_write_buffer_number\r\n";
        result += "  set:slot_0_8191:metadata:max_write_buffer_number:8\r\n";
      } else {
        HandleCFOptionCommand(sub_command, &result);
      }
    } else {
      result = "# CF Option\r\nserver is loading, cf option not available\r\n";
    }
  } else {
    reactor->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "unimplemented method in datanode"));
    return reactor;
  }

  response->set_result(result);
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

Worker *Server::SelectWorkerWithLeastConnections() {
  if (worker_threads_.empty()) {
    return nullptr;
  }

  Worker *selected_worker = worker_threads_[0]->GetWorker();
  size_t min_connections = selected_worker->GetConnectionCount();

  for (size_t i = 1; i < worker_threads_.size(); ++i) {
    Worker *worker = worker_threads_[i]->GetWorker();
    size_t conn_count = worker->GetConnectionCount();
    if (conn_count < min_connections) {
      min_connections = conn_count;
      selected_worker = worker;
    }
  }

  return selected_worker;
}
