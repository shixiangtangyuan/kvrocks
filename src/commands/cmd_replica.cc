#include "commands/cmd_replica.h"

#include "commands/error_constants.h"
#include "server/server.h"
#include "sync/sync_sender.h"

namespace redis {

Status CommandReplica::Parse(const std::vector<std::string> &args) {
  subcommand_ = util::ToLower(args[1]);

  if (subcommand_ == "getpoint") {
    if (args.size() > 4) {
      return {Status::RedisParseErr, "Invalid arguments: at most one slot id and log id"};
    }
    auto ret = ParseInt<int>(args[2], {0, kClusterSlots - 1});
    if (!ret) {
      return {Status::RedisParseErr, "Invalid slot id: " + args[2]};
    }
    slot_id_ = *ret;
    if (args.size() == 4) {
      auto ret = ParseInt<int64_t>(args[3]);
      if (!ret) {
        return {Status::RedisParseErr, "Invalid log id: " + args[3]};
      }
      if (*ret < 0) {
        return {Status::RedisParseErr, "Invalid log id: shound not be positive"};
      }
      log_id_ = *ret;
    }
    return Status::OK();
  }

  return {Status::RedisParseErr, "Invalid subcommand: REPLICA GETPOINT"};
}

Status CommandReplica::Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) {
  if (!srv->GetConfig()->cluster_enabled) {
    return {Status::RedisExecErr, "Cluster mode is not enabled"};
  }

  if (!conn->IsAdmin()) {
    return {Status::RedisExecErr, errAdminPermissionRequired};
  }

  if (subcommand_ == "getpoint") {
    auto ret = srv->cluster->GetSlotRangeBySlotId(slot_id_);
    if (!ret.IsOK()) return ret.ToStatus();
    kv::datanode::v1::SyncPoint sync_point;
    auto slot_range = std::move(ret.GetValue());
    if (log_id_ < 0) {
      auto ret = slot_range->GetSyncPoint();
      if (!ret.IsOK()) {
        return {Status::RedisExecErr, ret.Msg()};
      }
      sync_point = ret.GetValue();
    } else if (log_id_ == 0) {
      using Req = kv::datanode::v1::SyncDataRequest;
      using Resp = kv::datanode::v1::SyncDataResponse;
      auto err = SyncSender<Req, Resp>::CheckWALBoundary(slot_range, 1);
      if (err.has_value()) return {Status::RedisExecErr, err->message()};
      sync_point.set_next_seq_id(1);
    } else {
      auto ret = slot_range->GetSyncPoint(log_id_);
      if (!ret.IsOK()) return ret.ToStatus();
      sync_point = std::move(ret.GetValue());
    }
    output->append(redis::MultiLen(4));
    output->append(redis::BulkString(slot_range->GetName()));
    output->append(redis::Integer(sync_point.next_seq_id()));
    output->append(redis::Integer(sync_point.prev_log_ts()));
    output->append(redis::BulkString(sync_point.prev_rep_id()));
    return Status::OK();
  }

  return {Status::RedisExecErr, "Invalid subcommand: REPLICA GETPOINT"};
}

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandReplica>("replica", -3, "replication no-script", 0, 0, 0))

}  // namespace redis
