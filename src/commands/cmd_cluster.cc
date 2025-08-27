#include "commands/cmd_cluster.h"

#include "cluster/cluster.h"
#include "cluster/cluster_defs.h"
#include "commands/error_constants.h"
#include "common/status.h"
#include "server/server.h"

namespace redis {

Status CommandCluster::Parse(const std::vector<std::string> &args) {
  subcommand_ = util::ToLower(args[1]);

  if (subcommand_ == "keyslot") {
    if (args.size() != 3) {
      return {Status::RedisParseErr, "Invalid key: need one key"};
    }
    key_ = args[2];
    return Status::OK();
  }
  if (subcommand_ == "slotrange") {
    if (args.size() > 3) {
      return {Status::RedisParseErr, "Invalid slot id: at most one slot id"};
    }
    if (args.size() == 3) {
      auto slot_id = ParseInt<int>(args[2], {0, kClusterSlots - 1}, 10);
      if (!slot_id) {
        return {Status::RedisParseErr, "Invalid slot id: " + args[2]};
      }
      slot_id_ = slot_id.GetValue();
    }
    return Status::OK();
  }
  if (subcommand_ == "datanode") {
    if (args.size() > 3) {
      return {Status::RedisParseErr, "Invalid node id: at most one node id"};
    }
    if (args.size() == 3) {
      node_id_ = args[2];
    }
    return Status::OK();
  }

  return {Status::RedisParseErr, "Invalid subcommand: CLUSTER KEYSLOT|SLOTRANGE|DATANODE"};
}

Status CommandCluster::Execute(Server *srv, Connection *conn, std::string *output, engine::Storage *storage) {
  if (!srv->GetConfig()->cluster_enabled) {
    return {Status::RedisExecErr, "Cluster mode is not enabled"};
  }

  if (!conn->IsAdmin()) {
    return {Status::RedisExecErr, errAdminPermissionRequired};
  }

  if (subcommand_ == "keyslot") {
    *output = redis::Integer(GetSlotIdFromKey(key_));
    return Status::OK();
  }
  if (subcommand_ == "slotrange") {
    if (slot_id_ < 0) {
      auto ret = srv->cluster->GetSlotRangesInfo();
      if (!ret.IsOK()) {
        return ret.ToStatus();
      }
      output->append(redis::MultiLen(2));
      output->append(redis::Integer(ret->first));
      output->append(redis::MultiLen(ret->second.size()));
      for (auto &slot_range : ret->second) {
        output->append(redis::BulkString(slot_range));
      }
      return Status::OK();
    }
    auto ret = srv->cluster->GetSlotRangeInfo(slot_id_);
    if (!ret.IsOK()) {
      return ret.ToStatus();
    }
    output->append(redis::MultiLen(2));
    output->append(redis::Integer(ret->first));
    output->append(redis::BulkString(ret->second));
    return Status::OK();
  }
  if (subcommand_ == "datanode") {
    if (node_id_.empty()) {
      auto ret = srv->cluster->GetDatanodesInfo();
      if (!ret.IsOK()) {
        return ret.ToStatus();
      }
      std::sort(ret->second.begin(), ret->second.end(), [](std::string &i, std::string &j) { return i < j; });
      output->append(redis::MultiLen(2));
      output->append(redis::Integer(ret->first));
      output->append(redis::MultiLen(ret->second.size()));
      for (auto &datanode : ret->second) {
        output->append(redis::BulkString(datanode));
      }
      return Status::OK();
    }
    auto ret = srv->cluster->GetDatanodeInfo(node_id_);
    if (!ret.IsOK()) {
      return ret.ToStatus();
    }
    output->append(redis::MultiLen(2));
    output->append(redis::Integer(ret->first));
    output->append(redis::BulkString(ret->second));
    return Status::OK();
  }

  return {Status::RedisExecErr, "Invalid subcommand: CLUSTER KEYSLOT|SLOTRANGE|DATANODE"};
}

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandCluster>("cluster", -2, "cluster no-script", 0, 0, 0))

}  // namespace redis
