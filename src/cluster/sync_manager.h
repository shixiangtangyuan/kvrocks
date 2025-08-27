#pragma once

#include <gtest/gtest.h>
#include <kv/datanode/v1/common.pb.h>
#include <kv/datanode/v1/sync.pb.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"

class Server;

namespace redis {

class SlotRange;
class SyncReceiver;
class SyncPuller;
class DtsSender;
class ReplSender;

using SlotRangeReplicationDoneCB = std::function<void()>;
using SlotRangeWriteStoppableCB = std::function<void(const std::string&)>;

class SyncManager {
 public:
  SyncManager();
  ~SyncManager();
  SyncManager(const SyncManager&) = delete;
  SyncManager(SyncManager&&) = delete;
  SyncManager operator=(const SyncManager&) = delete;

  // In migration process, slave should work as a client to pull data from master.
  // Migration object invokes this func to create client and build streams to receive data.
  // Invoked by: Migration function
  Status CreateReplPuller(const std::string& cluster_id, const std::string& puller_node_id,
                          const std::string& sender_node_id, const std::string& sender_addr,
                          const std::shared_ptr<Server>& srv,
                          std::unordered_map<std::string, std::shared_ptr<SlotRange>>& slot_ranges,
                          SlotRangeWriteStoppableCB write_stoppable_cb, SlotRangeReplicationDoneCB callback);
  // Invoked by: grpc interface PullSyncData
  Status AddReplSender(const std::string& slot_range, ReplSender* repl_sender);
  // Invoked by: grpc interface PullSyncData
  Status AddDtsSender(const std::string& slot_range, DtsSender* dts_sender);
  // Invoked by: grpc interface PushSyncData
  Status AddDtsReceiver(const std::string& slot_range, SyncReceiver* dts_receiver);

  const std::unordered_map<std::string, SyncPuller*>& GetAllReplPuller() {
    std::lock_guard<std::mutex> guard(mu_);
    return repl_puller_;
  }

  // When add puller/receiver failed, object should be cleared.
  // However, since it does not yet exist in the SyncManager, the Remove interface cannot be used.
  void StopAndJoinReplPuller(SyncPuller* repl_puller, const std::optional<kv::datanode::v1::Error>& error);
  void StopAndJoinDtsReceiver(const std::string& slot_range, const std::optional<kv::datanode::v1::Error>& error);

  void StopAndJoinDtsSender(const std::string& slot_range, const std::optional<kv::datanode::v1::Error>& error);
  // If slave closed the replication stream, master should passively remove sender of the stream.
  // Mark the stream finished, and remove from syncmanager directly.
  // Invoked by: grpc actor SyncSender
  void RemoveReplSender(const std::string& slot_range, ReplSender* repl_sender,
                        const std::optional<kv::datanode::v1::Error>& error);
  // Invoked by: none
  void RemoveReplPuller(const std::string& slot_range, SyncPuller* repl_puller,
                        const std::optional<kv::datanode::v1::Error>& error);
  // Internal error occurred in grpc stream or stream is closed by peer
  // Invoked by: grpc actor SyncSender
  void RemoveDtsSender(const std::string& slot_range, DtsSender* dts_sender,
                       const std::optional<kv::datanode::v1::Error>& error);
  // 1. Internal error occurred in grpc stream or stream is closed by peer
  // Invoked by: grpc actor SyncReceiver
  // 2. Running status change: master stop writting to replicate last incremental data. Clear dts receiver to guarantee
  // no more incremetal data. Invoked by: SlotRange
  void RemoveDtsReceiver(const std::string& slot_range, SyncReceiver* dts_receiver,
                         const std::optional<kv::datanode::v1::Error>& error);

  // If new topo gotten during migration, replication tasks has to be stopped and cleared, in any following scenario
  // 1. Any changes of role,client_rw_status,dts_rw_status
  // 2. only slave is removed in new topo
  // 3. Any change of slot ranges (splitted/migrated...)
  // Invoked by: Cluster
  void ClearAllReplSenders(const std::optional<kv::datanode::v1::Error>& error, bool need_lock = true);
  // If new topo gotten during migration, stop and clear all replicaiton pullers
  // Invoked by: Cluster
  void ClearAllReplPullers(const std::optional<kv::datanode::v1::Error>& error, bool need_lock = true);
  // If any slot-range's replication task failed, all replicaiton task should be stop
  // Invoked by: Migration
  void StopAndJoinAllReplPullers(const std::optional<kv::datanode::v1::Error>& error);
  // If new topo gotten, role/client_rw_status/dts_rw_status changed, or slot-range changed of current datanode, clear
  // all dts senders.
  // Invoked by: Cluster
  void ClearAllDtsSenders(const std::optional<kv::datanode::v1::Error>& error, bool need_lock = true);
  // Topo change: role/client_rw_status/dts_rw_status changed, or slot-range changed of current datanode
  // Invoked by: Cluster
  void ClearAllDtsReceivers(const std::optional<kv::datanode::v1::Error>& error, bool need_lock = true);

  void ClearAll(const std::optional<kv::datanode::v1::Error>& error);

  void ClearAllOfOnSlotRange(const std::string& slot_range, const std::optional<kv::datanode::v1::Error>& error);

  void SetIsTopoUpdating(bool is_topo_updating) {
    std::lock_guard<std::mutex> guard(mu_);
    is_topo_updating_ = is_topo_updating;
  }

  void StopSrcWriteOfAllReplPuller();

 protected:
  Status addReplPuller(const std::string& slot_range, SyncPuller* repl_puller);

 private:
  FRIEND_TEST(Sync, RPC);

  ReplSender* getReplSender(const std::string& slot_range_name) {
    std::unique_lock<std::mutex> lk(mu_);
    auto iter = repl_sender_.find(slot_range_name);
    if (iter == repl_sender_.end()) {
      return nullptr;
    }
    return iter->second;
  }

  std::mutex mu_;
  std::unordered_map<std::string, ReplSender*> repl_sender_;
  std::unordered_map<std::string, SyncPuller*> repl_puller_;
  std::unordered_map<std::string, DtsSender*> dts_sender_;
  std::unordered_map<std::string, SyncReceiver*> dts_receiver_;
  // To prevent adding new sync objects during topo updating
  bool is_topo_updating_{false};
};

}  // namespace redis
