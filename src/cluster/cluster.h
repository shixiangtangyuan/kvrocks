#pragma once

#include <grpcpp/impl/status.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.pb.h>
#include <kv/datanode/v1/sync.pb.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>

#include "cluster/cluster_defs.h"
#include "common/pb_util.h"
#include "common/status.h"
#include "common/sync_status.h"
#include "common/time_util.h"
#include "storage/storage.h"

class MockServer;

namespace redis {

extern size_t sleepMSBeforeCleanData;

enum class WriteStatus {
  UNSPECIFIED = 0,
  RO,
  WR,
  // WR_PROHIBITED now only exists in the dts running state
  WR_PROHIBITED,
};

class DataNode {
 public:
  DataNode() = default;
  DataNode(std::string node_id, kv::controller::v1::Datanode::RWStatus client_rw_status,
           kv::controller::v1::Datanode::RWStatus dts_rw_status,
           kv::controller::v1::Datanode::ServingStatus serving_status, kv::controller::v1::IPAddr addr)
      : datanode_id_(std::move(node_id)),
        client_rw_status_(client_rw_status),
        dts_rw_status_(dts_rw_status),
        serving_status_(serving_status),
        addr_(std::move(addr)) {}

  std::string NodeId() const { return datanode_id_; }
  void SetNodeId(std::string node_id) { datanode_id_ = std::move(node_id); };

  const kv::controller::v1::Datanode::RWStatus &ClientRWStatus() const { return client_rw_status_; }
  void SetClientRWStatus(kv::controller::v1::Datanode::RWStatus status) { client_rw_status_ = status; }
  bool IsClientWritable() const {
    return client_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW ||
           client_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_WO;
  }
  bool IsClientReadable() const {
    return client_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW ||
           client_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO;
  }

  const kv::controller::v1::Datanode::RWStatus &DtsRWStatus() const { return dts_rw_status_; }
  void SetDtsRWStatus(kv::controller::v1::Datanode::RWStatus status) { dts_rw_status_ = status; }
  bool IsDtsWritable() const {
    return dts_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW ||
           dts_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_WO;
  }
  bool IsDtsReadable() const {
    return dts_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW ||
           dts_rw_status_ == kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO;
  }

  kv::controller::v1::Datanode::ServingStatus ServingStatus() const { return serving_status_; }
  void SetServingStatus(kv::controller::v1::Datanode::ServingStatus serving_status) {
    serving_status_ = serving_status;
  }
  bool IsServing() const { return serving_status_ == kv::controller::v1::Datanode::SERVING_STATUS_SERVING; }
  bool IsImporting() const { return serving_status_ == kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING; }
  std::string ServingStatusStr() const { return kv::controller::v1::Datanode_ServingStatus_Name(serving_status_); }

  kv::controller::v1::IPAddr Addr() const { return addr_; }
  void SetAddr(kv::controller::v1::IPAddr addr) { addr_ = std::move(addr); }

  void SetImportingId(std::set<std::string> id) { importing_id_ = std::move(id); }
  std::set<std::string> GetImportingId() const { return importing_id_; }
  friend std::ostringstream &operator<<(std::ostringstream &os, const DataNode &node) {
    os << node.datanode_id_ << " " << node.addr_ << " " << node.serving_status_ << " ";
    os << node.client_rw_status_ << " " << node.dts_rw_status_ << " ";
    if (node.importing_id_.empty()) {
      os << "none";
      return os;
    }
    for (auto iter = node.importing_id_.begin(); iter != node.importing_id_.end(); ++iter) {
      if (iter != node.importing_id_.begin()) {
        os << ",";
      }
      os << *iter;
    }
    return os;
  }

 public:
  // Datanode valid (role,client_rw_status,dts_rw_status) table
  using DataNodeStatusTuple =
      std::tuple<kv::controller::v1::Datanode::ServingStatus, kv::controller::v1::Datanode::RWStatus,
                 kv::controller::v1::Datanode::RWStatus>;
  static std::set<DataNodeStatusTuple> active_pool_valid_topo_status;
  static std::set<DataNodeStatusTuple> standby_pool_valid_topo_status;
  static bool IsValidTopoStatus(bool is_active_pool, kv::controller::v1::Datanode::ServingStatus serving_status,
                                kv::controller::v1::Datanode::RWStatus client_rw_status,
                                kv::controller::v1::Datanode::RWStatus dts_rw_status);

 private:
  // NOTE(mingfo): It OK to access DataNode's private members. Because all operations on DataNode members
  // are protected by the Cluster lock.
  std::string datanode_id_;
  kv::controller::v1::Datanode::RWStatus client_rw_status_{kv::controller::v1::Datanode_RWStatus_RW_STATUS_UNSPECIFIED};
  kv::controller::v1::Datanode::RWStatus dts_rw_status_{kv::controller::v1::Datanode_RWStatus_RW_STATUS_UNSPECIFIED};
  kv::controller::v1::Datanode::ServingStatus serving_status_{kv::controller::v1::Datanode::SERVING_STATUS_UNSPECIFIED};
  kv::controller::v1::IPAddr addr_;
  // NOTE(mingfo): Currently, there is only one importing, because SyncManager only support to replicate to one
  // importing. If multiple importings will be used in the future, SyncManager should be adjusted too.
  std::set<std::string> importing_id_;
};

class SlotRange {
 public:
  struct SlotRangeInfo {
    SlotRangeInfo() = delete;
    SlotRangeInfo(const SlotRangeInfo &info)
        : serving_node_id(info.serving_node_id), start(info.start), end(info.end), name(info.name) {}
    SlotRangeInfo(SlotRangeInfo &&info)
        : serving_node_id(std::move(info.serving_node_id)),
          start(std::move(info.start)),
          end(std::move(info.end)),
          name(std::move(info.name)) {}
    SlotRangeInfo(const std::string &node_id, int16_t st, int16_t ed) : serving_node_id(node_id), start(st), end(ed) {
      name = std::move(CreateSlotRangeName(start, end));
    }
    ~SlotRangeInfo() {}

    // Note(mingfo): Regardless of the current node's role, only the serving_node_id is saved here.
    // To access the current node object, you can directly access "my_datanode_" of Cluster.
    std::string serving_node_id;
    int16_t start;
    int16_t end;
    std::string name;
  };

  SlotRange(const std::string &node_id, int16_t start, int16_t end, std::shared_ptr<engine::Storage> storage)
      : info_(node_id, start, end), storage_(std::move(storage)) {}
  SlotRange(const SlotRangeInfo &info, std::shared_ptr<engine::Storage> storage)
      : info_(info), storage_(std::move(storage)) {}
  SlotRange(SlotRangeInfo &&info, std::shared_ptr<engine::Storage> storage)
      : info_(std::move(info)), storage_(std::move(storage)) {}

  ~SlotRange() = default;
  SlotRange(const SlotRange &) = delete;
  SlotRange operator=(const SlotRange &) = delete;
  SlotRange(SlotRange &&) = delete;
  SlotRange operator=(SlotRange &&) = delete;

  int16_t GetRangeStart() const { return info_.start; }
  int16_t GetRangeEnd() const { return info_.end; }
  void SetRangeStart(int16_t start) {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    info_.start = start;
    info_.name = std::move(CreateSlotRangeName(info_.start, info_.end));
  }
  void SetRangeEnd(int16_t end) {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    info_.end = end;
    info_.name = std::move(CreateSlotRangeName(info_.start, info_.end));
  }
  const std::string &GetName() { return info_.name; }
  const std::string &GetServingNodeId() { return info_.serving_node_id; }
  void SetServingNodeId(const std::string &node_id) { info_.serving_node_id = node_id; }
  SlotRangeInfo &GetInfo() { return info_; }

  std::shared_ptr<engine::Storage> GetStorage() { return storage_; }

  kv::controller::v1::SlotRange::HealthyStatus HealthyStatus() const {
    std::shared_lock<std::shared_mutex> lk(shared_mutex_);
    return healthy_status_;
  }
  void SetHealthyStatus(kv::controller::v1::SlotRange::HealthyStatus status) {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    healthy_status_ = status;
  }

  kv::controller::v1::SlotRange::ReplicationStatus ReplicationStatus() const {
    std::shared_lock<std::shared_mutex> lk(shared_mutex_);
    return replication_status_;
  }
  void SetReplicationStauts(kv::controller::v1::SlotRange::ReplicationStatus status) {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    replication_status_ = status;
  };

  int64_t SetClientWriteRunningStatus(WriteStatus status);

  int64_t SetClientWriteRunningStatus(WriteStatus status, uint64_t &version, bool check_version);

  WriteStatus GetClientRunningStatus() {
    std::shared_lock<std::shared_mutex> lk(shared_mutex_);
    return client_write_running_status_;
  }

  WriteStatus GetDtsWriteRunningStatus() const {
    std::shared_lock<std::shared_mutex> lk(shared_mutex_);
    return dts_write_running_status_;
  }

  std::pair<WriteStatus, WriteStatus> GetRunningStatus() const {
    std::shared_lock<std::shared_mutex> lk(shared_mutex_);
    return {client_write_running_status_, dts_write_running_status_};
  }

  void SetDtsWriteRunningStatus(WriteStatus status);

  void SetDtsWriteRunningStatus(WriteStatus status, uint64_t &version, bool check_version);

  OptionalSyncError CanSyncReceiveDataCrossPool(const std::string &) const;

  OptionalSyncError CanSyncSendDataCrossPool(const std::string &) const;

  StatusOr<kv::datanode::v1::SyncPoint> GetSyncPoint();

  StatusOr<kv::datanode::v1::SyncPoint> GetSyncPoint(rocksdb::SequenceNumber seq_id);

  StatusOr<kv::datanode::v1::CDCPoint> GetCDCPoint();

  StatusOr<kv::datanode::v1::CDCPoint> GetCDCPoint(rocksdb::SequenceNumber seq_id);

  StatusOr<kv::datanode::v1::CDCPoint> GetCDCRestartPoint();
  StatusOr<kv::datanode::v1::CDCPoint> GetCDCOldestPoint();

 private:
  static constexpr auto kNoStopWrteTimepoint = std::chrono::steady_clock::time_point::max();

  mutable std::shared_mutex shared_mutex_;
  SlotRangeInfo info_;
  std::shared_ptr<engine::Storage> storage_;
  kv::controller::v1::SlotRange::HealthyStatus healthy_status_{kv::controller::v1::SlotRange::HEALTHY_STATUS_OK};

  kv::controller::v1::SlotRange::ReplicationStatus replication_status_{
      kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED};

  std::chrono::steady_clock::time_point stop_write_time_point_ = kNoStopWrteTimepoint;
  WriteStatus client_write_running_status_{WriteStatus::UNSPECIFIED};
  uint64_t client_write_running_status_version_ = 0;
  WriteStatus dts_write_running_status_{WriteStatus::UNSPECIFIED};
  uint64_t dts_write_running_status_version_ = 0;
};

class Cluster {
 public:
  Cluster(Server *srv, std::string local_addr, std::string local_host);
  ~Cluster() = default;

  Cluster(const Cluster &) = delete;
  Cluster operator=(Cluster &) = delete;
  Cluster(Cluster &&) = delete;
  Cluster operator=(Cluster &&) = delete;

  Status SetTopo(const kv::controller::v1::ReportDataNodeResponse &);

  std::string GetSlotRangeNameByKey(const std::string &key);

  std::string GetSlotRangeNameBySlotId(int16_t slot_id);

  std::shared_ptr<SlotRange> GetSlotRangeByKey(const std::string &key);

  int16_t GetSlotRangeEndBySlot(int16_t slot);

  StatusOr<std::shared_ptr<engine::Storage>> CanExecByMySelf(uint64_t cmd_flags, const std::string &slot_range_name,
                                                             const std::string &key, int16_t slot = -1,
                                                             bool by_slot = false);

  bool TopoHasInited() { return version_.load() > 0; }

  void InitControllerRpcRequest(kv::controller::v1::ReportDataNodeRequest &, bool is_blocked);

  static std::string GetLocalIPAddr(const std::string &addr_name);

  uint64_t Version() { return version_.load(); }
  void IncrSetTopoOkCount() { set_topo_ok_count_.fetch_add(1); }
  void IncrSetTopoErrCount() { set_topo_err_count_.fetch_add(1); }
  uint64_t SetTopoOkCount() { return set_topo_ok_count_.load(); }
  uint64_t SetTopoErrCount() { return set_topo_err_count_.load(); }
  // immutable elements
  const std::string &ClusterId() { return cluster_id_; };
  const std::string &Pool() { return pool_; };
  const std::string &DatanodeId() { return datanode_id_; };

  std::map<std::string, std::shared_ptr<SlotRange>> LocalSlotRanges() const {
    std::shared_lock<std::shared_mutex> lock(shared_mutex_);
    return my_slot_ranges_;
  }

  std::tuple<std::string, std::shared_ptr<engine::Storage>> GetOneSlotRangeAndStorage() {
    std::shared_lock<std::shared_mutex> lock(shared_mutex_);
    return {my_slot_ranges_.begin()->first, my_slot_ranges_.begin()->second->GetStorage()};
  }

  std::set<std::string> GetAllLocalSlotRangeNames();

  Status CanScriptExecbyMyself();

  Status CheckAndGetSlotRangesServedByMySelf(
      const std::set<std::string> &slot_range_names,
      std::unordered_map<std::string, std::shared_ptr<SlotRange>> *slot_ranges = nullptr);

  // get local slot range by index
  std::shared_ptr<SlotRange> GetSlotRangeByIndex(const kv::controller::v1::SlotRangeIndex &);

  // get local slot range by index
  std::shared_ptr<SlotRange> GetSlotRangeByIndex(uint16_t start, uint16_t end);

  Status SetClientWriteRunningStatusWrite();

  void ClearWriteRunningStatus() {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);

    ClearWriteRunningStatusLocked();
  }

  void ClearWriteRunningStatusLocked();

  void ClearReplicationStatus() {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    for (auto &sl : my_slot_ranges_) {
      sl.second->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
    }
  }

  OptionalSyncError CanSyncSendData(const std::string &node_id, bool across_pool);
  OptionalSyncError CanSyncReceiveDataCrossPool(const std::string &node_id);
  OptionalSyncError CanSyncPullDataSamePool(const std::string &) const;

  OptionalSyncError CanSendCDCData();

  StatusOr<kv::controller::v1::IPAddr> GetDatanodeAddr(const std::string &node_id);
  StatusOr<std::string> GetDatanodeGrpcAddr(const std::string &node_id);

  // TODO: better to put this func to class failover
  kv::controller::v1::Datanode::MigrationResult SlotRangesReplicationResult();

  std::shared_ptr<DataNode> GetDatanodeById(const std::string &node_id) {
    auto it = datanodes_.find(node_id);
    if (it == datanodes_.end()) return nullptr;
    return it->second;
  }

  bool IsActivePool() { return is_active_pool_.load(); }

  static Status ShiftStorageReplId(std::map<std::string, std::shared_ptr<SlotRange>> &my_slot_ranges);

  StatusOr<std::shared_ptr<SlotRange>> GetSlotRangeBySlotId(int slot_id);

  StatusOr<std::pair<uint64_t, std::vector<std::string>>> GetSlotRangesInfo();

  StatusOr<std::pair<uint64_t, std::string>> GetSlotRangeInfo(int slot_id);

  StatusOr<std::pair<uint64_t, std::vector<std::string>>> GetDatanodesInfo();

  StatusOr<std::pair<uint64_t, std::string>> GetDatanodeInfo(const std::string &node_id);

  Status CheckKeyInSlotRange(const std::string &slot_range, const std::string &key);

 private:
  friend class ::MockServer;
  FRIEND_TEST(SyncSender, Dts);
  FRIEND_TEST(ClusterTest, SetTopoSimple);
  FRIEND_TEST(ClusterTest, SetTopoDatanodeError);
  FRIEND_TEST(ClusterTest, CanExecByMySelfOnMaster);

  std::unordered_map<std::string, std::ostringstream> getSlotRangesDescLocked();

  std::string getSlotRangesDescLocked(const std::string &node_id);

  Status checkTopo(const kv::controller::v1::ReportDataNodeResponse &resp, uint64_t *new_version, bool *is_active_pool);

  Status parseTopo(const kv::controller::v1::Cluster &cluster,
                   std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>> *all_route_slot_ranges,
                   std::map<std::string, std::shared_ptr<SlotRange>> *my_slot_ranges,
                   std::unordered_map<std::string, std::shared_ptr<DataNode>> *datanodes,
                   std::shared_ptr<DataNode> *my_datanode);

  Status applyTopo(bool is_active_pool, uint64_t new_topo_version,
                   std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>> &all_route_slot_ranges,
                   std::map<std::string, std::shared_ptr<SlotRange>> &my_slot_ranges,
                   std::unordered_map<std::string, std::shared_ptr<DataNode>> &datanodes,
                   std::shared_ptr<DataNode> &my_datanode);

  Status checkMySlotRanges(std::map<std::string, std::shared_ptr<SlotRange>> &my_slot_ranges);

  Status applyFirstTopo(bool is_active_pool, uint64_t new_topo_version,
                        std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>> &all_route_slot_ranges,
                        std::map<std::string, std::shared_ptr<SlotRange>> &my_slot_ranges,
                        std::unordered_map<std::string, std::shared_ptr<DataNode>> &datanodes,
                        std::shared_ptr<DataNode> &my_datanode);

  Status updateExistingTopo(bool is_active_pool, uint64_t new_topo_version,
                            std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>> &all_route_slot_ranges,
                            std::map<std::string, std::shared_ptr<SlotRange>> &my_slot_ranges,
                            std::unordered_map<std::string, std::shared_ptr<DataNode>> &datanodes,
                            std::shared_ptr<DataNode> &my_datanode);

  Status checkLocalNodeNewTopoStatus(bool is_active_pool, std::shared_ptr<DataNode> &new_node);
  Status doLocalNodeTopoApply(std::shared_ptr<DataNode> &new_node, bool is_active_pool,
                              std::map<std::string, std::shared_ptr<SlotRange>> &my_slot_ranges,
                              bool &need_clean_running_status);

  static Status resetDisableAutoCompactionsOption(std::map<std::string, std::shared_ptr<SlotRange>> &slot_ranges);

  mutable std::shared_mutex shared_mutex_;
  Server *srv_;

  // Cluster info
  std::atomic<uint64_t> version_{0};
  std::string cluster_id_;
  std::string pool_;
  std::string datanode_id_;
  std::string local_addr_;
  std::string local_host_;

  // Datanode info
  // object of current datanode
  std::shared_ptr<DataNode> my_datanode_ = std::make_shared<DataNode>();
  // all datanode objects including importing/unspecified nodes
  std::unordered_map<std::string, std::shared_ptr<DataNode>> datanodes_;

  // Slotrange info
  // Note(mingfo): Create SlotRange objects only for these belonging to myself in topo.
  std::map<std::string, std::shared_ptr<SlotRange>> my_slot_ranges_;
  // Note(mingfo): In routing table, store the slotrange info of serving only.
  std::shared_ptr<SlotRange::SlotRangeInfo> route_slot_ranges_[kClusterSlots];

  std::atomic<bool> is_active_pool_{false};

  // statistcs
  std::atomic<uint64_t> set_topo_ok_count_{0};
  std::atomic<uint64_t> set_topo_err_count_{0};
};

}  // namespace redis
