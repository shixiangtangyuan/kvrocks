#include "cluster.h"

#include <arpa/inet.h>
#include <ifaddrs.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <unordered_map>

#include "cluster/cluster_defs.h"
#include "common/pb_util.h"
#include "common/scope_exit.h"
#include "common/sync_status.h"
#include "lock/lock.h"
#include "server/server.h"
#include "status.h"
#include "storage/redis_metadata.h"

#define CLS_INFO LOG(INFO) << "[cluster] "
#define CLS_WARNING LOG(WARNING) << "[cluster] "
#define CLS_ERROR LOG(ERROR) << "[cluster] "
#define CLS_FATAL LOG(FATAL) << "[cluster] "

namespace redis {
// sleep for a while before lock the stale slot range and clean data
// to avoid hlod the lock for a long time when db write stalled even
// write stopped, this may block the requests which require the lock
// of the stale slot range too
size_t sleepMSBeforeCleanData = 60000;

inline std::ostream& operator<<(std::ostream& os, const WriteStatus& status) {
  switch (status) {
    case WriteStatus::UNSPECIFIED:
      os << "unspecified";
      break;
    case WriteStatus::RO:
      os << "read-only";
      break;
    case WriteStatus::WR:
      os << "read-write";
      break;
    // WR_PROHIBITED now only exists in the dts running state
    case WriteStatus::WR_PROHIBITED:
      os << "read-write-prohibited";
      break;
    default:
      os << "unknown";
      break;
  }
  return os;
}

// NOTE(mingfo): Valid statuses refer to Doc: https://confluence.shopee.io/pages/viewpage.action?pageId=2220112706
std::set<DataNode::DataNodeStatusTuple> DataNode::active_pool_valid_topo_status = {
    {kv::controller::v1::Datanode::SERVING_STATUS_SERVING, kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW,
     kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO},  // serving
    {kv::controller::v1::Datanode::SERVING_STATUS_SERVING, kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO,
     kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO},  // serving for HA
    {kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING, kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO,
     kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO},  // importing
    {kv::controller::v1::Datanode::SERVING_STATUS_UNSPECIFIED, kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO,
     kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO}  // unspecified
};

std::set<DataNode::DataNodeStatusTuple> DataNode::standby_pool_valid_topo_status = {
    {kv::controller::v1::Datanode::SERVING_STATUS_SERVING, kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO,
     kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW},  // serving
    {kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING, kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO,
     kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO},  // importing
    {kv::controller::v1::Datanode::SERVING_STATUS_UNSPECIFIED, kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO,
     kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO}  // unspecified
};

bool DataNode::IsValidTopoStatus(bool is_active_pool, kv::controller::v1::Datanode::ServingStatus serving_status,
                                 kv::controller::v1::Datanode::RWStatus client_rw_status,
                                 kv::controller::v1::Datanode::RWStatus dts_rw_status) {
  auto target = std::tie(serving_status, client_rw_status, dts_rw_status);
  // active pool
  if (is_active_pool) {
    return std::any_of(DataNode::active_pool_valid_topo_status.begin(), DataNode::active_pool_valid_topo_status.end(),
                       [&target](const DataNodeStatusTuple& datanode_status) { return target == datanode_status; });
  }
  // standby pool
  return std::any_of(DataNode::standby_pool_valid_topo_status.begin(), DataNode::standby_pool_valid_topo_status.end(),
                     [&target](const DataNodeStatusTuple& datanode_status) { return target == datanode_status; });
}

Cluster::Cluster(Server* srv, std::string local_addr, std::string local_host)
    : srv_(srv),
      cluster_id_(srv_->GetConfig()->cluster_id),
      pool_(srv_->GetConfig()->pool),
      datanode_id_(srv_->GetConfig()->datanode_id),
      local_addr_(std::move(local_addr)),
      local_host_(std::move(local_host)) {}

std::string Cluster::GetSlotRangeNameByKey(const std::string& key) {
  auto slot = GetSlotIdFromKey(key);
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  return route_slot_ranges_[slot]->name;
}

std::string Cluster::GetSlotRangeNameBySlotId(int16_t slot) {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  return route_slot_ranges_[slot]->name;
}

std::shared_ptr<SlotRange> Cluster::GetSlotRangeByKey(const std::string& key) {
  auto slot = GetSlotIdFromKey(key);
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto sr_name = route_slot_ranges_[slot]->name;
  auto iter = my_slot_ranges_.find(sr_name);
  if (iter == my_slot_ranges_.end()) {
    return nullptr;
  }
  return iter->second;
}

int16_t Cluster::GetSlotRangeEndBySlot(int16_t slot) {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  return route_slot_ranges_[slot]->end;
}

// 1. Check slotragne belongs to myself
// 2. Check running status of slotrange
// 3. Check datanode serving status
// 4. Check datanode client_rw_status
StatusOr<std::shared_ptr<engine::Storage>> Cluster::CanExecByMySelf(uint64_t cmd_flags,
                                                                    const std::string& slot_range_name,
                                                                    const std::string& key, int16_t slot_id,
                                                                    bool by_slot) {
  int16_t slot = -1;
  if (by_slot) {
    if (slot_id < 0 || slot_id > kClusterSlots) {
      return Status{Status::NotOK, fmt::format("Wrong target slot {}", slot_id)};
    }
    slot = slot_id;
  } else {
    slot = static_cast<int16_t>(GetSlotIdFromKey(key));
  }
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  // Check slotrange belongs to me
  auto sr_it = my_slot_ranges_.find(slot_range_name);
  if (sr_it == my_slot_ranges_.end()) {
    auto serving_node = GetDatanodeById(route_slot_ranges_[slot]->serving_node_id);
    if (serving_node == nullptr) {
      CLS_ERROR << "Failed to find serving datanode: " << route_slot_ranges_[slot]->serving_node_id << " of "
                << slot_range_name;
      return Status{Status::NotOK, fmt::format("Serving datanode not found for {}, nodeid: {}", slot_range_name,
                                               route_slot_ranges_[slot]->serving_node_id)};
    }
    std::stringstream stream;
    stream << "MOVED slot: " << slot << " slotrange: " << slot_range_name;
    if (!by_slot) {
      stream << " for key: " << key;
    }
    stream << fmt::format(" to {}:{}", serving_node->Addr().ip(), serving_node->Addr().port());
    CLS_INFO << stream.str();
    return Status{Status::ClusterRetry,
                  fmt::format("MOVED {} {}:{}", slot, serving_node->Addr().ip(), serving_node->Addr().port())};
  }

  auto slot_range = sr_it->second;
  CHECK(my_datanode_ != nullptr) << "Datanode is null, slotrange: " << slot_range_name;
  // Check running status of slotrange
  // NOTE(mingfo): The priority of slotrange running status is higher than datanode client_rw_status.
  auto running_status = slot_range->GetClientRunningStatus();
  // importing datanode opened writing after replication in migration
  if (running_status == WriteStatus::WR && IsActivePool()) return slot_range->GetStorage();
  // serving datanode stopped writing after replication in migration
  if ((cmd_flags & redis::kCmdWrite) && running_status == WriteStatus::RO) {
    CLS_INFO << "Retry" << slot_range_name << " write stopped";
    return Status{Status::ClusterRetryWriteStopped, fmt::format("Retry {} write stop", slot_range_name)};
  }

  // Check datanode serving status
  // Currently, both read and write requests are only processed on serving
  // Move requests from importing/unspecified node to serving node
  if (!my_datanode_->IsServing()) {
    std::stringstream stream;
    stream << "MOVED slot: " << slot << " slotrange: " << slot_range_name;
    if (!by_slot) {
      stream << " for key: " << key;
    }
    stream << " from " << kv::controller::v1::Datanode_ServingStatus_Name(my_datanode_->ServingStatus())
           << " datanode: " << my_datanode_->NodeId() << " to serving: " << route_slot_ranges_[slot]->serving_node_id;
    CLS_INFO << stream.str();
    auto serving_node = GetDatanodeById(route_slot_ranges_[slot]->serving_node_id);
    // Topology correctness validation during parsing topo ensures that the serving is not nullptr
    return Status{Status::ClusterRetry,
                  fmt::format("MOVED {} {}:{}", slot, serving_node->Addr().ip(), serving_node->Addr().port())};
  }

  // Check datanode client_rw_status for write and read requests
  if ((cmd_flags & redis::kCmdWrite) && !my_datanode_->IsClientWritable()) {
    if (!IsActivePool()) {
      CLS_ERROR << "Datanode is not writable, id: " << my_datanode_->NodeId()
                << ", client_rw_status: " << my_datanode_->ClientRWStatus();
      return Status{Status::NotOK, fmt::format("Datanode is not writable, id: {}", my_datanode_->NodeId())};
    }
    CLS_ERROR << "Retry Datanode is not writable, id: " << my_datanode_->NodeId()
              << ", client_rw_status: " << my_datanode_->ClientRWStatus();
    return Status{Status::ClusterRetryWriteStopped,
                  fmt::format("Retry Datanode is not writable id: {}", my_datanode_->NodeId())};
  }
  // need to consider Datanode_RWStatus_RW_STATUS_WO status of client_rw_status
  if ((cmd_flags & redis::kCmdReadOnly) && !my_datanode_->IsClientReadable()) {
    CLS_ERROR << "Datanode is not readable, id: " << my_datanode_->NodeId()
              << ", client_rw_status: " << my_datanode_->ClientRWStatus();
    return Status{Status::NotOK, fmt::format("Datanode is not readable, id: {}", my_datanode_->NodeId())};
  }

  return slot_range->GetStorage();
}

Status Cluster::SetTopo(const kv::controller::v1::ReportDataNodeResponse& resp) {
  auto start_ts = util::GetTimeStampUS();
  // Check correctness of topo information
  uint64_t new_topo_verion = 0;
  bool is_active_pool = true;
  auto s = checkTopo(resp, &new_topo_verion, &is_active_pool);
  if (!s.IsOK()) {
    return s;
  }
  CLS_INFO << fmt::format("Start setting new topo. local version: {}, new version: {}", version_.load(),
                          new_topo_verion);

  // Parse topo of my pool
  std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>> all_route_slot_ranges;
  std::map<std::string, std::shared_ptr<SlotRange>> my_slot_ranges;
  std::unordered_map<std::string, std::shared_ptr<DataNode>> datanodes;
  std::shared_ptr<DataNode> my_datanode;
  s = parseTopo((is_active_pool ? resp.ha_cluster().active() : resp.ha_cluster().standby()), &all_route_slot_ranges,
                &my_slot_ranges, &datanodes, &my_datanode);
  if (!s.IsOK()) {
    CLS_INFO << fmt::format("Failed to parse new topo. local version: {}, new version: {}", version_.load(),
                            new_topo_verion);
    return s;
  }
  CLS_INFO << fmt::format("Finished parsing new topo. local version: {}, new version: {}", version_.load(),
                          new_topo_verion);

  // Apply parsed topo information
  auto before_apply = util::GetTimeStampUS();
  s = applyTopo(is_active_pool, new_topo_verion, all_route_slot_ranges, my_slot_ranges, datanodes, my_datanode);
  if (!s.IsOK()) {
    CLS_INFO << fmt::format("Failed to apply new topo. local version: {}, new version: {}", version_.load(),
                            new_topo_verion);
    return s;
  }
  // report new version immediately
  srv_->ctrl_rpc_client->SendHeartbeatImmediately();

  auto end_ts = util::GetTimeStampUS();
  auto elapsed = end_ts - start_ts;
  thread_local_metric_array.Record(MetricType::APPLY_TOPO_LATENCY, {}, elapsed);
  CLS_INFO << fmt::format(
      "Finished setting new topo. local version: {}, new version: {}, elapsed: {}us, apply_topo_cost: {}us",
      version_.load(), new_topo_verion, elapsed, (end_ts - before_apply));

  return Status::OK();
}

Status Cluster::checkTopo(const kv::controller::v1::ReportDataNodeResponse& resp, uint64_t* new_topo_verion,
                          bool* is_active_pool) {
  // Check topo version
  *new_topo_verion = resp.ha_cluster().version();
  if (*new_topo_verion <= version_.load()) {
    CLS_WARNING << fmt::format("New topo version: {} <= local topo version: {}", *new_topo_verion, version_.load());
    return {Status::ClusterInvalidInfo, errInvalidClusterVersion};
  }
  // Check cluster role
  if (!resp.ha_cluster().has_active() && !resp.ha_cluster().has_standby()) {
    CLS_ERROR << fmt::format("No cluster set in topo");
    return {Status::ClusterInvalidInfo, "No cluster set in topo"};
  }
  if (resp.ha_cluster().has_active() &&
      (resp.ha_cluster().active().role() != kv::controller::v1::Cluster::ROLE_ACTIVE)) {
    CLS_WARNING << fmt::format("Wrong role for active pool. role: {}",
                               kv::controller::v1::Cluster_Role_Name(resp.ha_cluster().active().role()));
    return {Status::ClusterInvalidInfo, errInvalidClusterRole};
  }
  if (resp.ha_cluster().has_standby() &&
      (resp.ha_cluster().standby().role() != kv::controller::v1::Cluster::ROLE_STANDBY)) {
    CLS_WARNING << fmt::format("Wrong role for standby pool. role: {}",
                               kv::controller::v1::Cluster_Role_Name(resp.ha_cluster().standby().role()));
    return {Status::ClusterInvalidInfo, errInvalidClusterRole};
  }
  // Get my pool by comparing cluster's pool and local pool
  *is_active_pool = true;
  if (pool_ == resp.ha_cluster().standby().pool()) {
    *is_active_pool = false;
  } else if (pool_ != resp.ha_cluster().active().pool()) {
    CLS_WARNING << fmt::format("My pool: {} can't match active pool: {} or standby pool: {}", pool_,
                               resp.ha_cluster().active().pool(), resp.ha_cluster().standby().pool());
    return {Status::ClusterInvalidInfo, errInvalidClusterPool};
  }
  // Check if cluster id matches my cluster id
  if (*is_active_pool) {
    if (resp.ha_cluster().active().cluster_id() != cluster_id_) {
      CLS_WARNING << fmt::format("Cluster id does not match in active pool. my cluster_id: {}, topo cluster_id: {}",
                                 cluster_id_, resp.ha_cluster().active().cluster_id());
      return {Status::ClusterInvalidInfo, errClusterIDNotMatch};
    }
  } else {
    if (resp.ha_cluster().standby().cluster_id() != cluster_id_) {
      CLS_WARNING << fmt::format("Cluster id does not match in standby pool. my cluster_id: {}, topo cluster_id: {}",
                                 cluster_id_, resp.ha_cluster().standby().cluster_id());
      return {Status::ClusterInvalidInfo, errClusterIDNotMatch};
    }
  }

  return Status::OK();
}

// Parse and check target cluster's topo
// 1. Traverse shards, nodes, and slotranges to extract topo info and create corresponding objects
// 2. Validate the legality of the topology: No overlap in slotranges, and all 0~16383 slots are covered
// 3. Validate the legality of the local slotrange's dbID: by checking if the dbID of target slotrange
// exists in the StorageManager
// 4. Handling of Datanode roles:
//  a. Process only the Datanode for the local shard, and bind corresponding serving and importings
//  b. When the current Datanode is a importing, validate if its slot range matches the serving's
Status Cluster::parseTopo(const kv::controller::v1::Cluster& cluster,
                          std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>>* all_route_slot_ranges,
                          std::map<std::string, std::shared_ptr<SlotRange>>* my_slot_ranges,
                          std::unordered_map<std::string, std::shared_ptr<DataNode>>* datanodes,
                          std::shared_ptr<DataNode>* my_datanode) {
  CLS_INFO << fmt::format(
      "Start parsing topo of cluster: {} pool: {}, role: {}, my_clusterId: {}, my_datanodeId: {}, my_pool: {}",
      cluster.cluster_id(), cluster.pool(), kv::controller::v1::Cluster_Role_Name(cluster.role()), cluster_id_,
      datanode_id_, pool_);
  std::bitset<kClusterSlots> slots_flag;
  // std::map<slog_range, node_id> topo;
  std::unordered_map<std::string, std::string> serving_topo;
  std::unordered_map<std::string, std::set<std::string>> importing_topo;

  for (const auto& shard : cluster.shards()) {
    CLS_INFO << fmt::format("Parsing shard: {}", shard.shard_id());

    for (const auto& datanode : shard.datanodes()) {
      CLS_INFO << fmt::format("Parsing datanode: {}, role: {}", datanode.datanode_id(),
                              kv::controller::v1::Datanode_ServingStatus_Name(datanode.serving_status()));
      // Create datanode
      auto new_node =
          std::make_shared<DataNode>(datanode.datanode_id(), datanode.client_rw_status(), datanode.dts_rw_status(),
                                     datanode.serving_status(), datanode.ip_addr());
      datanodes->emplace(datanode.datanode_id(), new_node);
      if (datanode.datanode_id() == datanode_id_) {
        // record my node
        *my_datanode = new_node;
      }

      // Create slotranges
      for (const auto& slot_range : datanode.slot_range_list()) {
        auto db_id = slot_range.db_id();
        auto start = slot_range.index().start();
        auto end = slot_range.index().end();
        CLS_INFO << fmt::format("Parsing slotrange: [{},{}]", start, end);
        // check range
        if (start < 0 || end >= kClusterSlots || start > end) {
          CLS_ERROR << fmt::format("Parse failed. Slot range is invalid. range: [{},{}], datanode: {}", start, end,
                                   datanode.datanode_id());
          return {Status::ClusterInvalidInfo, errSlotOutOfRange};
        }
        // create route slot range info
        if (datanode.serving_status() == kv::controller::v1::Datanode::SERVING_STATUS_SERVING) {
          CLS_INFO << fmt::format("Creating route info with serving slotrange: [{},{}]", start, end);
          auto sr_info = std::make_shared<SlotRange::SlotRangeInfo>(datanode.datanode_id(), start, end);
          all_route_slot_ranges->emplace_back(sr_info);
          // check slotrange overlap
          for (auto i = start; i <= end; i++) {
            if (slots_flag.test(i)) {
              CLS_ERROR << fmt::format("Parse failed. Slot range overlapped. range:[{},{}]", start, end);
              return {Status::ClusterInvalidInfo, errSlotOverlapped};
            }
            slots_flag.set(i, true);
          }
        }
        // create and record local slotrange object
        if (datanode.datanode_id() == datanode_id_) {
          CLS_INFO << fmt::format("Creating my slotrange: [{},{}]", start, end);
          // check db-id to get storage for slot range
          auto storage = srv_->storage_mgr->GetStorageByDBID(db_id);
          if (storage == nullptr) {
            CLS_ERROR << fmt::format("Parse failed. Failed to get storage by db-id: {} for slotrange: [{},{}]", db_id,
                                     start, end);
            return {Status::ClusterInvalidInfo, errInvalidDBID};
          }
          // create slotrange object
          auto sr = std::make_shared<SlotRange>(std::string(), start, end, storage);
          my_slot_ranges->emplace(sr->GetName(), sr);
        }

        std::string slot_range_key{fmt::format("{}_{}", start, end)};

        switch (datanode.serving_status()) {
          case kv::controller::v1::Datanode::SERVING_STATUS_SERVING:
            if (serving_topo.find(slot_range_key) != serving_topo.end()) {
              CLS_WARNING << "Parse failed. datanode : " << datanode.datanode_id() << ", slot_range: " << slot_range_key
                          << " already exists";
              return {Status::ClusterInvalidInfo, errSlotRangeExists};
            }
            serving_topo[slot_range_key] = datanode.datanode_id();
            break;
          case kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING:
            // support to register multi importing datanodes
            importing_topo[slot_range_key].insert(datanode.datanode_id());
            break;
          case kv::controller::v1::Datanode::SERVING_STATUS_UNSPECIFIED:
            CLS_INFO << "Parse failed. datanode : " << datanode.datanode_id() << ", slot_range: " << slot_range_key
                     << ", serving status is unspecified";
            break;
          default:
            CLS_ERROR << "Parse failed. Wrong datanode serving status: "
                      << kv::controller::v1::Datanode_ServingStatus_Name(datanode.serving_status());
            return {Status::ClusterInvalidInfo, errInvalidNodeServingStatus};
        }
      }  // end slotranges loop
    }    // end datanodes loop
  }      // end shards loop

  if (!*my_datanode) {
    CLS_ERROR << "Parse failed. my datanode id: " << datanode_id_ << " not found in topo";
    return {Status::ClusterInvalidInfo, errDatanodeNotFoundInTopo};
  }

  // set serving node id of my_slot_ranges
  for (auto& sr : *my_slot_ranges) {
    std::string slot_range_key{fmt::format("{}_{}", sr.second->GetRangeStart(), sr.second->GetRangeEnd())};
    auto serving_node = serving_topo.find(slot_range_key);
    if (serving_node == serving_topo.end()) {
      CLS_ERROR << "Parse failed. my slot range: " << slot_range_key << " not find serving node";
      return {Status::ClusterInvalidInfo, errSlotRangeNotMatchWithServing};
    }

    sr.second->SetServingNodeId(serving_node->second);
  }

  // set importing id of serving node
  if (!importing_topo.empty()) {
    for (auto& [slot_range, node_id] : serving_topo) {
      auto importing_node = importing_topo.find(slot_range);
      if (importing_node != importing_topo.end()) {
        auto serving_node = datanodes->find(node_id);
        if (serving_node == datanodes->end()) {
          CLS_ERROR << "Parse failed. datanode: " << node_id << ", slot_range: " << slot_range
                    << " not find importing node";
          return {Status::ClusterInvalidInfo, errSlotRangeNotMatchWithImporting};
        }

        serving_node->second->SetImportingId(importing_node->second);
      }
    }
  }

  // Check slot ranges cover all 16384 slots
  if (slots_flag.count() != kClusterSlots) {
    CLS_ERROR << fmt::format("Parse failed. Slot ranges can't cover all 16384 slots, curr_size: {}",
                             slots_flag.count());
    return {Status::ClusterInvalidInfo, errSlotNotEnough};
  }

  // Check my new slot ranges for updating topo
  auto s = checkMySlotRanges(*my_slot_ranges);
  if (!s.IsOK()) {
    CLS_ERROR << "Parse failed. " << s.Msg();
    return {Status::ClusterInvalidInfo, errInvalidMySlotRanges};
  }

  CLS_INFO << fmt::format("Finished parsing topo of cluster: {}", cluster.cluster_id());
  return Status::OK();
}

Status Cluster::checkMySlotRanges(std::map<std::string, std::shared_ptr<SlotRange>>& my_slot_ranges) {
  if (my_slot_ranges_.size() <= 0) {
    return Status::OK();
  }

  int16_t diff_cnt = 0;
  if (my_slot_ranges.size() == my_slot_ranges_.size()) {
    auto old_map_iter = my_slot_ranges_.begin();
    auto new_map_iter = my_slot_ranges.begin();
    for (; old_map_iter != my_slot_ranges_.end(); old_map_iter++, new_map_iter++) {
      if (old_map_iter->first != new_map_iter->first) {
        diff_cnt++;
        auto old_slot_range = old_map_iter->second;
        auto new_slot_range = new_map_iter->second;
        if (old_slot_range->GetRangeStart() == new_slot_range->GetRangeStart() &&
            old_slot_range->GetRangeEnd() != new_slot_range->GetRangeEnd()) {
          if (old_slot_range->GetRangeEnd() < new_slot_range->GetRangeEnd()) {
            CLS_ERROR << "New slot range end should be less than old, old: " << old_slot_range->GetName()
                      << ", new: " << new_slot_range->GetName();
            return Status{Status::NotOK, "Wrong new range end"};
          }
        } else if (old_slot_range->GetRangeStart() != new_slot_range->GetRangeStart() &&
                   old_slot_range->GetRangeEnd() == new_slot_range->GetRangeEnd()) {
          if (old_slot_range->GetRangeStart() > new_slot_range->GetRangeStart()) {
            CLS_ERROR << "New slot range start should be greater than old, old: " << old_slot_range->GetName()
                      << ", new: " << new_slot_range->GetName();
            return Status{Status::NotOK, "Wrong new range start"};
          }
        } else {
          CLS_ERROR << "New slotrange's start or end should be matched with old; old: " << old_slot_range->GetName()
                    << ", range:[" << old_slot_range->GetRangeStart() << "," << old_slot_range->GetRangeEnd() << "]"
                    << ", new: " << new_slot_range->GetName() << ", range:[" << new_slot_range->GetRangeStart() << ","
                    << new_slot_range->GetRangeEnd() << "]";
          return Status{Status::NotOK, "Wrong new range"};
        }
      }
    }
    if (diff_cnt > 1) {
      CLS_ERROR << "My slot_range diff count:" << diff_cnt
                << " can't be more than 1 when new slot_ranges size == local size";
      return Status{Status::ClusterInvalidInfo, "My slot_range diff count greater than 1"};
    }
  } else if (my_slot_ranges.size() < my_slot_ranges_.size()) {
    // Apply slot_range removing
    // NOTE(mingfo): Except for the removed slot_range, all other slot_ranges should match the local slot_ranges.
    int16_t max_diff = my_slot_ranges_.size() - my_slot_ranges.size();
    for (auto start = my_slot_ranges_.begin(); start != my_slot_ranges_.end(); start++) {
      auto iter = my_slot_ranges.find(start->first);
      if (iter == my_slot_ranges.end()) {
        diff_cnt++;
      }
    }
    if (diff_cnt > max_diff) {
      CLS_ERROR << "My slot_range diff exceeds the expected, max diff: " << max_diff << ", current diff: " << diff_cnt;
      return Status{Status::ClusterInvalidInfo, "My slot_range diff exceeds the max diff"};
    }
  } else {
    CLS_ERROR << "My new slot_ranges more than local, new size: " << my_slot_ranges.size()
              << ", local size: " << my_slot_ranges_.size();
    return Status{Status::ClusterInvalidInfo, "New slot_ranges more than local"};
  }

  return Status::OK();
}

Status Cluster::applyTopo(bool is_active_pool, uint64_t new_topo_version,
                          std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>>& all_route_slot_ranges,
                          std::map<std::string, std::shared_ptr<SlotRange>>& my_slot_ranges,
                          std::unordered_map<std::string, std::shared_ptr<DataNode>>& datanodes,
                          std::shared_ptr<DataNode>& my_datanode) {
  // Check topo status of this datanode before apply
  auto s = checkLocalNodeNewTopoStatus(is_active_pool, my_datanode);
  if (!s.IsOK()) {
    CLS_ERROR << "Apply failed. Wrong local topo status";
    return s;
  }

  // First time to apply topo
  if (version_.load() < 1) {
    s = applyFirstTopo(is_active_pool, new_topo_version, all_route_slot_ranges, my_slot_ranges, datanodes, my_datanode);
    if (!s.IsOK()) {
      CLS_INFO << "Failed to apply first topo. new_ver: " << new_topo_version
               << ", pool: " << (is_active_pool ? "ACTIVE_POOL" : "STANDBY_POOL");
    }
    return s;
  }

  // Update existing topo
  s = updateExistingTopo(is_active_pool, new_topo_version, all_route_slot_ranges, my_slot_ranges, datanodes,
                         my_datanode);
  if (!s.IsOK()) {
    CLS_INFO << "Failed to update topo. new_ver: " << new_topo_version
             << ", pool: " << (is_active_pool ? "ACTIVE_POOL" : "STANDBY_POOL") << ", err: " << s.Msg();
    return s;
  }

  // record new topo info
  {
    std::stringstream stream;
    stream << "Finished applying new topo, version: " << new_topo_version
           << ", pool: " << (is_active_pool ? "ACTIVE_AZ" : "STANDBY_AZ")
           << ", serving_status: " << my_datanode_->ServingStatusStr() << ", datanode_id: " << datanode_id_
           << ", importing_nodes: [";
    for (const auto& id : my_datanode_->GetImportingId()) {
      stream << id << ",";
    }
    stream << "] local slotranges: [";
    for (const auto& name : GetAllLocalSlotRangeNames()) {
      stream << name << ",";
    }
    stream << "]";
    CLS_INFO << stream.str();
  }
  return Status::OK();
}

Status Cluster::applyFirstTopo(bool is_active_pool, uint64_t new_topo_version,
                               std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>>& all_route_slot_ranges,
                               std::map<std::string, std::shared_ptr<SlotRange>>& my_slot_ranges,
                               std::unordered_map<std::string, std::shared_ptr<DataNode>>& datanodes,
                               std::shared_ptr<DataNode>& my_datanode) {
  CLS_INFO << "First time to apply topo. version: " << new_topo_version
           << " pool: " << (is_active_pool ? "ACTIVE" : "STANDBY");
  // shift serving replicationId
  if (is_active_pool && my_datanode->IsServing()) {
    auto s = Cluster::ShiftStorageReplId(my_slot_ranges);
    if (!s.IsOK()) {
      CLS_ERROR << s.Msg();
      return s;
    }
  } else if (my_datanode->IsServing()) {
    auto s = Cluster::resetDisableAutoCompactionsOption(my_slot_ranges);
    if (!s.IsOK()) return s;
  }

  std::lock_guard<std::shared_mutex> guard(shared_mutex_);
  // set datanodes
  datanodes_ = std::move(datanodes);
  my_datanode_ = my_datanode;
  // set slotranges
  my_slot_ranges_ = std::move(my_slot_ranges);
  for (const auto& slot_range : all_route_slot_ranges) {
    for (auto slot = slot_range->start; slot <= slot_range->end; slot++) {
      route_slot_ranges_[slot] = slot_range;
    }
  }
  // set pool
  is_active_pool_.store(is_active_pool);
  // set version
  version_.store(new_topo_version);

  return Status::OK();
}

struct DelRangeTask {
  int16_t start, end;
  std::string old_range;
  std::string new_range;
  std::shared_ptr<engine::Storage> storage;

  friend std::ostream& operator<<(std::ostream& os, const DelRangeTask& task) {
    os << "[db_id=" << task.storage->GetDBId() << ", del_range=" << CreateSlotRangeName(task.start, task.end)
       << ", old_range=" << task.old_range << ", new_range" << task.new_range << "]";
    return os;
  }
};

using DelDBTask = std::shared_ptr<SlotRange>;

std::ostream& operator<<(std::ostream& os, const DelDBTask& task) {
  os << "[db_id=" << task->GetStorage()->GetDBId() << ", db_dir=" << task->GetStorage()->GetDBDir()
     << ", slot_range=" << task->GetName() << "]";
  return os;
};

Status Cluster::updateExistingTopo(bool is_active_pool, uint64_t new_topo_version,
                                   std::vector<std::shared_ptr<SlotRange::SlotRangeInfo>>& all_route_slot_ranges,
                                   std::map<std::string, std::shared_ptr<SlotRange>>& my_slot_ranges,
                                   std::unordered_map<std::string, std::shared_ptr<DataNode>>& datanodes,
                                   std::shared_ptr<DataNode>& my_datanode) {
  auto start_ts = util::GetTimeStampUS();
  CLS_INFO << "Update topo. version: " << new_topo_version << " pool: " << (is_active_pool ? "ACTIVE" : "STANDBY");
  // NOTE(mingfo): Lock migration task and sync_manager to prevent changes during modifying topo.
  srv_->migration->SetIsTopoUpdating(true);
  srv_->sync_manager->SetIsTopoUpdating(true);
  srv_->cdc_manager->SetIsTopoUpdating(true);
  auto exit = MakeScopeExit([this]() {
    srv_->migration->SetIsTopoUpdating(false);
    srv_->sync_manager->SetIsTopoUpdating(false);
    srv_->cdc_manager->SetIsTopoUpdating(false);
  });

  auto org_is_client_writeable = my_datanode_->IsClientWritable();
  auto new_is_client_writeable = my_datanode->IsClientWritable();
  // 1. Apply local topo change
  auto before_local_apply_ts = util::GetTimeStampUS();
  bool need_clean_running_status = false;
  auto s = doLocalNodeTopoApply(my_datanode, is_active_pool, my_slot_ranges, need_clean_running_status);
  if (!s.IsOK()) {
    return s;
  }

  // shift serving replicatonId
  // NOTE(mingfo): Shift replicationId in following scenarioes:
  // 1. active pool && importing -> serving
  // 2. standby pool -> active pool && role is serving
  if ((is_active_pool && !my_datanode_->IsServing() && my_datanode->IsServing()) ||
      (!is_active_pool_.load() && is_active_pool && my_datanode->IsServing())) {
    s = Cluster::ShiftStorageReplId(my_slot_ranges);
    if (!s.IsOK()) return s;
  } else if (!my_datanode_->IsServing() && my_datanode->IsServing()) {
    s = Cluster::resetDisableAutoCompactionsOption(my_slot_ranges);
    if (!s.IsOK()) return s;
  }

  auto before_update_ts = util::GetTimeStampUS();
  std::vector<DelRangeTask> del_range_tasks;
  std::set<DelDBTask> del_db_tasks;
  // 2. Set new topo
  {
    // Note(mingfo): datanodes_/route_slot_ranges_/my_datanode_ can be replace directly,
    // my_slotranges_ should be reused.
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    // set datanodes
    datanodes_ = std::move(datanodes);
    my_datanode_ = std::move(my_datanode);

    if (my_datanode_->IsServing()) {
      if (!org_is_client_writeable && new_is_client_writeable) {
        GlobalStatsInstance().FinishDatanodeOnlyRead();
      }
    }

    // set slotranges
    for (const auto& slot_range : all_route_slot_ranges) {
      for (auto slot = slot_range->start; slot <= slot_range->end; slot++) {
        route_slot_ranges_[slot] = slot_range;
      }
    }
    // update my slotranges when my datanode topo changed
    // 1. failover: serving node change
    // 2. scaling: a) split slot_range; b) remove slot_range;
    if (my_slot_ranges_.size() == my_slot_ranges.size()) {
      // andle 1.failover and 2.a)split slot_range
      int16_t delete_slot_start = 0, delete_slot_end = 0;
      // NOTE(mingfo): When the SlotRangeIndex changed, slot_range name will change, the item in my_slot_ranges_
      // should be replaced with new name. The old slot_ragne ptr should be reused.
      // 'erase_item' to record the old name, 'replace_item' to record the old slot_range ptr.
      std::string erase_item;
      std::shared_ptr<SlotRange> replace_item = nullptr;
      auto old_map_iter = my_slot_ranges_.begin();
      auto new_map_iter = my_slot_ranges.begin();
      for (; old_map_iter != my_slot_ranges_.end(); old_map_iter++, new_map_iter++) {
        if (old_map_iter->first == new_map_iter->first) {
          old_map_iter->second->SetServingNodeId(new_map_iter->second->GetServingNodeId());
        } else {
          auto old_slot_range = old_map_iter->second;
          auto new_slot_range = new_map_iter->second;
          if (old_slot_range->GetRangeStart() == new_slot_range->GetRangeStart() &&
              old_slot_range->GetRangeEnd() != new_slot_range->GetRangeEnd()) {
            CHECK(old_slot_range->GetRangeEnd() > new_slot_range->GetRangeEnd())  // it can't be false
                << "New slot range end should be less than old, old: " << old_slot_range->GetName()
                << ", new: " << new_slot_range->GetName();
            delete_slot_start = new_slot_range->GetRangeEnd() + 1;
            delete_slot_end = old_slot_range->GetRangeEnd();
            erase_item = old_map_iter->first;
            replace_item = old_slot_range;
            old_slot_range->SetRangeEnd(new_slot_range->GetRangeEnd());
          } else if (old_slot_range->GetRangeStart() != new_slot_range->GetRangeStart() &&
                     old_slot_range->GetRangeEnd() == new_slot_range->GetRangeEnd()) {
            CHECK(old_slot_range->GetRangeStart() < new_slot_range->GetRangeStart())
                << "New slot range start should be greater than old, old: " << old_slot_range->GetName()
                << ", new: " << new_slot_range->GetName();
            delete_slot_start = old_slot_range->GetRangeStart();
            delete_slot_end = new_slot_range->GetRangeStart() - 1;
            erase_item = old_map_iter->first;
            replace_item = old_slot_range;
            old_slot_range->SetRangeStart(new_slot_range->GetRangeStart());
          } else {
            CLS_FATAL << "New slotrange's start or end should be matched with old; old: " << old_slot_range->GetName()
                      << ", range:[" << old_slot_range->GetRangeStart() << "," << old_slot_range->GetRangeEnd() << "]"
                      << ", new: " << new_slot_range->GetName() << ", range:[" << new_slot_range->GetRangeStart() << ","
                      << new_slot_range->GetRangeEnd() << "]";
          }
          // replace slot_range item in map
          my_slot_ranges_.erase(erase_item);
          my_slot_ranges_.emplace(replace_item->GetName(), replace_item);
          // clear redundancy data in active pool only
          if (is_active_pool && my_datanode_->IsServing()) {
            del_range_tasks.emplace_back(DelRangeTask{.start = delete_slot_start,
                                                      .end = delete_slot_end,
                                                      .old_range = erase_item,
                                                      .new_range = replace_item->GetName(),
                                                      .storage = replace_item->GetStorage()});
          }
        }
      }
    } else {
      // Handle 2.b) remove slot_range
      auto start = my_slot_ranges_.begin();
      for (; start != my_slot_ranges_.end();) {
        auto iter = my_slot_ranges.find(start->first);
        if (iter == my_slot_ranges.end()) {
          del_db_tasks.emplace(start->second);
          start = my_slot_ranges_.erase(start);
        } else {
          start++;
        }
      }
    }

    if (need_clean_running_status) {
      ClearWriteRunningStatusLocked();
    }
    // set pool
    is_active_pool_.store(is_active_pool);
    // set version
    version_.store(new_topo_version);
  }

  // clean extra user data
  if (!del_range_tasks.empty() || !del_db_tasks.empty()) {
    for (auto& task : del_range_tasks) {
      CLS_WARNING << "[data clean] Add task to delete range data, task:" << task;
    }
    for (auto& task : del_db_tasks) {
      CLS_WARNING << "[data clean] Add task to delete db data, task:" << task;
    }
    auto s = util::MakeUniqueThread(
        "data_clean", [srv = srv_->shared_from_this(), del_range_tasks = std::move(del_range_tasks),
                       del_db_tasks = std::move(del_db_tasks)]() {
          Context ctx;
          const size_t max_retry = 3;
          auto lock_slot_range = [&srv, &ctx](const std::string& name) -> StatusOr<std::unique_ptr<SlotRangeLock>> {
            // get exclusive lock of slot range
            Status s = Status::OK();
            for (size_t retry = 0; retry < max_retry; ++retry) {
              if (retry) std::this_thread::sleep_for(std::chrono::seconds(1));
              auto ret = SlotRangeLock::AcquireSlotRangeLock(name, mgl::LockMode::LOCK_X, &ctx, srv->GetMGLockMgr());
              if (ret.IsOK()) return ret;
              s = ret.ToStatus();
            }
            return s;
          };
          // sleep for a while before lock the stale slot range and clean data
          // to avoid hold the lock for a long time when db write stalled even
          // write stopped, this may block the requests which require the lock
          // of the stale slot range too
          std::this_thread::sleep_for(std::chrono::milliseconds(sleepMSBeforeCleanData));
          for (auto& task : del_range_tasks) {
            // get exclusive lock of old slot range
            auto ret = lock_slot_range(task.old_range);
            if (!ret.IsOK()) {
              CLS_ERROR << "[data clean] Delete range data failed to lock slot range,"
                        << ", task:" << task << ", err:" << ret.Msg();
              continue;
            }
            CLS_WARNING << "[data clean] Delete range data succeed to lock slot range"
                        << ", task:" << task;
            // delete range data of slot range
            bool db_closing = false;
            rocksdb::Status s = rocksdb::Status::OK();
            auto key_start = ComposeSlotKeyPrefix(kDefaultNamespace, task.start);
            auto key_end = ComposeSlotKeyPrefix(kDefaultNamespace, task.end + 1);
            for (size_t retry = 0; retry < max_retry; ++retry) {
              if (retry) std::this_thread::sleep_for(std::chrono::seconds(1));
              auto lock = task.storage->ReadLockGuard();
              if (task.storage->IsClosing()) {
                db_closing = true;
                break;
              }
              while (!task.storage->setDisableAutoCompactionsOption(true).IsOK()) {
              }
              // NOTE(mingfo): Directly delete data of removed slotrange in subkey_cf
              // and zset_score_cf to avoid using the compaction filter for gc and
              // speed up the compaction rate.
              rocksdb::WriteBatch wb;
              wb.DeleteRange(task.storage->GetCFHandle(engine::kMetadataColumnFamilyName), key_start, key_end);
              wb.DeleteRange(task.storage->GetCFHandle(engine::kSubkeyColumnFamilyName), key_start, key_end);
              wb.DeleteRange(task.storage->GetCFHandle(engine::kZSetScoreColumnFamilyName), key_start, key_end);
              // slowdown when db write stall or write stop
              rocksdb::WriteOptions write_options = task.storage->DefaultWriteOptions();
              write_options.no_slowdown = false;
              s = task.storage->Write(write_options, &wb);
              while (!task.storage->resetDisableAutoCompactionsOption().IsOK()) {
              }
              if (s.ok()) break;
            }
            if (db_closing) {
              CLS_WARNING << "[data clean] Delete range data skipped to delete range"
                          << ", task:" << task;
            } else if (s.ok()) {
              CLS_WARNING << "[data clean] Delete range data succeed to delete range"
                          << ", task:" << task;
            } else {
              CLS_ERROR << "[data clean] Delete range data failed to delete range"
                        << ", task:" << task << ", err:" << s.ToString();
            }
          }
          for (auto& task : del_db_tasks) {
            // get exclusive lock of slot range
            auto lock_ret = lock_slot_range(task->GetName());
            if (!lock_ret.IsOK()) {
              CLS_ERROR << "[data clean] Delete db data failed to lock slot range,"
                        << ", task:" << task << ", err:" << lock_ret.Msg();
              continue;
            }
            CLS_WARNING << "[data clean] Delete db data succeed to lock slot range"
                        << ", task:" << task;
            // delete db data of slot range
            srv->storage_mgr->RemoveStorage(task->GetStorage()->GetDBId());
            auto db_dir = task->GetStorage()->GetDBDir();
            task->GetStorage()->ForceStop();
            std::error_code err;
            for (size_t retry = 0; retry < max_retry; ++retry) {
              if (retry) std::this_thread::sleep_for(std::chrono::seconds(1));
              std::filesystem::remove_all(db_dir, err);
              if (!err) break;
            }
            if (!err) {
              CLS_WARNING << "[data clean] Delete db data succeed to rm db dir"
                          << ", task:" << task;
            } else {
              CLS_ERROR << "[data clean] Delete db data failed to rm db dir"
                        << "task:" << task << ", err:" << err;
            }
          }
          CLS_WARNING << "[data clean] Data clean thread exited";
        });
    if (s.IsOK()) {
      CLS_WARNING << "[data clean] Data clean thread start succeed";
      s.GetValue()->detach();
    } else {
      CLS_ERROR << "[data clean] Data clean thread start failed, err:" << s.Msg();
    }
  }

  // Unlock migration task and sync manager after modifying topo
  srv_->migration->SetIsTopoUpdating(false);
  srv_->sync_manager->SetIsTopoUpdating(false);
  srv_->cdc_manager->SetIsTopoUpdating(false);
  auto end_ts = util::GetTimeStampUS();
  CLS_INFO << "Update topo version: " << new_topo_version << ", total_cost: " << end_ts - start_ts
           << "us, local_apply_cost: " << before_update_ts - before_local_apply_ts
           << "us, set_new_topo_cost: " << end_ts - before_update_ts << "us";

  return Status::OK();
}

Status Cluster::checkLocalNodeNewTopoStatus(bool is_active_pool, std::shared_ptr<DataNode>& new_node) {
  // Check role/client_rw_status/dts_rw_status
  if (!DataNode::IsValidTopoStatus(is_active_pool, new_node->ServingStatus(), new_node->ClientRWStatus(),
                                   new_node->DtsRWStatus())) {
    CLS_ERROR << fmt::format(
        "Wrong topo status for this datanode, id: {}, role: {}, client_rw_status: {}, dts_rw_status: {}",
        new_node->NodeId(), kv::controller::v1::Datanode_ServingStatus_Name(new_node->ServingStatus()),
        kv::controller::v1::Datanode_RWStatus_Name(new_node->ClientRWStatus()),
        kv::controller::v1::Datanode_RWStatus_Name(new_node->DtsRWStatus()));
    return {Status::ClusterInvalidInfo, errWrongNodeTopoStatus};
  }
  return Status::OK();
}

Status Cluster::doLocalNodeTopoApply(std::shared_ptr<DataNode>& new_node, bool is_active_pool,
                                     std::map<std::string, std::shared_ptr<SlotRange>>& my_slot_ranges,
                                     bool& need_clean_running_status) {
  // NOTE(mingfo): When an error occurs and it is necessary to exit this application flow,
  // if there exists modifications to my_datanode_'s status(es), they need to be recovered before returning.

  auto set_client_rw_ts = util::GetTimeStampUS();
  // Apply client_rw_status stop write
  {
    if (my_datanode_->IsClientWritable() && !new_node->IsClientWritable()) {
      CLS_INFO << fmt::format("Applying client_rw_status, from: {} to {}",
                              kv::controller::v1::Datanode_RWStatus_Name(my_datanode_->ClientRWStatus()),
                              kv::controller::v1::Datanode_RWStatus_Name(new_node->ClientRWStatus()));
      Context ctx;
      std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
      for (const auto& sr : my_slot_ranges_) {
        auto ret = SlotRangeLock::AcquireSlotRangeLock(sr.second->GetName(), mgl::LockMode::LOCK_X, &ctx,
                                                       srv_->GetMGLockMgr());
        if (!ret.IsOK()) {
          CLS_WARNING << fmt::format("Failed to get exclusive lock of {} while apply client_rw_status",
                                     sr.second->GetName());
          return {Status::NotOK, "Failed to apply client_rw_status"};
        }
        sr_locks.emplace_back(std::move(ret.GetValue()));
      }
      // NOTE(mingfo): Cluster lock has to be gotten after SlotRangeLock
      std::lock_guard<std::shared_mutex> guard(shared_mutex_);
      my_datanode_->SetClientRWStatus(new_node->ClientRWStatus());
      if (my_datanode_->IsServing()) {
        GlobalStatsInstance().StartDatanodeOnlyRead();
      }
    }
  }
  auto set_dts_rw_ts = util::GetTimeStampUS();
  // Apply dts_rw_status stop write
  {
    if (my_datanode_->IsDtsWritable() && !new_node->IsDtsWritable()) {
      CLS_INFO << fmt::format("Applying dts_rw_status, from: {} to {}",
                              kv::controller::v1::Datanode_RWStatus_Name(my_datanode_->DtsRWStatus()),
                              kv::controller::v1::Datanode_RWStatus_Name(new_node->DtsRWStatus()));
      std::lock_guard<std::shared_mutex> guard(shared_mutex_);
      my_datanode_->SetDtsRWStatus(new_node->DtsRWStatus());
      for (const auto& sr : my_slot_ranges_) {
        srv_->sync_manager->StopAndJoinDtsReceiver(sr.second->GetName(),
                                                   ClusterTopologyChangedSyncError("Topo changed"));
      }
    }
  }

  auto clear_replication_ts = util::GetTimeStampUS();
  // Apply clear migration replication puller when role or pool changed
  if (my_datanode_->ServingStatus() != new_node->ServingStatus() || is_active_pool_.load() != is_active_pool) {
    CLS_INFO << fmt::format(
        "Applying serving status or pool changes, local status: {} pool: {}, new status: {}, pool: {}",
        kv::controller::v1::Datanode_ServingStatus_Name(my_datanode_->ServingStatus()),
        (is_active_pool_ ? "ACTIVE" : "STANDBY"),
        kv::controller::v1::Datanode_ServingStatus_Name(new_node->ServingStatus()),
        (is_active_pool ? "ACTIVE" : "STANDBY"));
    // TODO(chris): refactor log??
    srv_->migration->ReceivedNewTopo();
    srv_->sync_manager->ClearAll(ClusterTopologyChangedSyncError("Topo changed"));
    srv_->cdc_manager->ClearAll(ClusterTopologyChangedSyncError("Topo changed"));
    need_clean_running_status = true;
  }

  // Apply clear replication senders if only importing changed
  if (my_datanode_->GetImportingId() != new_node->GetImportingId()) {
    srv_->sync_manager->ClearAllReplSenders(ClusterTopologyChangedSyncError("Topo changed"));
    need_clean_running_status = true;
  }

  auto apply_scale_ts = util::GetTimeStampUS();
  // Apply slot_range splitting
  // NOTE(mingfo): Invoke SyncManager::ClearAll will access locks of SyncManager and
  // sync objects(repl_puller/repl_sender...), it should not be under the protection of lock of Cluster.
  // So, we do this before modifying topo.
  if (my_slot_ranges.size() == my_slot_ranges_.size()) {
    auto old_map_iter = my_slot_ranges_.begin();
    auto new_map_iter = my_slot_ranges.begin();
    for (; old_map_iter != my_slot_ranges_.end(); old_map_iter++, new_map_iter++) {
      if (old_map_iter->first != new_map_iter->first) {
        CLS_INFO << fmt::format("Applying splitting slot_range, from: {} to {}", old_map_iter->first,
                                new_map_iter->first);
        srv_->migration->ReceivedNewTopo();
        srv_->sync_manager->ClearAll(ClusterTopologyChangedSyncError("Topo changed"));
        srv_->cdc_manager->ClearAll(ClusterTopologyChangedSyncError("Topo changed"));
        need_clean_running_status = true;
        break;
      }
    }
  } else if (my_slot_ranges.size() < my_slot_ranges_.size()) {
    // Apply slot_range removing
    for (auto start = my_slot_ranges_.begin(); start != my_slot_ranges_.end(); start++) {
      auto iter = my_slot_ranges.find(start->first);
      if (iter == my_slot_ranges.end()) {
        CLS_INFO << fmt::format("{} will be removed, clear its all sync tasks", start->first);
        srv_->migration->ReceivedNewTopo();
        srv_->sync_manager->ClearAllOfOnSlotRange(start->first, ClusterTopologyChangedSyncError("Topo changed"));
        srv_->cdc_manager->ClearAllForSlotRange(start->first, ClusterTopologyChangedSyncError("Topo changed"));
        need_clean_running_status = true;
      }
    }
  } else {
    CLS_ERROR << "My new slot_ranges more than local, new size: " << my_slot_ranges.size()
              << ", local size: " << my_slot_ranges_.size();
    return Status{Status::ClusterInvalidInfo, "New slot_ranges more than local"};
  }

  auto end_ts = util::GetTimeStampUS();
  CLS_INFO << "Finished applying local topo. set_client_rw_cost: " << set_dts_rw_ts - set_client_rw_ts
           << "us, set_dts_rw_cost: " << clear_replication_ts - set_dts_rw_ts
           << "us, clear_replication_cost: " << apply_scale_ts - clear_replication_ts
           << "us, apply_sacle_out_cost: " << end_ts - apply_scale_ts << "us";

  return Status::OK();
}

// NOTE(mingfo): Shift replicationId of my slotranges in following scenarioes:
// 1. pool is active && init datanode -> serving
// 2. pool is active && importing -> serving
// 3. pool is active && importing slotranges running status -> writable
// 4. pool standby -> active && role is serving
Status Cluster::ShiftStorageReplId(std::map<std::string, std::shared_ptr<SlotRange>>& my_slot_ranges) {
  for (const auto& my_sl : my_slot_ranges) {
    auto lock = my_sl.second->GetStorage()->ReadLockGuard();
    if (my_sl.second->GetStorage()->IsClosing()) {
      CLS_ERROR << "slot range " << my_sl.second->GetName() << " shift replica id fail, error: db is closing";
      return {Status::NotOK, "db is closing"};
    }
    auto status = my_sl.second->GetStorage()->setDisableAutoCompactionsOption(true);
    if (!status.IsOK()) {
      CLS_ERROR << "slot range " << my_sl.second->GetName()
                << " disable auto compactions fail, error: " << status.Msg();
      return status;
    }
    status = my_sl.second->GetStorage()->ShiftReplId();
    if (!status.IsOK()) {
      CLS_ERROR << "slot range " << my_sl.second->GetName() << " shift replica id fail. error:" << status.Msg();
      return status;
    }
    status = my_sl.second->GetStorage()->resetDisableAutoCompactionsOption();
    if (!status.IsOK()) {
      CLS_ERROR << "slot range " << my_sl.second->GetName()
                << " reset disable auto compactions fail, error: " << status.Msg();
      return status;
    }
  }
  return Status::OK();
}

Status Cluster::resetDisableAutoCompactionsOption(std::map<std::string, std::shared_ptr<SlotRange>>& slot_ranges) {
  for (const auto& slot_range : slot_ranges) {
    auto status = slot_range.second->GetStorage()->resetDisableAutoCompactionsOption();
    if (!status.IsOK()) {
      CLS_ERROR << "Reset auto compactions option after serving failed, db id:"
                << slot_range.second->GetStorage()->GetDBId() << ", slot range:" << slot_range.second->GetName()
                << ", err:" << status.Msg();
      return status;
    }
    CLS_INFO << "Reset auto compactions option after serving succeed, db id:"
             << slot_range.second->GetStorage()->GetDBId() << ", slot range:" << slot_range.second->GetName();
  }
  return Status::OK();
}

// NOTE(mingfo): This interface can only be used when the importing in the active pool sets the Running status
// after migration is completed. The caller needs to ensure the correctness of the calling scenario.
Status Cluster::SetClientWriteRunningStatusWrite() {
  std::lock_guard<std::shared_mutex> guard(shared_mutex_);
  // pool is active && importing slotranges running status -> writable
  auto s = Cluster::ShiftStorageReplId(my_slot_ranges_);
  if (!s.IsOK()) return s;
  // set slotranges' running status to writable
  for (auto& my_sr : my_slot_ranges_) {
    auto stop_write_time_ms = my_sr.second->SetClientWriteRunningStatus(WriteStatus::WR);
    if (stop_write_time_ms > 0) {
      GlobalStatsInstance().IncrReplSenderStopWriteTime(my_sr.second->GetName(), stop_write_time_ms);
    }
  }
  return Status::OK();
}

std::string Cluster::GetLocalIPAddr(const std::string& addr_name) {
  const char* ip_address = getenv(addr_name.c_str());
  if (ip_address) {
    CLS_INFO << "datanode get " << addr_name << " succ. ip:" << ip_address;
    return ip_address;
  }

  std::unordered_map<std::string, std::string> ip_addresses;  // <if_name, ip>
  ifaddrs* if_addr_struct = nullptr;
  std::unique_ptr<ifaddrs, decltype(&freeifaddrs)> ifaddrs_ptr(nullptr, &freeifaddrs);
  if (getifaddrs(&if_addr_struct) == -1) {
    CLS_INFO << "get local " << addr_name << " addrs failed";
    return "";
  }
  ifaddrs_ptr.reset(if_addr_struct);

  for (ifaddrs* ifa = if_addr_struct; ifa; ifa = ifa->ifa_next) {
    if (!ifa->ifa_addr) {
      continue;
    }
    void* tmp_addr_ptr = nullptr;
    if (ifa->ifa_addr->sa_family == AF_INET) {
      // check it is IPv4
      tmp_addr_ptr = &((sockaddr_in*)ifa->ifa_addr)->sin_addr;
      char address_buffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmp_addr_ptr, address_buffer, INET_ADDRSTRLEN);
      ip_addresses.insert({ifa->ifa_name, address_buffer});
    } else if (ifa->ifa_addr->sa_family == AF_INET6) {
      // check it is IPv6
      tmp_addr_ptr = &((sockaddr_in6*)ifa->ifa_addr)->sin6_addr;
      char address_buffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmp_addr_ptr, address_buffer, INET6_ADDRSTRLEN);
      ip_addresses.insert({ifa->ifa_name, address_buffer});
    }
  }

  std::vector<std::string> if_names;
  if_names.emplace_back("eth0");
  if_names.emplace_back("bond0.1000");
  if_names.emplace_back("bond0");

  for (const auto& name : if_names) {
    if (ip_addresses.find(name) != ip_addresses.end()) {
      CLS_INFO << "datanode get " << addr_name << " succ. if_name:" << name << ", ip:" << ip_addresses[name];
      return ip_addresses[name];
    }
  }
  CLS_ERROR << "datanode get " << addr_name << " fail.";

  return "";
}

void Cluster::InitControllerRpcRequest(kv::controller::v1::ReportDataNodeRequest& req, bool is_blocked) {
  bool log_serving_status = false;
  auto serving_status = kv::controller::v1::Datanode::SERVING_STATUS_UNSPECIFIED;

  std::shared_lock<std::shared_mutex> lk(shared_mutex_);

  req.set_is_blocked(is_blocked);
  req.set_cluster_id(cluster_id_);
  req.set_pool(pool_);
  req.set_id(datanode_id_);
  req.set_healthy_status(kv::controller::v1::Datanode_HealthyStatus::Datanode_HealthyStatus_HEALTHY_STATUS_OK);
  // report first version = 0. controller valid version: 1, 2, ...
  req.set_version(version_.load());
  req.set_node_ip(local_host_);
  {
    req.mutable_ip_addr()->set_ip(local_addr_);
    req.mutable_ip_addr()->set_port(srv_->GetConfig()->port);
  }

  for (const auto& sl : my_slot_ranges_) {
    kv::controller::v1::SlotRangeStatus status;

    kv::controller::v1::SlotRangeIndex index;
    index.set_start(sl.second->GetRangeStart());
    index.set_end(sl.second->GetRangeEnd());
    status.mutable_index()->CopyFrom(index);

    status.set_healthy_status(sl.second->HealthyStatus());

    status.set_replication_status(sl.second->ReplicationStatus());

    *req.add_slot_range_status_list() = status;
  }

  if (my_datanode_) {
    log_serving_status = true;
    serving_status = my_datanode_->ServingStatus();
  }

  lk.unlock();

  // NOTE(yanling.chen): migration info don't get in cluster mutex
  auto progress_info = srv_->migration->GetProgressInfo();
  req.mutable_migration_progress_update()->set_task_id(progress_info.first);
  req.mutable_migration_progress_update()->set_result(progress_info.second);

  CLS_INFO << req;
  if (log_serving_status) {
    CLS_INFO << "local datanode serving status:" << kv::controller::v1::Datanode_ServingStatus_Name(serving_status);
  }
}

std::shared_ptr<SlotRange> Cluster::GetSlotRangeByIndex(const kv::controller::v1::SlotRangeIndex& index) {
  return GetSlotRangeByIndex(index.start(), index.end());
}

std::shared_ptr<SlotRange> Cluster::GetSlotRangeByIndex(uint16_t start, uint16_t end) {
  if (start < 0 || start >= kClusterSlots || start > end || end < 0 || end >= kClusterSlots) {
    return nullptr;
  }
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto sr_name = CreateSlotRangeName(start, end);
  auto it = my_slot_ranges_.find(sr_name);
  if (it == my_slot_ranges_.end()) {
    CLS_INFO << fmt::format("{} is not belonging to current datanode", sr_name);
    return nullptr;
  }
  return it->second;
}

std::set<std::string> Cluster::GetAllLocalSlotRangeNames() {
  std::set<std::string> slot_range_names;
  std::shared_lock<std::shared_mutex> lock(shared_mutex_);
  for (const auto& sl : my_slot_ranges_) {
    slot_range_names.emplace(sl.first);
  }
  return slot_range_names;
}

Status Cluster::CanScriptExecbyMyself() {
  std::shared_lock<std::shared_mutex> lock(shared_mutex_);
  // check running status
  for (const auto& sr : my_slot_ranges_) {
    if (sr.second->GetClientRunningStatus() == WriteStatus::RO) {
      return Status{Status::NotOK, "node is read only"};
    }
  }
  // check serving status
  if (!my_datanode_->IsServing()) {
    return Status{Status::NotOK, "node is not serving"};
  }
  // check client writable
  if (!my_datanode_->IsClientWritable()) {
    return Status{Status::NotOK, "node is not writable"};
  }
  return Status::OK();
}

Status Cluster::CheckAndGetSlotRangesServedByMySelf(
    const std::set<std::string>& slot_range_names,
    std::unordered_map<std::string, std::shared_ptr<SlotRange>>* slot_ranges) {
  std::shared_lock<std::shared_mutex> lock(shared_mutex_);
  for (const auto& name : slot_range_names) {
    auto it = my_slot_ranges_.find(name);
    if (it == my_slot_ranges_.end()) {
      return Status{Status::NotFound, fmt::format("{} is not in my local slot_ranges", name)};
    }
    if (slot_ranges != nullptr) {
      slot_ranges->emplace(it->first, it->second);
    }
  }
  return Status::OK();
}

void Cluster::ClearWriteRunningStatusLocked() {
  for (auto& sl : my_slot_ranges_) {
    auto stop_write_time_ms = sl.second->SetClientWriteRunningStatus(WriteStatus::UNSPECIFIED);
    if (stop_write_time_ms > 0) {
      GlobalStatsInstance().IncrReplSenderStopWriteTime(sl.second->GetName(), stop_write_time_ms);
    }
    sl.second->SetDtsWriteRunningStatus(WriteStatus::UNSPECIFIED);
  }
}

OptionalSyncError Cluster::CanSyncSendData(const std::string& node_id, bool across_pool) {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  // my_datanode_ must be valid when topo has inited
  if (!TopoHasInited()) {
    return UnknownSyncError("datanode topo not init");
  }
  if (across_pool && !is_active_pool_.load()) {
    return ClusterRoleMismatchSyncError("datanode not in active pool");
  }
  if (!my_datanode_->IsServing()) {
    return SlotRangeNodeServingStatusMismatchSyncError("datandoe role not serving");
  }
  if (across_pool && !my_datanode_->IsDtsReadable()) {
    return SlotRangeStatusMismatchSyncError("datanode dts status not readable");
  }

  if (!across_pool && my_datanode_->GetImportingId().count(node_id) <= 0) {
    return SlotRangeNodeIdNotFoundSyncError("datanode not serving of puller");
  }

  return std::nullopt;
}

OptionalSyncError Cluster::CanSyncReceiveDataCrossPool(const std::string& node_id) {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  // my_datanode_ must be valid when topo has inited
  if (!TopoHasInited()) {
    return UnknownSyncError("datanode topo not init");
  }
  if (is_active_pool_.load()) {
    return ClusterRoleMismatchSyncError("datanode not in standby pool");
  }
  if (!my_datanode_->IsServing()) {
    return SlotRangeNodeServingStatusMismatchSyncError("datanode serving status not serving");
  }
  if (!my_datanode_->IsDtsWritable()) {
    return SlotRangeStatusMismatchSyncError("datanode dts status not writable");
  }

  return std::nullopt;
}

StatusOr<kv::controller::v1::IPAddr> Cluster::GetDatanodeAddr(const std::string& node_id) {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto it = datanodes_.find(node_id);
  if (it == datanodes_.end()) {
    return {Status::NotFound, "node not exist"};
  }
  return it->second->Addr();
}

StatusOr<std::string> Cluster::GetDatanodeGrpcAddr(const std::string& node_id) {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto it = datanodes_.find(node_id);
  if (it != datanodes_.end()) {
    auto port = Config::GetGrpcPort(it->second->Addr().port());
    return it->second->Addr().ip() + ":" + std::to_string(port);
  }
  return {Status::NotFound, "node not exist"};
}

kv::controller::v1::Datanode::MigrationResult Cluster::SlotRangesReplicationResult() {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);

  bool replicating = false;
  for (const auto& range : my_slot_ranges_) {
    auto st = range.second->ReplicationStatus();
    CLS_INFO << "slot range:" << range.second->GetName()
             << ", replication status:" << kv::controller::v1::SlotRange_ReplicationStatus_Name(st);
    switch (st) {
      case kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED:
        replicating = true;
        break;
      case kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATING:
        replicating = true;
        break;
      case kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED:
        break;
      case kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_TIMEOUT:
        return kv::controller::v1::Datanode::MIGRATION_RESULT_STOP_WRITE_TIMEOUT;
      default:
        return kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL;
    }
  }
  if (replicating) {
    return kv::controller::v1::Datanode::MIGRATION_RESULT_DOING;
  }
  return kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH;
}

OptionalSyncError Cluster::CanSyncPullDataSamePool(const std::string& node_id) const {
  // can pull sync data from node_id or not
  // 1. self role should be importing
  // 2. node_id should be serving in my pool
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  if (!my_datanode_->IsImporting()) {
    return SlotRangeNodeServingStatusMismatchSyncError("role should be serving");
  }

  // NOTE: Check whether the serving node of slot_range is correct by the upper layer
  return std::nullopt;
}

StatusOr<std::shared_ptr<SlotRange>> Cluster::GetSlotRangeBySlotId(int slot_id) {
  if (slot_id < 0 || slot_id >= kClusterSlots) {
    return {Status::NotOK, "invalid slot id"};
  }
  if (version_.load() <= 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto slot_range = route_slot_ranges_[slot_id];
  if (!slot_range) {
    return {Status::NotOK, "slot range not found in topo"};
  }
  auto iter = my_slot_ranges_.find(slot_range->name);
  if (iter == my_slot_ranges_.end()) {
    return {Status::NotOK, "slot range not found in local"};
  }
  return iter->second;
}

StatusOr<std::pair<uint64_t, std::string>> Cluster::GetSlotRangeInfo(int slot_id) {
  if (slot_id < 0 || slot_id >= kClusterSlots) {
    return {Status::NotOK, "invalid slot id"};
  }
  if (version_.load() <= 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto slot_range = route_slot_ranges_[slot_id];
  if (!slot_range) {
    return {Status::NotOK, "no such slot range"};
  }
  std::ostringstream ss;
  ss << slot_range->name << " " << slot_range->serving_node_id;
  auto iter = my_slot_ranges_.find(slot_range->name);
  if (iter != my_slot_ranges_.end() && iter->second) {
    auto ret = iter->second->GetRunningStatus();
    ss << " " << ret.first << " " << ret.second;
  }
  return {version_.load(), ss.str()};
}

StatusOr<std::pair<uint64_t, std::vector<std::string>>> Cluster::GetSlotRangesInfo() {
  if (version_.load() <= 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  std::vector<std::string> slot_ranges;
  for (size_t start = 0; start < kClusterSlots;) {
    auto slot_range = route_slot_ranges_[start];
    if (!slot_range) {
      ++start;
      continue;
    }
    std::ostringstream ss;
    ss << slot_range->name << " " << slot_range->serving_node_id;
    auto iter = my_slot_ranges_.find(slot_range->name);
    if (iter != my_slot_ranges_.end() && iter->second) {
      auto ret = iter->second->GetRunningStatus();
      ss << " " << ret.first << " " << ret.second;
    }
    slot_ranges.emplace_back(ss.str());
    start = slot_range->end + 1;
  }
  return {version_.load(), slot_ranges};
}

StatusOr<std::pair<uint64_t, std::string>> Cluster::GetDatanodeInfo(const std::string& node_id) {
  if (version_.load() <= 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto iter = datanodes_.find(node_id);
  if (iter == datanodes_.end() || !iter->second) {
    return {Status::NotOK, "no such datanode"};
  }
  std::ostringstream ss;
  ss << *(iter->second.get());
  ss << " " << getSlotRangesDescLocked(node_id);
  return {version_.load(), ss.str()};
}

StatusOr<std::pair<uint64_t, std::vector<std::string>>> Cluster::GetDatanodesInfo() {
  if (version_.load() <= 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  auto node_slots_map = getSlotRangesDescLocked();
  std::vector<std::string> datanodes;
  datanodes.reserve(datanodes_.size());
  for (auto& [_, node] : datanodes_) {
    if (!node) continue;
    std::ostringstream ss;
    ss << *(node.get());
    auto iter = node_slots_map.find(node->NodeId());
    if (iter != node_slots_map.end()) {
      ss << " " << iter->second.str();
    } else {
      ss << " none";
    }
    datanodes.emplace_back(ss.str());
  }
  return {version_.load(), datanodes};
}

std::string Cluster::getSlotRangesDescLocked(const std::string& node_id) {
  std::ostringstream ss;
  for (size_t start = 0; start < kClusterSlots;) {
    auto slot_range = route_slot_ranges_[start];
    if (!slot_range) {
      ++start;
      continue;
    }
    if (slot_range->serving_node_id == node_id) {
      if (ss.tellp() > 0) {
        ss << ",";
      }
      ss << slot_range->name;
    }
    start = slot_range->end + 1;
  }
  return ss.tellp() > 0 ? ss.str() : "none";
}

std::unordered_map<std::string, std::ostringstream> Cluster::getSlotRangesDescLocked() {
  std::unordered_map<std::string, std::ostringstream> node_slots_map;
  for (size_t start = 0; start < kClusterSlots;) {
    auto slot_range = route_slot_ranges_[start];
    if (!slot_range) {
      ++start;
      continue;
    }
    auto iter = node_slots_map.find(slot_range->serving_node_id);
    if (iter == node_slots_map.end()) {
      std::ostringstream oss;
      oss << slot_range->name;
      node_slots_map.emplace(slot_range->serving_node_id, std::move(oss));
    } else {
      iter->second << "," << slot_range->name;
    }
    start = slot_range->end + 1;
  }
  return node_slots_map;
}

Status Cluster::CheckKeyInSlotRange(const std::string& slot_range, const std::string& key) {
  auto slot = GetSlotIdFromKey(key);
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  if (slot_range == route_slot_ranges_[slot]->name) return Status::OK();
  return Status{Status::NotOK,
                fmt::format("key:{} belongs to {} not in {}", key, route_slot_ranges_[slot]->name, slot_range)};
}

OptionalSyncError Cluster::CanSendCDCData() {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  // my_datanode_ must be valid when topo has inited
  if (!TopoHasInited()) {
    return UnknownSyncError("datanode topo not init");
  }
  if (!is_active_pool_.load()) {
    return ClusterRoleMismatchSyncError("datanode not in active pool");
  }
  if (!my_datanode_->IsServing()) {
    return SlotRangeNodeServingStatusMismatchSyncError("datandoe role not serving");
  }
  return std::nullopt;
}

OptionalSyncError SlotRange::CanSyncReceiveDataCrossPool(const std::string& node_id) const {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  if (dts_write_running_status_ == WriteStatus::RO) {
    return SlotRangeStatusMismatchSyncError("datanode dts running status not writable");
  }

  if (dts_write_running_status_ == WriteStatus::WR_PROHIBITED) {
    return SlotRangeStatusMismatchSyncError("datanode dts running status read and write prohibited");
  }

  return std::nullopt;
}

OptionalSyncError SlotRange::CanSyncSendDataCrossPool(const std::string& node_id) const {
  std::shared_lock<std::shared_mutex> lk(shared_mutex_);
  if (dts_write_running_status_ == WriteStatus::WR_PROHIBITED) {
    return SlotRangeStatusMismatchSyncError("datanode dts running status read and write prohibited");
  }

  return std::nullopt;
}

StatusOr<kv::datanode::v1::SyncPoint> SlotRange::GetSyncPoint() {
  if (!storage_) {
    return {Status::NotOK, "not such slot range at local"};
  }
  return storage_->GetSyncPoint();
}

StatusOr<kv::datanode::v1::SyncPoint> SlotRange::GetSyncPoint(rocksdb::SequenceNumber seq_id) {
  if (!storage_) {
    return {Status::NotOK, "not such slot range at local"};
  }
  return storage_->GetSyncPoint(seq_id);
}

StatusOr<kv::datanode::v1::CDCPoint> SlotRange::GetCDCPoint() {
  if (!storage_) {
    return {Status::NotOK, "not such slot range at local"};
  }
  return storage_->GetCDCPoint();
}

StatusOr<kv::datanode::v1::CDCPoint> SlotRange::GetCDCPoint(rocksdb::SequenceNumber seq_id) {
  if (!storage_) {
    return {Status::NotOK, "not such slot range at local"};
  }
  return storage_->GetCDCPoint(seq_id);
}

StatusOr<kv::datanode::v1::CDCPoint> SlotRange::GetCDCRestartPoint() {
  if (!storage_) {
    return {Status::NotOK, "not such slot range at local"};
  }
  return storage_->GetCDCRestartPoint();
}

StatusOr<kv::datanode::v1::CDCPoint> SlotRange::GetCDCOldestPoint() {
  if (!storage_) {
    return {Status::NotOK, "not such slot range at local"};
  }
  return storage_->GetCDCOldestPoint();
}

int64_t SlotRange::SetClientWriteRunningStatus(WriteStatus status) {
  uint64_t version = 0;
  return SetClientWriteRunningStatus(status, version, false);
}

int64_t SlotRange::SetClientWriteRunningStatus(WriteStatus status, uint64_t& version, bool check_version) {
  int64_t stop_write_time_ms = 0;
  {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    if (check_version && version != client_write_running_status_version_) {
      CLS_INFO << "Skip set client running status for version changed, slot range:" << info_.name
               << ", expect:" << version << ", actual: " << client_write_running_status_version_;
      return 0;
    }
    if (client_write_running_status_ != WriteStatus::RO && status == WriteStatus::RO) {
      stop_write_time_point_ = std::chrono::steady_clock::now();
    }
    if (client_write_running_status_ == WriteStatus::RO && status != WriteStatus::RO) {
      if (stop_write_time_point_ != kNoStopWrteTimepoint) {
        stop_write_time_ms = util::GetDurationMSSince(stop_write_time_point_);
        stop_write_time_point_ = kNoStopWrteTimepoint;
      }
    }
    version = ++client_write_running_status_version_;
    client_write_running_status_ = status;
  }
  CLS_INFO << "Set client running status to " << status << ", slot range:" << info_.name;
  return stop_write_time_ms;
}

void SlotRange::SetDtsWriteRunningStatus(WriteStatus status) {
  uint64_t version = 0;
  return SetDtsWriteRunningStatus(status, version, false);
}

void SlotRange::SetDtsWriteRunningStatus(WriteStatus status, uint64_t& version, bool check_version) {
  {
    std::lock_guard<std::shared_mutex> guard(shared_mutex_);
    if (check_version && version != dts_write_running_status_version_) {
      CLS_INFO << "Skip set dts running status for version changed, slot range:" << info_.name << ", expect:" << version
               << ", actual: " << dts_write_running_status_version_;
      return;
    }
    version = ++dts_write_running_status_version_;
    dts_write_running_status_ = status;
  }
  CLS_INFO << "Set dts running status to " << status << ", slot range:" << info_.name;
}

}  // namespace redis
