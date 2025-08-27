#pragma once

#include <kv/controller/v1/api.pb.h>
#include <kv/controller/v1/model.pb.h>
#include <kv/datanode/v1/cdc.pb.h>
#include <kv/datanode/v1/sync.pb.h>

namespace redis {

inline std::string CreateSlotRangeName(int32_t start, int32_t end) {
  return "[" + std::to_string(start) + "," + std::to_string(end) + "]";
}

inline std::string SlotRangeIndexToString(const kv::controller::v1::SlotRangeIndex& slot_range) {
  return CreateSlotRangeName(slot_range.start(), slot_range.end());
}

inline kv::datanode::v1::CDCPoint CovertSyncPointToCDCPoint(const kv::datanode::v1::SyncPoint& sync_point) {
  kv::datanode::v1::CDCPoint ret;
  ret.set_next_seq_id(sync_point.next_seq_id());
  ret.set_prev_log_ts(sync_point.prev_log_ts());
  ret.set_prev_rep_id(sync_point.prev_rep_id());
  return ret;
}

}  // namespace redis

namespace kv {

namespace controller {
namespace v1 {

inline std::ostream& operator<<(std::ostream& os, const Datanode::RWStatus& status) {
  switch (status) {
    case Datanode_RWStatus_RW_STATUS_UNSPECIFIED:
      os << "unspecified";
      break;
    case Datanode_RWStatus_RW_STATUS_RO:
      os << "read-only";
      break;
    case Datanode_RWStatus_RW_STATUS_WO:
      os << "write-only";
      break;
    case Datanode_RWStatus_RW_STATUS_RW:
      os << "read-write";
      break;
    default:
      os << "unknown";
      break;
  }
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const IPAddr& addr) {
  os << addr.ip() << ":" << addr.port();
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const Datanode::ServingStatus& status) {
  switch (status) {
    case Datanode_ServingStatus_SERVING_STATUS_UNSPECIFIED:
      os << "unspecified";
      break;
    case Datanode_ServingStatus_SERVING_STATUS_SERVING:
      os << "serving";
      break;
    case Datanode_ServingStatus_SERVING_STATUS_IMPORTING:
      os << "importing";
      break;
    default:
      os << "unknown";
      break;
  }
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const SlotRangeIndex& slot_range) {
  os << redis::CreateSlotRangeName(slot_range.start(), slot_range.end());
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const ReportDataNodeRequest& req) {
  os << "report request:{cluster_id=" << req.cluster_id() << ", pool=" << req.pool() << ", id=" << req.id()
     << ", slot_range_status_list={";

  for (const auto& slot_range_status : req.slot_range_status_list()) {
    os << slot_range_status.index()
       << "healthy_status=" << kv::controller::v1::SlotRange_HealthyStatus_Name(slot_range_status.healthy_status())
       << ", replication_status="
       << kv::controller::v1::SlotRange_ReplicationStatus_Name(slot_range_status.replication_status());
  }

  os << "}, healthy_status=" << kv::controller::v1::Datanode_HealthyStatus_Name(req.healthy_status())
     << ", ip_addr=" << req.ip_addr().ip() << ":" << req.ip_addr().port() << ", version=" << req.version()
     << ", is_backup_launcher=" << std::boolalpha << req.is_backup_launcher() << ", is_blocked=" << std::boolalpha
     << req.is_blocked() << ", migration_progress_update={task_id=" << req.migration_progress_update().task_id()
     << ", result=" << kv::controller::v1::Datanode_MigrationResult_Name(req.migration_progress_update().result())
     << "}}";

  return os;
}

}  // namespace v1
}  // namespace controller

namespace datanode {
namespace v1 {

inline std::ostream& operator<<(std::ostream& os, const SyncPoint& sync_point) {
  os << "(" << sync_point.next_seq_id() << "," << sync_point.prev_log_ts() << "," << sync_point.prev_rep_id() << ")";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const PullSyncDataRequest& req) {
  os << "[slot_range=" << req.slot_range() << ", puller_id=" << req.puller_node_id()
     << ", sync_point=" << req.sync_point() << ", max_seq=" << req.max_seq_id() << "]";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const PushSyncDataRequest& req) {
  os << "[slot_range=" << req.slot_range() << ", pusher_id=" << req.pusher_node_id()
     << ", sync_point=" << req.sync_point() << "]";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const Error& err) {
  os << "[" << err.code() << "," << err.message() << "]";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const ReportSyncErrorRequest& req) {
  os << "[slot_range=" << req.slot_range() << ", pusher_id=" << req.pusher_node_id()
     << ", sync_point=" << req.sync_point() << ", sync_error=" << req.sync_error() << "]";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const SyncDataRequest& req) {
  os << "[slot_range=" << req.slot_range() << ", puller_id=" << req.puller_node_id()
     << ", sync_point=" << req.sync_point() << ", stop_write=" << req.stop_write() << "]";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const CDCPoint& cdc_point) {
  os << "(" << cdc_point.next_seq_id() << "," << cdc_point.prev_log_ts() << "," << cdc_point.prev_rep_id() << ")";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const CDCGetEventsRequest& req) {
  os << "[slot_range=" << req.slotrange_idx() << ", cdc_point=" << req.point() << ", take_over=" << req.is_take_over();
  return os;
}

}  // namespace v1
}  // namespace datanode
}  // namespace kv
