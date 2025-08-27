#pragma once

#include <google/rpc/status.pb.h>  // NOLINT
#include <grpcpp/impl/status.h>
#include <kv/controller/v1/model.pb.h>
#include <kv/datanode/v1/common.pb.h>

using kv::controller::v1::SlotRange;
using kv::datanode::v1::Error;
using kv::datanode::v1::ErrorCode;
using OptionalSyncError = std::optional<Error>;

extern const Error kUnknownError;
extern const Error kClusterIdMismatchError;
extern const Error kClusterRoleMismatchError;
extern const Error kSlotRangeNotFoundError;
extern const Error kSlotRangeStatusMismatchError;
extern const Error kSlotRangeNodeIdNotFoundError;
extern const Error kSlotRangeNodeServingStatusMismatchError;
extern const Error kSyncSeqIdOutOfRangeError;
extern const Error kSyncReplicaIdMismatchError;
extern const Error kSyncLogTimestampMismatchError;
extern const Error kSyncPointMismatchError;
extern const Error kSyncStreamExistedError;
extern const Error kSyncStopWriteTimeoutError;
extern const Error kClusterTopologyChangedError;
extern const Error kMigrationTaskFailedError;
extern const Error kStreamUnavailableError;
extern const Error kDataNodeIdMismatchError;
extern const Error kMigrationTaskDoingError;
extern const Error kTimeoutPointExceededError;
extern const Error kCdcSeqIdExceededError;
extern const Error kCdcSeqIdMismatchError;
extern const Error kCdcReplIdMismatchError;
extern const Error kCdcLogTsMismatchError;
extern const Error kCdcWalParseError;
extern const Error kCdcWalDeletedError;

extern const grpc::Status kOkStatus;
extern const grpc::Status kUnknownStatus;
extern const grpc::Status kClusterIdMismatchStatus;
extern const grpc::Status kClusterRoleMismatchStatus;
extern const grpc::Status kSlotRangeNotFoundStatus;
extern const grpc::Status kSlotRangeStatusMismatchStatus;
extern const grpc::Status kSlotRangeNodeIdNotFoundStatus;
extern const grpc::Status kSlotRangeNodeServingStatusMismatchStatus;
extern const grpc::Status kSyncSeqIdOutOfRangeStatus;
extern const grpc::Status kSyncReplicaIdMismatchStatus;
extern const grpc::Status kSyncLogTimestampMismatchStatus;
extern const grpc::Status kSyncPointMismatchStatus;
extern const grpc::Status kSyncStreamExistedStatus;
extern const grpc::Status kSyncStopWriteTimeoutStatus;
extern const grpc::Status kClusterTopologyChangedStatus;
extern const grpc::Status kMigrationTaskFailedStatus;
extern const grpc::Status kStreamUnavailableStatus;
extern const grpc::Status kDataNodeIdMismatchStatus;
extern const grpc::Status kMigrationTaskDoingStatus;
extern const grpc::Status kTimeoutPointExceededStatus;
extern const grpc::Status kCdcSeqIdExceededStatus;
extern const grpc::Status kCdcSeqIdMismatchStatus;
extern const grpc::Status kCdcReplIdMismatchStatus;
extern const grpc::Status kCdcLogTsMismatchStatus;
extern const grpc::Status kCdcWalParseStatus;
extern const grpc::Status kCdcWalDeletedStatus;

inline Error SyncError(ErrorCode code, const std::string& msg) {
  Error error;
  error.set_code(code);
  error.set_message(msg);
  return error;
};

inline grpc::Status SyncStatus(const Error& error) {
  google::rpc::Status s;
  s.add_details()->PackFrom(error);
  s.set_code(static_cast<int>(grpc::StatusCode::UNKNOWN));
  return {grpc::StatusCode::UNKNOWN, "", s.SerializeAsString()};
};

inline grpc::Status SyncStatus(const OptionalSyncError& error) {
  if (error.has_value()) {
    return SyncStatus(error.value());
  }
  return kOkStatus;
};

#define ERROR_STATUS_FUNC_GENERATE(code, name)                                                                  \
  inline Error name##SyncError(const std::string& msg) { return SyncError(ErrorCode::ERROR_CODE_##code, msg); } \
                                                                                                                \
  inline grpc::Status name##SyncStatus(const std::string& msg) {                                                \
    auto error = name##SyncError(msg);                                                                          \
    return SyncStatus(error);                                                                                   \
  }

ERROR_STATUS_FUNC_GENERATE(UNSPECIFIED, Unknown);
ERROR_STATUS_FUNC_GENERATE(CLUSTER_ID_MISMATCH, ClusterIdMismatch);
ERROR_STATUS_FUNC_GENERATE(CLUSTER_ROLE_MISMATCH, ClusterRoleMismatch);
ERROR_STATUS_FUNC_GENERATE(SLOT_RANGE_NOT_FOUND, SlotRangeNotFound);
ERROR_STATUS_FUNC_GENERATE(SLOT_RANGE_STATUS_MISMATCH, SlotRangeStatusMismatch);
ERROR_STATUS_FUNC_GENERATE(SLOT_RANGE_NODE_ID_NOT_FOUND, SlotRangeNodeIdNotFound);
ERROR_STATUS_FUNC_GENERATE(SLOT_RANGE_NODE_SERVING_STATUS_MISMATCH, SlotRangeNodeServingStatusMismatch);
ERROR_STATUS_FUNC_GENERATE(SYNC_SEQ_ID_OUT_OF_RANGE, SyncSeqIdOutOfRange);
ERROR_STATUS_FUNC_GENERATE(SYNC_REPLICA_ID_MISMATCH, SyncReplicaIdMismatch);
ERROR_STATUS_FUNC_GENERATE(SYNC_LOG_TIMESTAMP_MISMATCH, SyncLogTimestampMismatch);
ERROR_STATUS_FUNC_GENERATE(SYNC_POINT_MISMATCH, SyncPointMismatch);
ERROR_STATUS_FUNC_GENERATE(SYNC_STREAM_EXISTED, SyncStreamExisted);
ERROR_STATUS_FUNC_GENERATE(SYNC_STOP_WRITE_TIMEOUT, SyncStopWriteTimeout);
ERROR_STATUS_FUNC_GENERATE(CLUSTER_TOPOLOGY_CHANGED, ClusterTopologyChanged);
ERROR_STATUS_FUNC_GENERATE(MIGRATION_TASK_FAILED, MigrationTaskFailed);
ERROR_STATUS_FUNC_GENERATE(STREAM_UNAVAILABLE, StreamUnavailable)
ERROR_STATUS_FUNC_GENERATE(DATA_NODE_ID_MISMATCH, DataNodeIdMismatch);
ERROR_STATUS_FUNC_GENERATE(MIGRATION_TASK_DOING, MigrationTaskDoing);
ERROR_STATUS_FUNC_GENERATE(TIMEOUT_POINT_EXCEEDED, TimeoutPointExceeded);
ERROR_STATUS_FUNC_GENERATE(CDC_SEQ_ID_EXCEEDED, CdcSeqIdExceeded);
ERROR_STATUS_FUNC_GENERATE(CDC_SEQ_ID_MISMATCH, CdcSeqIdMismatch);
ERROR_STATUS_FUNC_GENERATE(CDC_REPL_ID_MISMATCH, CdcReplIdMismatch);
ERROR_STATUS_FUNC_GENERATE(CDC_LOG_TS_MISMATCH, CdcLogTsMismatch);
ERROR_STATUS_FUNC_GENERATE(CDC_WAL_PARSE_ERROR, CdcWalParseError);
ERROR_STATUS_FUNC_GENERATE(CDC_WAL_DELETED, CdcWalDeleted);

inline std::ostream& operator<<(std::ostream& os, const OptionalSyncError& error) {
  if (error.has_value()) {
    os << "[failed, code=" << error->code() << ", msg=" << error->message() << "]";
  } else {
    os << "[ok]";
  }
  return os;
}

inline std::optional<SlotRange::ReplicationStatus> SyncErrorToReplStatus(ErrorCode code) {
  switch (code) {
    case ErrorCode::ERROR_CODE_CLUSTER_ID_MISMATCH:
    case ErrorCode::ERROR_CODE_CLUSTER_ROLE_MISMATCH:
    case ErrorCode::ERROR_CODE_SLOT_RANGE_NOT_FOUND:
    case ErrorCode::ERROR_CODE_SLOT_RANGE_STATUS_MISMATCH:
    case ErrorCode::ERROR_CODE_SLOT_RANGE_NODE_ID_NOT_FOUND:
    case ErrorCode::ERROR_CODE_SLOT_RANGE_NODE_SERVING_STATUS_MISMATCH:
      return SlotRange::REPLICATION_STATUS_ERROR_TOPOLOGY_MISMATCH;
    case ErrorCode::ERROR_CODE_SYNC_SEQ_ID_OUT_OF_RANGE:
      return SlotRange::REPLICATION_STATUS_ERROR_LOG_GAP;
    case ErrorCode::ERROR_CODE_SYNC_REPLICA_ID_MISMATCH:
    case ErrorCode::ERROR_CODE_SYNC_LOG_TIMESTAMP_MISMATCH:
      return SlotRange::REPLICATION_STATUS_ERROR_LOG_CONFLICT;
    case ErrorCode::ERROR_CODE_SYNC_STOP_WRITE_TIMEOUT:
      return SlotRange::REPLICATION_STATUS_ERROR_TIMEOUT;
    case ErrorCode::ERROR_CODE_CLUSTER_TOPOLOGY_CHANGED:
      return SlotRange::REPLICATION_STATUS_ERROR_TOPOLOGY_MISMATCH;
    default:
      return SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN;
  }
}

inline bool IsStopWriteTimeoutError(const OptionalSyncError& error) {
  if (!error.has_value()) {
    return false;
  }
  return error->code() == kv::datanode::v1::ERROR_CODE_SYNC_STOP_WRITE_TIMEOUT;
}

inline OptionalSyncError ParseSyncError(const grpc::Status& status) {
  if (status.ok()) {
    return std::nullopt;
  }
  if (!status.error_details().empty()) {
    google::rpc::Status rpc_status;
    if (rpc_status.ParseFromString(status.error_details())) {
      kv::datanode::v1::Error error;
      for (auto& detail : rpc_status.details()) {
        if (detail.UnpackTo(&error)) {
          return error;
        }
      }
    }
  }
  std::stringstream ss;
  ss << "bad grpc status, code: " << status.error_code() << ", message: " << status.error_message()
     << ", details: " << status.error_details();
  return UnknownSyncError(ss.str());
}

inline std::ostream& operator<<(std::ostream& os, const std::optional<SlotRange::ReplicationStatus>& status) {
  if (status.has_value()) {
    os << SlotRange::ReplicationStatus_Name(status.value());
  } else {
    os << "none";
  }
  return os;
}

enum SyncStatusEnum : int {
  Init = 0,
  Syncing = 1,
  WriteStop = 2,
  CaughtUp = 3,
  Done = 4,
  Size = 5,
};

inline std::string SyncStatusEnumToString(const SyncStatusEnum& status) {
  switch (status) {
    case SyncStatusEnum::Init:
      return "init";
    case SyncStatusEnum::Syncing:
      return "syncing";
    case SyncStatusEnum::WriteStop:
      return "write-stop";
    case SyncStatusEnum::CaughtUp:
      return "caught-up";
    case SyncStatusEnum::Done:
      return "done";
    default:
      return "unknown";
  }
}

inline std::ostream& operator<<(std::ostream& os, const SyncStatusEnum& status) {
  os << SyncStatusEnumToString(status);
  return os;
}

enum SyncStreamType {
  DtsSender = 0,
  DtsRecver = 1,
  ReplSender = 2,
  ReplPuller = 3,
};
