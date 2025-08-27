#include "common/sync_status.h"

#define CONST_ERROR_STATUS_GENERATE(name, msg)        \
  const Error k##name##Error = name##SyncError(#msg); \
  const grpc::Status k##name##Status = name##SyncStatus(#msg)

const grpc::Status kOkStatus = grpc::Status();
CONST_ERROR_STATUS_GENERATE(Unknown, unknown error);
CONST_ERROR_STATUS_GENERATE(ClusterIdMismatch, cluster id mismatch);
CONST_ERROR_STATUS_GENERATE(ClusterRoleMismatch, cluster role mismatch);
CONST_ERROR_STATUS_GENERATE(SlotRangeNotFound, slot range not found);
CONST_ERROR_STATUS_GENERATE(SlotRangeStatusMismatch, slot range status mismatch);
CONST_ERROR_STATUS_GENERATE(SlotRangeNodeIdNotFound, node id not found in slot range);
CONST_ERROR_STATUS_GENERATE(SlotRangeNodeServingStatusMismatch, node serving status mismatch in slot range);
CONST_ERROR_STATUS_GENERATE(SyncSeqIdOutOfRange, seq id out of range to sync);
CONST_ERROR_STATUS_GENERATE(SyncReplicaIdMismatch, replica id mismatch to sync);
CONST_ERROR_STATUS_GENERATE(SyncLogTimestampMismatch, log timestamp mismatch to sync);
CONST_ERROR_STATUS_GENERATE(SyncPointMismatch, sync point mismatch);
CONST_ERROR_STATUS_GENERATE(SyncStreamExisted, sync stream existed);
CONST_ERROR_STATUS_GENERATE(SyncStopWriteTimeout, sync stop write timeout);
CONST_ERROR_STATUS_GENERATE(ClusterTopologyChanged, cluster topology changed);
CONST_ERROR_STATUS_GENERATE(MigrationTaskFailed, migration task over failed);
CONST_ERROR_STATUS_GENERATE(StreamUnavailable, stream unavailable);
CONST_ERROR_STATUS_GENERATE(DataNodeIdMismatch, datanode id mismatch);
CONST_ERROR_STATUS_GENERATE(MigrationTaskDoing, migration task doing);
CONST_ERROR_STATUS_GENERATE(TimeoutPointExceeded, timeout point exceeded);
CONST_ERROR_STATUS_GENERATE(CdcSeqIdExceeded, cdc seq id exceeded);
CONST_ERROR_STATUS_GENERATE(CdcSeqIdMismatch, cdc seq id mismatch);
CONST_ERROR_STATUS_GENERATE(CdcReplIdMismatch, cdc replica id mismatch);
CONST_ERROR_STATUS_GENERATE(CdcLogTsMismatch, cdc log ts mismatch);
CONST_ERROR_STATUS_GENERATE(CdcWalParseError, cdc wal parse error);
CONST_ERROR_STATUS_GENERATE(CdcWalDeleted, cdc wal deleted);
