#include "common/sync_status.h"

#include <grpcpp/impl/status.h>
#include <gtest/gtest.h>
#include <kv/datanode/v1/common.pb.h>

void CheckStatus(const grpc::Status& status, const kv::datanode::v1::Error& expect_error) {
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.error_code(), grpc::StatusCode::UNKNOWN);
  ASSERT_TRUE(status.error_message().empty());
  ASSERT_FALSE(status.error_details().empty());
  auto parse_err = ParseSyncError(status);
  ASSERT_TRUE(parse_err.has_value());
  ASSERT_EQ(parse_err->code(), expect_error.code());
  ASSERT_EQ(parse_err->message(), expect_error.message());
}

TEST(SyncStatus, Base) {
  // check grpc ok status
  ASSERT_TRUE(kOkStatus.ok());
  ASSERT_FALSE(ParseSyncError(kOkStatus).has_value());
  // check grpc canceled status
  auto err = ParseSyncError(grpc::Status::CANCELLED);
  ASSERT_TRUE(err.has_value());
  ASSERT_EQ(err->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_UNSPECIFIED);
  ASSERT_FALSE(err->message().empty());
  // check datanode inner status
  CheckStatus(kUnknownStatus, kUnknownError);
  CheckStatus(kClusterIdMismatchStatus, kClusterIdMismatchError);
  CheckStatus(kClusterRoleMismatchStatus, kClusterRoleMismatchError);
  CheckStatus(kSlotRangeNotFoundStatus, kSlotRangeNotFoundError);
  CheckStatus(kSlotRangeStatusMismatchStatus, kSlotRangeStatusMismatchError);
  CheckStatus(kSlotRangeNodeIdNotFoundStatus, kSlotRangeNodeIdNotFoundError);
  CheckStatus(kSlotRangeNodeServingStatusMismatchStatus, kSlotRangeNodeServingStatusMismatchError);
  CheckStatus(kSyncSeqIdOutOfRangeStatus, kSyncSeqIdOutOfRangeError);
  CheckStatus(kSyncReplicaIdMismatchStatus, kSyncReplicaIdMismatchError);
  CheckStatus(kSyncLogTimestampMismatchStatus, kSyncLogTimestampMismatchError);
  CheckStatus(kSyncPointMismatchStatus, kSyncPointMismatchError);
  CheckStatus(kSyncStreamExistedStatus, kSyncStreamExistedError);
  CheckStatus(kSyncStopWriteTimeoutStatus, kSyncStopWriteTimeoutError);
  CheckStatus(kClusterTopologyChangedStatus, kClusterTopologyChangedError);
  CheckStatus(kMigrationTaskFailedStatus, kMigrationTaskFailedError);
  CheckStatus(kStreamUnavailableStatus, kStreamUnavailableError);
  CheckStatus(kDataNodeIdMismatchStatus, kDataNodeIdMismatchError);
  CheckStatus(kMigrationTaskDoingStatus, kMigrationTaskDoingError);
  CheckStatus(kTimeoutPointExceededStatus, kTimeoutPointExceededError);
}
