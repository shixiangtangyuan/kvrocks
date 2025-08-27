#include <gtest/gtest.h>
#include <kv/datanode/v1/common.pb.h>

#include "common/sync_status.h"
#include "mock/mock_server.h"
#include "sync_test_util.h"

namespace redis {

TEST(SyncErrorReport, Base) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto status = ApplyTopo(srv, db_id, false, true, start, end);
  ASSERT_TRUE(status.IsOK());
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range != nullptr);
  auto storage = srv.GetStorage(db_id);
  ASSERT_TRUE(storage != nullptr);

  auto ret = slot_range->GetSyncPoint();
  ASSERT_TRUE(ret.IsOK());
  auto valid_sync_point = ret.GetValue();
  auto valid_status = kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED;
  kv::datanode::v1::ReportSyncErrorRequest valid_req;
  valid_req.set_cluster_id(opt.cluster_id);
  valid_req.mutable_slot_range()->set_start(start);
  valid_req.mutable_slot_range()->set_end(end);
  valid_req.mutable_sync_point()->CopyFrom(valid_sync_point);
  valid_req.mutable_sync_error()->set_code(kv::datanode::v1::ERROR_CODE_SYNC_SEQ_ID_OUT_OF_RANGE);

  grpc::CallbackServerContext ctx;
  kv::datanode::v1::ReportSyncErrorRequest req;
  kv::datanode::v1::ReportSyncErrorResponse resp;
  // request success
  req.CopyFrom(valid_req);
  auto repl_status_ret = SyncErrorToReplStatus(req.sync_error().code());
  ASSERT_TRUE(repl_status_ret.has_value());
  auto exp_repl_status = repl_status_ret.value();
  ASSERT_NE(exp_repl_status, valid_status);
  ASSERT_NE(slot_range->ReplicationStatus(), exp_repl_status);
  srv.ReportSyncError(&ctx, &req, &resp);
  ASSERT_EQ(slot_range->ReplicationStatus(), exp_repl_status);
  slot_range->SetReplicationStauts(valid_status);
  // wrong cluster id
  req.CopyFrom(valid_req);
  req.set_cluster_id("wrong " + opt.cluster_id);
  srv.ReportSyncError(&ctx, &req, &resp);
  ASSERT_EQ(slot_range->ReplicationStatus(), valid_status);
  // wrong slot range
  req.CopyFrom(valid_req);
  req.mutable_slot_range()->set_start(start + 1);
  srv.ReportSyncError(&ctx, &req, &resp);
  ASSERT_EQ(slot_range->ReplicationStatus(), valid_status);
  // wrong sync point
  req.CopyFrom(valid_req);
  req.mutable_sync_point()->set_next_seq_id(valid_sync_point.next_seq_id() + 1);
  srv.ReportSyncError(&ctx, &req, &resp);
  ASSERT_EQ(slot_range->ReplicationStatus(), valid_status);
  // report sync error
  req.CopyFrom(valid_req);
  for (int i = kv::datanode::v1::ErrorCode_MIN; i <= kv::datanode::v1::ErrorCode_MAX; i++) {
    auto code = (ErrorCode)(i);
    req.mutable_sync_error()->set_code(code);
    slot_range->SetReplicationStauts(valid_status);
    srv.ReportSyncError(&ctx, &req, &resp);
    auto repl_status_ret = SyncErrorToReplStatus(code);
    ASSERT_TRUE(repl_status_ret.has_value());
    ASSERT_NE(repl_status_ret.value(), valid_status);
    ASSERT_EQ(slot_range->ReplicationStatus(), repl_status_ret.value());
  }
}

}  // namespace redis
