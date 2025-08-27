#include <kv/datanode/v1/sync.pb.h>

#include "mock/mock_server.h"
#include "sync_test_util.h"

namespace redis {

TEST(SyncReceiver, Base) {
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
  ASSERT_TRUE(storage->ShiftReplId().IsOK());

  kv::datanode::v1::PushSyncDataRequest valid_req;
  valid_req.set_cluster_id(opt.cluster_id);
  valid_req.mutable_slot_range()->set_start(start);
  valid_req.mutable_slot_range()->set_end(end);
  auto ret = slot_range->GetSyncPoint();
  ASSERT_TRUE(ret.IsOK());
  auto valid_sync_point = ret.GetValue();
  valid_req.mutable_sync_point()->CopyFrom(valid_sync_point);

  grpc::CallbackServerContext ctx;
  kv::datanode::v1::PushSyncDataResponse resp;
  // read stream failed
  auto recv = srv.PushSyncData(&ctx, &resp);
  recv->OnReadDone(false);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kStreamUnavailableError.code());
  recv->OnDone();
  // stop by other
  recv = srv.PushSyncData(&ctx, &resp);
  ASSERT_TRUE(recv->MarkFinished(kSlotRangeStatusMismatchError));
  recv->OnReadDone(true);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kSlotRangeStatusMismatchError.code());
  recv->OnDone();
  // wrong cluser id
  recv = srv.PushSyncData(&ctx, &resp);
  recv->req_.CopyFrom(valid_req);
  recv->req_.set_cluster_id("wrong " + opt.cluster_id);
  recv->OnReadDone(true);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kClusterIdMismatchError.code());
  recv->OnDone();
  // wrong slot range
  ASSERT_TRUE(start < end);
  std::vector<std::pair<int, int>> invalid_ranges = {
      {start - 1, end - 1}, {start - 1, end},     {start + 1, end + 1},   {start, end - 1},
      {start, end + 1},     {start + 1, end - 1}, {start + 1, end},       {start + 1, end + 1},
      {end, start},         {kClusterSlots, end}, {start, kClusterSlots},
  };
  for (const auto& range : invalid_ranges) {
    recv = srv.PushSyncData(&ctx, &resp);
    recv->req_.CopyFrom(valid_req);
    recv->req_.mutable_slot_range()->set_start(range.first);
    recv->req_.mutable_slot_range()->set_end(range.second);
    recv->OnReadDone(true);
    ASSERT_TRUE(recv->IsFinished());
    ASSERT_EQ(recv->finish_error_->code(), kSlotRangeNotFoundError.code());
    recv->OnDone();
  }
  // wrong dts running status
  recv = srv.PushSyncData(&ctx, &resp);
  recv->req_.CopyFrom(valid_req);
  slot_range->SetDtsWriteRunningStatus(WriteStatus::RO);
  recv->OnReadDone(true);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kSlotRangeStatusMismatchError.code());
  slot_range->SetDtsWriteRunningStatus(WriteStatus::UNSPECIFIED);
  recv->OnDone();
  // mismatch next seq id
  recv = srv.PushSyncData(&ctx, &resp);
  recv->req_.CopyFrom(valid_req);
  recv->req_.mutable_sync_point()->set_next_seq_id(valid_sync_point.next_seq_id() + 1);
  recv->OnReadDone(true);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kSyncPointMismatchError.code());
  ASSERT_EQ(recv->finish_error_->message(), SyncReceiver::kNextSeqIdMismatchErr.message());
  recv->OnDone();
  // mismatch prev log ts
  recv = srv.PushSyncData(&ctx, &resp);
  recv->req_.CopyFrom(valid_req);
  recv->req_.mutable_sync_point()->set_prev_log_ts(valid_sync_point.prev_log_ts() - 1);
  recv->OnReadDone(true);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kSyncPointMismatchError.code());
  ASSERT_EQ(recv->finish_error_->message(), SyncReceiver::kPrevLogTsMismatchErr.message());
  recv->OnDone();
  // mismatch prev replica id
  recv = srv.PushSyncData(&ctx, &resp);
  recv->req_.CopyFrom(valid_req);
  auto invalid_replica_id = valid_sync_point.prev_rep_id();
  ASSERT_GT(invalid_replica_id.size(), 0);
  invalid_replica_id[0]++;
  recv->req_.mutable_sync_point()->set_prev_rep_id(invalid_replica_id);
  recv->OnReadDone(true);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kSyncPointMismatchError.code());
  ASSERT_EQ(recv->finish_error_->message(), SyncReceiver::kPrevRepIdMismatchErr.message());
  recv->OnDone();
  // apply write batch success
  std::string key{"key1"}, val{"val1"}, read_val;
  rocksdb::WriteBatch wb;
  wb.Put(key, val);
  auto s = storage->Get(storage->DefaultScanOptions(), key, &read_val);
  ASSERT_TRUE(s.IsNotFound() && read_val.empty());
  recv = srv.PushSyncData(&ctx, &resp);
  recv->req_.CopyFrom(valid_req);
  recv->req_.add_write_batches(wb.Data());
  recv->OnReadDone(true);
  ASSERT_FALSE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_, std::nullopt);
  s = storage->Get(storage->DefaultMultiGetOptions(), key, &read_val);
  ASSERT_TRUE(s.ok() && read_val == val);
  recv->OnDone();
  // apply write batch failed
  recv = srv.PushSyncData(&ctx, &resp);
  recv->req_.CopyFrom(valid_req);
  auto point_ret = slot_range->GetSyncPoint();
  ASSERT_TRUE(point_ret.IsOK());
  recv->req_.mutable_sync_point()->CopyFrom(point_ret.GetValue());
  recv->req_.add_write_batches("");
  recv->OnReadDone(true);
  ASSERT_TRUE(recv->IsFinished());
  ASSERT_EQ(recv->finish_error_->code(), kUnknownError.code());
  recv->OnDone();
}

}  // namespace redis
