#include <absl/time/clock.h>
#include <gtest/gtest.h>

#include "../sync/sync_test_util.h"
#include "mock/mock_server.h"

TEST(CDCPoint, RPC) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto status = ApplyTopo(srv, db_id, true, false, start, end);
  ASSERT_TRUE(status.IsOK());
  auto storage = srv.GetStorage(db_id);
  ASSERT_TRUE(storage != nullptr);
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range != nullptr);

  {
    // cdc get restart point rpc
    kv::datanode::v1::CDCGetRestartPointRequest valid_req;
    valid_req.set_cluster_id(opt.cluster_id);
    valid_req.mutable_slotrange_idx()->set_start(start);
    valid_req.mutable_slotrange_idx()->set_end(end);
    grpc::CallbackServerContext ctx;
    kv::datanode::v1::CDCGetRestartPointRequest req;
    kv::datanode::v1::CDCGetRestartPointResponse resp;
    // wrong cluster id
    resp.Clear();
    req.CopyFrom(valid_req);
    req.set_cluster_id("wrong " + opt.cluster_id);
    srv.GetCDCRestartPoint(&ctx, &req, &resp);
    ASSERT_FALSE(resp.has_db_point());
    // wrong slot range
    resp.Clear();
    req.CopyFrom(valid_req);
    req.mutable_slotrange_idx()->set_start(start + 1);
    srv.GetCDCRestartPoint(&ctx, &req, &resp);
    ASSERT_FALSE(resp.has_db_point());
    // request success
    resp.Clear();
    req.CopyFrom(valid_req);
    srv.GetCDCRestartPoint(&ctx, &req, &resp);
    ASSERT_TRUE(resp.has_db_point());
    ASSERT_TRUE(resp.db_point().next_seq_id() == 1);
    ASSERT_TRUE(resp.db_point().prev_log_ts() == 0);
    ASSERT_TRUE(resp.db_point().prev_rep_id().empty());
  }

  kv::datanode::v1::CDCGetLatestPointRequest valid_req;
  valid_req.set_cluster_id(opt.cluster_id);
  valid_req.mutable_slotrange_idx()->set_start(start);
  valid_req.mutable_slotrange_idx()->set_end(end);
  grpc::CallbackServerContext ctx;
  kv::datanode::v1::CDCGetLatestPointRequest req;
  kv::datanode::v1::CDCGetLatestPointResponse resp;
  // wrong cluster id
  resp.Clear();
  req.CopyFrom(valid_req);
  req.set_cluster_id("wrong " + opt.cluster_id);
  srv.GetCDCPoint(&ctx, &req, &resp);
  ASSERT_FALSE(resp.has_db_point());
  // wrong slot range
  req.CopyFrom(valid_req);
  req.mutable_slotrange_idx()->set_start(start + 1);
  srv.GetCDCPoint(&ctx, &req, &resp);
  ASSERT_FALSE(resp.has_db_point());
  // empty db
  resp.Clear();
  req.CopyFrom(valid_req);
  srv.GetCDCPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.db_point().next_seq_id(), 1);
  ASSERT_EQ(resp.db_point().prev_log_ts(), 0);
  ASSERT_TRUE(resp.db_point().prev_rep_id().empty());
  // get replica id from db(key not exist)
  rocksdb::WriteOptions options;
  options.disableWAL = true;
  rocksdb::WriteBatch wb;
  wb.Put("key1", "val1");
  ASSERT_TRUE(storage->Write(options, &wb).ok());
  resp.Clear();
  req.CopyFrom(valid_req);
  srv.GetCDCPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.db_point().next_seq_id(), storage->LatestSeqNumber() + 1);
  ASSERT_EQ(resp.db_point().prev_log_ts(), 0);
  ASSERT_TRUE(resp.db_point().prev_rep_id().empty());
  // get replica id from db(key existed)
  wb.Clear();
  auto cf = storage->GetCFHandle(engine::kPropagateColumnFamilyName);
  auto repl_id = "xxxxxxxx";
  wb.Put(cf, "replication_id_", repl_id);
  ASSERT_TRUE(storage->Write(options, &wb).ok());
  resp.Clear();
  req.CopyFrom(valid_req);
  srv.GetCDCPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.db_point().next_seq_id(), storage->LatestSeqNumber() + 1);
  ASSERT_EQ(resp.db_point().prev_log_ts(), 0);
  ASSERT_EQ(resp.db_point().prev_rep_id(), repl_id);
  // get replica id from wal
  status = ApplyTopo(srv, db_id, true, true, start, end);
  ASSERT_TRUE(status.IsOK());
  resp.Clear();
  req.CopyFrom(valid_req);
  srv.GetCDCPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.db_point().next_seq_id(), storage->LatestSeqNumber() + 1);
  ASSERT_GT(resp.db_point().prev_log_ts(), 0);
  ASSERT_NE(resp.db_point().prev_rep_id(), repl_id);
  ASSERT_EQ(resp.db_point().prev_rep_id().size(), kReplIdLength);
  // storage closed
  storage->ForceStop();
  ASSERT_TRUE(storage->IsClosing());
  ASSERT_TRUE(storage->GetDB() == nullptr);
  ASSERT_TRUE(slot_range->GetStorage() != nullptr);
  auto s1 = slot_range->GetSyncPoint();
  ASSERT_TRUE(!s1.IsOK());
  auto s2 = slot_range->GetSyncPoint(1);
  ASSERT_TRUE(!s2.IsOK());
  ASSERT_TRUE(s1.Msg() == s2.Msg());
  auto s3 = slot_range->GetCDCPoint();
  ASSERT_TRUE(!s3.IsOK());
  auto s4 = slot_range->GetCDCPoint(1);
  ASSERT_TRUE(!s4.IsOK());
  ASSERT_TRUE(s3.Msg() == s4.Msg());
  auto s5 = slot_range->GetCDCRestartPoint();
  ASSERT_TRUE(s5.IsOK());
  ASSERT_TRUE(s5->next_seq_id() == 1);
  ASSERT_TRUE(s5->prev_log_ts() == 0);
  ASSERT_TRUE(s5->prev_rep_id().empty());
  resp.Clear();
  req.CopyFrom(valid_req);
  srv.GetCDCPoint(&ctx, &req, &resp);
  ASSERT_FALSE(resp.has_db_point());
}

TEST(CDCPoint, RPCOldestPoint) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto status = ApplyTopo(srv, db_id, true, true, start, end);
  ASSERT_TRUE(status.IsOK());
  auto storage = srv.GetStorage(db_id);
  ASSERT_TRUE(storage != nullptr);
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range != nullptr);

  {
    kv::datanode::v1::CDCGetOldestPointRequest valid_req_oldest;
    valid_req_oldest.set_cluster_id(opt.cluster_id);
    valid_req_oldest.mutable_slotrange_idx()->set_start(start);
    valid_req_oldest.mutable_slotrange_idx()->set_end(end);
    grpc::CallbackServerContext ctx;
    kv::datanode::v1::CDCGetOldestPointRequest req_oldest;
    kv::datanode::v1::CDCGetOldestPointResponse resp_oldest;
    // wrong cluster id
    resp_oldest.Clear();
    req_oldest.CopyFrom(valid_req_oldest);
    req_oldest.set_cluster_id("wrong " + opt.cluster_id);
    srv.GetCDCOldestPoint(&ctx, &req_oldest, &resp_oldest);
    ASSERT_FALSE(resp_oldest.has_db_point());
    // wrong slot range
    req_oldest.CopyFrom(valid_req_oldest);
    req_oldest.mutable_slotrange_idx()->set_start(start + 1);
    srv.GetCDCOldestPoint(&ctx, &req_oldest, &resp_oldest);
    ASSERT_FALSE(resp_oldest.has_db_point());
    // request success
    resp_oldest.Clear();
    req_oldest.CopyFrom(valid_req_oldest);
    srv.GetCDCOldestPoint(&ctx, &req_oldest, &resp_oldest);
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto s = storage->GetWALIter(0, &iter);
    ASSERT_TRUE(s.IsOK());
    auto batch = iter->GetBatch();
    ReplIdExtractor log_data_handler;
    rocksdb::Status ret = batch.writeBatchPtr->Iterate(&log_data_handler);
    ASSERT_TRUE(ret.ok());
    auto prev_rep_id = log_data_handler.GetReplId();
    auto prev_log_ts = log_data_handler.GetTimeNanos();
    ASSERT_TRUE(resp_oldest.has_db_point());
    ASSERT_TRUE(resp_oldest.db_point().next_seq_id() == 2);
    ASSERT_TRUE(resp_oldest.db_point().prev_log_ts() == prev_log_ts);
    ASSERT_TRUE(resp_oldest.db_point().prev_rep_id() == prev_rep_id);
  }
}
