#include <absl/time/clock.h>
#include <gtest/gtest.h>

#include <limits>
#include <random>

#include "mock/mock_server.h"
#include "server/server.h"
#include "sync_test_util.h"

std::string RandomReplicaId() {
  static constexpr std::string_view charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::random_device rd;
  std::mt19937 gen(rd() + getpid());
  std::uniform_int_distribution<size_t> distrib(0, charset.size() - 1);
  std::string rand_str(kReplIdLength, 0);
  for (int i = 0; i < kReplIdLength; i++) {
    rand_str[i] = charset[distrib(gen)];
  }
  return rand_str;
}

TEST(SyncPoint, Base) {
  // invalid server log datas
  std::vector<ServerLogData> invalid_datas = {
      ServerLogData{},
      ServerLogData{ServerLogType::kServerLogNone, std::string(ServerLogData::kReplIdTag, 1)},
      ServerLogData{ServerLogType::kReplIdLog, "invalid " + RandomReplicaId()},
  };
  for (auto& encode_data : invalid_datas) {
    ServerLogData decode_data;
    auto encode_string = encode_data.Encode();
    auto status = decode_data.Decode(encode_string);
    ASSERT_FALSE(status.IsOK());
  }
  // valid server log datas
  std::vector<ServerLogData> valid_datas = {
      ServerLogData{ServerLogType::kReplIdLog, 0, RandomReplicaId()},
      ServerLogData{ServerLogType::kReplIdLog, RandomReplicaId()},
      ServerLogData{ServerLogType::kReplIdLog, std::numeric_limits<uint64_t>::max(), RandomReplicaId()},
  };
  for (auto& encode_data : valid_datas) {
    ServerLogData decode_data;
    auto encode_string = encode_data.Encode();
    auto status = decode_data.Decode(encode_string);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(encode_data, decode_data);
  }
}

TEST(SyncPoint, RPC) {
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

  kv::datanode::v1::GetSyncPointRequest valid_req;
  valid_req.set_cluster_id(opt.cluster_id);
  valid_req.mutable_slot_range()->set_start(start);
  valid_req.mutable_slot_range()->set_end(end);

  grpc::CallbackServerContext ctx;
  kv::datanode::v1::GetSyncPointRequest req;
  kv::datanode::v1::GetSyncPointResponse resp;
  // request success
  req.CopyFrom(valid_req);
  srv.GetSyncPoint(&ctx, &req, &resp);
  ASSERT_TRUE(resp.has_sync_point());
  resp.Clear();
  // wrong cluster id
  req.CopyFrom(valid_req);
  req.set_cluster_id("wrong " + opt.cluster_id);
  srv.GetSyncPoint(&ctx, &req, &resp);
  ASSERT_FALSE(resp.has_sync_point());
  // wrong slot range
  req.CopyFrom(valid_req);
  req.mutable_slot_range()->set_start(start + 1);
  srv.GetSyncPoint(&ctx, &req, &resp);
  ASSERT_FALSE(resp.has_sync_point());
  // empty db
  req.CopyFrom(valid_req);
  srv.GetSyncPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.sync_point().next_seq_id(), 1);
  ASSERT_EQ(resp.sync_point().prev_log_ts(), 0);
  ASSERT_TRUE(resp.sync_point().prev_rep_id().empty());
  // get replica id from db(key not exist)
  rocksdb::WriteOptions options;
  options.disableWAL = true;
  rocksdb::WriteBatch wb;
  wb.Put("key1", "val1");
  ASSERT_TRUE(storage->Write(options, &wb).ok());
  req.CopyFrom(valid_req);
  srv.GetSyncPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.sync_point().next_seq_id(), storage->LatestSeqNumber() + 1);
  ASSERT_EQ(resp.sync_point().prev_log_ts(), 0);
  ASSERT_TRUE(resp.sync_point().prev_rep_id().empty());
  // get replica id from db(key existed)
  wb.Clear();
  auto cf = storage->GetCFHandle(engine::kPropagateColumnFamilyName);
  auto repl_id = "xxxxxxxx";
  wb.Put(cf, "replication_id_", repl_id);
  ASSERT_TRUE(storage->Write(options, &wb).ok());
  req.CopyFrom(valid_req);
  srv.GetSyncPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.sync_point().next_seq_id(), storage->LatestSeqNumber() + 1);
  ASSERT_EQ(resp.sync_point().prev_log_ts(), 0);
  ASSERT_EQ(resp.sync_point().prev_rep_id(), repl_id);
  // get replica id from wal
  status = ApplyTopo(srv, db_id, true, true, start, end);
  ASSERT_TRUE(status.IsOK());
  req.CopyFrom(valid_req);
  srv.GetSyncPoint(&ctx, &req, &resp);
  ASSERT_EQ(resp.sync_point().next_seq_id(), storage->LatestSeqNumber() + 1);
  ASSERT_GT(resp.sync_point().prev_log_ts(), 0);
  ASSERT_NE(resp.sync_point().prev_rep_id(), repl_id);
  ASSERT_EQ(resp.sync_point().prev_rep_id().size(), kReplIdLength);
}
