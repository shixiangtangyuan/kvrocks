#include <gtest/gtest.h>
#include <kv/controller/v1/model.pb.h>

#include "../sync/sync_test_util.h"
#include "mock/mock_server.h"
#include "storage/batch_extractor_cdc.h"

namespace redis {

TEST(CDCSender, RPC) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto status = ApplyTopo(srv, db_id, true, true, start, end);
  ASSERT_TRUE(status.IsOK());
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range != nullptr);
  auto storage = srv.GetStorage(db_id);
  ASSERT_TRUE(storage != nullptr);
  storage->GetConfig()->enable_cdc_sync = true;

  auto wait_us_for_resp = 20000;
  auto ret = slot_range->GetCDCPoint();
  ASSERT_TRUE(ret.IsOK());
  auto valid_cdc_point = ret.GetValue();
  ASSERT_FALSE(valid_cdc_point.prev_rep_id().empty());
  ASSERT_NE(valid_cdc_point.prev_log_ts(), 0);
  kv::datanode::v1::CDCGetEventsRequest valid_req;
  valid_req.set_cluster_id(opt.cluster_id);
  valid_req.mutable_slotrange_idx()->set_start(start);
  valid_req.mutable_slotrange_idx()->set_end(end);
  valid_req.mutable_point()->CopyFrom(valid_cdc_point);
  // prepare some data
  size_t bytes_per_wb = 0;
  const int64_t wb_cnt = 6, updates_per_wb = 6;
  auto worker0 = srv.GetWorkerThreads()[0]->GetWorker();
  redis::Connection conn{nullptr, worker0};
  std::string ouput;
  for (int64_t i = 0; i < wb_cnt; i++) {
    std::ostringstream oss;
    oss << "key_" << std::setfill('0') << std::setw(3) << i;
    CommandTokens cmd_tokens{"HSET", oss.str()};
    // meta key will be updated
    for (int64_t j = 0; j < updates_per_wb - 1; j++) {
      std::ostringstream oss;
      oss << "field_" << std::setfill('0') << std::setw(3) << j;
      cmd_tokens.emplace_back(oss.str());
      oss.clear();
      oss.str("");
      oss << "value_" << std::setfill('0') << std::setw(3) << j;
      cmd_tokens.emplace_back(oss.str());
    }
    std::unique_ptr<Commander> cmd;
    auto s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd);
    ASSERT_TRUE(s.OK());
    cmd->SetArgs(cmd_tokens);
    s = cmd->Parse();
    ASSERT_TRUE(s.OK());
    s = cmd->Execute(srv.GetServer().get(), &conn, &ouput, storage.get());
    ASSERT_TRUE(s.IsOK());
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    ASSERT_TRUE(storage->GetWALIter(storage->LatestSeqNumber(), &iter).IsOK());
    ASSERT_TRUE(iter->Valid());
    uint64_t size = 0;
    google::protobuf::RepeatedPtrField<kv::datanode::v1::CDCEvent> events;
    s = cdc::GetCDCDataFromBatch(opt.cluster_id, valid_req.slotrange_idx(), iter->GetBatch(), &events, &size);
    EXPECT_TRUE(s.IsOK());
    if (i == 0) {
      bytes_per_wb = size;
    } else {
      ASSERT_GE(size, bytes_per_wb);
      ASSERT_LE(size, bytes_per_wb + 10);
    }
  }

  grpc::CallbackServerContext ctx;
  kv::datanode::v1::CDCGetEventsRequest req;
  // wrong cluster id
  req.CopyFrom(valid_req);
  std::string wrong_cid = opt.cluster_id;
  ++wrong_cid[0];
  req.set_cluster_id("wrong " + opt.cluster_id);
  auto send = srv.PullCDCData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kClusterIdMismatchError.code());
  send->OnDone();
  // wrong slot range
  req.CopyFrom(valid_req);
  req.mutable_slotrange_idx()->set_start(start + 1);
  send = srv.PullCDCData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSlotRangeNotFoundError.code());
  send->OnDone();
  // invalid next seq id
  req.CopyFrom(valid_req);
  req.mutable_point()->set_next_seq_id(0);
  req.mutable_point()->clear_prev_log_ts();
  req.mutable_point()->clear_prev_rep_id();
  send = srv.PullCDCData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), UnknownSyncError("").code());
  send->OnDone();
  // next seq bigger than upper bound
  req.CopyFrom(valid_req);
  req.mutable_point()->set_next_seq_id(storage->LatestSeqNumber() + 2);
  req.mutable_point()->clear_prev_log_ts();
  req.mutable_point()->clear_prev_rep_id();
  send = srv.PullCDCData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kCdcSeqIdExceededError.code());
  send->OnDone();
  // inconsistent next seq to sync
  req.CopyFrom(valid_req);
  req.mutable_point()->set_next_seq_id(storage->LatestSeqNumber());
  req.mutable_point()->clear_prev_log_ts();
  req.mutable_point()->clear_prev_rep_id();
  send = srv.PullCDCData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kCdcSeqIdMismatchError.code());
  send->OnDone();
  // log ts mismatch
  req.CopyFrom(valid_req);
  req.mutable_point()->set_prev_log_ts(valid_cdc_point.prev_log_ts() + 1);
  send = srv.PullCDCData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kCdcLogTsMismatchError.code());
  send->OnDone();
  // replica id mismatch
  auto wrong_replica_id = valid_cdc_point.prev_rep_id();
  ++wrong_replica_id[0];
  req.CopyFrom(valid_req);
  req.mutable_point()->set_prev_rep_id(wrong_replica_id);
  send = srv.PullCDCData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kCdcReplIdMismatchError.code());
  send->OnDone();
  // write stream failed
  req.CopyFrom(valid_req);
  send = srv.PullCDCData(&ctx, &req);  // send the first empty response
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->resp_.events().size(), 0);
  ASSERT_EQ(send->GetNextSeq(), valid_cdc_point.next_seq_id());
  send->OnWriteDone(false);
  usleep(wait_us_for_resp);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kStreamUnavailableError.code());
  ASSERT_EQ(send->GetNextSeq(), valid_cdc_point.next_seq_id());
  send->OnDone();
  // stopped by others
  req.CopyFrom(valid_req);
  send = srv.PullCDCData(&ctx, &req);
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->resp_.events().size(), 0);
  ASSERT_EQ(send->GetNextSeq(), valid_cdc_point.next_seq_id());
  send->MarkFinished(kSlotRangeStatusMismatchError);
  usleep(wait_us_for_resp);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSlotRangeStatusMismatchError.code());
  send->OnDone();
  // reach max_delay_bytes
  int64_t wbs_per_resp = 2;
  ASSERT_GT(wb_cnt - 1 - wbs_per_resp, wbs_per_resp);
  req.CopyFrom(valid_req);
  send = srv.PullCDCData(&ctx, &req);
  usleep(wait_us_for_resp);
  send->OnWriteDone(true);  // send the first write batch
  usleep(wait_us_for_resp);
  ASSERT_EQ(send->resp_.events().size(), 1);
  ASSERT_EQ(send->GetNextSeq(), valid_cdc_point.next_seq_id() + updates_per_wb);
  kv::datanode::v1::SyncConfig cfg_to_update, cfg_af_updated;
  cfg_to_update.Clear();
  cfg_to_update.set_max_delay_bytes(bytes_per_wb * wbs_per_resp - 1);
  cfg_to_update.set_max_delay_updates(updates_per_wb * wbs_per_resp + 1);
  send->UpdateSyncConfig(cfg_to_update);
  cfg_af_updated = send->GetSyncConfig();
  ASSERT_EQ(cfg_af_updated.max_delay_bytes(), cfg_to_update.max_delay_bytes());
  ASSERT_EQ(cfg_af_updated.max_delay_updates(), cfg_to_update.max_delay_updates());
  ASSERT_GT(cfg_af_updated.max_bytes_per_second(), 0);
  send->OnWriteDone(true);
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->resp_.events().size(), wbs_per_resp);
  ASSERT_EQ(send->GetNextSeq(), valid_cdc_point.next_seq_id() + updates_per_wb * (wbs_per_resp + 1));
  send->MarkFinished(std::nullopt);
  send->OnDone();
  // reach max_delay_updates
  wbs_per_resp = 2;
  ASSERT_GT(wb_cnt - 1 - wbs_per_resp, wbs_per_resp);
  req.CopyFrom(valid_req);
  send = srv.PullCDCData(&ctx, &req);
  usleep(wait_us_for_resp);
  send->OnWriteDone(true);  // send the first write batch
  usleep(wait_us_for_resp);
  ASSERT_EQ(send->resp_.events().size(), 1);
  ASSERT_EQ(send->GetNextSeq(), valid_cdc_point.next_seq_id() + updates_per_wb);
  cfg_to_update.Clear();
  cfg_to_update.set_max_delay_bytes(bytes_per_wb * wbs_per_resp + 1);
  cfg_to_update.set_max_delay_updates(updates_per_wb * wbs_per_resp - 1);
  send->UpdateSyncConfig(cfg_to_update);
  cfg_af_updated = send->GetSyncConfig();
  ASSERT_EQ(cfg_af_updated.max_delay_bytes(), cfg_to_update.max_delay_bytes());
  ASSERT_EQ(cfg_af_updated.max_delay_updates(), cfg_to_update.max_delay_updates());
  ASSERT_GT(cfg_af_updated.max_bytes_per_second(), 0);
  send->OnWriteDone(true);
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->resp_.events().size(), wbs_per_resp);
  ASSERT_EQ(send->GetNextSeq(), valid_cdc_point.next_seq_id() + updates_per_wb * (wbs_per_resp + 1));
  send->MarkFinished(std::nullopt);
  send->OnDone();
  // iterator update
  auto latest_seq_id = storage->LatestSeqNumber();
  req.CopyFrom(valid_req);
  auto point_ret = slot_range->GetCDCPoint();
  ASSERT_TRUE(point_ret.IsOK());
  req.mutable_point()->CopyFrom(point_ret.GetValue());
  send = srv.PullCDCData(&ctx, &req);
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetNextSeq(), latest_seq_id + 1);
  ASSERT_EQ(send->resp_.events().size(), 0);
  send->OnWriteDone(true);
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetNextSeq(), latest_seq_id + 1);
  ASSERT_EQ(send->resp_.events().size(), 0);
  cfg_to_update.Clear();
  cfg_to_update.set_max_delay_bytes(bytes_per_wb);
  cfg_to_update.set_max_delay_updates(updates_per_wb);
  send->UpdateSyncConfig(cfg_to_update);
  cfg_af_updated = send->GetSyncConfig();
  ASSERT_EQ(cfg_af_updated.max_delay_bytes(), cfg_to_update.max_delay_bytes());
  ASSERT_EQ(cfg_af_updated.max_delay_updates(), cfg_to_update.max_delay_updates());
  ASSERT_GT(cfg_af_updated.max_bytes_per_second(), 0);
  std::vector<rocksdb::BatchResult> write_wbs;
  std::thread writer = std::thread([&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(1000));
    for (int64_t i = wb_cnt; i < 2 * wb_cnt; ++i) {
      std::ostringstream oss;
      oss << "key_" << std::setfill('0') << std::setw(3) << i;
      CommandTokens cmd_tokens{"HSET", oss.str()};
      // meta key will be updated
      for (int64_t j = 0; j < updates_per_wb - 1; j++) {
        std::ostringstream oss;
        oss << "field_" << std::setfill('0') << std::setw(3) << j;
        cmd_tokens.emplace_back(oss.str());
        oss.clear();
        oss.str("");
        oss << "value_" << std::setfill('0') << std::setw(3) << j;
        cmd_tokens.emplace_back(oss.str());
      }
      std::unique_ptr<Commander> cmd;
      auto s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd);
      ASSERT_TRUE(s.OK());
      cmd->SetArgs(cmd_tokens);
      s = cmd->Parse();
      ASSERT_TRUE(s.OK());
      s = cmd->Execute(srv.GetServer().get(), &conn, &ouput, storage.get());
      ASSERT_TRUE(s.IsOK());
      std::unique_ptr<rocksdb::TransactionLogIterator> iter;
      ASSERT_TRUE(storage->GetWALIter(storage->LatestSeqNumber(), &iter).IsOK());
      ASSERT_TRUE(iter->Valid());
      write_wbs.emplace_back(iter->GetBatch());
    }
  });
  writer.join();
  auto& cluster_id = srv.GetServer()->cluster->ClusterId();
  for (int i = 0; i < wb_cnt; ++i) {
    if (i > 0) {
      send->OnWriteDone(true);
    }
    usleep(wait_us_for_resp);
    ASSERT_FALSE(send->IsFinished());
    ASSERT_FALSE(send->GetFinishError().has_value());
    ASSERT_EQ(send->resp_.events().size(), 1);
    google::protobuf::RepeatedPtrField<::kv::datanode::v1::CDCEvent> exp_events;
    ASSERT_TRUE(cdc::GetCDCDataFromBatch(cluster_id, req.slotrange_idx(), write_wbs[i], &exp_events).IsOK());
    ASSERT_EQ(send->resp_.events().size(), exp_events.size());
    for (int i = 0; i < exp_events.size(); ++i) {
      ASSERT_EQ(send->resp_.events()[i].DebugString(), exp_events[i].DebugString());
    }
    ReplIdExtractor log_data_handler;
    ASSERT_TRUE(write_wbs[i].writeBatchPtr->Iterate(&log_data_handler).ok());
    ASSERT_EQ(send->resp_.point().next_seq_id(), send->GetNextSeq());
    ASSERT_EQ(send->GetNextSeq(), latest_seq_id + (i + 1) * updates_per_wb + 1);
    ASSERT_EQ(send->resp_.point().prev_rep_id(), log_data_handler.GetReplId());
    ASSERT_EQ(send->resp_.point().prev_log_ts(), log_data_handler.GetTimeNanos());
  }
  send->MarkFinished(std::nullopt);
  send->OnDone();
}

}  // namespace redis
