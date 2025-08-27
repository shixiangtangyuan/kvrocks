#include <gtest/gtest.h>
#include <kv/controller/v1/model.pb.h>

#include "mock/mock_server.h"
#include "sync_test_util.h"

namespace redis {

TEST(SyncSender, Dts) {
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

  auto wait_us_for_resp = 20000;
  auto ret = slot_range->GetSyncPoint();
  ASSERT_TRUE(ret.IsOK());
  auto valid_sync_point = ret.GetValue();
  ASSERT_FALSE(valid_sync_point.prev_rep_id().empty());
  ASSERT_NE(valid_sync_point.prev_log_ts(), 0);
  kv::datanode::v1::PullSyncDataRequest valid_req;
  valid_req.set_cluster_id(opt.cluster_id);
  valid_req.mutable_slot_range()->set_start(start);
  valid_req.mutable_slot_range()->set_end(end);
  valid_req.mutable_sync_point()->CopyFrom(valid_sync_point);
  valid_req.set_across_pool(true);
  // prepare some data
  kv::datanode::v1::SyncPoint latest_sync_point;
  size_t bytes_per_wb = 0;
  const int64_t wb_cnt = 6, updates_per_wb = 6;
  for (int64_t i = 0; i < wb_cnt; i++) {
    rocksdb::WriteBatch wb;
    for (int64_t j = 0; j < updates_per_wb; j++) {
      int64_t idx = i * updates_per_wb + j;
      std::ostringstream oss;
      oss << "key_" << std::setfill('0') << std::setw(3) << idx;
      auto key = oss.str();
      oss.clear();
      oss.str("");
      oss << "val_" << std::setfill('0') << std::setw(3) << idx;
      auto val = oss.str();
      wb.Put(key, val);
    }
    if (bytes_per_wb == 0) {
      bytes_per_wb = wb.GetDataSize();
    } else {
      ASSERT_EQ(wb.GetDataSize(), bytes_per_wb);
    }
    if (i == wb_cnt - 1) {
      auto ret = slot_range->GetSyncPoint();
      ASSERT_TRUE(ret.IsOK());
      latest_sync_point = ret.GetValue();
    }
    ASSERT_TRUE(storage->Write(storage->DefaultWriteOptions(), &wb).ok());
  }

  grpc::CallbackServerContext ctx;
  kv::datanode::v1::PullSyncDataRequest req;
  // wrong cluster id
  req.CopyFrom(valid_req);
  req.set_cluster_id("wrong " + opt.cluster_id);
  auto send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kClusterIdMismatchError.code());
  send->OnDone();
  // wrong slot range
  req.CopyFrom(valid_req);
  req.mutable_slot_range()->set_start(start + 1);
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSlotRangeNotFoundError.code());
  send->OnDone();
  // wrong node role
  req.CopyFrom(valid_req);
  srv.GetServer()->cluster->my_datanode_->SetServingStatus(kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING);
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  srv.GetServer()->cluster->my_datanode_->SetServingStatus(kv::controller::v1::Datanode::SERVING_STATUS_SERVING);
  send->OnDone();
  // invalid next seq id
  req.CopyFrom(valid_req);
  req.mutable_sync_point()->set_next_seq_id(0);
  req.mutable_sync_point()->clear_prev_log_ts();
  req.mutable_sync_point()->clear_prev_rep_id();
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), UnknownSyncError("").code());
  send->OnDone();
  // next seq bigger than upper bound
  req.CopyFrom(valid_req);
  req.mutable_sync_point()->set_next_seq_id(storage->LatestSeqNumber() + 2);
  req.mutable_sync_point()->clear_prev_log_ts();
  req.mutable_sync_point()->clear_prev_rep_id();
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSyncSeqIdOutOfRangeError.code());
  send->OnDone();
  // inconsistent next seq to sync
  req.CopyFrom(valid_req);
  req.mutable_sync_point()->set_next_seq_id(storage->LatestSeqNumber());
  req.mutable_sync_point()->clear_prev_log_ts();
  req.mutable_sync_point()->clear_prev_rep_id();
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSyncSeqIdOutOfRangeError.code());
  send->OnDone();
  // log ts mismatch
  req.CopyFrom(valid_req);
  req.mutable_sync_point()->set_prev_log_ts(valid_sync_point.prev_log_ts() + 1);
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSyncLogTimestampMismatchError.code());
  send->OnDone();
  // replica id mismatch
  auto wrong_replica_id = valid_sync_point.prev_rep_id();
  wrong_replica_id[0]++;
  req.CopyFrom(valid_req);
  req.mutable_sync_point()->set_prev_rep_id(wrong_replica_id);
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSyncReplicaIdMismatchError.code());
  send->OnDone();
  // max seq smaller than next seq
  req.CopyFrom(valid_req);
  req.set_max_seq_id(valid_sync_point.next_seq_id() - 1);
  send = srv.PullSyncData(&ctx, &req);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  send->OnDone();
  // get & update config
  req.CopyFrom(valid_req);
  send = srv.PullSyncData(&ctx, &req);
  auto cfg_bf_update = send->GetSyncConfig();
  kv::datanode::v1::SyncConfig cfg_to_update;
  cfg_to_update.set_max_delay_bytes(bytes_per_wb * wb_cnt);
  cfg_to_update.set_max_delay_updates(updates_per_wb * wb_cnt);
  cfg_to_update.set_max_bytes_per_second(bytes_per_wb * wb_cnt);
  auto cfg_updated = send->UpdateSyncConfig(cfg_to_update);
  ASSERT_EQ(cfg_updated.max_delay_bytes(), cfg_bf_update.max_delay_bytes());
  ASSERT_EQ(cfg_updated.max_delay_updates(), cfg_bf_update.max_delay_updates());
  ASSERT_EQ(cfg_updated.max_bytes_per_second(), cfg_bf_update.max_bytes_per_second());
  auto cfg_af_updated = send->GetSyncConfig();
  ASSERT_EQ(cfg_af_updated.max_delay_bytes(), cfg_to_update.max_delay_bytes());
  ASSERT_EQ(cfg_af_updated.max_delay_updates(), cfg_to_update.max_delay_updates());
  ASSERT_EQ(cfg_af_updated.max_bytes_per_second(), cfg_to_update.max_bytes_per_second());
  send->MarkFinished(std::nullopt);
  send->OnDone();
  // write stream failed
  req.CopyFrom(valid_req);
  send = srv.PullSyncData(&ctx, &req);  // send the first empty response
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetResponse().write_batches().size(), 0);
  ASSERT_EQ(send->GetNextSeq(), valid_sync_point.next_seq_id());
  send->OnWriteDone(false);
  usleep(wait_us_for_resp);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kStreamUnavailableError.code());
  ASSERT_EQ(send->GetNextSeq(), valid_sync_point.next_seq_id());
  send->OnDone();
  // stopped by others
  req.CopyFrom(valid_req);
  send = srv.PullSyncData(&ctx, &req);
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  send->MarkFinished(kSlotRangeStatusMismatchError);
  usleep(wait_us_for_resp);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kSlotRangeStatusMismatchError.code());
  send->OnDone();
  // reach max_delay_bytes
  int64_t wbs_per_resp = 2;
  ASSERT_GT(wb_cnt - 1 - wbs_per_resp, wbs_per_resp);
  req.CopyFrom(valid_req);
  send = srv.PullSyncData(&ctx, &req);
  usleep(wait_us_for_resp);
  send->OnWriteDone(true);  // send the first write batch
  usleep(wait_us_for_resp);
  ASSERT_EQ(send->GetResponse().write_batches().size(), 1);
  ASSERT_EQ(send->GetNextSeq(), valid_sync_point.next_seq_id() + updates_per_wb);
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
  ASSERT_EQ(send->GetResponse().write_batches().size(), wbs_per_resp);
  ASSERT_EQ(send->GetNextSeq(), valid_sync_point.next_seq_id() + updates_per_wb * (wbs_per_resp + 1));
  send->MarkFinished(std::nullopt);
  send->OnDone();
  // reach max_delay_updates
  wbs_per_resp = 2;
  ASSERT_GT(wb_cnt - 1 - wbs_per_resp, wbs_per_resp);
  req.CopyFrom(valid_req);
  send = srv.PullSyncData(&ctx, &req);
  usleep(wait_us_for_resp);
  send->OnWriteDone(true);  // send the first write batch
  usleep(wait_us_for_resp);
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
  ASSERT_EQ(send->GetResponse().write_batches().size(), wbs_per_resp);
  ASSERT_EQ(send->GetNextSeq(), valid_sync_point.next_seq_id() + updates_per_wb * (wbs_per_resp + 1));
  send->MarkFinished(std::nullopt);
  send->OnDone();
  // iterator update
  req.CopyFrom(valid_req);
  req.mutable_sync_point()->CopyFrom(latest_sync_point);
  send = srv.PullSyncData(&ctx, &req);
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetNextSeq(), latest_sync_point.next_seq_id());
  ASSERT_EQ(send->GetResponse().write_batches().size(), 0);
  send->OnWriteDone(true);  // send the first write batch
  usleep(wait_us_for_resp);
  auto latest_seq_id = storage->LatestSeqNumber();
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetNextSeq(), latest_seq_id + 1);
  ASSERT_EQ(send->GetResponse().write_batches().size(), 1);
  cfg_to_update.Clear();
  cfg_to_update.set_max_delay_bytes(bytes_per_wb);
  cfg_to_update.set_max_delay_updates(updates_per_wb);
  send->UpdateSyncConfig(cfg_to_update);
  cfg_af_updated = send->GetSyncConfig();
  ASSERT_EQ(cfg_af_updated.max_delay_bytes(), cfg_to_update.max_delay_bytes());
  ASSERT_EQ(cfg_af_updated.max_delay_updates(), cfg_to_update.max_delay_updates());
  ASSERT_GT(cfg_af_updated.max_bytes_per_second(), 0);
  std::vector<std::string> write_wbs;
  std::thread writer = std::thread([&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(1000));
    rocksdb::WriteBatch wb;
    for (int i = 0; i < wb_cnt; ++i) {
      wb.Clear();
      for (int64_t j = 0; j < updates_per_wb; j++) {
        int64_t idx = wb_cnt * updates_per_wb + j;
        std::ostringstream oss;
        oss << "key_" << std::setfill('0') << std::setw(3) << idx;
        auto key = oss.str();
        oss.clear();
        oss.str("");
        oss << "val_" << std::setfill('0') << std::setw(3) << idx;
        auto val = oss.str();
        wb.Put(key, val);
      }
      ASSERT_EQ(wb.GetDataSize(), bytes_per_wb);
      ASSERT_TRUE(storage->Write(storage->DefaultWriteOptions(), &wb).ok());
      write_wbs.emplace_back(wb.Data());
    }
  });
  writer.join();
  for (int i = 0; i < wb_cnt; ++i) {
    send->OnWriteDone(true);
    usleep(wait_us_for_resp);
    ASSERT_FALSE(send->IsFinished());
    ASSERT_FALSE(send->GetFinishError().has_value());
    ASSERT_EQ(send->GetResponse().write_batches().size(), 1);
    ASSERT_EQ(send->GetResponse().write_batches()[0], write_wbs[i]);
    ASSERT_EQ(send->GetNextSeq(), latest_seq_id + (i + 1) * updates_per_wb + 1);
  }
  send->MarkFinished(std::nullopt);
  send->OnDone();
  // reach max_seq
  wbs_per_resp = 2;
  ASSERT_GT(updates_per_wb, 1);
  ASSERT_GT(wb_cnt - 1 - wbs_per_resp, wbs_per_resp);
  req.CopyFrom(valid_req);
  req.set_max_seq_id(valid_sync_point.next_seq_id() + updates_per_wb * (wbs_per_resp + 1) + 1);
  send = srv.PullSyncData(&ctx, &req);
  usleep(wait_us_for_resp);
  send->OnWriteDone(true);  // send the first write batch
  usleep(wait_us_for_resp);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  cfg_to_update.Clear();
  cfg_to_update.set_max_delay_bytes(bytes_per_wb * (wbs_per_resp + 1));
  cfg_to_update.set_max_delay_updates(updates_per_wb * (wbs_per_resp + 1));
  send->UpdateSyncConfig(cfg_to_update);
  cfg_af_updated = send->GetSyncConfig();
  ASSERT_EQ(cfg_af_updated.max_delay_bytes(), cfg_to_update.max_delay_bytes());
  ASSERT_EQ(cfg_af_updated.max_delay_updates(), cfg_to_update.max_delay_updates());
  ASSERT_GT(cfg_af_updated.max_bytes_per_second(), 0);
  send->OnWriteDone(true);
  usleep(wait_us_for_resp);
  ASSERT_EQ(send->GetResponse().write_batches().size(), wbs_per_resp);
  ASSERT_EQ(send->GetNextSeq(), valid_sync_point.next_seq_id() + updates_per_wb * (wbs_per_resp + 1));
  send->OnWriteDone(true);
  usleep(wait_us_for_resp);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetResponse().write_batches().size(), 0);
  ASSERT_EQ(send->GetNextSeq(), valid_sync_point.next_seq_id() + updates_per_wb * (wbs_per_resp + 1));
  send->MarkFinished(std::nullopt);
  send->OnDone();
}

TEST(SyncSender, Repl) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto imporing_node_id = "importing_node_id";
  auto status = ApplyTopo(srv, db_id, true, true, start, end, imporing_node_id);
  ASSERT_TRUE(status.IsOK());
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range != nullptr);
  auto storage = srv.GetStorage(db_id);
  ASSERT_TRUE(storage != nullptr);

  grpc::CallbackServerContext ctx;
  auto ret = slot_range->GetSyncPoint();
  ASSERT_TRUE(ret.IsOK());
  auto valid_sync_point = ret.GetValue();
  kv::datanode::v1::SyncDataRequest valid_req;
  valid_req.set_cluster_id(opt.cluster_id);
  valid_req.set_puller_node_id(imporing_node_id);
  valid_req.mutable_slot_range()->set_start(start);
  valid_req.mutable_slot_range()->set_end(end);
  valid_req.mutable_sync_point()->CopyFrom(valid_sync_point);

  // read stream failed(without req)
  auto send = srv.SyncData(&ctx);
  send->OnReadDone(false);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_STREAM_UNAVAILABLE);
  send->OnDone();
  // read stream failed(with req)
  send = srv.SyncData(&ctx);
  send->req_.CopyFrom(valid_req);
  send->OnReadDone(true);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  send->OnReadDone(false);
  usleep(10000);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_STREAM_UNAVAILABLE);
  send->OnDone();
  // write stream failed(without req)
  send = srv.SyncData(&ctx);
  send->OnWriteDone(false);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_STREAM_UNAVAILABLE);
  send->OnDone();
  // write stream failed(with req)
  send = srv.SyncData(&ctx);
  send->req_.CopyFrom(valid_req);
  send->OnReadDone(true);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  send->OnWriteDone(false);
  usleep(10000);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_STREAM_UNAVAILABLE);
  send->OnDone();
  // request canceled(without req)
  send = srv.SyncData(&ctx);
  send->OnCancel();
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_UNSPECIFIED);
  send->OnDone();
  // request canceled(with req)
  send = srv.SyncData(&ctx);
  send->req_.CopyFrom(valid_req);
  send->OnReadDone(true);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  send->OnCancel();
  usleep(10000);
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_UNSPECIFIED);
  send->OnDone();
  // syncing and stop write timeout
  srv.GetServer()->GetConfig()->stop_write_timeout_ms = 20;
  srv.GetServer()->GetConfig()->stop_write_wait_topo_timeout_ms = 2000;
  send = srv.SyncData(&ctx);
  send->req_.CopyFrom(valid_req);
  send->OnReadDone(true);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  send->req_.CopyFrom(valid_req);  // stop write
  send->req_.set_stop_write(true);
  send->OnReadDone(true);
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::WriteStop);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  usleep(25000);  // stop write timeout
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_SYNC_STOP_WRITE_TIMEOUT);
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::Done);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  send->OnDone();
  // caught up and stop write timeout
  srv.GetServer()->GetConfig()->stop_write_timeout_ms = 20;
  srv.GetServer()->GetConfig()->stop_write_wait_topo_timeout_ms = 20;
  send = srv.SyncData(&ctx);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  send->req_.CopyFrom(valid_req);  // stop write
  send->req_.set_stop_write(true);
  send->OnReadDone(true);
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::WriteStop);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnWriteDone(true);  // caught up
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::CaughtUp);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  usleep(25000);  // stop write timeout
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_SYNC_STOP_WRITE_TIMEOUT);
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::Done);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnDone();
  usleep(30000);  // wait topo timeout
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  // caught up and local topo update
  srv.GetServer()->GetConfig()->stop_write_timeout_ms = 20;
  srv.GetServer()->GetConfig()->stop_write_wait_topo_timeout_ms = 20;
  send = srv.SyncData(&ctx);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  send->req_.CopyFrom(valid_req);  // stop write
  send->req_.set_stop_write(true);
  send->OnReadDone(true);
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::WriteStop);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnWriteDone(true);  // caught up
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::CaughtUp);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->MarkFinished(kClusterTopologyChangedError, true);  // local topo update
  send->OnDone();
  usleep(30000);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  slot_range->SetClientWriteRunningStatus(WriteStatus::UNSPECIFIED);
  slot_range->SetDtsWriteRunningStatus(WriteStatus::UNSPECIFIED);
  // caught up, stop write timeout and local topo update
  srv.GetServer()->GetConfig()->stop_write_timeout_ms = 20;
  srv.GetServer()->GetConfig()->stop_write_wait_topo_timeout_ms = 20;
  send = srv.SyncData(&ctx);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  send->req_.CopyFrom(valid_req);  // stop write
  send->req_.set_stop_write(true);
  send->OnReadDone(true);
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::WriteStop);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnWriteDone(true);  // caught up
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::CaughtUp);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  usleep(25000);  // stop write timeout
  ASSERT_TRUE(send->IsFinished());
  ASSERT_EQ(send->GetFinishError()->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_SYNC_STOP_WRITE_TIMEOUT);
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::Done);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnDone();
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  slot_range->SetClientWriteRunningStatus(WriteStatus::WR);  // local topo update
  slot_range->SetDtsWriteRunningStatus(WriteStatus::WR);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::WR);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::WR);
  usleep(30000);  // wait topo timeout
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::WR);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::WR);
  slot_range->SetClientWriteRunningStatus(WriteStatus::UNSPECIFIED);
  slot_range->SetDtsWriteRunningStatus(WriteStatus::UNSPECIFIED);
  // caught up and remote topo update
  srv.GetServer()->GetConfig()->stop_write_timeout_ms = 20;
  srv.GetServer()->GetConfig()->stop_write_wait_topo_timeout_ms = 20;
  send = srv.SyncData(&ctx);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  send->req_.CopyFrom(valid_req);  // stop write
  send->req_.set_stop_write(true);
  send->OnReadDone(true);
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::WriteStop);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnWriteDone(true);  // caught up
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::CaughtUp);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnCancel();  // remote topo update
  send->OnDone();
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  usleep(30000);  // wait topo timeout
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  // caught up and remote + local topo update
  srv.GetServer()->GetConfig()->stop_write_timeout_ms = 20;
  srv.GetServer()->GetConfig()->stop_write_wait_topo_timeout_ms = 20;
  send = srv.SyncData(&ctx);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::UNSPECIFIED);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::UNSPECIFIED);
  send->req_.CopyFrom(valid_req);  // stop write
  send->req_.set_stop_write(true);
  send->OnReadDone(true);
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::WriteStop);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  send->OnWriteDone(true);  // caught up
  usleep(5000);
  ASSERT_FALSE(send->IsFinished());
  ASSERT_FALSE(send->GetFinishError().has_value());
  ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::CaughtUp);
  send->OnReadDone(false);  // remote topo update
  send->OnDone();
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::RO);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::RO);
  slot_range->SetClientWriteRunningStatus(WriteStatus::WR);  // local topo update
  slot_range->SetDtsWriteRunningStatus(WriteStatus::WR);
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::WR);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::WR);
  usleep(30000);  // wait topo timeout
  ASSERT_EQ(slot_range->GetDtsWriteRunningStatus(), WriteStatus::WR);
  ASSERT_EQ(slot_range->GetClientRunningStatus(), WriteStatus::WR);
  slot_range->SetClientWriteRunningStatus(WriteStatus::UNSPECIFIED);
  slot_range->SetDtsWriteRunningStatus(WriteStatus::UNSPECIFIED);
}

}  // namespace redis
