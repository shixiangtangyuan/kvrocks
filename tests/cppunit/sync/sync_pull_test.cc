#include <gtest/gtest.h>

#include "sync/sync_puller.h"
#include "sync_test_util.h"

namespace redis {

TEST(SyncPuller, Base) {
  SetInTest();

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

  bool stop_cb_flag = false, done_cb_flag = false;
  SlotRangeWriteStoppableCB write_stoppable_cb = [&stop_cb_flag](const std::string&) { stop_cb_flag = true; };
  SlotRangeReplicationDoneCB cb = [&done_cb_flag]() { done_cb_flag = true; };
  auto new_puller = [&]() -> redis::SyncPuller* {
    auto ret = slot_range->GetSyncPoint();
    EXPECT_TRUE(ret.IsOK());
    return new SyncPuller(opt.cluster_id, opt.datanode_id, srv.GetServer(), slot_range, ret.GetValue(),
                          write_stoppable_cb, cb);
  };

  // check req
  auto puller = new_puller();
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
  ASSERT_EQ(puller->req_.cluster_id(), opt.cluster_id);
  ASSERT_EQ(puller->req_.puller_node_id(), opt.datanode_id);
  ASSERT_EQ(puller->req_.slot_range().start(), start);
  ASSERT_EQ(puller->req_.slot_range().end(), end);
  ASSERT_FALSE(puller->req_.stop_write());
  auto ret = slot_range->GetSyncPoint();
  ASSERT_TRUE(ret.IsOK());
  auto sync_point = ret.GetValue();
  ASSERT_EQ(puller->req_.sync_point().next_seq_id(), sync_point.next_seq_id());
  ASSERT_EQ(puller->req_.sync_point().prev_log_ts(), sync_point.prev_log_ts());
  ASSERT_EQ(puller->req_.sync_point().prev_rep_id(), sync_point.prev_rep_id());
  done_cb_flag = false;
  puller->OnDone(kOkStatus);
  ASSERT_TRUE(done_cb_flag);
  ASSERT_FALSE(puller->finish_error_.has_value());
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::Done);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  // apply wb
  puller = new_puller();
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::Init);
  std::string key{"key1"}, val{"val1"}, read_val;
  rocksdb::WriteBatch wb;
  wb.Put(key, val);
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber() + 1);
  puller->resp_.add_write_batches(wb.Data());
  puller->resp_.set_caught_up(false);
  auto s = storage->Get(storage->DefaultScanOptions(), key, &read_val);
  ASSERT_TRUE(s.IsNotFound() && read_val.empty());
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::WriteStop);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATING);
  s = storage->Get(storage->DefaultScanOptions(), key, &read_val);
  ASSERT_TRUE(s.ok() && read_val == val);
  stop_cb_flag = false, done_cb_flag = false;
  puller->OnDone(kSyncSeqIdOutOfRangeStatus);
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_GAP);
  // apply wb and caught up
  puller = new_puller();
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber());
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
  key = "key2", val = "key2", read_val = "";
  wb.Clear();
  wb.Put(key, val);
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber() + 1);
  puller->resp_.add_write_batches(wb.Data());
  puller->resp_.set_caught_up(true);
  s = storage->Get(storage->DefaultScanOptions(), key, &read_val);
  ASSERT_TRUE(s.IsNotFound() && read_val.empty());
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::CaughtUp);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
  s = storage->Get(storage->DefaultScanOptions(), key, &read_val);
  ASSERT_TRUE(s.ok() && read_val == val);
  stop_cb_flag = false, done_cb_flag = false;
  puller->OnDone(kSyncReplicaIdMismatchStatus);
  ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);  // caught up and not timeout error
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_CONFLICT);
  // apply caught up
  puller = new_puller();
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber());
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();  // stop write
  ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::WriteStop);
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();  // already stop write
  ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::WriteStop);
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber());
  puller->resp_.set_caught_up(true);
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();  // set caught up
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::CaughtUp);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();  // already set caught up
  ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::CaughtUp);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
  stop_cb_flag = false, done_cb_flag = false;
  puller->OnDone(kSyncStopWriteTimeoutStatus);
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);  // caught up and timeout error
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_TIMEOUT);
  // read failed
  puller = new_puller();
  stop_cb_flag = false, done_cb_flag = false;
  puller->OnReadDone(false);
  ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
  stop_cb_flag = false, done_cb_flag = false;
  puller->OnDone(kSyncLogTimestampMismatchStatus);
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_CONFLICT);
  // mark finished
  std::vector<std::pair<kv::datanode::v1::Error, kv::controller::v1::SlotRange::ReplicationStatus>> error_status_pairs;
  error_status_pairs.emplace_back(kClusterTopologyChangedError,
                                  kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_TOPOLOGY_MISMATCH);
  error_status_pairs.emplace_back(kMigrationTaskFailedError,
                                  kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  error_status_pairs.emplace_back(kTimeoutPointExceededError,
                                  kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  for (auto& [error, status] : error_status_pairs) {
    puller = new_puller();
    ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
    stop_cb_flag = false, done_cb_flag = false;
    ASSERT_TRUE(puller->MarkFinished(error));
    ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
    stop_cb_flag = false, done_cb_flag = false;
    puller->OnDone(kSyncStopWriteTimeoutStatus);  // already done
    ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
  }
  // stream finished
  std::vector<std::pair<grpc::Status, kv::controller::v1::SlotRange::ReplicationStatus>> status_pairs;
  status_pairs.emplace_back(kUnknownStatus, kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  status_pairs.emplace_back(kClusterIdMismatchStatus,
                            kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_TOPOLOGY_MISMATCH);
  status_pairs.emplace_back(kSyncSeqIdOutOfRangeStatus,
                            kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_GAP);
  status_pairs.emplace_back(kSyncReplicaIdMismatchStatus,
                            kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_CONFLICT);
  status_pairs.emplace_back(kSyncLogTimestampMismatchStatus,
                            kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_CONFLICT);
  status_pairs.emplace_back(kSyncStopWriteTimeoutStatus,
                            kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_TIMEOUT);
  for (auto& [rpc_status, repl_status] : status_pairs) {
    puller = new_puller();
    ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
    stop_cb_flag = false, done_cb_flag = false;
    puller->OnDone(rpc_status);
    ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
    ASSERT_EQ(slot_range->ReplicationStatus(), repl_status);
  }
  // apply failed
  puller = new_puller();
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber());
  puller->resp_.add_write_batches("");
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::Done);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  puller->OnDone(grpc::Status::CANCELLED);
  // check stop write failed
  puller = new_puller();
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber() - 1);
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::Done);
  ASSERT_EQ(puller->finish_error_->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_UNSPECIFIED);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  puller->OnDone(grpc::Status::CANCELLED);
  // check caught up failed(without stop write)
  puller = new_puller();
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber());
  puller->resp_.set_caught_up(true);
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::Done);
  ASSERT_EQ(puller->finish_error_->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_UNSPECIFIED);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  puller->OnDone(grpc::Status::CANCELLED);
  // check caught up failed(log id mismatch)
  puller = new_puller();
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber());
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
  puller->resp_.set_sender_seq_id(storage->LatestSeqNumber() + 1);
  puller->resp_.set_caught_up(true);
  stop_cb_flag = false, done_cb_flag = false;
  puller->handleResp();
  ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
  ASSERT_EQ(puller->sync_status_, SyncStatusEnum::Done);
  ASSERT_EQ(puller->finish_error_->code(), kv::datanode::v1::ErrorCode::ERROR_CODE_UNSPECIFIED);
  ASSERT_EQ(slot_range->ReplicationStatus(), kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN);
  puller->OnDone(grpc::Status::CANCELLED);

  srv.Stop();
  SetOutTest();
}

}  // namespace redis
