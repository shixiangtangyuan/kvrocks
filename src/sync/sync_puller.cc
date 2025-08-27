#include "sync/sync_puller.h"

#include "common/pb_util.h"
#include "common/test_flag.h"

namespace redis {

SyncPuller::SyncPuller(const std::string& cluster_id, const std::string& puller_node_id,
                       const std::shared_ptr<Server>& srv, const std::shared_ptr<redis::SlotRange>& slot_range,
                       const kv::datanode::v1::SyncPoint& sync_point, SlotRangeWriteStoppableCB write_stoppable_cb,
                       SlotRangeReplicationDoneCB repl_done_cb)
    : srv_(srv),
      slot_range_(slot_range),
      write_stoppeable_cb_(std::move(write_stoppable_cb)),
      repl_done_cb_(std::move(repl_done_cb)),
      req_(buildReq(cluster_id, puller_node_id, slot_range, sync_point)),
      slot_range_index_name_(SlotRangeIndexToString(req_.slot_range())),
      storage_(slot_range->GetStorage()) {
  slot_range_->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
}

SyncPuller::SyncPuller(const std::string& cluster_id, const std::string& puller_node_id,
                       const std::string& sender_node_id, const std::shared_ptr<Server>& srv,
                       const std::shared_ptr<redis::SlotRange>& slot_range,
                       const kv::datanode::v1::SyncPoint& sync_point,
                       const std::shared_ptr<kv::datanode::v1::DataNodeService::Stub>& stub,
                       SlotRangeWriteStoppableCB write_stoppable_cb, SlotRangeReplicationDoneCB repl_done_cb)
    : srv_(srv),
      slot_range_(slot_range),
      stub_(stub),
      write_stoppeable_cb_(std::move(write_stoppable_cb)),
      repl_done_cb_(std::move(repl_done_cb)),
      req_(buildReq(cluster_id, puller_node_id, slot_range, sync_point)),
      slot_range_index_name_(SlotRangeIndexToString(req_.slot_range())),
      storage_(slot_range->GetStorage()) {
  slot_range_->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_UNSPECIFIED);
  if (is_in_test) {
    LOG(INFO) << "[sync pull] Skip send request for test, req:" << req_ << ", sender:" << sender_node_id;
    return;
  }
  LOG(INFO) << "[sync pull] Send request, req:" << req_ << ", sender:" << sender_node_id;
  GlobalStatsInstance().IncrSyncCount(slot_range_index_name_, SyncStreamType::ReplPuller);
  stub_->async()->SyncData(&ctx_, this);
  LOG(INFO) << "[sync pull] Async send request done";
  StartWrite(&req_);
  StartRead(&resp_);
  AddHold();
  StartCall();
}

kv::datanode::v1::SyncDataRequest SyncPuller::buildReq(const std::string& cluster_id, const std::string& puller_node_id,
                                                       const std::shared_ptr<redis::SlotRange>& slot_range,
                                                       const kv::datanode::v1::SyncPoint& sync_point) {
  kv::datanode::v1::SyncDataRequest req;
  req.set_cluster_id(cluster_id);
  req.mutable_slot_range()->set_start(slot_range->GetRangeStart());
  req.mutable_slot_range()->set_end(slot_range->GetRangeEnd());
  req.set_puller_node_id(puller_node_id);
  req.mutable_sync_point()->CopyFrom(sync_point);
  req.set_stop_write(false);
  return req;
}

bool SyncPuller::StopWrite() {
  if (!canRemoveHold()) {
    return false;
  }
  LOG(INFO) << "[sync pull] Send request to stop write, req:" << req_;
  GlobalStatsInstance().IncrReplPullerStopWriteCount(slot_range_index_name_);
  req_.set_stop_write(true);
  if (!is_in_test) {
    StartWrite(&req_);
    RemoveHold();
  }
  return true;
}

void SyncPuller::OnWriteDone(bool ok) {
  if (!ok) {
    LOG(ERROR) << "[sync pull] Write stream failed, req:" << req_;
    tryRemoveHold();
    return;
  }
}

void SyncPuller::OnReadDone(bool ok) {
  if (!ok) {
    LOG(ERROR) << "[sync pull] Read stream failed, req:" << req_;
    tryRemoveHold();
    return;
  }

  handleResp();
  resp_.Clear();
  StartRead(&resp_);
}

void SyncPuller::handleResp() {
  if (resp_.caught_up()) {
    LOG(INFO) << "[sync pull] Recv response to set caught up, req:" << req_;
  }
  size_t apply_updates = 0;
  size_t apply_bytes = 0;
  bool stop_write = false;
  bool set_caught_up = false;
  OptionalSyncError error{std::nullopt};
  SyncStatusEnum status = SyncStatusEnum::Init;
  {
    std::unique_lock<std::mutex> lk(apply_mtx_);
    auto ret = switchStatus(SyncStatusEnum::Syncing);
    status = ret.first ? SyncStatusEnum::Syncing : ret.second;
    // apply data to local
    if (status < SyncStatusEnum::CaughtUp) {
      for (auto& wb : resp_.write_batches()) {
        if (status = getSyncStatus(); status >= SyncStatusEnum::CaughtUp) {
          break;
        }
        auto s = storage_->ReplicaApplyWriteBatch(storage_->DefaultWriteOptionsForPuller(), wb);
        if (!s.IsOK()) {
          LOG(ERROR) << "[sync pull] Apply write batch failed, req:" << req_ << ", error:" << s.Msg() << ", batch:0x"
                     << util::StringToHex(wb);
          error = UnknownSyncError("apply write batch failed:" + s.Msg());
          break;
        }
        auto ret = s.GetValue();
        apply_updates += ret.first;
        apply_bytes += ret.second;
      }
    }
    /// check stop write or not
    if (status == SyncStatusEnum::Syncing && !error.has_value()) {
      auto local = storage_->LatestSeqNumber(), sender = resp_.sender_seq_id();
      if (local > sender) {
        LOG(ERROR) << "[sync pull] Local log id is bigger than sync sender, req:" << req_ << ", local log id:" << local
                   << ", sender log id:" << sender;
        error = UnknownSyncError("local log id is bigger than sync sender");
      } else {
        stop_write = sender - local <= srv_->GetConfig()->stop_write_log_gap;
        if (stop_write) {
          LOG(INFO) << "[sync pull] Log gap is small enough to stop write, req:" << req_ << ", local log id:" << local
                    << ", sender log id:" << sender;
        }
      }
    }
    // check set caught up or not
    if (resp_.caught_up() && !error.has_value()) {
      if (status < SyncStatusEnum::WriteStop) {
        LOG(ERROR) << "[sync pull] Invalid sync status to set caught up, req:" << req_ << ", sync status:" << status;
        error = UnknownSyncError("invalid sync status to set caught up");
      } else if (status <= SyncStatusEnum::CaughtUp) {
        auto local = storage_->LatestSeqNumber(), sender = resp_.sender_seq_id();
        if (local != sender) {
          LOG(ERROR) << "[sync pull] Log id mismatch to set caught up, req:" << req_ << ", local log id:" << local
                     << ", sender log id:" << sender;
          error = UnknownSyncError("mismatch log id to set caught up");
        } else {
          set_caught_up = status == SyncStatusEnum::WriteStop;
          if (set_caught_up) {
            LOG(INFO) << "[sync pull] Puller has caught up, req:" << req_ << ", local log id:" << local;
          }
        }
      }
    }
  }

  GlobalStatsInstance().IncrSyncReadStreamStats(slot_range_index_name_, SyncStreamType::ReplPuller, apply_updates,
                                                apply_bytes);
  if (error.has_value()) {
    if (switchStatus(SyncStatusEnum::Done, error).first) {
      tryRemoveHold();
      ctx_.TryCancel();
    }
    return;
  }
  if (stop_write) {
    switchStatus(SyncStatusEnum::WriteStop);
    return;
  }
  if (set_caught_up) {
    auto s = storage_->SyncWal();
    if (!s.IsOK()) {
      LOG(ERROR) << "[sync pull] Sync wal failed, req:" << req_ << ", err:" << s.Msg();
      error = UnknownSyncError(s.Msg());
      if (switchStatus(SyncStatusEnum::Done, error).first) {
        tryRemoveHold();
        ctx_.TryCancel();
      }
      return;
    }
    auto ret = switchStatus(SyncStatusEnum::CaughtUp);
    if (ret.first) {
      GlobalStatsInstance().IncrReplPullerCaughtUpCount(slot_range_index_name_);
    }
    return;
  }
}

void SyncPuller::OnDone(const grpc::Status& status) {
  auto error = ParseSyncError(status);
  switchStatus(SyncStatusEnum::Done, error);
  srv_->sync_manager->RemoveReplPuller(slot_range_->GetName(), this, finish_error_);
  LOG(INFO) << "[sync pull] Done request, req:" << req_ << ", recv error:" << error << ", error:" << finish_error_;
  GlobalStatsInstance().IncrSyncErrorCount(slot_range_index_name_, SyncStreamType::ReplPuller, finish_error_);
  delete this;
}

}  // namespace redis
