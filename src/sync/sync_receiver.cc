#include "sync_receiver.h"

#include "common/pb_util.h"
#include "common/sync_status.h"

namespace redis {

const kv::datanode::v1::Error SyncReceiver::kNextSeqIdMismatchErr = SyncPointMismatchSyncError("next seq id mismatch");
const kv::datanode::v1::Error SyncReceiver::kPrevLogTsMismatchErr = SyncPointMismatchSyncError("prev log ts mismatch");
const kv::datanode::v1::Error SyncReceiver::kPrevRepIdMismatchErr =
    SyncPointMismatchSyncError("prev replica id mismatch");

SyncReceiver::SyncReceiver(const std::shared_ptr<Server>& srv, grpc::CallbackServerContext* ctx,
                           kv::datanode::v1::PushSyncDataResponse* resp)
    : srv_(srv), ctx_(ctx), resp_(resp) {
  StartRead(&req_);
}

OptionalSyncError SyncReceiver::CheckSyncPoint(const kv::datanode::v1::SyncPoint& actual,
                                               const kv::datanode::v1::SyncPoint& expect) {
  if (actual.next_seq_id() != expect.next_seq_id()) {
    return kNextSeqIdMismatchErr;
  }
  if (actual.prev_log_ts() != expect.prev_log_ts()) {
    return kPrevLogTsMismatchErr;
  }
  if (actual.prev_rep_id() != expect.prev_rep_id()) {
    return kPrevRepIdMismatchErr;
  }
  return std::nullopt;
}

void SyncReceiver::OnReadDone(bool ok) {
  if (!ok) {
    // TODO(ying.qiu): get and log error in detail
    stopRead(StreamUnavailableSyncError("read stream failed"));
    return;
  }

  // first push sync data request
  if (!slot_range_) {
    LOG(INFO) << "[sync recv] Recv request, req:" << req_;
    first_req_.CopyFrom(req_);

    // add to sync manager
    auto& slot_range_idx = req_.slot_range();
    slot_range_index_name_ = SlotRangeIndexToString(slot_range_idx);
    GlobalStatsInstance().IncrSyncCount(slot_range_index_name_, SyncStreamType::DtsRecver);
    auto slot_range_name = CreateSlotRangeName(slot_range_idx.start(), slot_range_idx.end());
    auto status = srv_->sync_manager->AddDtsReceiver(slot_range_name, this);
    if (!status.IsOK()) {
      stopRead(SyncStreamExistedSyncError(status.Msg()));
      return;
    }
    slot_range_name_ = std::move(slot_range_name);
    // basic check for req
    std::unique_lock<std::mutex> lk(storage_mtx_);
    if (IsFinished()) {
      finish();
      return;
    }
    if (!srv_->CheckClusterId(req_.cluster_id())) {
      stopRead(kClusterIdMismatchError);
      return;
    }
    auto error = srv_->cluster->CanSyncReceiveDataCrossPool(req_.pusher_node_id());
    if (error.has_value()) {
      stopRead(error);
      return;
    }
    auto slot_range = srv_->GetSlotRangeByIndex(slot_range_idx);
    if (!slot_range) {
      stopRead(kSlotRangeNotFoundError);
      return;
    }
    const auto& pusher_node_id = req_.pusher_node_id();
    error = slot_range->CanSyncReceiveDataCrossPool(pusher_node_id);
    if (error.has_value()) {
      stopRead(error);
      return;
    }
    const auto ret = slot_range->GetSyncPoint();
    if (!ret.IsOK()) {
      error = UnknownSyncError(ret.Msg());
      stopRead(error);
      return;
    }
    error = CheckSyncPoint(req_.sync_point(), ret.GetValue());
    if (error.has_value()) {
      LOG(ERROR) << "[sync recv] Sync point mismatch, req:" << req_ << ", local sync point:" << ret.GetValue();
      stopRead(error);
      return;
    }

    slot_range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATING);
    storage_ = slot_range->GetStorage();
    slot_range_ = std::move(slot_range);
  }

  OptionalSyncError apply_error = std::nullopt;
  size_t apply_updates = 0;
  size_t apply_bytes = 0;
  bool finished = false;
  {
    std::unique_lock<std::mutex> lk(storage_mtx_);
    if (finished = IsFinished(); !finished) {
      for (auto& write_batch : req_.write_batches()) {
        if (finished = IsFinished(); finished) {
          break;
        }
        auto s = storage_->ReplicaApplyWriteBatch(storage_->DefaultWriteOptionsForReceiver(), write_batch);
        if (!s.IsOK()) {
          LOG(ERROR) << "[sync recv] Apply write batch failed, req:" << req_ << ", error:" << s.Msg() << ", batch:0x"
                     << util::StringToHex(write_batch);
          apply_error = UnknownSyncError("apply write batch failed:" + s.Msg());
          break;
        }
        auto ret = s.GetValue();
        apply_updates += ret.first;
        apply_bytes += ret.second;
      }
    }
  }

  GlobalStatsInstance().IncrSyncReadStreamStats(slot_range_index_name_, SyncStreamType::DtsRecver, apply_updates,
                                                apply_bytes);
  if (finished) {
    finish();
    return;
  }
  if (apply_error.has_value()) {
    stopRead(apply_error);
    return;
  }
  req_.Clear();
  StartRead(&req_);
}

void SyncReceiver::OnDone() {
  if (!slot_range_name_.empty()) {
    srv_->sync_manager->RemoveDtsReceiver(slot_range_name_, this, finish_error_);
  }

  LOG(INFO) << "[sync recv] Done request, req:" << first_req_ << ", error:" << finish_error_;
  GlobalStatsInstance().IncrSyncErrorCount(slot_range_index_name_, SyncStreamType::DtsRecver, finish_error_);
  delete this;
}

void SyncReceiver::stopRead(const OptionalSyncError& error) {
  MarkFinished(error);
  finish();
}

}  // namespace redis
