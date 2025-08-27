#include "sync/sync_sender.h"

#include "common/pb_util.h"
#include "common/thread_util.h"

namespace redis {

template <class Request, class Response>
OptionalSyncError SyncSender<Request, Response>::CheckWALBoundary(std::shared_ptr<redis::SlotRange>& slot_range,
                                                                  rocksdb::SequenceNumber seq) {
  auto storage = slot_range->GetStorage();
  auto upper_bound = storage->LatestSeqNumber() + 1;
  if (seq == upper_bound) {
    return std::nullopt;
  }
  // Upper bound
  if (seq > upper_bound) {
    return SyncSeqIdOutOfRangeSyncError("req log id exceed local upper bound, local upper bound:" +
                                        std::to_string(upper_bound));
  }
  // Lower bound
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = storage->GetWALIter(seq, &iter);
  if (!s.IsOK()) {
    return SyncSeqIdOutOfRangeSyncError("get wal iterator failed, err:" + s.Msg());
  }
  auto batch = iter->GetBatch();
  if (seq != batch.sequence) {
    return SyncSeqIdOutOfRangeSyncError("get inconsistent next seq to sync, local next seq:" +
                                        std::to_string(batch.sequence));
  }
  return std::nullopt;
}

template <class Request, class Response>
OptionalSyncError SyncSender<Request, Response>::CheckSyncPoint(const kv::datanode::v1::SyncPoint& req,
                                                                const kv::datanode::v1::SyncPoint& local) {
  if (req.next_seq_id() != local.next_seq_id()) {
    return SyncSeqIdOutOfRangeSyncError("get inconsistent next seq to sync, local next seq:" +
                                        std::to_string(local.next_seq_id()));
  }
  auto& req_rep_id = req.prev_rep_id();
  auto& local_rep_id = local.prev_rep_id();
  if (!req_rep_id.empty() && !local_rep_id.empty() && req_rep_id != local_rep_id) {
    return kSyncReplicaIdMismatchError;
  }
  auto req_log_ts = req.prev_log_ts();
  auto local_log_ts = local.prev_log_ts();
  if (req_log_ts != local_log_ts) {
    return kSyncLogTimestampMismatchError;
  }
  return std::nullopt;
}

template <class Request, class Response>
OptionalSyncError SyncSender<Request, Response>::CheckSyncPoint(const Request& req,
                                                                std::shared_ptr<SlotRange>& slot_range) {
  auto& req_sync_point = req.sync_point();
  if (req_sync_point.prev_rep_id().empty() && req_sync_point.prev_log_ts() == 0) {
    LOG(WARNING) << "[sync send] Skip check req sync point, req:" << req;
    return std::nullopt;
  }
  auto ret = slot_range->GetSyncPoint(req_sync_point.next_seq_id() - 1);
  if (!ret.IsOK()) {
    LOG(WARNING) << "[sync send] Get local sync point failed, req:" << req << ", err:" << ret.Msg();
    return std::nullopt;
  }
  auto error = CheckSyncPoint(req_sync_point, ret.GetValue());
  if (error.has_value()) {
    LOG(ERROR) << "[sync send] Sync point mismatch, req:" << req << ", local:" << ret.GetValue();
  }
  return error;
}

template <class Request, class Response>
bool SyncSender<Request, Response>::setRequest(const Request& req) {
  req_.CopyFrom(req);

  auto& slot_range_idx = req.slot_range();
  slot_range_index_name_ = SlotRangeIndexToString(slot_range_idx);
  GlobalStatsInstance().IncrSyncCount(slot_range_index_name_, stream_type_);
  // next seq id not zero
  if (req.sync_point().next_seq_id() == 0) {
    MarkFinished(UnknownSyncError("invalid next seq id: should not be zero"), false);
    return false;
  }
  // add to sync manager
  auto slot_range_name = CreateSlotRangeName(slot_range_idx.start(), slot_range_idx.end());
  auto status = AddToSyncManger(srv_, slot_range_name);
  if (!status.IsOK()) {
    MarkFinished(SyncStreamExistedSyncError(status.Msg()), false);
    return false;
  }
  slot_range_name_ = std::move(slot_range_name);
  // basic check for req
  if (!srv_->CheckClusterId(req.cluster_id())) {
    MarkFinished(kClusterIdMismatchError, false);
    return false;
  }
  auto error = srv_->cluster->CanSyncSendData(req.puller_node_id(), stream_type_ == SyncStreamType::DtsSender);
  if (error.has_value()) {
    MarkFinished(error, false);
    return false;
  }
  auto slot_range = srv_->GetSlotRangeByIndex(slot_range_idx);
  if (!slot_range) {
    MarkFinished(kSlotRangeNotFoundError, false);
    return false;
  }

  if (stream_type_ == SyncStreamType::DtsSender) {
    error = slot_range->CanSyncSendDataCrossPool(req.puller_node_id());
    if (error.has_value()) {
      MarkFinished(error, false);
      return false;
    }
  }

  error = CheckSyncPoint(req, slot_range);
  if (error.has_value()) {
    MarkFinished(error, false);
    return false;
  }
  error = CheckWALBoundary(slot_range, req.sync_point().next_seq_id());
  if (error.has_value()) {
    MarkFinished(error, false);
    return false;
  }
  if (stream_type_ == SyncStreamType::ReplSender) {
    auto ret = srv_->cluster->GetDatanodeGrpcAddr(req.puller_node_id());
    if (!ret.IsOK()) {
      MarkFinished(SlotRangeNodeIdNotFoundSyncError(ret.Msg()), false);
      return false;
    }
  }
  // already exceed max seq
  if (ExceedMaxSeq(req, req.sync_point().next_seq_id())) {
    MarkFinished(std::nullopt, false);
    return false;
  }
  // update status to syncing
  SyncStatusEnum init_status = SyncStatusEnum::Init;
  if (!sync_status_.compare_exchange_strong(init_status, SyncStatusEnum::Syncing)) {
    MarkFinished(UnknownSyncError("invalid sync status:" + SyncStatusEnumToString(sync_status_.load())), false);
    return false;
  }
  // create thread to write stream
  slot_range_ = std::move(slot_range);
  storage_ = slot_range_->GetStorage();
  next_seq_.store(req.sync_point().next_seq_id());
  auto s = util::MakeUniqueThread(slot_range_name_, [this] { this->writeStreamThreadFunc(); });
  if (!s.IsOK()) {
    sync_status_.store(SyncStatusEnum::Init);
    MarkFinished(UnknownSyncError("create sync thread failed:" + s.Msg()), false);
    return false;
  }

  write_thread_ = std::move(s.GetValue());
  notifyWriteStream();
  return true;
}

template <class Request, class Response>
void SyncSender<Request, Response>::writeStreamThreadFunc() {
  auto deadline = kNoStopWriteDeadline;
  uint64_t client_status_version = 0;
  uint64_t dts_status_version = 0;
  bool caught_up_sent = false;
  bool reach_max_seq = false;
  bool first_batch = true;
  bool first_resp = true;
  int cv_flag = 0;
  while (true) {
    {
      auto check = [&] {
        cv_flag = cv_flag_;
        cv_flag_ &= ~kWriteStreamFlag;
        return cv_flag & kFlagMask;
      };
      std::unique_lock<std::mutex> lk(cv_mtx_);
      if (deadline == kNoStopWriteDeadline) {
        cv_.wait(lk, check);
      } else if (!cv_.wait_until(lk, deadline, check)) {
        finish_error_ = kSyncStopWriteTimeoutError;
        break;
      }
    }
    if (cv_flag & kCloseStreamFlag) {
      finish_error_ = cv_error_;
      break;
    }
    if (cv_flag & kDoStopWriteFlag) {
      stopWrite(deadline, client_status_version, dts_status_version);
    }
    if (cv_flag & kWriteStreamFlag) {
      size_t updates = 0;
      size_t bytes = 0;
      resp_.Clear();

      if (first_resp || reach_max_seq) {
        {
          std::unique_lock<std::mutex> lk(cv_mtx_);
          cv_flag = cv_flag_;
        }
        if (cv_flag & kCloseStreamFlag) {
          finish_error_ = cv_error_;
          break;
        }
        if (reach_max_seq) {
          break;
        }
        // we must send the first resp with empty logs:
        // 1. notify repl puller to stop write when log gap is zero
        // 2. notify dts puller sync has started when log gap is zero
        first_resp = false;
      } else {
        while (true) {
          if (isStopWriteTimeout(deadline)) {
            finish_error_ = kSyncStopWriteTimeoutError;
            goto finish;
          }
          {
            std::unique_lock<std::mutex> lk(cv_mtx_);
            cv_flag = cv_flag_;
          }
          if (cv_flag & kCloseStreamFlag) {
            finish_error_ = cv_error_;
            goto finish;
          }
          if (cv_flag & kDoStopWriteFlag) {
            stopWrite(deadline, client_status_version, dts_status_version);
          }
          static const uint32_t yield_microseconds = 2 * 1000;
          switch (prepareData(first_batch, updates, bytes)) {
            case PrepareAction::Continue:
              break;
            case PrepareAction::Sleep:
              usleep(yield_microseconds);
              break;
            case PrepareAction::Write:
              goto write;
            case PrepareAction::Finish:
              if (finish_error_ == std::nullopt && !resp_.write_batches().empty()) {
                reach_max_seq = true;
                goto write;
              }
              goto finish;
          }
        }
      }
    write:
      // TODO(ying.qiu): limit rate to send sync data
      GlobalStatsInstance().IncrSyncWriteStreamStats(slot_range_index_name_, stream_type_, updates, bytes);
      auto caught_up = sync_status_.load() == SyncStatusEnum::CaughtUp;
      if (caught_up) {
        LOG(INFO) << "[sync send] Send resposne to set caught up, req:" << req_
                  << ", local log id:" << storage_->LatestSeqNumber();
        caught_up_sent = true;
      }
      SetSenderSeqIdInResp(resp_, storage_);
      SetCaughtUpInResp(resp_, caught_up);
      StartWrite(&resp_);
    }
  }

finish:
  auto prev_status = sync_status_.exchange(SyncStatusEnum::Done);
  if (prev_status >= SyncStatusEnum::WriteStop && deadline != kNoStopWriteDeadline && !has_topo_changed_.load()) {
    std::weak_ptr<SlotRange> slot_range_weak = slot_range_;
    auto clean_status_func = [srv = srv_, slot_range_weak, slot_range_index_name = slot_range_index_name_,
                              client_status_version, dts_status_version](void*) mutable {
      auto slot_range = slot_range_weak.lock();
      if (!slot_range) return;
      auto stop_write_time_ms =
          slot_range->SetClientWriteRunningStatus(WriteStatus::UNSPECIFIED, client_status_version, true);
      if (stop_write_time_ms > 0) {
        GlobalStatsInstance().IncrReplSenderStopWriteTime(slot_range_index_name, stop_write_time_ms);
        GlobalStatsInstance().IncrReplSenderWaitTopoTimeoutCount(slot_range_index_name);
        LOG(WARNING) << fmt::format("[sync send] SlotRange {} stop write timeout", slot_range->GetName());
      }
      slot_range->SetDtsWriteRunningStatus(WriteStatus::UNSPECIFIED, dts_status_version, true);
    };
    if (!caught_up_sent) {
      clean_status_func(nullptr);
    } else {
      auto timeout_ms = srv_->GetConfig()->stop_write_wait_topo_timeout_ms;
      LOG(INFO) << "[sync send] Add timeout task to clean running status, req:" << req_
                << ", timeout in ms:" << timeout_ms;
      srv_->timeout_mgr->RegisterTimeoutTask(timeout_ms, clean_status_func, nullptr);
    }
  }
  auto local = storage_->LatestSeqNumber();
  auto sent = next_seq_.load();
  if (sent > 0) --sent;
  log_iter_.reset();
  storage_.reset();
  slot_range_.reset();
  storage_mtx_.unlock();

  LOG(INFO) << "[sync send] Send thread exit, req:" << req_ << ", error:" << finish_error_ << ", local log id:" << local
            << ", sent log id:" << sent;
  Finish(SyncStatus(finish_error_));
}

template <class Request, class Response>
PrepareAction SyncSender<Request, Response>::prepareData(bool& first_batch, size_t& updates, size_t& bytes) {
  auto curr_seq = next_seq_.load();
  // make sure storage has log data with seq >= curr_seq
  if (!storage_->WALHasNewData(curr_seq)) {
    if (sync_status_.load() == SyncStatusEnum::WriteStop) {
      LOG(INFO) << "[sync send] Replica has caught up, req:" << req_ << ", sent log id:" << curr_seq - 1;
      GlobalStatsInstance().IncrReplSenderCaughtUpCount(slot_range_index_name_);
      sync_status_.store(SyncStatusEnum::CaughtUp);
      return PrepareAction::Write;
    }
    return PrepareAction::Sleep;
  }
  // move forward the log iter
  if (log_iter_ && log_iter_->Valid()) {
    log_iter_->Next();
  }
  if (!log_iter_ || !log_iter_->Valid()) {
    if (log_iter_) {
      LOG(INFO) << "[sync send] WAL was rotated, would reopen again, req:" << req_;
    }
    if (!storage_->GetWALIter(curr_seq, &log_iter_).IsOK()) {
      log_iter_ = nullptr;
      return PrepareAction::Sleep;
    }
  }
  // valid log iter
  if (sync_status_.load() == SyncStatusEnum::CaughtUp) {
    LOG(WARNING) << "[sync send] Iter new log data after caught up, req:" << req_;
    finish_error_ = kSyncStopWriteTimeoutError;
    return PrepareAction::Finish;
  }
  auto batch = log_iter_->GetBatch();
  // inconsistent seq
  if (batch.sequence != curr_seq) {
    LOG(ERROR) << "[sync send] WAL iterator is discrete, some seq might be lost, req:" << req_
               << ", expect seq:" << curr_seq << ", actual seq:" << batch.sequence;
    finish_error_ = UnknownSyncError("get inconsistent seq during sync");
    return PrepareAction::Finish;
  }
  curr_seq = batch.sequence + batch.writeBatchPtr->Count();
  // exceed max seq id before append
  if (ExceedMaxSeq(req_, curr_seq - 1)) {
    return PrepareAction::Finish;
  }
  updates += batch.writeBatchPtr->Count();
  bytes += batch.writeBatchPtr->GetDataSize();
  resp_.add_write_batches(batch.writeBatchPtr->Data());
  next_seq_.store(curr_seq);
  // exceed max seq after append
  if (ExceedMaxSeq(req_, curr_seq)) {
    return PrepareAction::Finish;
  }
  if (first_batch || bytes >= max_delay_bytes_.load() || updates >= max_delay_updates_.load() ||
      storage_->LatestSeqNumber() - batch.sequence <= max_delay_updates_.load()) {
    first_batch = false;
    return PrepareAction::Write;
  }
  return PrepareAction::Continue;
}

template <class Request, class Response>
bool SyncSender<Request, Response>::stopWrite(std::chrono::steady_clock::time_point& deadline,
                                              uint64_t& client_status_version, uint64_t& dts_status_version) {
  if (sync_status_.load() < SyncStatusEnum::WriteStop) {
    auto s = stopWriteForSlotRange(client_status_version, dts_status_version);
    if (!s.IsOK()) {
      LOG(ERROR) << "[sync send] Stop write failed, req:" << req_ << ", error:" << s.Msg();
      GlobalStatsInstance().IncrReplSenderStopWriteFailCount(slot_range_index_name_);
      return false;
    } else {
      deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(srv_->GetConfig()->stop_write_timeout_ms);
      LOG(INFO) << "[sync send] Stop write success, req:" << req_ << ", local log id:" << storage_->LatestSeqNumber();
      GlobalStatsInstance().IncrReplSenderStopWriteSuccCount(slot_range_index_name_);
      sync_status_.store(SyncStatusEnum::WriteStop);
    }
  }
  std::unique_lock<std::mutex> lk(cv_mtx_);
  cv_flag_ &= ~kDoStopWriteFlag;
  return true;
}

template <class Request, class Response>
Status SyncSender<Request, Response>::stopWriteForSlotRange(uint64_t& client_status_version,
                                                            uint64_t& dts_status_version) {
  {
    Context ctx;
    auto s = SlotRangeLock::AcquireSlotRangeLock(slot_range_name_, mgl::LockMode::LOCK_X, &ctx, srv_->GetMGLockMgr(),
                                                 srv_->GetConfig()->stop_write_try_lock_timeout_ms);
    if (!s.IsOK()) {
      return s.ToStatus();
    }
    slot_range_->SetClientWriteRunningStatus(WriteStatus::RO, client_status_version, false);
  }
  {
    slot_range_->SetDtsWriteRunningStatus(WriteStatus::RO, dts_status_version, false);
    srv_->sync_manager->StopAndJoinDtsReceiver(slot_range_name_,
                                               SlotRangeStatusMismatchSyncError("dts running status is read only"));
  }
  return Status::OK();
}

template class SyncSender<kv::datanode::v1::PullSyncDataRequest, kv::datanode::v1::PullSyncDataResponse>;
template class SyncSender<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse>;

}  // namespace redis
