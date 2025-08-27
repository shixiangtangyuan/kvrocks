#include "cdc/cdc_sender.h"

#include "common/thread_util.h"
#include "storage/batch_extractor_cdc.h"

namespace redis {

OptionalSyncError CDCSender::checkWALBoundary(std::shared_ptr<redis::SlotRange>& slot_range,
                                              const rocksdb::SequenceNumber& seq) {
  auto storage = slot_range->GetStorage();
  auto upper_bound = storage->LatestSeqNumber() + 1;
  if (seq == upper_bound) {
    return std::nullopt;
  }
  // Upper bound
  if (seq > upper_bound) {
    return CdcSeqIdExceededSyncError("req log id exceed local upper bound, local upper bound:" +
                                     std::to_string(upper_bound));
  }
  // Lower bound
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = storage->GetWALIter(seq, &iter);
  if (!s.IsOK()) {
    return UnknownSyncError("get wal iterator failed, err:" + s.Msg());
  }
  auto batch = iter->GetBatch();
  if (batch.sequence > seq) {
    return CdcWalDeletedSyncError("req log id maybe outdated, iter log id:" + std::to_string(batch.sequence));
  }
  if (seq != batch.sequence) {
    return CdcSeqIdMismatchSyncError("get inconsistent next seq to sync, local next seq:" +
                                     std::to_string(batch.sequence));
  }
  return std::nullopt;
}

OptionalSyncError CDCSender::checkCDCPoint(const kv::datanode::v1::CDCPoint& req,
                                           const kv::datanode::v1::CDCPoint& local) {
  if (req.next_seq_id() != local.next_seq_id()) {
    return CdcSeqIdMismatchSyncError("get inconsistent next seq to sync, local next seq:" +
                                     std::to_string(local.next_seq_id()));
  }
  auto& req_rep_id = req.prev_rep_id();
  auto& local_rep_id = local.prev_rep_id();
  if (!req_rep_id.empty() && !local_rep_id.empty() && req_rep_id != local_rep_id) {
    return kCdcReplIdMismatchError;
  }
  auto req_log_ts = req.prev_log_ts();
  auto local_log_ts = local.prev_log_ts();
  if (req_log_ts != 0 && local_log_ts != 0 && req_log_ts != local_log_ts) {
    return kCdcLogTsMismatchError;
  }
  return std::nullopt;
}

OptionalSyncError CDCSender::checkCDCPoint(std::shared_ptr<SlotRange>& slot_range,
                                           const kv::datanode::v1::CDCPoint& req) {
  if (req.prev_rep_id().empty() && req.prev_log_ts() == 0) {
    LOG(WARNING) << "[cdc send] Skip check cdc point, req:" << req;
    return std::nullopt;
  }
  auto ret = slot_range->GetCDCPoint(req.next_seq_id() - 1);
  if (!ret.IsOK()) {
    LOG(WARNING) << "[cdc send] Get local cdc point failed, req:" << req << ", err:" << ret.Msg();
    return std::nullopt;
  }
  auto error = checkCDCPoint(req, ret.GetValue());
  if (error.has_value()) {
    LOG(ERROR) << "[cdc send] Sync point mismatch, req:" << req << ", local:" << ret.GetValue();
    return error;
  }
  return std::nullopt;
}

bool CDCSender::checkRequest() {
  auto& cdc_point = req_->point();
  auto& slot_range_idx = req_->slotrange_idx();
  slot_range_index_name_ = SlotRangeIndexToString(slot_range_idx);
  GlobalStatsInstance().IncrCDCSenderCount(slot_range_index_name_);
  if (!srv_->GetConfig()->enable_cdc_sync) {
    MarkFinished(UnknownSyncError("cdc sync not enabled"));
    return false;
  }
  // next seq id not zero
  if (cdc_point.next_seq_id() == 0) {
    MarkFinished(UnknownSyncError("invalid next seq id: should not be zero"));
    return false;
  }
  // add to cdc manager
  auto slot_range_name = CreateSlotRangeName(slot_range_idx.start(), slot_range_idx.end());
  auto error = srv_->cdc_manager->AddCDCSender(slot_range_name, this, req_->is_take_over());
  if (error.has_value()) {
    MarkFinished(error);
    return false;
  }
  slot_range_name_ = std::move(slot_range_name);
  // basic check for req
  if (!srv_->CheckClusterId(req_->cluster_id())) {
    MarkFinished(kClusterIdMismatchError);
    return false;
  }
  error = srv_->cluster->CanSendCDCData();
  if (error.has_value()) {
    MarkFinished(error);
    return false;
  }
  auto slot_range = srv_->GetSlotRangeByIndex(slot_range_idx);
  if (!slot_range) {
    MarkFinished(kSlotRangeNotFoundError);
    return false;
  }
  error = checkCDCPoint(slot_range, cdc_point);
  if (error.has_value()) {
    MarkFinished(error);
    return false;
  }
  error = checkWALBoundary(slot_range, cdc_point.next_seq_id());
  if (error.has_value()) {
    MarkFinished(error);
    return false;
  }
  // update status to syncing
  SyncStatusEnum init_status = SyncStatusEnum::Init;
  if (!sync_status_.compare_exchange_strong(init_status, SyncStatusEnum::Syncing)) {
    MarkFinished(UnknownSyncError("invalid sync status:" + SyncStatusEnumToString(sync_status_.load())));
    return false;
  }
  // create thread to write stream
  slot_range_ = std::move(slot_range);
  storage_ = slot_range_->GetStorage();
  next_seq_.store(cdc_point.next_seq_id());
  auto s = util::MakeUniqueThread("cdc_" + slot_range_name_, [this] { this->writeStreamThreadFunc(); });
  if (!s.IsOK()) {
    sync_status_.store(SyncStatusEnum::Init);
    MarkFinished(UnknownSyncError("create sync thread failed:" + s.Msg()));
    return false;
  }

  write_thread_ = std::move(s.GetValue());
  notifyWriteStream();
  return true;
}

void CDCSender::writeStreamThreadFunc() {
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
      cv_.wait(lk, check);
    }
    if (cv_flag & kCloseStreamFlag) {
      finish_error_ = cv_error_;
      break;
    }
    if (cv_flag & kWriteStreamFlag) {
      size_t updates = 0;
      size_t bytes = 0;
      resp_.Clear();

      if (first_resp) {
        {
          std::unique_lock<std::mutex> lk(cv_mtx_);
          cv_flag = cv_flag_;
        }
        if (cv_flag & kCloseStreamFlag) {
          finish_error_ = cv_error_;
          break;
        }
        // we must send the first resp with empty logs:
        // 1. notify cdc puller sync has started when log gap is zero
        first_resp = false;
      } else {
        while (true) {
          {
            std::unique_lock<std::mutex> lk(cv_mtx_);
            cv_flag = cv_flag_;
          }
          if (cv_flag & kCloseStreamFlag) {
            finish_error_ = cv_error_;
            goto finish;
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
              goto finish;
          }
        }
      }
    write:
      // TODO(ying.qiu): limit rate to send data
      GlobalStatsInstance().IncrCDCSenderWriteStats(slot_range_index_name_, updates, bytes, next_seq_.load());
      thread_local_metric_array.RecordCDCSendBatchSizeHistogram(slot_range_index_name_, updates);
      StartWrite(&resp_);
    }
  }

finish:
  sync_status_.store(SyncStatusEnum::Done);
  auto local = storage_->LatestSeqNumber();
  auto sent = next_seq_.load();
  if (sent > 0) --sent;
  log_iter_.reset();
  storage_.reset();
  slot_range_.reset();
  storage_mtx_.unlock();

  LOG(INFO) << "[cdc send] Send thread exit, req:" << *req_ << ", error:" << finish_error_ << ", local log id:" << local
            << ", sent log id:" << sent;
  Finish(SyncStatus(finish_error_));
}

CDCSender::PrepareAction CDCSender::prepareData(bool& first_batch, size_t& updates, size_t& bytes) {
  auto start_ts = std::chrono::steady_clock::now();
  auto curr_seq = next_seq_.load();
  // make sure storage has log data with seq >= curr_seq
  if (!storage_->WALHasNewData(curr_seq)) {
    GlobalStatsInstance().RecordCDCDataLag(slot_range_index_name_, 0, 0);
    return PrepareAction::Sleep;
  }
  // move forward the log iter
  if (log_iter_ && log_iter_->Valid()) {
    log_iter_->Next();
  }
  if (!log_iter_ || !log_iter_->Valid()) {
    if (log_iter_) {
      LOG(INFO) << "[cdc send] WAL was rotated, would reopen again, req:" << *req_;
    }
    if (!storage_->GetWALIter(curr_seq, &log_iter_).IsOK()) {
      log_iter_ = nullptr;
      return PrepareAction::Sleep;
    }
  }
  // valid log iter
  auto batch = log_iter_->GetBatch();
  // inconsistent seq
  if (batch.sequence != curr_seq) {
    LOG(ERROR) << "[cdc send] WAL iterator is discrete, some seq might be lost, req:" << *req_
               << ", expect seq:" << curr_seq << ", actual seq:" << batch.sequence;
    finish_error_ = UnknownSyncError("get inconsistent seq during sync");
    return PrepareAction::Finish;
  }
  // parse events
  auto& cluster_id = srv_->cluster->ClusterId();
  auto s = cdc::GetCDCDataFromBatch(cluster_id, req_->slotrange_idx(), batch, resp_.mutable_events(), &bytes);
  if (!s.IsOK()) {
    LOG(ERROR) << "[cdc send] Parse cdc events from wal failed, req: " << *req_ << ", err: " << s.Msg();
    finish_error_ = CdcWalParseErrorSyncError("parse cdc events from wal failed");
    return PrepareAction::Finish;
  }
  auto batch_events = resp_.events_size() - updates;
  updates = resp_.events_size();
  curr_seq = batch.sequence + batch.writeBatchPtr->Count();
  next_seq_.store(curr_seq);

  // record parse latency
  auto end_ts = std::chrono::steady_clock::now();
  auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end_ts - start_ts).count();
  thread_local_metric_array.RecordCDCParseBatchLatency(slot_range_index_name_, latency);
  thread_local_metric_array.RecordCDCParseBatchSizeHistogram(slot_range_index_name_, batch_events);
  // record data lag
  if (end_ts - record_lag_last_ts_ >= std::chrono::seconds(5)) {  // record lag every 5s
    record_lag_last_ts_ = end_ts;
    recordDataLag(batch);
  }

  // TODO: (chris) support to dynamically modify updates limit by config
  if (first_batch || bytes >= max_delay_bytes_.load() || updates >= 10000 ||
      storage_->LatestSeqNumber() - batch.sequence <= 16) {
    first_batch = false;
    ReplIdExtractor log_data_handler;
    rocksdb::Status s = batch.writeBatchPtr->Iterate(&log_data_handler);
    if (!s.ok()) {
      LOG(WARNING) << "[cdc send] Parse next cdc point from wal failed, req:" << *req_ << ", err:" << s.ToString();
      finish_error_ = CdcWalParseErrorSyncError("parse next cdc point from wal failed");
      return PrepareAction::Finish;
    }
    resp_.mutable_point()->set_next_seq_id(curr_seq);
    resp_.mutable_point()->set_prev_rep_id(log_data_handler.GetReplId());
    resp_.mutable_point()->set_prev_log_ts(log_data_handler.GetTimeNanos());
    return PrepareAction::Write;
  }
  return PrepareAction::Continue;
}

void CDCSender::recordDataLag(const rocksdb::BatchResult& batch) {
  uint64_t log_time_nanos = 0;
  ReplIdExtractor log_data_handler;
  rocksdb::Status s = batch.writeBatchPtr->Iterate(&log_data_handler);
  if (!s.ok()) return;
  auto ret = getLatestLogTimeNanos(&log_time_nanos);
  if (log_time_nanos == 0 || !ret.IsOK()) return;
  auto latest_next_seq = storage_->LatestSeqNumber() + 1;
  auto next_seq = batch.sequence + batch.writeBatchPtr->Count();
  auto lag_duration_ms = (log_time_nanos - log_data_handler.GetTimeNanos()) / 1000000;
  GlobalStatsInstance().RecordCDCDataLag(slot_range_index_name_, lag_duration_ms, latest_next_seq - next_seq);
}

Status CDCSender::getLatestLogTimeNanos(uint64_t* log_time_nanos) {
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = storage_->GetWALIter(storage_->LatestSeqNumber(), &iter);
  if (!s.IsOK()) return s;

  ReplIdExtractor log_data_handler;
  rocksdb::Status ret = iter->GetBatch().writeBatchPtr->Iterate(&log_data_handler);
  if (!ret.ok()) return {Status::NotOK, ret.ToString()};

  *log_time_nanos = log_data_handler.GetTimeNanos();
  return Status::OK();
}

}  // namespace redis
