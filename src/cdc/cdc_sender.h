#pragma once

#include <grpc++/grpc++.h>
#include <kv/datanode/v1/cdc.pb.h>
#include <rocksdb/types.h>

#include <condition_variable>
#include <mutex>
#include <thread>

#include "cluster/cluster.h"
#include "common/pb_util.h"
#include "common/sync_status.h"
#include "server/server.h"
#include "storage/storage.h"

namespace redis {

class CDCSender : public grpc::ServerWriteReactor<kv::datanode::v1::CDCGetEventsResponse> {
 public:
  CDCSender(const std::shared_ptr<Server>& srv, grpc::CallbackServerContext* ctx,
            const kv::datanode::v1::CDCGetEventsRequest* req)
      : srv_(srv), ctx_(ctx), req_(req) {
    LOG(INFO) << "[cdc send] Recv request, req:" << *req_;
    storage_mtx_.lock();
    checkRequest();
  }

  ~CDCSender() override = default;

  CDCSender(const CDCSender&) = delete;

  CDCSender& operator=(const CDCSender&) = delete;

  void OnWriteDone(bool ok) override {
    if (!ok) {
      MarkFinished(StreamUnavailableSyncError("write stream failed"));
      return;
    }

    notifyWriteStream();
  }

  void OnCancel() override { MarkFinished(UnknownSyncError("request canceled")); }

  void OnDone() override {
    if (write_thread_) {
      write_thread_->join();
    }
    if (!slot_range_name_.empty()) {
      srv_->cdc_manager->RemoveCDCSender(slot_range_name_, this, finish_error_);
    }
    LOG(INFO) << "[cdc send] Done request, req:" << *req_ << ", error:" << finish_error_;
    GlobalStatsInstance().IncrCDCSenderErrorCount(slot_range_index_name_, finish_error_);
    delete this;
  }

  void MarkFinished(const OptionalSyncError& err) {
    SyncStatusEnum init_status = SyncStatusEnum::Init;
    if (sync_status_.compare_exchange_strong(init_status, SyncStatusEnum::Done)) {
      storage_mtx_.unlock();
      finish_error_ = err;
      Finish(SyncStatus(finish_error_));
      return;
    }
    if (sync_status_.load() == SyncStatusEnum::Done) {
      return;
    }
    {
      std::unique_lock<std::mutex> lk(cv_mtx_);
      if (cv_flag_ & kCloseStreamFlag) {
        return;
      }
      cv_flag_ |= kCloseStreamFlag;
      cv_error_ = err;
    }
    cv_.notify_one();
  }

  // WARNING: you should call MarkFinished() before StopIterData()
  void StopIterData() { std::unique_lock<std::mutex> lk(storage_mtx_); }

  bool IsFinished() { return sync_status_.load() == SyncStatusEnum::Done; }

  OptionalSyncError GetFinishError() { return finish_error_; }

  rocksdb::SequenceNumber GetNextSeq() { return next_seq_.load(); }

  kv::datanode::v1::SyncConfig GetSyncConfig() {
    kv::datanode::v1::SyncConfig config;
    config.mutable_slot_range()->CopyFrom(req_->slotrange_idx());
    config.set_max_delay_bytes(max_delay_bytes_.load());
    config.set_max_delay_updates(max_delay_updates_.load());
    config.set_max_bytes_per_second(max_bytes_per_second_.load());
    return config;
  }

  kv::datanode::v1::SyncConfig UpdateSyncConfig(const kv::datanode::v1::SyncConfig& config) {
    auto updated_config = GetSyncConfig();
    if (config.max_delay_bytes() > 0) {
      max_delay_bytes_.store(config.max_delay_bytes());
    }
    if (config.max_delay_updates() > 0) {
      max_delay_updates_.store(config.max_delay_updates());
    }
    if (config.max_bytes_per_second() > 0) {
      max_bytes_per_second_.store(config.max_bytes_per_second());
    }
    return updated_config;
  }

 private:
  FRIEND_TEST(CDCSender, RPC);

  // notify flags
  static constexpr int kWriteStreamFlag = 1 << 0;
  static constexpr int kCloseStreamFlag = 1 << 1;
  static constexpr int kFlagMask = kWriteStreamFlag | kCloseStreamFlag;
  // batch send config
  static const uint64_t kDefMaxDelayUpdates = 16;
  static const uint64_t kDefMaxDelayBytes = 32 * 1024;
  static const uint64_t kDefMaxBytesPerSecond = 20 * 1024 * 1024;
  // iter prepare action
  enum class PrepareAction : int { Continue = 0, Write = 1, Sleep = 2, Finish = 3 };

  static OptionalSyncError checkCDCPoint(const kv::datanode::v1::CDCPoint& req,
                                         const kv::datanode::v1::CDCPoint& local);

  static OptionalSyncError checkCDCPoint(std::shared_ptr<SlotRange>& slot_range, const kv::datanode::v1::CDCPoint& req);

  static OptionalSyncError checkWALBoundary(std::shared_ptr<SlotRange>&, const rocksdb::SequenceNumber&);

  bool checkRequest();

  void notifyWriteStream() { notifyWithFlag(kWriteStreamFlag); }

  void notifyWithFlag(int flag) {
    if (sync_status_.load() == SyncStatusEnum::Done) {
      return;
    }
    flag = flag & kFlagMask;
    {
      std::unique_lock<std::mutex> lk(cv_mtx_);
      if ((cv_flag_ & flag) == flag) {
        return;
      }
      cv_flag_ |= flag;
    }
    cv_.notify_one();
  }

  void writeStreamThreadFunc();

  PrepareAction prepareData(bool& first_resp, size_t& updates, size_t& bytes);

  void recordDataLag(const rocksdb::BatchResult& batch);

  Status getLatestLogTimeNanos(uint64_t* log_time_nanos);

  // build param
  const std::shared_ptr<Server> srv_;
  grpc::CallbackServerContext* ctx_;
  const kv::datanode::v1::CDCGetEventsRequest* req_;
  std::string slot_range_name_;
  std::string slot_range_index_name_;
  std::shared_ptr<SlotRange> slot_range_;
  std::mutex storage_mtx_;  // protect storage from close
  std::shared_ptr<engine::Storage> storage_;
  // sync notifier
  std::mutex cv_mtx_;
  std::condition_variable cv_;
  int cv_flag_ = 0;
  OptionalSyncError cv_error_ = std::nullopt;
  // sync thread
  std::unique_ptr<std::thread> write_thread_;
  kv::datanode::v1::CDCGetEventsResponse resp_;
  OptionalSyncError finish_error_ = std::nullopt;
  std::unique_ptr<rocksdb::TransactionLogIterator> log_iter_;
  std::atomic<rocksdb::SequenceNumber> next_seq_ = 0;
  std::atomic<SyncStatusEnum> sync_status_ = SyncStatusEnum::Init;
  std::chrono::steady_clock::time_point record_lag_last_ts_ = std::chrono::steady_clock::now();
  // sync batch config
  std::atomic<uint64_t> max_delay_bytes_{kDefMaxDelayBytes};
  std::atomic<uint64_t> max_delay_updates_{kDefMaxDelayUpdates};
  std::atomic<uint64_t> max_bytes_per_second_{kDefMaxBytesPerSecond};
};

}  // namespace redis
