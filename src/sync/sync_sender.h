#pragma once

#include <grpc++/grpc++.h>
#include <kv/datanode/v1/sync.pb.h>
#include <rocksdb/types.h>

#include <condition_variable>
#include <mutex>
#include <thread>

#include "cluster/cluster.h"
#include "common/sync_status.h"
#include "server/server.h"
#include "storage/storage.h"

namespace redis {

enum class PrepareAction : int { Continue = 0, Write = 1, Sleep = 2, Finish = 3 };

template <class Request, class Response>
class SyncSender {
 public:
  SyncSender(const std::shared_ptr<Server>& srv, grpc::CallbackServerContext* ctx, bool across_pool)
      : srv_(srv), ctx_(ctx), stream_type_(across_pool ? SyncStreamType::DtsSender : SyncStreamType::ReplSender) {
    storage_mtx_.lock();
  };

  virtual ~SyncSender() = default;

  // WARNING: join the thread before you destrcut the derived class
  // for that this thread function will call prue virtual function.
  void ThreadJoin() {
    if (write_thread_) {
      write_thread_->join();
    }
  }

  SyncSender(const SyncSender&) = delete;

  SyncSender& operator=(const SyncSender&) = delete;

  virtual void Finish(grpc::Status) = 0;

  virtual void StartWrite(const Response*) = 0;

  virtual void StartWriteAndFinish(const Response*, grpc::WriteOptions, grpc::Status) = 0;

  virtual Status AddToSyncManger(const std::shared_ptr<Server>&, const std::string&) = 0;

  virtual void RemoveFromSyncManager(const std::shared_ptr<Server>&, const std::string&, const OptionalSyncError&) = 0;

  virtual void SetCaughtUpInResp(Response&, bool) {}

  virtual void SetSenderSeqIdInResp(Response&, const std::shared_ptr<engine::Storage>&) {}

  virtual bool ExceedMaxSeq(const Request&, rocksdb::SequenceNumber) { return false; }

  static OptionalSyncError CheckWALBoundary(std::shared_ptr<redis::SlotRange>&, rocksdb::SequenceNumber);

  static OptionalSyncError CheckSyncPoint(const kv::datanode::v1::SyncPoint& req,
                                          const kv::datanode::v1::SyncPoint& local);

  OptionalSyncError CheckSyncPoint(const Request& req, std::shared_ptr<SlotRange>& slot_range);

  // WARNING: you must clean slot range running status when is_topo_changed is true
  void MarkFinished(const OptionalSyncError& err, bool is_topo_changed = true) {
    if (is_topo_changed) {
      has_topo_changed_.store(true);
    }
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

  SyncStatusEnum GetSyncStatus() { return sync_status_.load(); }

  OptionalSyncError& GetFinishError() { return finish_error_; };

  kv::datanode::v1::SyncConfig GetSyncConfig() {
    kv::datanode::v1::SyncConfig config;
    config.mutable_slot_range()->CopyFrom(req_.slot_range());
    config.set_puller_node_id(req_.puller_node_id());
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

  Response& GetResponse() { return resp_; }

  rocksdb::SequenceNumber GetNextSeq() { return next_seq_.load(); }

 protected:
  bool setRequest(const Request&);

  void notifyStopWrite() { notifyWithFlag(kDoStopWriteFlag); }

  void notifyWriteStream() { notifyWithFlag(kWriteStreamFlag); }

  std::shared_ptr<Server>& getServer() { return srv_; };

  Request& getRequest() { return req_; };

  std::string& getSlotRangeIndexName() { return slot_range_index_name_; };

  std::string& getSlotRangeName() { return slot_range_name_; };

 private:
  // notify flags
  static constexpr int kWriteStreamFlag = 1 << 0;
  static constexpr int kDoStopWriteFlag = 1 << 1;
  static constexpr int kCloseStreamFlag = 1 << 2;
  static constexpr int kFlagMask = kWriteStreamFlag | kDoStopWriteFlag | kCloseStreamFlag;
  // sync batch config
  static const uint64_t kDefMaxDelayUpdates = 16;
  static const uint64_t kDefMaxDelayBytes = 16 * 1024;
  static const uint64_t kDefMaxBytesPerSecond = 20 * 1024 * 1024;
  // unlimited deadline
  static constexpr auto kNoStopWriteDeadline = std::chrono::steady_clock::time_point::max();

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

  Status stopWriteForSlotRange(uint64_t& client_status_version, uint64_t& dts_status_version);

  bool stopWrite(std::chrono::steady_clock::time_point& deadline, uint64_t& client_status_version,
                 uint64_t& dts_status_version);

  bool isStopWriteTimeout(std::chrono::steady_clock::time_point& deadline) {
    return deadline != kNoStopWriteDeadline && std::chrono::steady_clock::now() >= deadline;
  }

  int64_t getLeftMsToDeadline(std::chrono::steady_clock::time_point& deadline) {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
  }

  // build param
  std::shared_ptr<Server> srv_;
  grpc::CallbackServerContext* ctx_;
  const SyncStreamType stream_type_;
  // sync req & resp
  Request req_;
  Response resp_;
  std::string slot_range_index_name_ = "uninitialized";
  std::string slot_range_name_;
  std::shared_ptr<SlotRange> slot_range_;
  // sync iterator
  std::mutex storage_mtx_;
  std::shared_ptr<engine::Storage> storage_;
  std::unique_ptr<rocksdb::TransactionLogIterator> log_iter_;
  // sync notifier
  std::atomic<bool> has_topo_changed_ = false;
  std::mutex cv_mtx_;
  std::condition_variable cv_;
  int cv_flag_ = 0;
  OptionalSyncError cv_error_ = std::nullopt;
  // sync thread
  std::unique_ptr<std::thread> write_thread_;
  OptionalSyncError finish_error_ = std::nullopt;
  std::atomic<rocksdb::SequenceNumber> next_seq_ = 0;
  std::atomic<SyncStatusEnum> sync_status_ = SyncStatusEnum::Init;
  // sync batch config
  std::atomic<uint64_t> max_delay_bytes_{kDefMaxDelayBytes};
  std::atomic<uint64_t> max_delay_updates_{kDefMaxDelayUpdates};
  std::atomic<uint64_t> max_bytes_per_second_{kDefMaxBytesPerSecond};
};

}  // namespace redis
