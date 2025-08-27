#pragma once

#include <grpcpp/support/server_callback.h>
#include <gtest/gtest.h>
#include <kv/datanode/v1/sync.pb.h>
#include <rocksdb/types.h>
#include <rocksdb/write_batch.h>

#include "cluster/cluster.h"
#include "server/server.h"

namespace redis {

class SyncReceiver : public grpc::ServerReadReactor<kv::datanode::v1::PushSyncDataRequest> {
 public:
  explicit SyncReceiver(const std::shared_ptr<Server> &, grpc::CallbackServerContext *ctx,
                        kv::datanode::v1::PushSyncDataResponse *resp);

  ~SyncReceiver() override = default;

  SyncReceiver(const SyncReceiver &) = delete;

  SyncReceiver operator=(const SyncReceiver &) = delete;

  std::string GetPusherNodeId() const { return first_req_.pusher_node_id(); };

  static OptionalSyncError CheckSyncPoint(const kv::datanode::v1::SyncPoint &actual,
                                          const kv::datanode::v1::SyncPoint &expect);

  // mark finished to stop syncing from pusher
  bool MarkFinished(const OptionalSyncError &error) {
    std::unique_lock<std::mutex> lk(mtx_);
    if (finished_) {
      return false;
    }
    // TODO(ying.qiu): stream won't finish until next OnReadDone() func called
    finish_error_ = error;
    finished_ = true;
    return true;
  }

  bool IsFinished() {
    std::unique_lock<std::mutex> lk(mtx_);
    return finished_;
  }

  void StopApplyData() {
    DCHECK(finished_ == true);
    std::unique_lock<std::mutex> lk(storage_mtx_);
  }

  void OnCancel() override { MarkFinished(UnknownSyncError("request canceled")); }

  void OnDone() override;

  void OnReadDone(bool ok) override;

  static const kv::datanode::v1::Error kNextSeqIdMismatchErr;
  static const kv::datanode::v1::Error kPrevLogTsMismatchErr;
  static const kv::datanode::v1::Error kPrevRepIdMismatchErr;

 private:
  FRIEND_TEST(SyncReceiver, Base);

  void stopRead(const OptionalSyncError &status);

  grpc::Status status() {
    std::unique_lock<std::mutex> lk(mtx_);
    return finish_error_.has_value() ? SyncStatus(finish_error_.value()) : kOkStatus;
  }

  void finish() { Finish(status()); };

  // build param
  const std::shared_ptr<Server> srv_;
  grpc::CallbackServerContext *const ctx_;
  kv::datanode::v1::PushSyncDataResponse *const resp_;
  // sync request
  std::string slot_range_name_;
  std::shared_ptr<redis::SlotRange> slot_range_;
  kv::datanode::v1::PushSyncDataRequest req_;
  kv::datanode::v1::PushSyncDataRequest first_req_;
  std::string slot_range_index_name_{"uninitialized"};
  // sync applyer
  std::mutex storage_mtx_;
  std::shared_ptr<engine::Storage> storage_;
  // sync status
  std::mutex mtx_;
  bool finished_{false};
  OptionalSyncError finish_error_{std::nullopt};
};

}  // namespace redis
