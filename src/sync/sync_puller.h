#pragma once

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/datanode/v1/service.grpc.pb.h>
#include <kv/datanode/v1/sync.pb.h>

#include "cluster/cluster.h"
#include "cluster/sync_manager.h"
#include "common/pb_util.h"
#include "common/sync_status.h"
#include "common/test_flag.h"
#include "server/server.h"

namespace redis {

class SyncPuller
    : public grpc::ClientBidiReactor<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse> {
 public:
  SyncPuller(const std::string &cluster_id, const std::string &puller_node_id, const std::string &sender_node_id,
             const std::shared_ptr<Server> &, const std::shared_ptr<redis::SlotRange> &,
             const kv::datanode::v1::SyncPoint &, const std::shared_ptr<kv::datanode::v1::DataNodeService::Stub> &,
             SlotRangeWriteStoppableCB, SlotRangeReplicationDoneCB);

  ~SyncPuller() override = default;

  SyncPuller(const SyncPuller &) = delete;

  SyncPuller operator=(const SyncPuller &) = delete;

  bool MarkFinished(const OptionalSyncError &error) {
    auto ret = updateStatus(SyncStatusEnum::Done, error);
    if (ret.first) {
      tryRemoveHold();
      ctx_.TryCancel();
    }
    return ret.first;
  }

  void StopApplyData() {
    DCHECK(sync_status_ == SyncStatusEnum::Done);
    std::unique_lock<std::mutex> lk(apply_mtx_);
  }

  bool StopWrite();

  void OnWriteDone(bool ok) override;

  void OnReadDone(bool ok) override;

  void OnDone(const grpc::Status &) override;

 private:
  FRIEND_TEST(Sync, RPC);
  FRIEND_TEST(SyncPuller, Base);

  // construct sync puller without rpc for test
  SyncPuller(const std::string &cluster_id, const std::string &puller_node_id, const std::shared_ptr<Server> &,
             const std::shared_ptr<redis::SlotRange> &, const kv::datanode::v1::SyncPoint &, SlotRangeWriteStoppableCB,
             SlotRangeReplicationDoneCB);

  static kv::datanode::v1::SyncDataRequest buildReq(const std::string &cluster_id, const std::string &puller_node_id,
                                                    const std::shared_ptr<redis::SlotRange> &,
                                                    const kv::datanode::v1::SyncPoint &);

  void handleResp();

  static constexpr bool valid_switch[SyncStatusEnum::Size][SyncStatusEnum::Size]{
      {false, true, false, false, true},  {false, false, true, false, true},   {false, false, false, true, true},
      {false, false, false, false, true}, {false, false, false, false, false},
  };

  SyncStatusEnum getSyncStatus() {
    std::unique_lock<std::mutex> lk(mtx_);
    return sync_status_;
  }

  // switchStatus() is expected to execute serially
  std::pair<bool, SyncStatusEnum> switchStatus(SyncStatusEnum status, const OptionalSyncError &error = std::nullopt) {
    auto ret = updateStatus(status, error);
    if (!ret.first) {
      return ret;
    }
    LOG(INFO) << "[sync pull] Sync status changed, req:" << req_ << ", prev status:" << ret.second
              << ", curr status:" << status;
    updateSlotRangeReplStatus(status, error);
    if (status == SyncStatusEnum::WriteStop) {
      write_stoppeable_cb_(slot_range_->GetName());
    }
    if (status == SyncStatusEnum::CaughtUp ||
        (status == SyncStatusEnum::Done &&
         (ret.second != SyncStatusEnum::CaughtUp || IsStopWriteTimeoutError(error)))) {
      repl_done_cb_();
    }
    return ret;
  }

  std::pair<bool, SyncStatusEnum> updateStatus(SyncStatusEnum status, const OptionalSyncError &error = std::nullopt) {
    std::unique_lock<std::mutex> lk(mtx_);
    if (!valid_switch[sync_status_][status]) {
      return {false, sync_status_};
    }
    auto prev = sync_status_;
    sync_status_ = status;
    finish_error_ = error;
    return {true, prev};
  }

  void updateSlotRangeReplStatus(SyncStatusEnum status, const OptionalSyncError &error = std::nullopt) {
    switch (status) {
      case SyncStatusEnum::Syncing: {
        slot_range_->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATING);
        break;
      }
      case SyncStatusEnum::CaughtUp: {
        slot_range_->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
        break;
      }
      case SyncStatusEnum::Done: {
        auto repl_status = kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_UNKNOWN;
        if (error.has_value()) {
          auto status = SyncErrorToReplStatus(error->code());
          if (status.has_value()) {
            repl_status = status.value();
          }
        }
        slot_range_->SetReplicationStauts(repl_status);
        break;
      }
      default: {
        break;
      }
    }
  }

  bool canRemoveHold() {
    bool hold = false;
    return hold_removed_.compare_exchange_strong(hold, true);
  }

  bool tryRemoveHold() {
    if (canRemoveHold()) {
      if (!is_in_test) {
        RemoveHold();
      }
      return true;
    }
    return false;
  }

  // build param
  std::shared_ptr<Server> const srv_;
  std::shared_ptr<redis::SlotRange> slot_range_;
  std::shared_ptr<kv::datanode::v1::DataNodeService::Stub> stub_;
  SlotRangeWriteStoppableCB write_stoppeable_cb_;
  SlotRangeReplicationDoneCB repl_done_cb_;
  // sync req & resp
  grpc::ClientContext ctx_;
  kv::datanode::v1::SyncDataRequest req_;
  std::string slot_range_index_name_;
  kv::datanode::v1::SyncDataResponse resp_;
  std::atomic<bool> hold_removed_ = false;
  // sync applyer
  std::mutex apply_mtx_;
  std::shared_ptr<engine::Storage> storage_;
  // sync status
  std::mutex mtx_;
  OptionalSyncError finish_error_{std::nullopt};
  SyncStatusEnum sync_status_{SyncStatusEnum::Init};
};

}  // namespace redis
