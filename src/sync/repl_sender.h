#pragma once

#include <grpc++/grpc++.h>
#include <gtest/gtest.h>
#include <kv/datanode/v1/sync.pb.h>

#include "common/pb_util.h"
#include "sync/sync_sender.h"

namespace redis {

class ReplSender final
    : public grpc::ServerBidiReactor<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse>,
      public SyncSender<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse> {
 public:
  ReplSender(const std::shared_ptr<Server>& srv, grpc::CallbackServerContext* ctx)
      : SyncSender<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse>(srv, ctx, false) {
    StartRead(&req_);
  }

  ~ReplSender() override = default;

  void OnReadDone(bool ok) override {
    if (!ok) {
      MarkFinished(StreamUnavailableSyncError("read stream failed"), false);
      return;
    }

    LOG(INFO) << "[sync send] Recv request, req:" << req_;
    if (is_first_req_) {
      setRequest(req_);
      is_first_req_ = false;
    }
    if (req_.stop_write()) {
      notifyStopWrite();
    }
    req_.Clear();
    StartRead(&req_);
  };

  void OnWriteDone(bool ok) override {
    if (!ok) {
      MarkFinished(StreamUnavailableSyncError("write stream failed"), false);
      return;
    }

    notifyWriteStream();
  }

  void OnCancel() override { MarkFinished(UnknownSyncError("request canceled"), false); }

  void OnDone() override {
    ThreadJoin();
    // clean sync manager
    auto& slot_range_name = getSlotRangeName();
    auto& finish_error = GetFinishError();
    auto srv = getServer();
    if (!slot_range_name.empty()) {
      srv->sync_manager->RemoveReplSender(slot_range_name, this, finish_error);
    }
    // add sync stats
    LOG(INFO) << "[sync send] Done request, req:" << getRequest() << ", error:" << finish_error;
    GlobalStatsInstance().IncrSyncErrorCount(getSlotRangeIndexName(), SyncStreamType::ReplSender, finish_error);
    delete this;
  }

  void StartWrite(const kv::datanode::v1::SyncDataResponse* resp) override {
    return grpc::ServerBidiReactor<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse>::StartWrite(
        resp);
  }

  void StartWriteAndFinish(const kv::datanode::v1::SyncDataResponse* resp, grpc::WriteOptions options,
                           grpc::Status status) override {
    return grpc::ServerBidiReactor<kv::datanode::v1::SyncDataRequest,
                                   kv::datanode::v1::SyncDataResponse>::StartWriteAndFinish(resp, options, status);
  }

  void Finish(grpc::Status status) override {
    return grpc::ServerBidiReactor<kv::datanode::v1::SyncDataRequest, kv::datanode::v1::SyncDataResponse>::Finish(
        status);
  }

  Status AddToSyncManger(const std::shared_ptr<Server>& srv, const std::string& slot_range_name) override {
    return srv->sync_manager->AddReplSender(slot_range_name, this);
  }

  void RemoveFromSyncManager(const std::shared_ptr<Server>& srv, const std::string& slot_range_name,
                             const OptionalSyncError& err) override {
    return srv->sync_manager->RemoveReplSender(slot_range_name, this, err);
  }

  void SetCaughtUpInResp(kv::datanode::v1::SyncDataResponse& resp, bool caught_up) override {
    resp.set_caught_up(caught_up);
  }

  void SetSenderSeqIdInResp(kv::datanode::v1::SyncDataResponse& resp,
                            const std::shared_ptr<engine::Storage>& storage) override {
    resp.set_sender_seq_id(storage->LatestSeqNumber());
  }

 private:
  FRIEND_TEST(SyncSender, Repl);

  bool is_first_req_ = true;
  kv::datanode::v1::SyncDataRequest req_;
};

}  // namespace redis
