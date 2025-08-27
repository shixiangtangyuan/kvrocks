#pragma once

#include "common/pb_util.h"
#include "sync/sync_sender.h"

namespace redis {

class DtsSender final
    : public grpc::ServerWriteReactor<kv::datanode::v1::PullSyncDataResponse>,
      public SyncSender<kv::datanode::v1::PullSyncDataRequest, kv::datanode::v1::PullSyncDataResponse> {
 public:
  DtsSender(const std::shared_ptr<Server>& srv, grpc::CallbackServerContext* ctx,
            const kv::datanode::v1::PullSyncDataRequest* req)
      : SyncSender<kv::datanode::v1::PullSyncDataRequest, kv::datanode::v1::PullSyncDataResponse>(srv, ctx, true) {
    LOG(INFO) << "[sync send] Recv request, req:" << *req;
    setRequest(*req);
  }

  ~DtsSender() override = default;

  void OnWriteDone(bool ok) override {
    if (!ok) {
      MarkFinished(StreamUnavailableSyncError("write stream failed"), false);
      return;
    }

    notifyWriteStream();
  }

  void OnCancel() override { MarkFinished(UnknownSyncError("request canceled"), false); };

  void OnDone() override {
    ThreadJoin();
    // clean sync manager
    auto& slot_range_name = getSlotRangeName();
    auto& finish_error = GetFinishError();
    auto srv = getServer();
    if (!slot_range_name.empty()) {
      srv->sync_manager->RemoveDtsSender(slot_range_name, this, finish_error);
    }
    // add sync stats
    LOG(INFO) << "[sync send] Done request, req:" << getRequest() << ", error:" << finish_error;
    GlobalStatsInstance().IncrSyncErrorCount(getSlotRangeIndexName(), SyncStreamType::DtsSender, finish_error);
    delete this;
  }

  void StartWrite(const kv::datanode::v1::PullSyncDataResponse* resp) override {
    return grpc::ServerWriteReactor<kv::datanode::v1::PullSyncDataResponse>::StartWrite(resp);
  }

  void StartWriteAndFinish(const kv::datanode::v1::PullSyncDataResponse* resp, grpc::WriteOptions options,
                           grpc::Status status) override {
    return grpc::ServerWriteReactor<kv::datanode::v1::PullSyncDataResponse>::StartWriteAndFinish(resp, options, status);
  }

  void Finish(grpc::Status status) override {
    return grpc::ServerWriteReactor<kv::datanode::v1::PullSyncDataResponse>::Finish(status);
  }

  Status AddToSyncManger(const std::shared_ptr<Server>& srv, const std::string& slot_range_name) override {
    return srv->sync_manager->AddDtsSender(slot_range_name, this);
  }

  void RemoveFromSyncManager(const std::shared_ptr<Server>& srv, const std::string& slot_range_name,
                             const OptionalSyncError& err) override {
    return srv->sync_manager->RemoveDtsSender(slot_range_name, this, err);
  }

  bool ExceedMaxSeq(const kv::datanode::v1::PullSyncDataRequest& req, rocksdb::SequenceNumber seq) override {
    return req.max_seq_id() > 0 && seq > req.max_seq_id();
  }
};

}  // namespace redis
