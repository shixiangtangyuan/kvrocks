#pragma once

#include "cdc/cdc_sender.h"
#include "config/config.h"
#include "server/server.h"
#include "sync/dts_sender.h"
#include "sync/repl_sender.h"
#include "sync/sync_receiver.h"

struct MockOptions {
  int workers{1};
  int heartbeat_interval_milliseconds{10};
  uint32_t port{10000 + kDefaultPort};
  std::unordered_set<uint64_t> db_ids{1};
  std::string root_dir{"datanode-unittest.XXXXXX"};
  std::string controller_addr{"controller-addr-1"};
  std::string datanode_id{"datanode-id-1"};
  std::string cluster_id{"cluster-id-1"};
  std::string pool{"pool-1"};
  std::string log_level{"info"};
  int vlog_level{4};
  int32_t grpc_client_initial_reconnect_backoff_ms{1000};

  CLIOptions GetCliOptions();
};

class MockServer {
 public:
  explicit MockServer(MockOptions opts);

  ~MockServer();

  MockServer(const MockServer &) = delete;

  MockServer operator=(const MockServer &) = delete;

  std::shared_ptr<Server> GetServer() { return srv_; }

  MockOptions &GetMockOptions() { return opts_; }

  void Stop() {
    bool running = false;
    if (srv_ && stopped_.compare_exchange_strong(running, true)) {
      srv_->Stop();
      srv_->Join();
    }
  }

  void StopCtrlClient() { srv_->ctrl_rpc_client->Stop(); }

  std::shared_ptr<engine::Storage> GetStorage(uint64_t db_id) const {
    return srv_->storage_mgr->GetStorageByDBID(db_id);
  }

  std::shared_ptr<redis::SlotRange> GetSlotRange(uint16_t start, uint16_t end) {
    return srv_->cluster->GetSlotRangeByIndex(start, end);
  }

  grpc::ServerUnaryReactor *GetSyncPoint(grpc::CallbackServerContext *ctx,
                                         const kv::datanode::v1::GetSyncPointRequest *req,
                                         kv::datanode::v1::GetSyncPointResponse *resp) {
    return srv_->GetSyncPoint(ctx, req, resp);
  }

  redis::DtsSender *PullSyncData(grpc::CallbackServerContext *ctx, const kv::datanode::v1::PullSyncDataRequest *req) {
    return reinterpret_cast<redis::DtsSender *>(srv_->PullSyncData(ctx, req));
  }

  redis::SyncReceiver *PushSyncData(grpc::CallbackServerContext *ctx, kv::datanode::v1::PushSyncDataResponse *resp) {
    return reinterpret_cast<redis::SyncReceiver *>(srv_->PushSyncData(ctx, resp));
  }

  grpc::ServerUnaryReactor *ReportSyncError(grpc::CallbackServerContext *ctx,
                                            const kv::datanode::v1::ReportSyncErrorRequest *req,
                                            kv::datanode::v1::ReportSyncErrorResponse *resp) {
    return srv_->ReportSyncError(ctx, req, resp);
  }

  redis::ReplSender *SyncData(grpc::CallbackServerContext *ctx) {
    return reinterpret_cast<redis::ReplSender *>(srv_->SyncData(ctx));
  }

  grpc::ServerUnaryReactor *GetCDCPoint(grpc::CallbackServerContext *ctx,
                                        const kv::datanode::v1::CDCGetLatestPointRequest *req,
                                        kv::datanode::v1::CDCGetLatestPointResponse *resp) {
    return srv_->CDCGetLatestPoint(ctx, req, resp);
  }

  grpc::ServerUnaryReactor *GetCDCRestartPoint(grpc::CallbackServerContext *ctx,
                                               const kv::datanode::v1::CDCGetRestartPointRequest *req,
                                               kv::datanode::v1::CDCGetRestartPointResponse *resp) {
    return srv_->CDCGetRestartPoint(ctx, req, resp);
  }

  grpc::ServerUnaryReactor *GetCDCOldestPoint(grpc::CallbackServerContext *ctx,
                                              const kv::datanode::v1::CDCGetOldestPointRequest *req,
                                              kv::datanode::v1::CDCGetOldestPointResponse *resp) {
    return srv_->CDCGetOldestPoint(ctx, req, resp);
  }

  redis::CDCSender *PullCDCData(grpc::CallbackServerContext *ctx, const kv::datanode::v1::CDCGetEventsRequest *req) {
    return reinterpret_cast<redis::CDCSender *>(srv_->CDCGetEvents(ctx, req));
  }

  std::vector<std::unique_ptr<WorkerThread>> &GetWorkerThreads() { return srv_->worker_threads_; }

  grpc::ServerUnaryReactor *Ingest(grpc::CallbackServerContext *ctx, const kv::datanode::v1::IngestRequest *req,
                                   kv::datanode::v1::IngestResponse *resp) {
    return srv_->Ingest(ctx, req, resp);
  }

  grpc::ServerUnaryReactor *GetIngestInfo(grpc::CallbackServerContext *ctx,
                                          const kv::datanode::v1::GetIngestInfoRequest *req,
                                          kv::datanode::v1::GetIngestInfoResponse *resp) {
    return srv_->GetIngestInfo(ctx, req, resp);
  }

  grpc::ServerUnaryReactor *StopDatanodeDts(grpc::CallbackServerContext *ctx,
                                            const kv::datanode::v1::StopDatanodeDtsRequest *req,
                                            kv::datanode::v1::StopDatanodeDtsResponse *resp) {
    return srv_->StopDatanodeDts(ctx, req, resp);
  }

  grpc::ServerUnaryReactor *StartDatanodeDts(grpc::CallbackServerContext *ctx,
                                             const kv::datanode::v1::StartDatanodeDtsRequest *req,
                                             kv::datanode::v1::StartDatanodeDtsResponse *resp) {
    return srv_->StartDatanodeDts(ctx, req, resp);
  }

 private:
  Status start();

  Config cfg_;
  MockOptions opts_;
  std::shared_ptr<Server> srv_;
  std::atomic<bool> stopped_ = false;
};
