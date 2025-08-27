#include "rpc/rpc_cluster_controller.h"

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <kv/controller/v1/api.grpc.pb.h>
#include <kv/controller/v1/api.pb.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

#include "server/grpc_interceptor.h"
#include "server/server.h"
#include "stats/stats.h"

#define HB_INFO LOG(INFO) << "[heartbeat] "
#define HB_WARNING LOG(WARNING) << "[heartbeat] "
#define HB_ERROR LOG(ERROR) << "[heartbeat] "
#define HB_FATAL LOG(FATAL) << "[heartbeat] "

const std::string ControllerApiClient::kControllerNotLeaderMsg = "kvcontroller: not controller leader";
const std::string ControllerApiClient::kAllBackendUnhealthyMsg = "connections to all backends failing";

ControllerApiClient::~ControllerApiClient() { Stop(); }

std::unique_ptr<ControllerApiClient> ControllerApiClient::Create(Server *srv, const std::string &addr) {
  auto args = srv->GetConfig()->BuildControllerChannelArgs();
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptor_creators;
  interceptor_creators.push_back(std::make_unique<redis::ClientStatsInterceptorFactory>());
  auto channel = grpc::experimental::CreateCustomChannelWithInterceptors(addr, grpc::InsecureChannelCredentials(), args,
                                                                         std::move(interceptor_creators));
  if (channel == nullptr) {
    HB_ERROR << "create channel failed, addr=" << addr;
    return nullptr;
  }
  auto stub = ControllerApiService::NewStub(channel);
  if (stub == nullptr) {
    HB_ERROR << "create stub failed, addr=" << addr;
    return nullptr;
  }
  auto context = std::make_unique<grpc::ClientContext>();
  auto stream = stub->ReportDataNode(context.get());
  if (stream == nullptr) {
    HB_ERROR << "create stream failed, addr=" << addr;
    return nullptr;
  }
  auto client = std::make_unique<ControllerApiClient>(srv, std::move(stub), std::move(context), std::move(stream));
  client->Start();
  return client;
}

ControllerApiClient::ControllerApiClient(Server *srv, std::unique_ptr<ControllerApiService::Stub> stub,
                                         std::unique_ptr<grpc::ClientContext> context,
                                         std::unique_ptr<HeartbeatStream> stream)
    : srv_(srv), stub_(std::move(stub)), context_(std::move(context)), stream_(std::move(stream)) {}

void ControllerApiClient::Start() {
  send_hb_thread_ = std::thread(&ControllerApiClient::sendHeartbeatThreadFunc, this);
  receive_topo_thread_ = std::thread(&ControllerApiClient::receiveTopoThreadFunc, this);
}

bool ControllerApiClient::checkClientStub() {
  if (stub_) {
    return true;
  }
  auto args = srv_->GetConfig()->BuildControllerChannelArgs();
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptor_creators;
  interceptor_creators.push_back(std::make_unique<redis::ClientStatsInterceptorFactory>());
  auto channel = grpc::experimental::CreateCustomChannelWithInterceptors(
      srv_->GetConfig()->controller_addr, grpc::InsecureChannelCredentials(), args, std::move(interceptor_creators));
  if (!channel) {
    HB_ERROR << "refresh channel failed";
    return false;
  }
  // wait an appropriate time for connection ready
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(srv_->GetConfig()->controller_client_wait_connect_ready_timeout_ms);
  if (!channel->WaitForConnected(deadline)) {
    HB_ERROR << "refresh connection failed";
    return false;
  }
  auto stub = ControllerApiService::NewStub(channel);
  if (!stub) {
    HB_ERROR << "refresh stub failed";
    return false;
  }
  HB_INFO << "refresh stub succeed";
  stub_ = std::move(stub);
  return true;
}

bool ControllerApiClient::checkHeartbeatStream() {
  if (stream_) {
    return true;
  }
  auto context = std::make_unique<grpc::ClientContext>();
  auto stream = stub_->ReportDataNode(context.get());
  if (!stream) {
    HB_ERROR << "refresh stream failed";
    return false;
  }
  // refresh context and stream
  setCtxAndStream(std::move(context), std::move(stream));
  HB_INFO << "refresh stream succeed";
  return true;
}

grpc::Status ControllerApiClient::resetCtxAndStream(bool try_cancel) {
  if (!stream_) {
    return grpc::Status::OK;
  }
  stream_->WritesDone();
  if (try_cancel && context_) {
    context_->TryCancel();
  }
  // ensure thread safety between Finish() and Read();
  std::unique_lock<std::mutex> lk(stream_mtx_);
  ReportDataNodeResponse resp;
  while (stream_->Read(&resp)) {
  }
  auto status = stream_->Finish();
  stream_.reset();
  context_.reset();
  return status;
}

void ControllerApiClient::initHeartbeatRequest(ReportDataNodeRequest &req) {
  req.Clear();

  bool is_blocked = false;
  auto blocked_threshold = std::chrono::seconds(srv_->GetConfig()->worker_blocked_threshold_seconds);
  if (blocked_threshold > std::chrono::seconds::zero()) {
    is_blocked =
        std::chrono::duration_cast<std::chrono::seconds>(srv_->GetWorkersBlockedDuration()) > blocked_threshold;
    if (is_blocked != GlobalStatsInstance().is_blocked.load()) {
      GlobalStatsInstance().is_blocked.store(is_blocked);
    }
  }

  srv_->cluster->InitControllerRpcRequest(req, is_blocked);
}

bool ControllerApiClient::sendHeartbeatRequest(ReportDataNodeRequest &req) {
  // reset flag before init request
  resetSendHBImmediatelyFlag();
  initHeartbeatRequest(req);
  if (stream_->Write(req)) {
    HB_INFO << "send heartbeat succeed, cluster_id=" << req.cluster_id() << ", version=" << req.version();
    return true;
  }
  auto status = resetCtxAndStream(false);
  auto reset_stub = status.error_message().find(kControllerNotLeaderMsg) != std::string::npos ||
                    status.error_message().find(kAllBackendUnhealthyMsg) != std::string::npos;
  HB_ERROR << "send heartbeat failed, code=" << status.error_code() << ", message=" << status.error_message()
           << ", details=" << status.error_details() << ", reset_stub=" << reset_stub;
  // reset stub of heartbeat stream
  if (reset_stub) {
    stub_.reset();
  }
  return false;
}

void ControllerApiClient::sendHeartbeatThreadFunc() {
  util::ThreadSetName("hb-sender");

  uint hb_retry_count = 0;
  ReportDataNodeRequest req;
  while (!isStopped()) {
    bool send_hb_succeed = false;
    if (checkClientStub() && checkHeartbeatStream()) {
      send_hb_succeed = sendHeartbeatRequest(req);
    }
    auto wait_for_ms = srv_->GetConfig()->controller_heartbeat_interval_milliseconds;
    if (send_hb_succeed) {
      hb_retry_count = 0;
      send_hb_ok_count_.fetch_add(1);
    } else {
      if (++hb_retry_count >= 3 && !stream_ && stub_) {
        HB_WARNING << "force reset stub";
        hb_retry_count = 0;
        stub_.reset();
      }
      send_hb_err_count_.fetch_add(1);
      wait_for_ms = wait_for_ms / 2;
    }
    wait_for_ms = std::max(wait_for_ms, 1);
    // wait (send heartbeat immediately when previous heartbeat is ok)
    // or stopped for heartbeat interval or heartbeat interval / 2
    // when previous heartbeat failed
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait_for(lk, std::chrono::milliseconds(wait_for_ms),
                 [&]() { return (send_hb_succeed && send_hb_immediately_) || stopped_; });
  }

  auto status = resetCtxAndStream(true);
  HB_INFO << "send heartbeat thread exited, code=" << status.error_code() << ", message=" << status.error_message()
          << ", details=" << status.error_details();
}

void ControllerApiClient::receiveTopoStreamLoop(ReportDataNodeResponse &resp) {
  std::unique_lock<std::mutex> lk(stream_mtx_);
  if (stream_) {
    for (resp.Clear(); stream_->Read(&resp); resp.Clear()) {
      HB_INFO << "receive topo succeed, cluster_id=" << resp.ha_cluster().active().cluster_id()
              << ", version=" << resp.ha_cluster().version();
      receive_topo_ok_count_.fetch_add(1);
      if (auto status = srv_->cluster->SetTopo(resp); !status.IsOK()) {
        srv_->cluster->IncrSetTopoErrCount();
        HB_ERROR << "set topo failed, err=" << status.Msg();
      } else {
        srv_->cluster->IncrSetTopoOkCount();
        HB_INFO << "set topo succeed";
      }
    }
    HB_ERROR << "receive topo failed";
    receive_topo_err_count_.fetch_add(1);
  }
}

void ControllerApiClient::receiveTopoThreadFunc() {
  util::ThreadSetName("topo-receiver");

  ReportDataNodeResponse resp;
  while (!isStopped()) {
    // read stream until failed
    receiveTopoStreamLoop(resp);
    // wait stream refreshed or stopped
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [&]() {
      if (stream_refreshed_) {
        stream_refreshed_ = false;
        return true;
      }
      return stopped_;
    });
  }

  HB_INFO << "receive topo thread exited";
}

void ControllerApiClient::Stop() {
  {
    std::unique_lock<std::mutex> lk(mtx_);
    if (stopped_) {
      return;
    }
    stopped_ = true;
  }

  cv_.notify_all();
  send_hb_thread_.join();
  receive_topo_thread_.join();
  HB_INFO << "controller client stopped";
}
