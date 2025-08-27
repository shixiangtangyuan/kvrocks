#pragma once

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/channel_arguments.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>
#include <kv/controller/v1/api.pb.h>

#include <condition_variable>
#include <memory>
#include <thread>

class Server;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using kv::controller::v1::ControllerApiService;
using kv::controller::v1::ReportDataNodeRequest;
using kv::controller::v1::ReportDataNodeResponse;
using HeartbeatStream = grpc::ClientReaderWriter<ReportDataNodeRequest, ReportDataNodeResponse>;

class ControllerApiClient {
 public:
  static std::unique_ptr<ControllerApiClient> Create(Server *srv, const std::string &addr);

  ControllerApiClient(Server *srv, std::unique_ptr<ControllerApiService::Stub> stub,
                      std::unique_ptr<grpc::ClientContext> context, std::unique_ptr<HeartbeatStream> stream);

  ~ControllerApiClient();

  void Start();

  void Stop();

  void SendHeartbeatImmediately() {
    {
      std::unique_lock<std::mutex> lk(mtx_);
      send_hb_immediately_ = true;
    }
    cv_.notify_all();
  }

  uint64_t SendHeartbeatOKCount() { return send_hb_ok_count_.load(); }

  uint64_t SendHeartbeatErrCount() { return send_hb_err_count_.load(); }

  uint64_t ReceiveTopoOKCount() { return receive_topo_ok_count_.load(); }

  uint64_t ReceiveTopoErrCount() { return receive_topo_err_count_.load(); }

 private:
  FRIEND_TEST(ControllerRPCTest, HeartbeatStream);

  static const std::string kControllerNotLeaderMsg;
  static const std::string kAllBackendUnhealthyMsg;

  bool isStopped() {
    std::unique_lock<std::mutex> lk(mtx_);
    return stopped_;
  }

  void resetSendHBImmediatelyFlag() {
    std::unique_lock<std::mutex> lk(mtx_);
    send_hb_immediately_ = false;
  }

  grpc::Status resetCtxAndStream(bool try_cancel);

  void setCtxAndStream(std::unique_ptr<ClientContext> context, std::unique_ptr<HeartbeatStream> stream) {
    {
      std::unique_lock<std::mutex> lk(stream_mtx_);
      context_ = std::move(context);
      stream_ = std::move(stream);
    }
    {
      std::unique_lock<std::mutex> lk(mtx_);
      stream_refreshed_ = true;
    }
    cv_.notify_all();
  }

  bool checkClientStub();

  bool checkHeartbeatStream();

  void initHeartbeatRequest(ReportDataNodeRequest &);

  bool sendHeartbeatRequest(ReportDataNodeRequest &);

  void sendHeartbeatThreadFunc();

  void receiveTopoStreamLoop(ReportDataNodeResponse &);

  void receiveTopoThreadFunc();

  // build pramas
  Server *srv_{nullptr};
  std::unique_ptr<ControllerApiService::Stub> stub_{nullptr};
  // thread common
  std::mutex mtx_;
  std::condition_variable cv_;
  bool stopped_ = false;
  bool stream_refreshed_ = false;
  bool send_hb_immediately_ = false;
  std::mutex stream_mtx_;
  std::unique_ptr<ClientContext> context_{nullptr};
  std::unique_ptr<HeartbeatStream> stream_{nullptr};
  // thread params
  std::thread send_hb_thread_;
  std::thread receive_topo_thread_;
  std::atomic<uint64_t> send_hb_ok_count_ = 0;
  std::atomic<uint64_t> send_hb_err_count_ = 0;
  std::atomic<uint64_t> receive_topo_ok_count_ = 0;
  std::atomic<uint64_t> receive_topo_err_count_ = 0;
};
