#include <glog/logging.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <memory>
#include <string>

#include "mock/mock_server.h"

using kv::controller::v1::ControllerApiService;
using kv::controller::v1::ReportDataNodeRequest;
using kv::controller::v1::ReportDataNodeResponse;

const std::string TEST_RPC_ADDRESS = "localhost:50066";
const std::string test_cluster_id = "test-cluster-id";
const std::string test_datanode_id = "test-datanode-id";
const std::string test_pool = "test-pool";
const uint64_t test_db_id = 13457;
const uint64_t test_version = 11;

std::atomic<bool> server_should_exit(false);

class TestControllerApiServiceImpl final : public ControllerApiService::Service {
 public:
  grpc::Status ReportDataNode(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<ReportDataNodeResponse, ReportDataNodeRequest>* stream) override {
    ReportDataNodeRequest request;
    while (!server_should_exit.load() && stream->Read(&request)) {
      LOG(INFO) << "Received heartbeat version:" << request.version();
      report_cnt.fetch_add(1);

      EXPECT_EQ(test_cluster_id, request.cluster_id());
      EXPECT_EQ(test_datanode_id, request.id());
      EXPECT_EQ(test_pool, request.pool());
      EXPECT_TRUE(request.version() == 0 || request.version() == test_version);

      // range index
      kv::controller::v1::SlotRangeIndex index;
      index.set_start(0);
      index.set_end(kClusterSlots - 1);

      // slot range
      kv::controller::v1::SlotRange range;
      range.mutable_index()->CopyFrom(index);
      range.set_db_id(test_db_id);

      // datanode
      kv::controller::v1::Datanode datanode;
      *datanode.add_slot_range_list() = range;
      datanode.set_datanode_id(test_datanode_id);
      datanode.set_dts_rw_status(kv::controller::v1::Datanode::RW_STATUS_RO);
      datanode.set_client_rw_status(kv::controller::v1::Datanode::RW_STATUS_RW);
      datanode.set_serving_status(kv::controller::v1::Datanode::SERVING_STATUS_SERVING);

      // shard
      kv::controller::v1::Shard shard;
      *shard.add_datanodes() = datanode;

      // cluster
      kv::controller::v1::Cluster cluster;
      cluster.set_pool(test_pool);
      cluster.set_cluster_id(test_cluster_id);
      cluster.set_role(kv::controller::v1::Cluster::ROLE_ACTIVE);
      cluster.add_shards()->CopyFrom(shard);

      // response
      ReportDataNodeResponse response;
      response.mutable_ha_cluster()->mutable_active()->CopyFrom(cluster);
      response.mutable_ha_cluster()->set_version(test_version);

      stream->Write(response);
    }

    return grpc::Status::OK;
  }

  std::atomic<int> report_cnt{0};
};

void TestRunRPCServer() {
  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  grpc::ServerBuilder builder;
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 8000);
  builder.AddListeningPort(TEST_RPC_ADDRESS, grpc::InsecureServerCredentials());
  TestControllerApiServiceImpl service;
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  ASSERT_TRUE(server);
  auto hcs = server->GetHealthCheckService();
  hcs->SetServingStatus(ControllerApiService::service_full_name(), true);

  while (!server_should_exit.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  server->Shutdown();

  server->Wait();
  LOG(INFO) << "report cnt:" << service.report_cnt;
  EXPECT_GE(service.report_cnt, 5);
}

TEST(ControllerRPCTest, Basic) {
  server_should_exit.store(false);
  std::thread server_thread(TestRunRPCServer);

  {
    MockOptions opt;
    opt.heartbeat_interval_milliseconds = 10;
    opt.port = 12345;
    opt.controller_addr = TEST_RPC_ADDRESS;
    opt.db_ids.clear();
    opt.db_ids.emplace(test_db_id);

    opt.cluster_id = test_cluster_id;
    opt.datanode_id = test_datanode_id;
    opt.pool = test_pool;

    auto srv = MockServer(opt);
    std::this_thread::sleep_for(std::chrono::milliseconds(opt.heartbeat_interval_milliseconds * 5));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(11));
  server_should_exit.store(true);  // Signal the server to exit.

  server_thread.join();
}

TEST(ControllerRPCTest, HeartbeatStream) {
  class CtrlService : public kv::controller::v1::ControllerApiService::Service {
   public:
    std::mutex mtx;
    bool close_stream;
    std::string close_msg;
    std::atomic<uint64_t> version;

    grpc::Status ReportDataNode(
        grpc::ServerContext* context,
        grpc::ServerReaderWriter<ReportDataNodeResponse, ReportDataNodeRequest>* stream) override {
      // range index
      kv::controller::v1::SlotRangeIndex index;
      index.set_start(0);
      index.set_end(kClusterSlots - 1);
      // slot range
      kv::controller::v1::SlotRange range;
      range.mutable_index()->CopyFrom(index);
      range.set_db_id(test_db_id);
      // datanode
      kv::controller::v1::Datanode datanode;
      *datanode.add_slot_range_list() = range;
      datanode.set_datanode_id(test_datanode_id);
      datanode.set_dts_rw_status(kv::controller::v1::Datanode::RW_STATUS_RO);
      datanode.set_client_rw_status(kv::controller::v1::Datanode::RW_STATUS_RW);
      datanode.set_serving_status(kv::controller::v1::Datanode::SERVING_STATUS_SERVING);
      // shard
      kv::controller::v1::Shard shard;
      *shard.add_datanodes() = datanode;
      // cluster
      kv::controller::v1::Cluster cluster;
      cluster.set_pool(test_pool);
      cluster.set_cluster_id(test_cluster_id);
      cluster.set_role(kv::controller::v1::Cluster::ROLE_ACTIVE);
      cluster.add_shards()->CopyFrom(shard);
      // response
      ReportDataNodeResponse resp;
      resp.mutable_ha_cluster()->mutable_active()->CopyFrom(cluster);
      resp.mutable_ha_cluster()->set_version(version.fetch_add(1) + 1);
      stream->Write(resp);
      std::thread reader([this, stream]() {
        ReportDataNodeRequest req;
        while (IsEnableStream()) {
          if (req.Clear(); !stream->Read(&req)) {
            return;
          }
        }
      });
      reader.join();
      return grpc::Status{grpc::StatusCode::UNKNOWN, close_msg};
    }

    bool IsEnableStream() {
      std::unique_lock<std::mutex> lk(mtx);
      return !close_stream;
    }

    void EnableStram() {
      std::unique_lock<std::mutex> lk(mtx);
      close_stream = false;
      close_msg = "";
    }

    void DisableStream(std::string msg) {
      std::unique_lock<std::mutex> lk(mtx);
      close_stream = true;
      close_msg = std::move(msg);
    }
  };

  class CtrlServer {
   public:
    CtrlService service;
    std::unique_ptr<grpc::Server> srv;

    CtrlServer() { Start(); }

    ~CtrlServer() { Stop(); }

    CtrlServer(const CtrlServer&) = delete;

    CtrlServer(CtrlServer&&) = delete;

    CtrlServer operator=(const CtrlServer&) = delete;

    CtrlServer operator=(CtrlServer&&) = delete;

    void Start() {
      service.EnableStram();
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      builder.RegisterService(&service);
      builder.AddListeningPort(TEST_RPC_ADDRESS, grpc::InsecureServerCredentials());
      builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
      builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 8000);
      ASSERT_FALSE(srv);
      srv = builder.BuildAndStart();
      ASSERT_TRUE(srv);
      auto health_service = srv->GetHealthCheckService();
      health_service->SetServingStatus(ControllerApiService::service_full_name(), true);
    }

    void Stop() {
      ASSERT_TRUE(srv);
      auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(10);
      srv->Shutdown(deadline);
      srv->Wait();
      srv.reset();
    }

    void Restart() {
      Stop();
      Start();
    }
  };

  // init ctrl server
  CtrlServer ctrl_server;
  // init ctrl client
  MockOptions opt;
  opt.db_ids.clear();
  opt.db_ids.emplace(test_db_id);
  opt.pool = test_pool;
  opt.cluster_id = test_cluster_id;
  opt.datanode_id = test_datanode_id;
  opt.controller_addr = TEST_RPC_ADDRESS;
  opt.heartbeat_interval_milliseconds = 100000;
  opt.grpc_client_initial_reconnect_backoff_ms = 1;
  auto srv = MockServer(opt);
  auto& ctrl_client = srv.GetServer()->ctrl_rpc_client;
  // case 1: send first heartbeat
  usleep(10000);
  ASSERT_GT(ctrl_client->SendHeartbeatOKCount(), 0);
  ASSERT_EQ(ctrl_client->SendHeartbeatErrCount(), 0);
  ASSERT_GT(ctrl_client->ReceiveTopoOKCount(), 0);
  ASSERT_EQ(ctrl_client->ReceiveTopoErrCount(), 0);
  auto send_hb_ok_cnt = ctrl_client->SendHeartbeatOKCount();
  auto recv_hb_ok_cnt = ctrl_client->ReceiveTopoOKCount();
  usleep(10000);
  ASSERT_EQ(ctrl_client->SendHeartbeatOKCount(), send_hb_ok_cnt);
  ASSERT_EQ(ctrl_client->SendHeartbeatErrCount(), 0);
  ASSERT_EQ(ctrl_client->ReceiveTopoOKCount(), recv_hb_ok_cnt);
  ASSERT_EQ(ctrl_client->ReceiveTopoErrCount(), 0);
  // case 2: send heartbeat immediately
  send_hb_ok_cnt = ctrl_client->SendHeartbeatOKCount();
  recv_hb_ok_cnt = ctrl_client->ReceiveTopoOKCount();
  ctrl_client->SendHeartbeatImmediately();
  usleep(10000);
  ASSERT_FALSE(ctrl_client->send_hb_immediately_);
  ASSERT_GT(ctrl_client->SendHeartbeatOKCount(), send_hb_ok_cnt);
  ASSERT_EQ(ctrl_client->SendHeartbeatErrCount(), 0);
  ASSERT_EQ(ctrl_client->ReceiveTopoOKCount(), recv_hb_ok_cnt);
  ASSERT_EQ(ctrl_client->ReceiveTopoErrCount(), 0);
  // case 3: update heartbeat interval
  srv.GetServer()->GetConfig()->controller_heartbeat_interval_milliseconds = 5;
  ctrl_client->SendHeartbeatImmediately();
  usleep(10000);
  ASSERT_FALSE(ctrl_client->send_hb_immediately_);
  // case 4: send heartbeat timely
  send_hb_ok_cnt = ctrl_client->SendHeartbeatOKCount();
  recv_hb_ok_cnt = ctrl_client->ReceiveTopoOKCount();
  usleep(10000);
  ASSERT_GT(ctrl_client->SendHeartbeatOKCount(), send_hb_ok_cnt);
  ASSERT_EQ(ctrl_client->SendHeartbeatErrCount(), 0);
  ASSERT_EQ(ctrl_client->ReceiveTopoOKCount(), recv_hb_ok_cnt);
  ASSERT_EQ(ctrl_client->ReceiveTopoErrCount(), 0);
  usleep(10000);
  // case 5: server restart
  auto send_hb_err_cnt = ctrl_client->SendHeartbeatErrCount();
  auto recv_hb_err_cnt = ctrl_client->ReceiveTopoErrCount();
  ctrl_server.Restart();
  send_hb_ok_cnt = ctrl_client->SendHeartbeatOKCount();
  usleep(200000);
  ASSERT_GT(ctrl_client->SendHeartbeatOKCount(), send_hb_ok_cnt);
  ASSERT_GT(ctrl_client->SendHeartbeatErrCount(), send_hb_err_cnt);
  ASSERT_GT(ctrl_client->ReceiveTopoErrCount(), recv_hb_err_cnt);
  // case 6: server close stream with following message
  // a. ""
  // b. "kvcontroller: not controller leader"
  // c. "connections to all backends failing"
  std::vector<std::string> msgs{"", ControllerApiClient::kControllerNotLeaderMsg,
                                ControllerApiClient::kAllBackendUnhealthyMsg};
  for (auto& msg : msgs) {
    send_hb_err_cnt = ctrl_client->SendHeartbeatErrCount();
    recv_hb_err_cnt = ctrl_client->ReceiveTopoErrCount();
    ctrl_server.service.DisableStream(msg);
    usleep(10000);
    send_hb_ok_cnt = ctrl_client->SendHeartbeatOKCount();
    ctrl_server.service.EnableStram();
    usleep(10000);
    ASSERT_GT(ctrl_client->SendHeartbeatOKCount(), send_hb_ok_cnt);
    ASSERT_GT(ctrl_client->SendHeartbeatErrCount(), send_hb_err_cnt);
    ASSERT_GT(ctrl_client->ReceiveTopoErrCount(), recv_hb_err_cnt);
  }
  // case 7: stop client
  ctrl_client->Stop();
  ASSERT_TRUE(ctrl_client->stopped_);
  // case 8: stop client twice
  srv.Stop();
}
