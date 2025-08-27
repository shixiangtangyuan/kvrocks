#include <glog/logging.h>
#include <gtest/gtest.h>

#include "cluster_util.h"
#include "common/test_flag.h"
#include "common/time_util.h"
#include "migration/migration.h"
#include "mock/mock_server.h"

#define CLUSTER_ACTIVE kv::controller::v1::Cluster::ROLE_ACTIVE
#define CLUSTER_STANDBY kv::controller::v1::Cluster::ROLE_STANDBY
#define DATANODE_SERVING kv::controller::v1::Datanode::SERVING_STATUS_SERVING
#define DATANODE_IMPORTING kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING
#define DATANODE_UNSPECIFIED kv::controller::v1::Datanode::SERVING_STATUS_UNSPECIFIED

#define DATANODE_CLIENT_UNSPECIFIED kv::controller::v1::Datanode_RWStatus_RW_STATUS_UNSPECIFIED
#define DATANODE_CLIENT_RW kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW
#define DATANODE_CLIENT_RO kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO
#define DATANODE_CLIENT_WO kv::controller::v1::Datanode_RWStatus_RW_STATUS_WO

#define DATANODE_DTS_UNSPECIFIED kv::controller::v1::Datanode_RWStatus_RW_STATUS_UNSPECIFIED
#define DATANODE_DTS_RW kv::controller::v1::Datanode_RWStatus_RW_STATUS_RW
#define DATANODE_DTS_RO kv::controller::v1::Datanode_RWStatus_RW_STATUS_RO
#define DATANODE_DTS_WO kv::controller::v1::Datanode_RWStatus_RW_STATUS_WO

const std::string test_active_cluster_id = "test-active-cluster-id";
const std::string test_active_datanode_id = "test-active-datanode-id";
const std::string test_active_src_datanode_id = "test-active-src-datanode-id";
const std::string test_active_pool = "test-active-pool";
const std::string test_standby_cluster_id = "test-standby-cluster-id";
const std::string test_standby_datanode_id = "test-standby-datanode-id";
const std::string test_standby_src_datanode_id = "test-standby-src-datanode-id";
const std::string test_standby_pool = "test-standby-pool";
const uint64_t test_db_id = 13457;
const uint64_t test_version = 11;

using kv::datanode::v1::FailoverRequest;
using kv::datanode::v1::FailoverResponse;
namespace redis {
struct TestFailoverRequest {
  uint64_t task_id;
  std::string src_datanode_id;
  std::string dst_datanode_id;
  int64_t timeout_point_ms;
  bool force;
};

FailoverRequest TestBuildFailoverRequest(const TestFailoverRequest &r) {
  FailoverRequest req;
  req.set_task_id(r.task_id);
  req.set_src_datanode_id(r.src_datanode_id);
  req.set_dst_datanode_id(r.dst_datanode_id);
  req.set_timeout_point_ms(r.timeout_point_ms);
  req.set_is_force(r.force);

  return req;
}

TEST(FailoverTest, RPCTest) {
  MockOptions opt;
  opt.datanode_id = test_active_datanode_id;
  opt.cluster_id = test_active_cluster_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // Set topo
  {
    TestDatanode datanode1{
        .datanode_id = test_active_src_datanode_id,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestDatanode datanode2{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_IMPORTING,
        .client_rw_status = DATANODE_CLIENT_RO,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestShard shard = {.datanodes = {datanode1, datanode2}};
    TestCluster active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{shard}},
    };
    TestPBHACluster allcluster{
        .version = test_version,
        .active_cluster = &active_cluster,
        .standby_cluster = nullptr,
    };
    const auto topo_resp = TestBuildControllerResp(allcluster);
    auto s = srv.GetServer()->cluster->SetTopo(topo_resp);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(test_version, srv.GetServer()->cluster->Version());
  }

  // Test failover
  TestFailoverRequest r{
      .task_id = 1,
      .src_datanode_id = test_active_src_datanode_id,
      .dst_datanode_id = test_active_src_datanode_id,  // wrong local datanodeId
      .timeout_point_ms = int64_t(util::GetTimeStampMS()) + 10,
      .force = true,
  };
  FailoverRequest req = TestBuildFailoverRequest(r);

  // test wrong local datanode_id
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  grpc::CallbackServerContext ctx;
  FailoverResponse resp;
  srv.GetServer()->Failover(&ctx, &req, &resp);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  // test timeout point exceeded
  r.dst_datanode_id = test_active_datanode_id;
  r.timeout_point_ms = int64_t(util::GetTimeStampMS()) - 10;
  FailoverRequest req1 = TestBuildFailoverRequest(r);
  srv.GetServer()->Failover(&ctx, &req, &resp);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
}

TEST(FailoverTest, ForceBasic) {
  MockOptions opt;
  opt.datanode_id = test_active_datanode_id;
  opt.cluster_id = test_active_cluster_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // Set topo
  {
    TestDatanode datanode1{
        .datanode_id = test_active_src_datanode_id,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestDatanode datanode2{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_IMPORTING,
        .client_rw_status = DATANODE_CLIENT_RO,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestShard shard = {.datanodes = {datanode1, datanode2}};
    TestCluster active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{shard}},
    };
    TestPBHACluster allcluster{
        .version = test_version,
        .active_cluster = &active_cluster,
        .standby_cluster = nullptr,
    };
    const auto topo_resp = TestBuildControllerResp(allcluster);
    auto s = srv.GetServer()->cluster->SetTopo(topo_resp);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(test_version, srv.GetServer()->cluster->Version());
  }

  // Test failover
  TestFailoverRequest r{
      .task_id = 1,
      .src_datanode_id = test_active_src_datanode_id,
      .dst_datanode_id = opt.datanode_id,
      .timeout_point_ms = int64_t(util::GetTimeStampMS()) + 100,
      .force = true,
  };
  FailoverRequest req = TestBuildFailoverRequest(r);

  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());

  grpc::CallbackServerContext ctx;
  FailoverResponse resp;
  srv.GetServer()->Failover(&ctx, &req, &resp);

  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), r.task_id);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  auto local_slot_ranges = srv.GetServer()->cluster->LocalSlotRanges();

  // timeout
  srv.GetServer()->migration->result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH;
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(120));
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT);
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());

  // reset
  srv.GetServer()->migration->reset();
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->migration->reset();  // support re reset
}

TEST(FailoverTest, ForceWithTopoUpdate) {
  MockOptions opt;
  opt.datanode_id = test_active_datanode_id;
  opt.cluster_id = test_active_cluster_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // init topo. serving_status is DATANODE_IMPORTING
  {
    TestDatanode datanode1{
        .datanode_id = test_active_src_datanode_id,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestDatanode datanode2{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_IMPORTING,
        .client_rw_status = DATANODE_CLIENT_RO,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestShard shard = {.datanodes = {datanode1, datanode2}};
    TestCluster active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{shard}},
    };
    TestPBHACluster allcluster{
        .version = test_version,
        .active_cluster = &active_cluster,
        .standby_cluster = nullptr,
    };
    const auto topo_resp = TestBuildControllerResp(allcluster);
    auto s = srv.GetServer()->cluster->SetTopo(topo_resp);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(test_version, srv.GetServer()->cluster->Version());
  }

  // do failover force
  TestFailoverRequest r{
      .task_id = 1,
      .src_datanode_id = test_active_src_datanode_id,
      .dst_datanode_id = opt.datanode_id,
      .timeout_point_ms = int64_t(util::GetTimeStampMS()) + 20,
      .force = true,
  };

  FailoverRequest req = TestBuildFailoverRequest(r);
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());

  grpc::CallbackServerContext ctx;
  FailoverResponse failover_resp;
  srv.GetServer()->Failover(&ctx, &req, &failover_resp);

  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());

  // update topo. serving_status is DATANODE_SERVING
  {
    TestDatanode datanode1{
        .datanode_id = test_active_src_datanode_id,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestDatanode datanode2{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_UNSPECIFIED,
        .client_rw_status = DATANODE_CLIENT_RO,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestShard shard = {.datanodes = {datanode1, datanode2}};
    TestCluster active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{shard}},
    };
    TestPBHACluster allcluster{
        .version = test_version + 1,
        .active_cluster = &active_cluster,
        .standby_cluster = nullptr,
    };
    const auto topo_resp = TestBuildControllerResp(allcluster);
    auto s = srv.GetServer()->cluster->SetTopo(topo_resp);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(test_version + 1, srv.GetServer()->cluster->Version());
  }

  // after topo updated, failover task has done
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());
}

TEST(FailoverTest, FailoverBasic) {
  SetInTest();

  MockOptions opt;
  opt.datanode_id = test_active_datanode_id;
  opt.cluster_id = test_active_cluster_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // init topo. serving_status is DATANODE_IMPORTING
  {
    TestDatanode datanode1{
        .datanode_id = test_active_src_datanode_id,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestDatanode datanode2{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_IMPORTING,
        .client_rw_status = DATANODE_CLIENT_RO,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestShard shard = {.datanodes = {datanode1, datanode2}};
    TestCluster active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{shard}},
    };
    TestPBHACluster allcluster{
        .version = test_version,
        .active_cluster = &active_cluster,
        .standby_cluster = nullptr,
    };
    const auto topo_resp = TestBuildControllerResp(allcluster);
    auto s = srv.GetServer()->cluster->SetTopo(topo_resp);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(test_version, srv.GetServer()->cluster->Version());
  }

  // Test Failover
  TestFailoverRequest r{
      .task_id = 1,
      .src_datanode_id = test_active_src_datanode_id,
      .dst_datanode_id = opt.datanode_id,
      .timeout_point_ms = int64_t(util::GetTimeStampMS()) + 10,
      .force = false,
  };
  FailoverRequest req = TestBuildFailoverRequest(r);
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());

  grpc::CallbackServerContext ctx;
  FailoverResponse resp;
  srv.GetServer()->Failover(&ctx, &req, &resp);

  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), r.task_id);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);

  auto local_slot_ranges = srv.GetServer()->cluster->LocalSlotRanges();
  // timeout
  srv.GetServer()->migration->result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH;
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT);
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());

  // reset
  srv.GetServer()->migration->reset();
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  srv.Stop();
  SetOutTest();
}

TEST(FailoverTest, FailoverReplicationCallback) {
  SetInTest();

  MockOptions opt;
  opt.datanode_id = test_active_datanode_id;
  opt.cluster_id = test_active_cluster_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // init topo. serving_status is DATANODE_IMPORTING
  {
    TestDatanode datanode1{
        .datanode_id = test_active_src_datanode_id,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestDatanode datanode2{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_IMPORTING,
        .client_rw_status = DATANODE_CLIENT_RO,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestShard shard = {.datanodes = {datanode1, datanode2}};
    TestCluster active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{shard}},
    };
    TestPBHACluster allcluster{
        .version = test_version,
        .active_cluster = &active_cluster,
        .standby_cluster = nullptr,
    };
    const auto topo_resp = TestBuildControllerResp(allcluster);
    auto s = srv.GetServer()->cluster->SetTopo(topo_resp);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(test_version, srv.GetServer()->cluster->Version());
  }

  // Test failover
  TestFailoverRequest r{
      .task_id = 1,
      .src_datanode_id = test_active_src_datanode_id,
      .dst_datanode_id = opt.datanode_id,
      .timeout_point_ms = int64_t(util::GetTimeStampMS()) + 100,
      .force = false,
  };
  FailoverRequest req = TestBuildFailoverRequest(r);
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());

  grpc::CallbackServerContext ctx;
  FailoverResponse failover_resp;
  srv.GetServer()->Failover(&ctx, &req, &failover_resp);

  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), r.task_id);
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_DOING, srv.GetServer()->migration->Result());

  auto local_slot_ranges = srv.GetServer()->cluster->LocalSlotRanges();

  // replication doing
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATING);
    break;
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_DOING, srv.GetServer()->migration->Result());

  // replication done
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH, srv.GetServer()->migration->Result());

  // replication stop write timeout
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_TIMEOUT);
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_STOP_WRITE_TIMEOUT, srv.GetServer()->migration->Result());

  // replication fail
  srv.GetServer()->migration->result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_DOING;
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_GAP);
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL, srv.GetServer()->migration->Result());

  srv.Stop();
  SetOutTest();
}

}  // namespace redis
