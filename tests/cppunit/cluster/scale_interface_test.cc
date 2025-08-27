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

using kv::datanode::v1::MigrateRequest;
using kv::datanode::v1::MigrateResponse;

namespace redis {

struct TestMigrateRequest {
  uint64_t task_id;
  std::map<std::string, std::vector<std::pair<int16_t, int16_t>>> src_datanodes;
  std::string dst_datanode_id;
  int64_t timeout_point_ms;
};

MigrateRequest BuildMigrateRequest(TestMigrateRequest info) {
  MigrateRequest req;
  req.set_task_id(info.task_id);
  req.set_dst_datanode_id(info.dst_datanode_id);
  req.set_timeout_point_ms(info.timeout_point_ms);

  for (const auto& src : info.src_datanodes) {
    kv::datanode::v1::SlotRangeList slot_range_list;
    for (const auto& range : src.second) {
      kv::controller::v1::SlotRangeIndex index;
      index.set_start(range.first);
      index.set_end(range.second);
      slot_range_list.add_slot_range_index()->CopyFrom(index);
    }
    req.mutable_src_datanodes()->emplace(src.first, slot_range_list);
  }

  return req;
}

TEST(ScaleInterfaceTest, OneSource) {
  SetInTest();

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

  // Create MigrateRequest
  TestMigrateRequest r;
  r.task_id = 1;
  r.dst_datanode_id = test_active_datanode_id;
  std::vector<std::pair<int16_t, int16_t>> slot_range_list{{0, kClusterSlots - 1}};
  r.src_datanodes.emplace(test_active_src_datanode_id, slot_range_list);
  r.timeout_point_ms = util::GetTimeStampMS() + 10;

  grpc::CallbackServerContext ctx;
  MigrateResponse resp;
  // test wrong dst datanodeid
  std::string fake_datanode_id = "fake_datanode_id";
  r.dst_datanode_id = fake_datanode_id;
  auto req = BuildMigrateRequest(r);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->Migrate(&ctx, &req, &resp);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  // test task expired
  r.dst_datanode_id = test_active_datanode_id;
  r.timeout_point_ms = util::GetTimeStampMS() - 20;
  auto req1 = BuildMigrateRequest(r);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->Migrate(&ctx, &req1, &resp);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  // test src slot_range is not in importing datanode
  r.timeout_point_ms = util::GetTimeStampMS() + 10;
  r.src_datanodes.erase(test_active_src_datanode_id);
  std::vector<std::pair<int16_t, int16_t>> slot_range_list1{{0, kClusterSlots - 1000}};
  r.src_datanodes.emplace(test_active_src_datanode_id, slot_range_list1);
  auto req2 = BuildMigrateRequest(r);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->Migrate(&ctx, &req2, &resp);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  // test migrate start ok
  r.timeout_point_ms = util::GetTimeStampMS() + 10;
  r.src_datanodes.erase(test_active_src_datanode_id);
  r.src_datanodes.emplace(test_active_src_datanode_id, slot_range_list);
  auto req3 = BuildMigrateRequest(r);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->Migrate(&ctx, &req3, &resp);
  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), r.task_id);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);

  srv.Stop();
  SetOutTest();
}

TEST(ScaleInterfaceTest, MultiSource) {
  const std::string test_active_src_datanode_id2 = "test-active-src-datanode-id2";
  SetInTest();

  MockOptions opt;
  opt.datanode_id = test_active_datanode_id;
  opt.cluster_id = test_active_cluster_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1};

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
                    .end = kClusterSlots / 2 - 1,
                },
            },
    };
    TestDatanode datanode2{
        .datanode_id = test_active_src_datanode_id2,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id + 1,
                    .start = kClusterSlots / 2,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestDatanode datanode3{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_IMPORTING,
        .client_rw_status = DATANODE_CLIENT_RO,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = kClusterSlots / 2 - 1,
                },
                {
                    .db_id = test_db_id + 1,
                    .start = kClusterSlots / 2,
                    .end = kClusterSlots - 1,
                },
            },
    };
    TestShard shard1 = {.datanodes = {datanode1, datanode3}};  // scale in
    TestShard shard2 = {.datanodes = {datanode2}};
    TestCluster active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{shard1, shard2}},
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

  // Create MigrateRequest
  TestMigrateRequest r;
  r.task_id = 1;
  r.dst_datanode_id = test_active_datanode_id;
  std::vector<std::pair<int16_t, int16_t>> slot_range_list1{{0, kClusterSlots / 2 - 1}};
  std::vector<std::pair<int16_t, int16_t>> slot_range_list2{{kClusterSlots / 2, kClusterSlots - 1}};
  r.src_datanodes.emplace(test_active_src_datanode_id, slot_range_list1);
  r.src_datanodes.emplace(test_active_src_datanode_id2, slot_range_list2);
  r.timeout_point_ms = util::GetTimeStampMS() + 10;

  grpc::CallbackServerContext ctx;
  MigrateResponse resp;
  // test src slot_range is not in importing datanode
  r.timeout_point_ms = util::GetTimeStampMS() + 10;
  r.src_datanodes.erase(test_active_src_datanode_id);
  std::vector<std::pair<int16_t, int16_t>> slot_range_list_tmp{{0, kClusterSlots - 1000}};
  r.src_datanodes.emplace(test_active_src_datanode_id, slot_range_list_tmp);
  auto req1 = BuildMigrateRequest(r);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->Migrate(&ctx, &req1, &resp);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  // test src slot_range count can't match importing datanode
  r.timeout_point_ms = util::GetTimeStampMS() + 10;
  r.src_datanodes.erase(test_active_src_datanode_id);
  auto req2 = BuildMigrateRequest(r);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->Migrate(&ctx, &req2, &resp);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  // test migrate multi src start
  r.timeout_point_ms = util::GetTimeStampMS() + 10;
  r.src_datanodes.erase(test_active_src_datanode_id);
  r.src_datanodes.emplace(test_active_src_datanode_id, slot_range_list1);
  auto req3 = BuildMigrateRequest(r);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  srv.GetServer()->Migrate(&ctx, &req3, &resp);
  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), r.task_id);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);

  srv.Stop();
  SetOutTest();
}

}  // namespace redis
