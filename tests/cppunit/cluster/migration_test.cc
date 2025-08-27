#include "migration/migration.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "cluster_util.h"
#include "common/test_flag.h"
#include "common/time_util.h"
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

namespace redis {

TEST(MigrationTest, WithoutReplDataBasic) {
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

  auto progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  // migration
  Migration::TaskInfo task_info;
  task_info.task_id = 1;
  task_info.dst_datanode_id = test_active_datanode_id;
  std::set<std::string> slot_range_names{CreateSlotRangeName(0, kClusterSlots - 1)};
  task_info.src_datanodes.emplace(test_active_src_datanode_id, slot_range_names);
  task_info.timeout_point_ms = int64_t(util::GetTimeStampMS()) + 50;
  task_info.need_replicate_data = false;
  auto ret = srv.GetServer()->migration->StartMigrateWithoutReplData(task_info);
  EXPECT_TRUE(ret.IsOK());
  EXPECT_EQ(srv.GetServer()->migration->info_, task_info);
  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), task_info.task_id);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // migration reentrant
  ret = srv.GetServer()->migration->StartMigrateWithoutReplData(task_info);
  EXPECT_TRUE(ret.IsOK());
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // another migration running with different id
  Migration::TaskInfo new_task_info = task_info;
  ++new_task_info.task_id;
  ret = srv.GetServer()->migration->StartMigrateWithoutReplData(new_task_info);
  EXPECT_TRUE(!ret.IsOK() && ret.GetCode() == Status::AnotherMigrationDoing);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // another migration running with different info
  new_task_info = task_info;
  ++new_task_info.timeout_point_ms;
  ret = srv.GetServer()->migration->StartMigrateWithoutReplData(new_task_info);
  EXPECT_TRUE(!ret.IsOK() && ret.GetCode() == Status::AnotherMigrationDoing);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT);

  // receive new topo
  srv.GetServer()->migration->ReceivedNewTopo();
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  // reset
  srv.GetServer()->migration->reset();
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
}

TEST(MigrationTest, WithoutReplData) {
  MockOptions opt;
  opt.datanode_id = test_active_datanode_id;
  opt.cluster_id = test_active_cluster_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // migration without data replication
  Migration::TaskInfo task_info;
  task_info.task_id = 1;
  task_info.dst_datanode_id = test_active_datanode_id;
  std::set<std::string> slot_range_names{CreateSlotRangeName(0, kClusterSlots - 1)};
  task_info.src_datanodes.emplace(test_active_src_datanode_id, slot_range_names);
  task_info.timeout_point_ms = int64_t(util::GetTimeStampMS()) + 20;
  task_info.need_replicate_data = false;

  // test start migration without topo
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  auto s = srv.GetServer()->migration->StartMigrateWithoutReplData(task_info);
  EXPECT_EQ(s.GetCode(), Status::ClusterInvalidInfo);

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

  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  s = srv.GetServer()->migration->StartMigrateWithoutReplData(task_info);
  EXPECT_TRUE(s.IsOK());
  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());

  // test reentrant
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);
  s = srv.GetServer()->migration->StartMigrateWithoutReplData(task_info);
  EXPECT_TRUE(s.IsOK());

  task_info.task_id = 2;
  s = srv.GetServer()->migration->StartMigrateWithoutReplData(task_info);
  EXPECT_EQ(s.GetCode(), Status::AnotherMigrationDoing);

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

TEST(MigrationTest, WithReplDataBasic) {
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

  // no migrate
  auto progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  // migtate
  Migration::TaskInfo task_info;
  task_info.task_id = 1;
  task_info.dst_datanode_id = test_active_datanode_id;
  std::set<std::string> slot_range_names{CreateSlotRangeName(0, kClusterSlots - 1)};
  task_info.src_datanodes.emplace(test_active_src_datanode_id, slot_range_names);
  task_info.timeout_point_ms = int64_t(util::GetTimeStampMS()) + 10;
  task_info.need_replicate_data = true;
  auto ret = srv.GetServer()->migration->StartMigrateWithReplData(task_info);
  EXPECT_TRUE(ret.IsOK());
  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), task_info.task_id);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);

  srv.GetServer()->migration->result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH;
  // migration reentrant
  ret = srv.GetServer()->migration->StartMigrateWithReplData(task_info);
  EXPECT_TRUE(ret.IsOK());
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // another migration running with different id
  Migration::TaskInfo new_task_info = task_info;
  ++new_task_info.task_id;
  ret = srv.GetServer()->migration->StartMigrateWithReplData(new_task_info);
  EXPECT_TRUE(!ret.IsOK() && ret.GetCode() == Status::AnotherMigrationDoing);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // another migration running with different info
  new_task_info = task_info;
  ++new_task_info.timeout_point_ms;
  ret = srv.GetServer()->migration->StartMigrateWithReplData(new_task_info);
  EXPECT_TRUE(!ret.IsOK() && ret.GetCode() == Status::AnotherMigrationDoing);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // timeout
  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_EQ(true, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_TIMEOUT);

  // receive new topo
  srv.GetServer()->migration->ReceivedNewTopo();
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  // reset
  srv.GetServer()->migration->reset();
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  srv.Stop();
  SetOutTest();
}

TEST(MigrationTest, WithReplData) {
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

  // no migrate
  auto progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  // migration with repl data
  Migration::TaskInfo task_info;
  task_info.task_id = 1;
  task_info.dst_datanode_id = test_active_datanode_id;
  std::set<std::string> slot_range_names{CreateSlotRangeName(0, kClusterSlots - 1)};
  task_info.src_datanodes.emplace(test_active_src_datanode_id, slot_range_names);
  task_info.timeout_point_ms = int64_t(util::GetTimeStampMS()) + 1000;
  task_info.need_replicate_data = true;
  auto ret = srv.GetServer()->migration->StartMigrateWithReplData(task_info);
  EXPECT_TRUE(ret.IsOK());
  EXPECT_EQ(srv.GetServer()->migration->info_, task_info);
  EXPECT_EQ(false, srv.GetServer()->migration->TaskHasDone());
  EXPECT_EQ(srv.GetServer()->migration->TaskId(), task_info.task_id);
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_DOING, srv.GetServer()->migration->Result());
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);

  auto local_slot_ranges = srv.GetServer()->cluster->LocalSlotRanges();
  // replication doing
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATING);
    break;
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_DOING, srv.GetServer()->migration->Result());
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_DOING);

  // replication done
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_REPLICATED);
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH, srv.GetServer()->migration->Result());
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FINISH);

  // replication stop write timeout
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_TIMEOUT);
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_STOP_WRITE_TIMEOUT, srv.GetServer()->migration->Result());
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_STOP_WRITE_TIMEOUT);

  // replication fail
  srv.GetServer()->migration->result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_DOING;
  for (const auto &[name, range] : local_slot_ranges) {
    range->SetReplicationStauts(kv::controller::v1::SlotRange::REPLICATION_STATUS_ERROR_LOG_GAP);
  }
  srv.GetServer()->migration->replicationCb();
  EXPECT_EQ(kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL, srv.GetServer()->migration->Result());
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL);

  // receive new topo
  srv.GetServer()->migration->ReceivedNewTopo();
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  // reset
  srv.GetServer()->migration->reset();
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  srv.Stop();
  SetOutTest();
}

TEST(MigrationTest, WithReplDataError) {
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

  auto progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);

  // failed to get rpc addr for wrong src datanode
  auto fake_datanode_id = "fake_datanode_id";
  Migration::TaskInfo task_info;
  task_info.task_id = 1;
  task_info.dst_datanode_id = test_active_datanode_id;
  std::set<std::string> slot_range_names{CreateSlotRangeName(0, kClusterSlots - 1)};
  task_info.src_datanodes.emplace(fake_datanode_id, slot_range_names);
  task_info.timeout_point_ms = int64_t(util::GetTimeStampMS()) + 20;
  task_info.need_replicate_data = true;
  auto ret = srv.GetServer()->migration->StartMigrateWithReplData(task_info);
  EXPECT_EQ(ret.GetCode(), Status::NotFound);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL);
  // slot_range is not served at local
  srv.GetServer()->migration->reset();
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  task_info.src_datanodes.erase(fake_datanode_id);
  std::set<std::string> slot_range_names1{CreateSlotRangeName(0, kClusterSlots - 1000)};
  task_info.src_datanodes.emplace(test_active_src_datanode_id, slot_range_names1);
  ret = srv.GetServer()->migration->StartMigrateWithReplData(task_info);
  EXPECT_EQ(ret.GetCode(), Status::NotFound);
  EXPECT_EQ(srv.GetServer()->migration->Result(), kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL);
  // serving datanode not match
  srv.GetServer()->migration->reset();
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, 0);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED);
  task_info.src_datanodes.erase(test_active_src_datanode_id);
  task_info.src_datanodes.emplace(test_active_datanode_id, slot_range_names);
  srv.GetServer()->migration->result_ = kv::controller::v1::Datanode::MIGRATION_RESULT_UNSPECIFIED;
  ret = srv.GetServer()->migration->StartMigrateWithReplData(task_info);
  EXPECT_EQ(ret.GetCode(), Status::NotOK);
  progress_info = srv.GetServer()->migration->GetProgressInfo();
  EXPECT_EQ(progress_info.first, task_info.task_id);
  EXPECT_EQ(progress_info.second, kv::controller::v1::Datanode::MIGRATION_RESULT_FAIL);
}

}  // namespace redis
