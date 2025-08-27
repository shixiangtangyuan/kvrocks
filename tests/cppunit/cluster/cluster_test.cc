#include "cluster/cluster.h"

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include "cluster/cluster_defs.h"
#include "cluster_util.h"
#include "mock/mock_server.h"
#include "scope_exit.h"
#include "slot_keys.h"

namespace redis {

class StorageTestHelper {
 public:
  static bool GetDisableAutoCompactionsOption(std::shared_ptr<engine::Storage> &storage) {
    bool ret = false, uninit = true;
    for (auto &cf_handler : storage->cf_handles_) {
      auto cf_options = storage->db_->GetOptions(cf_handler);
      if (uninit) {
        uninit = false, ret = cf_options.disable_auto_compactions;
      } else {
        EXPECT_EQ(ret, cf_options.disable_auto_compactions);
      }
    }
    return ret;
  }
};

#define CLUSTER_ACTIVE kv::controller::v1::Cluster::ROLE_ACTIVE
#define CLUSTER_STANDBY kv::controller::v1::Cluster::ROLE_STANDBY
#define DATANODE_SERVING kv::controller::v1::Datanode::SERVING_STATUS_SERVING
#define DATANODE_IMPORTING kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING

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
const std::string test_active_pool = "test-active-pool";
const std::string test_standby_cluster_id = "test-standby-cluster-id";
const std::string test_standby_datanode_id = "test-standby-datanode-id";
const std::string test_standby_pool = "test-standby-pool";
const uint64_t test_db_id = 13457;
const uint64_t test_version = 11;

TEST(ClusterTest, SetTopoSimple) {
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1, test_db_id + 2};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // create normal simple topology
  auto datanode_id1 = test_active_datanode_id;
  auto datanode_id2 = test_active_datanode_id + "2";
  TestDatanode datanode1{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = 5000,
              },
          },
  };
  TestDatanode datanode2 = {
      .datanode_id = datanode_id2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {// slotrnage 2
           {
               .db_id = test_db_id + 2,
               .start = 5001,
               .end = kClusterSlots - 1,
           }},
  };
  TestShard active_shard1 = {.datanodes = {datanode1}};
  TestShard active_shard2 = {.datanodes = {datanode2}};
  TestCluster active_cluster = {
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      {{active_shard1}, {active_shard2}},
  };
  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  // datanode not found in topo
  allcluster.active_cluster->shards[0].datanodes[0].datanode_id = datanode_id2;
  auto resp = TestBuildControllerResp(allcluster);
  auto s = srv.GetServer()->cluster->SetTopo(resp);
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), errDatanodeNotFoundInTopo);
  // apply topo succeed
  allcluster.active_cluster->shards[0].datanodes[0].datanode_id = datanode_id1;
  resp = TestBuildControllerResp(allcluster);
  s = srv.GetServer()->cluster->SetTopo(resp);
  EXPECT_TRUE(s.IsOK());

  // check cluster
  EXPECT_EQ(test_version, srv.GetServer()->cluster->Version());
  EXPECT_EQ(test_active_pool, srv.GetServer()->cluster->Pool());
  EXPECT_EQ(test_active_cluster_id, srv.GetServer()->cluster->ClusterId());
  EXPECT_EQ(test_active_datanode_id, srv.GetServer()->cluster->DatanodeId());
  EXPECT_TRUE(srv.GetServer()->cluster->IsActivePool());
  // check datanode
  EXPECT_EQ(2, srv.GetServer()->cluster->datanodes_.size());
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->DtsRWStatus(), DATANODE_DTS_RO);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->DtsRWStatus(), DATANODE_DTS_RO);
  // check slotranges
  EXPECT_EQ(2, srv.GetServer()->cluster->LocalSlotRanges().size());
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByIndex(0, 1000) != nullptr);
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByIndex(1001, 5000) != nullptr);
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByIndex(2001, 5000) == nullptr);
  // test funcs
  EXPECT_EQ(srv.GetServer()->cluster->GetSlotRangeNameByKey(global_slot_keys[100]), CreateSlotRangeName(0, 1000));
  EXPECT_EQ(srv.GetServer()->cluster->GetSlotRangeByKey(global_slot_keys[100])->GetName(),
            CreateSlotRangeName(0, 1000));
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByKey(global_slot_keys[6000]) == nullptr);
  EXPECT_EQ(srv.GetServer()->cluster->GetSlotRangeNameBySlotId(100), CreateSlotRangeName(0, 1000));
  EXPECT_EQ(srv.GetServer()->cluster->GetSlotRangeEndBySlot(100), 1000);
}

TEST(ClusterTest, TopoShrink) {
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1, test_db_id + 2};
  // tmp datanode id
  const auto datanode_id1 = test_active_datanode_id;
  const auto datanode_id1_importing = test_active_datanode_id + "-importing";
  const auto datanode_id2 = test_active_datanode_id + "2";

  // serving datanode server
  opt.datanode_id = datanode_id1;
  auto srv_serving = MockServer(opt);
  srv_serving.StopCtrlClient();
  // importing datanode server
  opt.port = opt.port + 1;
  opt.datanode_id = datanode_id1_importing;
  auto srv_importing = MockServer(opt);
  srv_importing.StopCtrlClient();

  // auto compaction disabled before serving
  for (auto id : {test_db_id, test_db_id + 1}) {
    if (id == test_db_id) {
      auto storage = srv_serving.GetStorage(id);
      EXPECT_TRUE(storage);
      EXPECT_TRUE(StorageTestHelper::GetDisableAutoCompactionsOption(storage));
    } else {
      auto storage = srv_importing.GetStorage(id);
      EXPECT_TRUE(storage);
      EXPECT_TRUE(StorageTestHelper::GetDisableAutoCompactionsOption(storage));
    }
  }
  // create normal simple topology
  TestDatanode datanode1{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
          },
  };
  TestDatanode datanode1_importing{
      .datanode_id = datanode_id1_importing,
      .serving_status = DATANODE_IMPORTING,
      .client_rw_status = DATANODE_CLIENT_RO,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = kClusterSlots - 1,
              },
          },
  };
  TestDatanode datanode2 = {
      .datanode_id = datanode_id2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {// slotrnage 2
           {
               .db_id = test_db_id + 2,
               .start = 1001,
               .end = kClusterSlots - 1,
           }},
  };
  TestShard active_shard1 = {{datanode1, datanode1_importing}};
  TestShard active_shard2 = {{datanode2}};
  TestCluster active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard1}, {active_shard2}},
  };
  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  // settopo to serving and importing server
  auto s = srv_serving.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());
  s = srv_importing.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());

  // check datanode
  EXPECT_EQ(srv_serving.GetServer()->cluster->GetDatanodeById(datanode_id1)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv_serving.GetServer()->cluster->GetDatanodeById(datanode_id1)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv_serving.GetServer()->cluster->GetDatanodeById(datanode_id1)->DtsRWStatus(), DATANODE_DTS_RO);
  EXPECT_EQ(srv_serving.GetServer()->cluster->GetDatanodeById(datanode_id2)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv_serving.GetServer()->cluster->GetDatanodeById(datanode_id2)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv_serving.GetServer()->cluster->GetDatanodeById(datanode_id2)->DtsRWStatus(), DATANODE_DTS_RO);
  // auto compaction disabled before serving
  for (auto id : {test_db_id, test_db_id + 1}) {
    if (id == test_db_id) {
      auto storage = srv_serving.GetStorage(id);
      EXPECT_TRUE(storage);
      EXPECT_FALSE(StorageTestHelper::GetDisableAutoCompactionsOption(storage));
    } else {
      auto storage = srv_importing.GetStorage(id);
      EXPECT_TRUE(storage);
      EXPECT_TRUE(StorageTestHelper::GetDisableAutoCompactionsOption(storage));
    }
  }

  // set importiong to serving
  TestDatanode datanode3{
      .datanode_id = datanode_id1_importing,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = kClusterSlots - 1,
              },
          },
  };

  TestDatanode datanode3_importing{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_IMPORTING,
      .client_rw_status = DATANODE_CLIENT_RO,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = kClusterSlots - 1,
              },
          },
  };

  TestShard active_shard3 = {{datanode3, datanode3_importing}};
  TestCluster active_cluster3{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard3}},
  };
  TestPBHACluster allcluster3{
      .version = test_version + 1,
      .active_cluster = &active_cluster3,
      .standby_cluster = nullptr,
  };
  s = srv_importing.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster3));
  EXPECT_TRUE(s.IsOK());
  // check datanode
  EXPECT_EQ(srv_importing.GetServer()->cluster->GetDatanodeById(datanode_id1_importing)->ServingStatus(),
            DATANODE_SERVING);
  EXPECT_EQ(srv_importing.GetServer()->cluster->GetDatanodeById(datanode_id1_importing)->ClientRWStatus(),
            DATANODE_CLIENT_RW);
  EXPECT_EQ(srv_importing.GetServer()->cluster->GetDatanodeById(datanode_id1_importing)->DtsRWStatus(),
            DATANODE_DTS_RO);

  // check slotranges
  EXPECT_TRUE(srv_importing.GetServer()->cluster->GetSlotRangeByIndex(0, 1000) != nullptr);
  EXPECT_TRUE(srv_importing.GetServer()->cluster->GetSlotRangeByIndex(1001, kClusterSlots - 1) != nullptr);
  EXPECT_TRUE(srv_importing.GetServer()->cluster->GetSlotRangeByIndex(2001, 5000) == nullptr);
  // auto compaction enabled after serving
  for (auto id : {test_db_id, test_db_id + 1}) {
    if (id == test_db_id) {
      auto storage = srv_serving.GetStorage(id);
      EXPECT_TRUE(storage);
      EXPECT_FALSE(StorageTestHelper::GetDisableAutoCompactionsOption(storage));
    } else {
      auto storage = srv_importing.GetStorage(id);
      EXPECT_TRUE(storage);
      EXPECT_FALSE(StorageTestHelper::GetDisableAutoCompactionsOption(storage));
    }
  }

  // TODO(zhangshixiang) check other datanode unspeciafied status
}

TEST(ClusterTest, TopoSplit) {
  auto prev = sleepMSBeforeCleanData;
  sleepMSBeforeCleanData = 1;
  auto exit = MakeScopeExit([&prev]() { sleepMSBeforeCleanData = prev; });
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1, test_db_id + 2};
  // tmp datanode id
  const auto datanode_id1 = test_active_datanode_id;
  const auto datanode_id2 = test_active_datanode_id + "2";

  // serving datanode server
  opt.datanode_id = datanode_id1;
  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // create normal simple topology
  TestDatanode datanode1{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = kClusterSlots - 1,
              },
          },
  };

  TestShard active_shard1 = {{datanode1}};
  TestCluster active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard1}},
  };
  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  auto s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());

  // write keys in different slot
  auto key_in_slot0(ComposeNamespaceKey(kDefaultNamespace, "", true));
  auto key_in_slot1000(ComposeNamespaceKey(kDefaultNamespace, "18679", true));
  auto key_in_slot1001(ComposeNamespaceKey(kDefaultNamespace, "13266", true));
  auto key_in_slot16383(ComposeNamespaceKey(kDefaultNamespace, "39296", true));
  std::vector<std::string> keys{key_in_slot0, key_in_slot1000, key_in_slot1001, key_in_slot16383};
  auto storage = srv.GetStorage(test_db_id);
  auto meta_cf_handle = storage->GetCFHandle(engine::kMetadataColumnFamilyName);
  {
    rocksdb::WriteBatch wb;
    for (auto &key : keys) {
      wb.Put(meta_cf_handle, key, key);
    }
    EXPECT_TRUE(storage->Write(storage->DefaultWriteOptions(), &wb).ok());
    for (auto &key : keys) {
      std::string val;
      auto s = storage->Get(storage->DefaultMultiGetOptions(), meta_cf_handle, key, &val);
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(key, val);
    }
  }

  // split datanode1 [slotrange0] to datanode1[slotrange1], datanode2[slotrange2]

  TestDatanode datanode_split1{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
          },
  };

  TestDatanode datanode_split2{
      .datanode_id = datanode_id2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 2
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = kClusterSlots - 1,
              },
          },
  };

  TestShard split_active_shard1 = {{datanode_split1}};
  TestShard split_active_shard2 = {{datanode_split2}};
  TestCluster split_active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{split_active_shard1}, {split_active_shard2}},
  };
  TestPBHACluster splitallcluster{
      .version = test_version + 1,
      .active_cluster = &split_active_cluster,
      .standby_cluster = nullptr,
  };

  // TODO(zhangshixiang) start two sever check topo
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(splitallcluster));
  EXPECT_TRUE(s.IsOK());
  // check datanode
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->DtsRWStatus(), DATANODE_DTS_RO);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->DtsRWStatus(), DATANODE_DTS_RO);

  // check slotranges
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByIndex(0, 1000) != nullptr);
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByIndex(0, kClusterSlots - 1) == nullptr);

  // check keys in different slot
  usleep(100000);
  for (auto &key : keys) {
    std::string val;
    auto s = storage->Get(storage->DefaultMultiGetOptions(), meta_cf_handle, key, &val);
    if (key == key_in_slot0 || key == key_in_slot1000) {
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(key, val);
    } else {
      EXPECT_TRUE(s.IsNotFound());
      EXPECT_TRUE(val.empty());
    }
  }
  EXPECT_FALSE(StorageTestHelper::GetDisableAutoCompactionsOption(storage));
}

TEST(ClusterTest, ExpansionWithoutsplit) {
  auto prev = sleepMSBeforeCleanData;
  sleepMSBeforeCleanData = 1;
  auto exit = MakeScopeExit([&prev]() { sleepMSBeforeCleanData = prev; });
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1, test_db_id + 2};
  // tmp datanode id
  const auto datanode_id1 = test_active_datanode_id;
  const auto datanode_id2 = test_active_datanode_id + "2";

  // serving datanode server
  opt.datanode_id = datanode_id1;
  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // create normal simple topology
  TestDatanode datanode1{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },

              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = kClusterSlots - 1,
              },

          },
  };

  TestShard active_shard1 = {{datanode1}};
  TestCluster active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard1}},
  };
  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  auto s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());

  // Expansion without splitting
  TestDatanode datanode3{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
          },
  };

  TestDatanode datanode2{
      .datanode_id = datanode_id2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = kClusterSlots - 1,
              },
          },
  };

  TestShard active_shard3 = {{datanode3}};
  TestShard active_shard2 = {{datanode2}};
  TestCluster active_cluster2{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard2}, {active_shard3}},
  };
  TestPBHACluster allcluster2{
      .version = test_version + 1,
      .active_cluster = &active_cluster2,
      .standby_cluster = nullptr,
  };

  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster2));
  EXPECT_TRUE(s.IsOK());
  // check datanode
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id1)->DtsRWStatus(), DATANODE_DTS_RO);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->ServingStatus(), DATANODE_SERVING);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->ClientRWStatus(), DATANODE_CLIENT_RW);
  EXPECT_EQ(srv.GetServer()->cluster->GetDatanodeById(datanode_id2)->DtsRWStatus(), DATANODE_DTS_RO);

  // check slotranges
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByIndex(0, 1000) != nullptr);
  EXPECT_TRUE(srv.GetServer()->cluster->GetSlotRangeByIndex(0, kClusterSlots - 1) == nullptr);
  usleep(100000);
}

TEST(ClusterTest, SetTopoClusterError) {
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1, test_db_id + 2};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // create normal simple topology
  auto datanode_id1 = test_active_datanode_id;
  auto datanode_id2 = test_active_datanode_id + "2";
  TestDatanode datanode1{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = 5000,
              },
          },
  };
  TestDatanode datanode2 = {
      .datanode_id = datanode_id2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {// slotrnage 2
           {
               .db_id = test_db_id + 2,
               .start = 5001,
               .end = kClusterSlots - 1,
           }},
  };
  TestShard active_shard1 = {{datanode1}};
  TestShard active_shard2 = {{datanode2}};
  TestCluster active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard1}, {active_shard2}},
  };
  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  const auto resp = TestBuildControllerResp(allcluster);
  auto s = srv.GetServer()->cluster->SetTopo(resp);
  EXPECT_TRUE(s.IsOK());

  // topo version <= current version
  s = srv.GetServer()->cluster->SetTopo(resp);
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), errInvalidClusterVersion);
  // cluster role
  allcluster.version = test_version + 1;
  allcluster.active_cluster->role = CLUSTER_STANDBY;
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_EQ(s.Msg(), errInvalidClusterRole);
  // pool error
  allcluster.active_cluster->role = CLUSTER_ACTIVE;
  allcluster.active_cluster->pool_name = "invalide-pool-name";
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_EQ(s.Msg(), errInvalidClusterPool);
  // cluster id error
  allcluster.active_cluster->pool_name = test_active_pool;
  allcluster.active_cluster->cluster_id = "invalid-cluster-id";
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_EQ(s.Msg(), errClusterIDNotMatch);
  // ok
  allcluster.active_cluster->cluster_id = test_active_cluster_id;
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());
}

// 1. datanode with slave
//   a. check slave has right master
//   b. check master has right slave
// 2. no master datanode
// 3. slave's slotranges cannot match master's
TEST(ClusterTest, SetTopoDatanodeError) {
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1, test_db_id + 2};
  // tmp datanode id
  const auto datanode_id1 = test_active_datanode_id;
  const auto datanode_id1_slave = test_active_datanode_id + "-salve";
  const auto datanode_id2 = test_active_datanode_id + "2";

  // master datanode server
  opt.datanode_id = datanode_id1;
  auto srv_master = MockServer(opt);
  srv_master.StopCtrlClient();
  // slave datanode server
  opt.port = opt.port + 1;
  opt.datanode_id = datanode_id1_slave;
  auto srv_slave = MockServer(opt);
  srv_slave.StopCtrlClient();

  // create normal simple topology
  TestDatanode datanode1{
      .datanode_id = datanode_id1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = 5000,
              },
          },
  };
  TestDatanode datanode1_slave{
      .datanode_id = datanode_id1_slave,
      .serving_status = DATANODE_IMPORTING,
      .client_rw_status = DATANODE_CLIENT_RO,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 1000,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = 5000,
              },
          },
  };
  TestDatanode datanode2 = {
      .datanode_id = datanode_id2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {// slotrnage 2
           {
               .db_id = test_db_id + 2,
               .start = 5001,
               .end = kClusterSlots - 1,
           }},
  };
  TestShard active_shard1 = {{datanode1, datanode1_slave}};
  TestShard active_shard2 = {{datanode2}};
  TestCluster active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard1}, {active_shard2}},
  };
  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  // settopo to master and slave server
  auto s = srv_master.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());
  s = srv_slave.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());

  // 1. check master has slave
  const auto &importint_id = srv_master.GetServer()->cluster->my_datanode_->GetImportingId();
  EXPECT_TRUE(importint_id.empty() == false);
  EXPECT_TRUE(importint_id.count(datanode_id1_slave) == 1);
  // todo(@shixiang.zhang): check every slot range has serving node

  // 2. datanode status error
  // wrong datanode status for master
  allcluster.version = allcluster.version + 1;
  allcluster.active_cluster->shards.front().datanodes.front().client_rw_status = DATANODE_CLIENT_UNSPECIFIED;
  s = srv_master.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), errWrongNodeTopoStatus);
  allcluster.active_cluster->shards.front().datanodes.front().client_rw_status = DATANODE_CLIENT_RW;

  // wrong datanode status for slave
  allcluster.active_cluster->shards.front().datanodes.back().client_rw_status = DATANODE_CLIENT_RW;
  s = srv_slave.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), errWrongNodeTopoStatus);
  allcluster.active_cluster->shards.front().datanodes.back().client_rw_status = DATANODE_CLIENT_RO;

  // 3. no master datanode
  allcluster.version = allcluster.version + 1;
  allcluster.active_cluster->shards.front().datanodes.front().serving_status = DATANODE_IMPORTING;
  allcluster.active_cluster->shards.front().datanodes.front().client_rw_status = DATANODE_CLIENT_RO;
  s = srv_master.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));

  // 4. slave's slotranges cannot match master's
  allcluster.active_cluster->shards.front().datanodes.front().serving_status = DATANODE_SERVING;
  allcluster.active_cluster->shards.front().datanodes.front().client_rw_status = DATANODE_CLIENT_RW;
  allcluster.active_cluster->shards.front().datanodes.back().slot_ranges.front().start = 100;
  s = srv_slave.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_EQ(s.Msg(), errSlotRangeNotMatchWithServing);
}

TEST(ClusterTest, SetTopoSlotRangeError) {
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1, test_db_id + 2};

  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // create normal simple topology
  auto datanode_id1 = test_active_datanode_id;
  auto datanode_id2 = test_active_datanode_id + "2";
  TestDatanode datanode1{.datanode_id = datanode_id1,
                         .serving_status = DATANODE_SERVING,
                         .client_rw_status = DATANODE_CLIENT_RW,
                         .dts_rw_status = DATANODE_DTS_RO,
                         .slot_ranges = {
                             // slotrange 0
                             {
                                 .db_id = test_db_id,
                                 .start = 0,
                                 .end = 1000,
                             },
                         }};
  TestDatanode datanode2{
      .datanode_id = datanode_id2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id + 1,
                  .start = 1001,
                  .end = kClusterSlots - 1,
              },
          },
  };
  TestShard active_shard1 = {{datanode1}};
  TestShard active_shard2 = {{datanode2}};
  TestCluster active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard1}, {active_shard2}},
  };
  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  const auto resp = TestBuildControllerResp(allcluster);
  auto s = srv.GetServer()->cluster->SetTopo(resp);
  EXPECT_TRUE(s.IsOK());

  allcluster.version = allcluster.version + 1;
  // 1. slotrange overlap
  allcluster.active_cluster->shards.front().datanodes.front().slot_ranges.front().end = 2000;
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_EQ(s.Msg(), errSlotOverlapped);
  // 2. slotrange cannot cover all slots
  allcluster.active_cluster->shards.front().datanodes.front().slot_ranges.front().end = 200;
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_EQ(s.Msg(), errSlotNotEnough);
  // 3. out of range
  allcluster.active_cluster->shards.front().datanodes.front().slot_ranges.front().end = 20000;
  s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_EQ(s.Msg(), errSlotOutOfRange);
}

// CanExecByMySelf on active master and slave
TEST(ClusterTest, CanExecByMySelfOnMaster) {
  const auto master_datanodeId1 = test_active_datanode_id + "-master1";
  const auto slave_datanodeId1 = test_active_datanode_id + "-slave1";
  const auto master_datanodeId2 = test_active_datanode_id + "-master2";

  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = master_datanodeId1;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};
  // master1 server
  opt.port = 16666;
  auto master_srv1 = MockServer(opt);
  master_srv1.StopCtrlClient();
  // slave1 server
  opt.port = 17777;
  opt.datanode_id = slave_datanodeId1;
  auto slave_srv1 = MockServer(opt);
  slave_srv1.StopCtrlClient();
  // master2 server
  opt.port = 18888;
  opt.datanode_id = master_datanodeId2;
  opt.db_ids = {test_db_id + 1};
  auto master_srv2 = MockServer(opt);
  master_srv2.StopCtrlClient();

  // cluster topo
  TestDatanode node_master1{
      .datanode_id = master_datanodeId1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 5000,
              },
          },
      .port = 16666,
  };
  TestDatanode node_slave1{
      .datanode_id = slave_datanodeId1,
      .serving_status = DATANODE_IMPORTING,
      .client_rw_status = DATANODE_CLIENT_RO,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 5000,
              },
          },
      .port = 17777,  // set different port for slave
  };
  TestDatanode node_master2{
      .datanode_id = master_datanodeId2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id + 1,
                  .start = 5001,
                  .end = kClusterSlots - 1,
              },
          },
      .port = 18888,
  };

  TestShard active_shard1 = {{node_master1, node_slave1}};
  TestShard active_shard2 = {{node_master2}};
  TestCluster active_cluster{
      .pool_name = test_active_pool,
      .cluster_id = test_active_cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{active_shard1, active_shard2}},
  };

  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = &active_cluster,
      .standby_cluster = nullptr,
  };

  // set master1 topo
  auto s = master_srv1.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());
  // set slave1 topo
  s = slave_srv1.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());
  // set master2 topo
  s = master_srv2.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());

  uint64_t cmd_write_flags = redis::kCmdWrite;
  uint64_t cmd_read_flags = redis::kCmdReadOnly;
  // 1. move read/write cmds from master1 -> master2
  std::string move_to_master2 = "MOVED 6000 127.0.0.1:18888";
  std::string key(global_slot_keys[6000]);
  auto target_slotrange_name = CreateSlotRangeName(5001, kClusterSlots - 1);
  {
    // write cmds
    auto st1 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, target_slotrange_name, key);
    EXPECT_TRUE(st1.IsRetry());
    EXPECT_EQ(st1.Msg(), move_to_master2);
    // read cmds
    auto st2 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, target_slotrange_name, key);
    EXPECT_TRUE(st2.IsRetry());
    EXPECT_EQ(st2.Msg(), move_to_master2);
  }
  // 2. move read/write cmds  from slave -> master
  std::string key1(global_slot_keys[4000]);
  std::string move_to_master1 = "MOVED 4000 127.0.0.1:16666";
  auto my_slotrange_name = CreateSlotRangeName(0, 5000);
  {
    // write cmds
    auto st1 = slave_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st1.IsRetry());
    EXPECT_EQ(st1.Msg(), move_to_master1);
    // read cmds
    auto st2 = slave_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st2.IsRetry());
    EXPECT_EQ(st2.Msg(), move_to_master1);
  }
  // 3. can execute write/read cmd
  {
    // write cmds
    auto st1 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st1.IsOK());
    // read cmds
    auto st2 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st2.IsOK());
  }
  // 4. master slotrange stop write
  {
    // write cmds
    auto slot_range = master_srv1.GetServer()->cluster->GetSlotRangeByIndex(0, 5000);
    slot_range->SetClientWriteRunningStatus(WriteStatus::RO);
    auto st1 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st1.IsRetry());
    EXPECT_TRUE(st1.Msg().find("Retry") != std::string::npos);
    // read cmds
    auto st2 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st2.IsOK());
    slot_range->SetClientWriteRunningStatus(WriteStatus::UNSPECIFIED);
  }
  // 5. slave slotrange can write
  {
    auto res = slave_srv1.GetServer()->cluster->SetClientWriteRunningStatusWrite();
    EXPECT_TRUE(res.IsOK());
    // write cmds
    auto st1 = slave_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st1.IsOK());
    // read cmds
    auto st2 = slave_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, my_slotrange_name, key1);
    EXPECT_TRUE(st2.IsOK());
    // reset running status
    slave_srv1.GetServer()->cluster->ClearWriteRunningStatus();
  }
  // 6. test ShifrStorageReplId
  {
    auto slot_range = master_srv1.GetServer()->cluster->GetSlotRangeByIndex(0, 5000);
    auto old_repl_id = slot_range->GetStorage()->GetReplIdFromDbEngine();
    auto &local_slotranges = master_srv1.GetServer()->cluster->my_slot_ranges_;
    auto res = master_srv1.GetServer()->cluster->ShiftStorageReplId(local_slotranges);
    auto new_slot_range = master_srv1.GetServer()->cluster->GetSlotRangeByIndex(0, 5000);
    auto new_repl_id = new_slot_range->GetStorage()->GetReplIdFromDbEngine();
    ASSERT_TRUE(old_repl_id.IsOK());
    ASSERT_TRUE(new_repl_id.IsOK());
    EXPECT_FALSE(old_repl_id->empty());
    EXPECT_FALSE(new_repl_id->empty());
    EXPECT_TRUE(old_repl_id.GetValue() != new_repl_id.GetValue());
  }

  // CanExecByMySelf by slot
  {
    std::vector<std::string> nodescan_tokens{"nodescan", "0-8191", "0"};
    std::unique_ptr<Commander> current_cmd;
    auto res = master_srv1.GetServer()->LookupAndCreateCommand(nodescan_tokens.front(), &current_cmd);
    ASSERT_TRUE(res.IsOK());
    const auto attributes = current_cmd->GetAttributes();
    auto cmd_flags = attributes->GenerateFlags(nodescan_tokens);

    std::string key;
    auto target_slotrange_name1 = CreateSlotRangeName(0, 5000);
    auto target_slotrange_name2 = CreateSlotRangeName(5001, kClusterSlots - 1);
    // 1. can exec get right storage
    int16_t slot1 = 4000, slot2 = 6000;
    auto s = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_flags, target_slotrange_name1, key, slot1, true);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(s.GetValue()->GetDBId(), test_db_id);
    auto s1 = master_srv2.GetServer()->cluster->CanExecByMySelf(cmd_flags, target_slotrange_name2, key, slot2, true);
    EXPECT_TRUE(s1.IsOK());
    EXPECT_EQ(s1.GetValue()->GetDBId(), test_db_id + 1);
    // 2. slot is not belonging to current node, move to the right node
    auto s2 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_flags, target_slotrange_name2, key, slot2, true);
    EXPECT_FALSE(s2.IsOK());
    EXPECT_EQ(s2.Msg(), move_to_master2);
    auto s3 = master_srv2.GetServer()->cluster->CanExecByMySelf(cmd_flags, target_slotrange_name1, key, slot1, true);
    EXPECT_FALSE(s3.IsOK());
    EXPECT_EQ(s3.Msg(), move_to_master1);
    // 3. current node is not serving, move from slave -> master
    auto s4 = slave_srv1.GetServer()->cluster->CanExecByMySelf(cmd_flags, target_slotrange_name1, key, slot1, true);
    EXPECT_FALSE(s4.IsOK());
    EXPECT_EQ(s4.Msg(), move_to_master1);
    // 4. node write only
    // no write only currently
  }

  // 7. datanode read only
  {
    auto client_rw_status = master_srv1.GetServer()->cluster->GetDatanodeById(master_datanodeId1)->ClientRWStatus();
    master_srv1.GetServer()->cluster->GetDatanodeById(master_datanodeId1)->SetClientRWStatus(DATANODE_CLIENT_RO);
    auto s = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, my_slotrange_name, key1);
    EXPECT_TRUE(s.IsRetry());
    EXPECT_TRUE(s.Msg().find("Retry") != std::string::npos);
    EXPECT_TRUE(s.Msg().find("not writable") != std::string::npos);
    master_srv1.GetServer()->cluster->GetDatanodeById(master_datanodeId1)->SetClientRWStatus(client_rw_status);
  }
}

// CanExecByMySelf on standby pool
TEST(ClusterTest, CanExecByMySelfOnStandby) {
  const auto master_datanodeId1 = test_active_datanode_id + "-master1";
  const auto master_datanodeId2 = test_active_datanode_id + "-master2";

  MockOptions opt;
  opt.cluster_id = test_standby_cluster_id;
  opt.datanode_id = master_datanodeId1;
  opt.pool = test_standby_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};
  // master1 server
  opt.port = 16666;
  auto master_srv1 = MockServer(opt);
  master_srv1.StopCtrlClient();
  // master2 server
  opt.port = 18888;
  opt.datanode_id = master_datanodeId2;
  opt.db_ids = {test_db_id + 1};
  auto master_srv2 = MockServer(opt);
  master_srv2.StopCtrlClient();

  // cluster topo
  TestDatanode node_master1{
      .datanode_id = master_datanodeId1,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RO,
      .dts_rw_status = DATANODE_DTS_RW,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id,
                  .start = 0,
                  .end = 5000,
              },
          },
      .port = 16666,
  };
  TestDatanode node_master2{
      .datanode_id = master_datanodeId2,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RO,
      .dts_rw_status = DATANODE_CLIENT_RW,
      .slot_ranges =
          {
              // slotrange 0
              {
                  .db_id = test_db_id + 1,
                  .start = 5001,
                  .end = kClusterSlots - 1,
              },
          },
      .port = 18888,
  };

  TestShard standby_shard1 = {{node_master1}};
  TestShard standby_shard2 = {{node_master2}};
  TestCluster standby_cluster{
      .pool_name = test_standby_pool,
      .cluster_id = test_standby_cluster_id,
      .role = CLUSTER_STANDBY,
      .shards = {{standby_shard1, standby_shard2}},
  };

  TestPBHACluster allcluster{
      .version = test_version,
      .active_cluster = nullptr,
      .standby_cluster = &standby_cluster,
  };

  // set master1 topo
  auto s = master_srv1.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());
  // set master2 topo
  s = master_srv2.GetServer()->cluster->SetTopo(TestBuildControllerResp(allcluster));
  EXPECT_TRUE(s.IsOK());

  uint64_t cmd_write_flags = redis::kCmdWrite;
  uint64_t cmd_read_flags = redis::kCmdReadOnly;
  // 1. move from master1 -> master2
  {
    std::string key(global_slot_keys[6000]);
    std::string move_to_master2 = "MOVED 6000 127.0.0.1:18888";
    auto target_slotrange_name = CreateSlotRangeName(5001, kClusterSlots - 1);
    {
      // write cmds
      auto st1 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, target_slotrange_name, key);
      EXPECT_TRUE(st1.IsRetry());
      EXPECT_EQ(st1.Msg(), move_to_master2);
      // read cmds
      auto st2 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, target_slotrange_name, key);
      EXPECT_TRUE(st2.IsRetry());
      EXPECT_EQ(st2.Msg(), move_to_master2);
    }
  }
  // 2. master1 read/write cmds
  std::string key1(global_slot_keys[4000]);
  auto master1_slotrange_name = CreateSlotRangeName(0, 5000);
  {
    // write cmds
    auto st1 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, master1_slotrange_name, key1);
    EXPECT_FALSE(st1.IsOK());
    EXPECT_TRUE(st1.Msg().find("not writable") != std::string::npos);
    EXPECT_TRUE(st1.Msg().find("Retry") == std::string::npos);
    // read cmds
    auto st2 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, master1_slotrange_name, key1);
    EXPECT_TRUE(st2.IsOK());
  }
  // 3. master2 read/write cmds
  std::string key2(global_slot_keys[6000]);
  auto master2_slotrange_name = CreateSlotRangeName(5001, kClusterSlots - 1);
  {
    // write cmds
    auto st1 = master_srv2.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, master2_slotrange_name, key2);
    EXPECT_FALSE(st1.IsOK());
    EXPECT_TRUE(st1.Msg().find("not writable") != std::string::npos);
    EXPECT_TRUE(st1.Msg().find("Retry") == std::string::npos);
    // read cmds
    auto st2 = master_srv2.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, master2_slotrange_name, key2);
    EXPECT_TRUE(st2.IsOK());
  }
  // 4. slotrange can write but standby pool is not writable
  {
    auto res = master_srv1.GetServer()->cluster->SetClientWriteRunningStatusWrite();
    EXPECT_TRUE(res.IsOK());
    // write cmds
    auto st1 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_write_flags, master1_slotrange_name, key1);
    EXPECT_FALSE(st1.IsOK());
    EXPECT_TRUE(st1.Msg().find("not writable") != std::string::npos);
    EXPECT_TRUE(st1.Msg().find("Retry") == std::string::npos);
    // read cmds
    auto st2 = master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, master1_slotrange_name, key1);
    EXPECT_TRUE(st2.IsOK());
    // reset running status
    master_srv1.GetServer()->cluster->ClearWriteRunningStatus();
  }

  // CanExecByMySelf by slot
  {
    uint64_t cmd_read_flags = redis::kCmdReadOnly;
    std::string key;
    std::string move_to_master2 = "MOVED 6000 127.0.0.1:18888";
    std::string move_to_master1 = "MOVED 4000 127.0.0.1:16666";
    auto target_slotrange_name1 = CreateSlotRangeName(0, 5000);
    auto target_slotrange_name2 = CreateSlotRangeName(5001, kClusterSlots - 1);
    // 1. can exec get right storage
    int16_t slot1 = 4000, slot2 = 6000;
    auto s1 =
        master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, target_slotrange_name1, key, slot1, true);
    EXPECT_TRUE(s1.IsOK());
    EXPECT_EQ(s1.GetValue()->GetDBId(), test_db_id);
    auto s2 =
        master_srv2.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, target_slotrange_name2, key, slot2, true);
    EXPECT_TRUE(s2.IsOK());
    EXPECT_EQ(s2.GetValue()->GetDBId(), test_db_id + 1);
    // 2. slot is not belonging to current node, move to the right node
    auto s3 =
        master_srv1.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, target_slotrange_name2, key, slot2, true);
    EXPECT_FALSE(s3.IsOK());
    EXPECT_EQ(s3.Msg(), move_to_master2);
    auto s4 =
        master_srv2.GetServer()->cluster->CanExecByMySelf(cmd_read_flags, target_slotrange_name1, key, slot1, true);
    EXPECT_FALSE(s4.IsOK());
    EXPECT_EQ(s4.Msg(), move_to_master1);
  }
}

TEST(ClusterTest, HASwitch) {
  MockOptions opts;
  auto mock_srv = MockServer(opts);
  mock_srv.StopCtrlClient();
  auto srv = mock_srv.GetServer();
  auto key = "key";
  // 0. set as active
  TestDatanode node{
      .datanode_id = opts.datanode_id,
      .serving_status = DATANODE_SERVING,
      .client_rw_status = DATANODE_CLIENT_RW,
      .dts_rw_status = DATANODE_DTS_RO,
      .slot_ranges = {{
          .db_id = *opts.db_ids.begin(),
          .start = 0,
          .end = kClusterSlots - 1,
      }},
      .port = static_cast<int>(opts.port),
  };
  TestCluster cluster{
      .pool_name = opts.pool,
      .cluster_id = opts.cluster_id,
      .role = CLUSTER_ACTIVE,
      .shards = {{{node}}},
  };
  TestPBHACluster ha_cluster;
  ha_cluster.version = 1;
  ha_cluster.active_cluster = &cluster;
  ha_cluster.standby_cluster = nullptr;
  auto topo = TestBuildControllerResp(ha_cluster);
  ASSERT_TRUE(srv->cluster->SetTopo(topo).IsOK());
  auto slot_range_name = srv->cluster->GetSlotRangeNameByKey(key);
  ASSERT_TRUE(srv->cluster->IsActivePool());
  ASSERT_FALSE(srv->cluster->CanSyncSendData("", true).has_value());
  ASSERT_TRUE(srv->cluster->CanSyncReceiveDataCrossPool("").has_value());
  ASSERT_TRUE(srv->cluster->CanExecByMySelf(kCmdWrite, slot_range_name, key).IsOK());
  // 1. swtich to standby
  node.client_rw_status = DATANODE_CLIENT_RO;  // update client rw status at first
  cluster.shards = {{{node}}};
  ha_cluster.version++;
  ha_cluster.active_cluster = &cluster;
  ha_cluster.standby_cluster = nullptr;
  topo = TestBuildControllerResp(ha_cluster);
  ASSERT_TRUE(srv->cluster->SetTopo(topo).IsOK());
  ASSERT_TRUE(srv->cluster->IsActivePool());
  ASSERT_FALSE(srv->cluster->CanSyncSendData("", true).has_value());
  ASSERT_TRUE(srv->cluster->CanSyncReceiveDataCrossPool("").has_value());
  ASSERT_FALSE(srv->cluster->CanExecByMySelf(kCmdWrite, slot_range_name, key).IsOK());
  node.dts_rw_status = DATANODE_DTS_RW;  // update dts rw status and cluster role
  cluster.shards = {{{node}}};
  cluster.role = CLUSTER_STANDBY;
  ha_cluster.version++;
  ha_cluster.active_cluster = nullptr;
  ha_cluster.standby_cluster = &cluster;
  topo = TestBuildControllerResp(ha_cluster);
  ASSERT_TRUE(srv->cluster->SetTopo(topo).IsOK());
  ASSERT_FALSE(srv->cluster->IsActivePool());
  ASSERT_TRUE(srv->cluster->CanSyncSendData("", true).has_value());
  ASSERT_FALSE(srv->cluster->CanSyncReceiveDataCrossPool("").has_value());
  ASSERT_FALSE(srv->cluster->CanExecByMySelf(kCmdWrite, slot_range_name, key).IsOK());
  // 2. swtich to active, same as switch forcely
  node.client_rw_status = DATANODE_CLIENT_RW;
  node.dts_rw_status = DATANODE_DTS_RO;
  cluster.shards = {{{node}}};
  cluster.role = CLUSTER_ACTIVE;
  ha_cluster.version++;
  ha_cluster.active_cluster = &cluster;
  ha_cluster.standby_cluster = nullptr;
  topo = TestBuildControllerResp(ha_cluster);
  ASSERT_TRUE(srv->cluster->SetTopo(topo).IsOK());
  ASSERT_TRUE(srv->cluster->IsActivePool());
  ASSERT_FALSE(srv->cluster->CanSyncSendData("", true).has_value());
  ASSERT_TRUE(srv->cluster->CanSyncReceiveDataCrossPool("").has_value());
  ASSERT_TRUE(srv->cluster->CanExecByMySelf(kCmdWrite, slot_range_name, key).IsOK());
  // 3. switch to standby forcely
  node.client_rw_status = DATANODE_CLIENT_RO;
  node.dts_rw_status = DATANODE_DTS_RW;
  cluster.shards = {{{node}}};
  cluster.role = CLUSTER_STANDBY;
  ha_cluster.version++;
  ha_cluster.active_cluster = nullptr;
  ha_cluster.standby_cluster = &cluster;
  topo = TestBuildControllerResp(ha_cluster);
  ASSERT_TRUE(srv->cluster->SetTopo(topo).IsOK());
  ASSERT_FALSE(srv->cluster->IsActivePool());
  ASSERT_TRUE(srv->cluster->CanSyncSendData("", true).has_value());
  ASSERT_FALSE(srv->cluster->CanSyncReceiveDataCrossPool("").has_value());
  ASSERT_FALSE(srv->cluster->CanExecByMySelf(kCmdWrite, slot_range_name, key).IsOK());
}

}  // namespace redis
