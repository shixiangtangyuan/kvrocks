#include "cluster/set_topo_util.h"

#include "mock/mock_server.h"

namespace redis {

Status SetTopo(MockServer &srv) {
  // create topo
  auto datanode_id1 = test_active_datanode_id;
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
                  .end = 8191,
              },
              // slotrange 1
              {
                  .db_id = test_db_id + 1,
                  .start = 8192,
                  .end = kClusterSlots - 1,
              },
          },
  };
  TestShard active_shard1 = {.datanodes = {datanode1}};
  TestCluster active_cluster = {
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

  const auto resp = TestBuildControllerResp(allcluster);
  return srv.GetServer()->cluster->SetTopo(resp);
}

}  // namespace redis
