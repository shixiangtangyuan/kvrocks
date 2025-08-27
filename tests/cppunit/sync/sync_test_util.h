#pragma once

#include <gtest/gtest.h>

#include "cluster/cluster_util.h"
#include "mock/mock_server.h"

inline Status ApplyTopo(MockServer &srv, uint64_t db_id, bool is_active_pool = true, bool is_serving = true,
                        int32_t start = 0, int32_t end = kClusterSlots - 1,
                        const std::string &another_node_id = "another_node_id") {
  struct TestPBCluster cluster {
    .version = srv.GetServer()->cluster->Version() + 1,
  };
  struct TestPBShard shard {
    .datanodes = {{.datanode_id = is_serving ? srv.GetMockOptions().datanode_id : another_node_id,
                   .serving_status = kv::controller::v1::Datanode::SERVING_STATUS_SERVING,
                   .slot_ranges = {{
                       .db_id = db_id,
                       .start = start,
                       .end = end,
                   }}},
                  {.datanode_id = is_serving ? another_node_id : srv.GetMockOptions().datanode_id,
                   .serving_status = kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING,
                   .slot_ranges = {{
                       .db_id = db_id,
                       .start = start,
                       .end = end,
                   }}}},
  };
  if (is_active_pool) {
    cluster.shards = {shard};
    cluster.pool_name = srv.GetMockOptions().pool;
    cluster.cluster_id = srv.GetMockOptions().cluster_id;
  } else {
    cluster.standby_shards = {shard};
    cluster.standby_pool_name = srv.GetMockOptions().pool;
    cluster.standby_cluster_id = srv.GetMockOptions().cluster_id;
  }
  auto resp = TestBuildControllerResp(cluster);
  return srv.GetServer()->cluster->SetTopo(resp);
}

inline Status ApplyTopo(MockServer &srv, std::vector<TestPBSlotRange> &slot_ranges, bool is_active_pool = true,
                        bool is_serving = true, const std::string &another_node_id = "another_node_id") {
  struct TestPBCluster cluster {
    .version = srv.GetServer()->cluster->Version() + 1,
  };
  struct TestPBShard shard {
    .datanodes = {{.datanode_id = is_serving ? srv.GetMockOptions().datanode_id : another_node_id,
                   .serving_status = kv::controller::v1::Datanode::SERVING_STATUS_SERVING,
                   .slot_ranges = slot_ranges},
                  {.datanode_id = is_serving ? another_node_id : srv.GetMockOptions().datanode_id,
                   .serving_status = kv::controller::v1::Datanode::SERVING_STATUS_IMPORTING,
                   .slot_ranges = slot_ranges}},
  };
  if (is_active_pool) {
    cluster.shards = {shard};
    cluster.pool_name = srv.GetMockOptions().pool;
    cluster.cluster_id = srv.GetMockOptions().cluster_id;
  } else {
    cluster.standby_shards = {shard};
    cluster.standby_pool_name = srv.GetMockOptions().pool;
    cluster.standby_cluster_id = srv.GetMockOptions().cluster_id;
  }
  auto resp = TestBuildControllerResp(cluster);
  return srv.GetServer()->cluster->SetTopo(resp);
}
