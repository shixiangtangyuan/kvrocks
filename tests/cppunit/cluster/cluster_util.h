#pragma once

#include <kv/controller/v1/api.grpc.pb.h>

#include "rpc/rpc_cluster_controller.h"

struct TestPBSlotRange {
  uint64_t db_id;
  int32_t start;
  int32_t end;
};

struct TestPBDataNode {
  std::string datanode_id;
  kv::controller::v1::Datanode::ServingStatus serving_status;
  std::vector<TestPBSlotRange> slot_ranges;
};

struct TestPBShard {
  std::vector<TestPBDataNode> datanodes;
};

struct TestPBCluster {
  uint64_t version;
  std::string pool_name;
  std::string cluster_id;
  std::vector<TestPBShard> shards;
  std::string standby_pool_name;
  std::string standby_cluster_id;
  std::vector<TestPBShard> standby_shards;
};

struct TestDatanode {
  std::string datanode_id;
  kv::controller::v1::Datanode::ServingStatus serving_status;
  kv::controller::v1::Datanode::RWStatus client_rw_status;
  kv::controller::v1::Datanode::RWStatus dts_rw_status;
  std::vector<TestPBSlotRange> slot_ranges;
  std::string ip = "127.0.0.1";
  int port = 6666;
};

struct TestShard {
  std::vector<TestDatanode> datanodes;
};

struct TestCluster {
  std::string pool_name;
  std::string cluster_id;
  kv::controller::v1::Cluster::Role role;
  std::vector<TestShard> shards;
};

struct TestPBHACluster {
  uint64_t version;
  TestCluster *active_cluster = nullptr;
  TestCluster *standby_cluster = nullptr;
};

ReportDataNodeResponse TestBuildControllerResp(const TestPBCluster &tcluster);
ReportDataNodeResponse TestBuildControllerResp(const TestPBHACluster &topo);
kv::controller::v1::Cluster TestBuildClusterTopo(TestCluster *cluster_topo);
