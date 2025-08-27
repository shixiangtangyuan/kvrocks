#pragma once

#include "cluster/cluster_defs.h"
#include "cluster/cluster_util.h"
#include "common/status.h"

class MockServer;

namespace redis {

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
const uint64_t test_db_id = 1;
const uint64_t test_version = 11;

Status SetTopo(MockServer &srv);

}  // namespace redis
