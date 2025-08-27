#include "cluster_util.h"

#include <glog/logging.h>

ReportDataNodeResponse TestBuildControllerResp(const TestPBCluster &tcluster) {
  ReportDataNodeResponse resp;
  resp.mutable_ha_cluster()->set_version(tcluster.version);

  kv::controller::v1::Cluster cluster;
  cluster.set_pool(tcluster.pool_name);
  cluster.set_cluster_id(tcluster.cluster_id);
  cluster.set_role(kv::controller::v1::Cluster::ROLE_ACTIVE);
  for (const auto &tshard : tcluster.shards) {
    kv::controller::v1::Shard shard;
    for (const auto &tdatanode : tshard.datanodes) {
      kv::controller::v1::Datanode datanode;
      for (const auto &tslotrange : tdatanode.slot_ranges) {
        kv::controller::v1::SlotRangeIndex index;
        index.set_start(tslotrange.start);
        index.set_end(tslotrange.end);

        kv::controller::v1::SlotRange range;
        range.mutable_index()->CopyFrom(index);
        range.set_db_id(tslotrange.db_id);

        *datanode.add_slot_range_list() = range;
      }
      if (tdatanode.serving_status == kv::controller::v1::Datanode::SERVING_STATUS_SERVING) {
        datanode.set_client_rw_status(kv::controller::v1::Datanode::RW_STATUS_RW);
      } else {
        datanode.set_client_rw_status(kv::controller::v1::Datanode::RW_STATUS_RO);
      }
      datanode.set_dts_rw_status(kv::controller::v1::Datanode::RW_STATUS_RO);
      datanode.set_datanode_id(tdatanode.datanode_id);
      datanode.set_serving_status(tdatanode.serving_status);
      *shard.add_datanodes() = datanode;
    }
    cluster.add_shards()->CopyFrom(shard);
  }
  resp.mutable_ha_cluster()->mutable_active()->CopyFrom(cluster);

  kv::controller::v1::Cluster standby_cluster;
  standby_cluster.set_pool(tcluster.standby_pool_name);
  standby_cluster.set_cluster_id(tcluster.standby_cluster_id);
  standby_cluster.set_role(kv::controller::v1::Cluster::ROLE_STANDBY);
  for (const auto &tshard : tcluster.standby_shards) {
    kv::controller::v1::Shard shard;
    for (const auto &tdatanode : tshard.datanodes) {
      kv::controller::v1::Datanode datanode;
      for (const auto &tslotrange : tdatanode.slot_ranges) {
        kv::controller::v1::SlotRangeIndex index;
        index.set_start(tslotrange.start);
        index.set_end(tslotrange.end);

        kv::controller::v1::SlotRange range;
        range.mutable_index()->CopyFrom(index);
        range.set_db_id(tslotrange.db_id);

        *datanode.add_slot_range_list() = range;
      }
      if (tdatanode.serving_status == kv::controller::v1::Datanode::SERVING_STATUS_SERVING) {
        datanode.set_dts_rw_status(kv::controller::v1::Datanode::RW_STATUS_RW);
      } else {
        datanode.set_dts_rw_status(kv::controller::v1::Datanode::RW_STATUS_RO);
      }
      datanode.set_client_rw_status(kv::controller::v1::Datanode::RW_STATUS_RO);
      datanode.set_datanode_id(tdatanode.datanode_id);
      datanode.set_serving_status(tdatanode.serving_status);
      *shard.add_datanodes() = datanode;
    }
    standby_cluster.add_shards()->CopyFrom(shard);
  }
  resp.mutable_ha_cluster()->mutable_standby()->CopyFrom(standby_cluster);

  return resp;
}

ReportDataNodeResponse TestBuildControllerResp(const TestPBHACluster &topo) {
  ReportDataNodeResponse resp;
  resp.mutable_ha_cluster()->set_version(topo.version);
  // create active pool cluster topo
  if (topo.active_cluster != nullptr) {
    LOG(INFO) << "Create active cluster topo";
    resp.mutable_ha_cluster()->mutable_active()->CopyFrom(TestBuildClusterTopo(topo.active_cluster));
  }
  // create standby pool cluster topo
  if (topo.standby_cluster != nullptr) {
    LOG(INFO) << "Create standby cluster topo";
    resp.mutable_ha_cluster()->mutable_standby()->CopyFrom(TestBuildClusterTopo(topo.standby_cluster));
  }

  return resp;
}

kv::controller::v1::Cluster TestBuildClusterTopo(TestCluster *tcluster) {
  kv::controller::v1::Cluster cluster;
  cluster.set_pool(tcluster->pool_name);
  cluster.set_cluster_id(tcluster->cluster_id);
  cluster.set_role(tcluster->role);
  for (size_t i = 0; i < tcluster->shards.size(); i++) {
    const auto &tshard = tcluster->shards[i];
    kv::controller::v1::Shard shard;
    shard.set_shard_id(i + 1);
    for (const auto &tdatanode : tshard.datanodes) {
      kv::controller::v1::Datanode datanode;
      for (const auto &tslotrange : tdatanode.slot_ranges) {
        kv::controller::v1::SlotRangeIndex index;
        index.set_start(tslotrange.start);
        index.set_end(tslotrange.end);

        kv::controller::v1::SlotRange range;
        range.mutable_index()->CopyFrom(index);
        range.set_db_id(tslotrange.db_id);

        datanode.add_slot_range_list()->CopyFrom(range);
      }

      datanode.set_client_rw_status(tdatanode.client_rw_status);
      datanode.set_dts_rw_status(tdatanode.dts_rw_status);
      datanode.set_datanode_id(tdatanode.datanode_id);
      datanode.set_serving_status(tdatanode.serving_status);
      kv::controller::v1::IPAddr addr;
      addr.set_ip(tdatanode.ip);
      addr.set_port(tdatanode.port);
      datanode.mutable_ip_addr()->CopyFrom(addr);
      shard.add_datanodes()->CopyFrom(datanode);
    }
    cluster.add_shards()->CopyFrom(shard);
  }
  return cluster;
}
