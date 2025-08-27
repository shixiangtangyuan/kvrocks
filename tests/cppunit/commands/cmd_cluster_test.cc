#include "commands/cmd_cluster.h"

#include <gtest/gtest.h>

#include "../sync/sync_test_util.h"

namespace redis {

TEST(CommandCluster, Base) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto status = ApplyTopo(srv, db_id, true, true, start, end);
  ASSERT_TRUE(status.IsOK());
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range);
  auto topo_version_str = std::to_string(srv.GetServer()->cluster->Version());
  auto& workers = srv.GetWorkerThreads();
  ASSERT_GT(workers.size(), 0);
  redis::Connection conn{nullptr, workers[0]->GetWorker()};
  conn.BecomeAdmin();

  std::string output;
  redis::CommandCluster cmd;
  // invalid subcommand
  ASSERT_FALSE(cmd.Parse({"cluster", "invalid subcommand"}).IsOK());
  // invalid keyslot param
  ASSERT_FALSE(cmd.Parse({"cluster", "keyslot"}).IsOK());
  ASSERT_FALSE(cmd.Parse({"cluster", "keyslot", "k1", "k2"}).IsOK());

  // valid keyslot param
  ASSERT_TRUE(cmd.Parse({"cluster", "keyslot", "k1"}));
  ASSERT_EQ(cmd.subcommand_, "keyslot");
  ASSERT_EQ(cmd.key_, "k1");
  output.clear();
  ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
  ASSERT_EQ(output, redis::Integer(GetSlotIdFromKey("k1")));
  // invalid slotrange param
  ASSERT_FALSE(cmd.Parse({"cluster", "slotrange", "slot_id_1", "slot_id_2"}).IsOK());
  ASSERT_FALSE(cmd.Parse({"cluster", "slotrange", "invalid_slot_id"}).IsOK());
  ASSERT_FALSE(cmd.Parse({"cluster", "slotrange", "-1"}).IsOK());
  ASSERT_FALSE(cmd.Parse({"cluster", "slotrange", std::to_string(kClusterSlots)}).IsOK());
  // get all slot ranges
  ASSERT_TRUE(cmd.Parse({"cluster", "slotrange"}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "slotrange");
  ASSERT_TRUE(cmd.slot_id_ < 0);
  output.clear();
  ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
  ASSERT_NE(output.find(topo_version_str), output.npos);
  ASSERT_NE(output.find(slot_range->GetName()), output.npos);
  // get one slot range
  std::vector<int> slot_ids = {0, kClusterSlots / 2, kClusterSlots - 1};
  for (auto& slot_id : slot_ids) {
    ASSERT_TRUE(cmd.Parse({"cluster", "slotrange", std::to_string(slot_id)}).IsOK());
    ASSERT_EQ(cmd.subcommand_, "slotrange");
    ASSERT_EQ(cmd.slot_id_, slot_id);
    output.clear();
    ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
    ASSERT_NE(output.find(topo_version_str), output.npos);
    ASSERT_NE(output.find(slot_range->GetName()), output.npos);
  }

  // invalid datanode param
  ASSERT_FALSE(cmd.Parse({"cluster", "datanode", "node_id_1", "node_id_2"}).IsOK());
  // get non-existent datanode
  auto invalid_node_id = "invalid " + opt.datanode_id;
  ASSERT_TRUE(cmd.Parse({"cluster", "datanode", invalid_node_id}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "datanode");
  ASSERT_EQ(cmd.node_id_, invalid_node_id);
  output.clear();
  ASSERT_FALSE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
  cmd.node_id_.clear();
  // get all datanodes
  ASSERT_TRUE(cmd.Parse({"cluster", "datanode"}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "datanode");
  ASSERT_EQ(cmd.node_id_, "");
  output.clear();
  ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
  ASSERT_NE(output.find(topo_version_str), output.npos);
  ASSERT_NE(output.find(opt.datanode_id), output.npos);
  ASSERT_NE(output.find(slot_range->GetName()), output.npos);
  // get one datanode
  ASSERT_TRUE(cmd.Parse({"cluster", "datanode", opt.datanode_id}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "datanode");
  ASSERT_EQ(cmd.node_id_, opt.datanode_id);
  output.clear();
  ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
  ASSERT_NE(output.find(topo_version_str), output.npos);
  ASSERT_NE(output.find(opt.datanode_id), output.npos);
  ASSERT_NE(output.find(slot_range->GetName()), output.npos);
}

}  // namespace redis
