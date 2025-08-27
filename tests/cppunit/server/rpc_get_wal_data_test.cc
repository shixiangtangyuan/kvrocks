#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "commands/cmd_test_util.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "server/redis_connection.h"

namespace redis {

TEST(GetWalDataTest, RPCTest) {
  MockOptions opt;
  opt.port = 16579;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1};
  // create server
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  // set topo
  auto s = SetTopo(srv);
  encode_hash_sub_flag.store(false);
  ASSERT_TRUE(s.IsOK());
  // create conn
  auto worker0 = srv.GetWorkerThreads()[0]->GetWorker();
  ASSERT_TRUE(worker0 != nullptr);
  redis::Connection conn{nullptr, worker0};

  // get server and storage ptr
  auto srv_ptr = srv.GetServer().get();
  auto storage1 = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  auto storage2 = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id + 1).get();

  // Prepare data
  // create cmds
  std::unique_ptr<Commander> mset_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("mset", &mset_cmd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> hset_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("hset", &hset_cmd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> sadd_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("sadd", &sadd_cmd);
  ASSERT_TRUE(s.IsOK());
  // construct cmd tokens
  std::string str_key1 = "strkey_{460}";
  std::string str_key2 = "strkey_{0QG}";
  std::string hash_key = "hkey_{MR}";
  std::string set_key = "skey_{5kZ}";
  CommandTokens mset_cmd_token{"MSET", str_key1, "val1", str_key2, "val2"};
  CommandTokens hset_cmd_token{"HSET", hash_key, "f1", "v1", "f2", "v2"};
  CommandTokens sadd_cmd_token{"SADD", set_key, "1", "v2", "x3"};
  // write data
  std::string output;
  s = GenericExecCmd(mset_cmd_token, &output, mset_cmd, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token, &output, hset_cmd, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(sadd_cmd_token, &output, sadd_cmd, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());

  // check data writed
  auto latest_seq = storage1->GetDB()->GetLatestSequenceNumber();
  ASSERT_EQ(latest_seq, 10);
  auto latest_seq2 = storage2->GetDB()->GetLatestSequenceNumber();
  ASSERT_EQ(latest_seq2, 1);

  // Test rpc interfaces
  {
    // Test get latest point
    grpc::CallbackServerContext ctx;
    kv::datanode::v1::GetLatestPointRequest get_point_req;
    kv::datanode::v1::GetLatestPointResponse get_point_resp;
    get_point_req.set_cluster_id("wrong_id");
    get_point_req.add_db_ids(test_db_id);
    get_point_req.add_db_ids(test_db_id + 1);
    srv_ptr->GetLatestPoint(&ctx, &get_point_req, &get_point_resp);
    EXPECT_TRUE(get_point_resp.db_points().empty());

    // get db_ids
    get_point_resp.Clear();
    get_point_req.set_cluster_id(opt.cluster_id);
    srv_ptr->GetLatestPoint(&ctx, &get_point_req, &get_point_resp);
    EXPECT_EQ(get_point_resp.db_points_size(), 2);
    EXPECT_EQ(get_point_resp.db_points()[0].db_id(), test_db_id);
    EXPECT_EQ(get_point_resp.db_points()[0].seq_id(), latest_seq);
    EXPECT_EQ(get_point_resp.db_points()[0].repl_id().size(), kReplIdLength);
    EXPECT_EQ(get_point_resp.db_points()[1].db_id(), test_db_id + 1);
    EXPECT_EQ(get_point_resp.db_points()[1].seq_id(), latest_seq2);
    EXPECT_EQ(get_point_resp.db_points()[1].repl_id().size(), kReplIdLength);

    // Test get data from wal
    kv::datanode::v1::GetDataWithCmdRequest get_wal_data_req;
    kv::datanode::v1::GetDataWithCmdResponse get_wal_data_resp;

    // get all data
    get_wal_data_req.set_db_id(test_db_id);
    get_wal_data_req.set_seq(1);
    srv_ptr->GetDataWithCmd(&ctx, &get_wal_data_req, &get_wal_data_resp);
    EXPECT_EQ(get_wal_data_resp.next_seq(), latest_seq + 1);
    EXPECT_TRUE(get_wal_data_resp.finished());
    EXPECT_EQ(get_wal_data_resp.resp_cmds_size(), 3);
    std::vector<std::string> r1{"MSET", str_key1, "val1", str_key2, "val2"};
    std::vector<std::string> r3{"HSET", hash_key, "f2", "v2", "f1", "v1"};
    std::vector<std::string> r5{"SADD", set_key, "1", "v2", "x3"};
    // check results
    {
      auto &e1 = get_wal_data_resp.resp_cmds()[0];
      EXPECT_EQ(e1, redis::MultiBulkString(r1, false));
      auto &e3 = get_wal_data_resp.resp_cmds()[1];
      EXPECT_EQ(e3, redis::MultiBulkString(r3, false));
      auto &e5 = get_wal_data_resp.resp_cmds()[2];
      EXPECT_EQ(e5, redis::MultiBulkString(r5, false));
    }

    // get partial data
    get_wal_data_resp.Clear();
    get_wal_data_req.set_seq(7);
    srv_ptr->GetDataWithCmd(&ctx, &get_wal_data_req, &get_wal_data_resp);
    EXPECT_EQ(get_wal_data_resp.next_seq(), latest_seq + 1);
    EXPECT_TRUE(get_wal_data_resp.finished());
    EXPECT_EQ(get_wal_data_resp.resp_cmds_size(), 1);
    // check results
    {
      auto &e1 = get_wal_data_resp.resp_cmds()[0];
      EXPECT_EQ(e1, redis::MultiBulkString(r5, false));
    }

    // has finished
    get_wal_data_resp.Clear();
    get_wal_data_req.set_seq(11);
    srv_ptr->GetDataWithCmd(&ctx, &get_wal_data_req, &get_wal_data_resp);
    EXPECT_EQ(get_wal_data_resp.next_seq(), latest_seq + 1);
    EXPECT_TRUE(get_wal_data_resp.finished());
    EXPECT_EQ(get_wal_data_resp.resp_cmds_size(), 0);

    // wrong seq
    get_wal_data_resp.Clear();
    get_wal_data_req.set_seq(12);
    srv_ptr->GetDataWithCmd(&ctx, &get_wal_data_req, &get_wal_data_resp);
    EXPECT_EQ(get_wal_data_resp.next_seq(), 0);
    EXPECT_FALSE(get_wal_data_resp.finished());
    EXPECT_EQ(get_wal_data_resp.resp_cmds_size(), 0);

    get_wal_data_resp.Clear();
    get_wal_data_req.set_seq(5);  // start from middle of a batch
    srv_ptr->GetDataWithCmd(&ctx, &get_wal_data_req, &get_wal_data_resp);
    EXPECT_EQ(get_wal_data_resp.next_seq(), 0);
    EXPECT_FALSE(get_wal_data_resp.finished());
    EXPECT_EQ(get_wal_data_resp.resp_cmds_size(), 0);

    // wrong db_id
    get_wal_data_req.clear_db_id();
    get_wal_data_req.set_db_id(test_db_id + 3);
    get_wal_data_resp.Clear();
    get_wal_data_req.set_seq(1);
    srv_ptr->GetDataWithCmd(&ctx, &get_wal_data_req, &get_wal_data_resp);
    EXPECT_EQ(get_wal_data_resp.next_seq(), 0);
    EXPECT_FALSE(get_wal_data_resp.finished());
    EXPECT_EQ(get_wal_data_resp.resp_cmds_size(), 0);
  }
}

}  // namespace redis
