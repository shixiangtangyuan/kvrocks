#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <cstddef>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "commands/cmd_test_util.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "server/redis_connection.h"

namespace redis {

TEST(HashCDCCmdTest, CmdTest) {
  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id};
  // create server
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  encode_hash_sub_flag.store(false);
  // set topo
  {
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
    auto s = srv.GetServer()->cluster->SetTopo(resp);
    ASSERT_TRUE(s.IsOK());
  }
  // create conn
  auto worker0 = srv.GetWorkerThreads()[0]->GetWorker();
  ASSERT_TRUE(worker0 != nullptr);
  redis::Connection conn{nullptr, worker0};
  conn.BecomeAdmin();
  // get server
  auto srv_ptr = srv.GetServer().get();
  // get storage
  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();

  // create commands
  std::unique_ptr<Commander> cmd_hset;
  auto s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd_hset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hmset;
  s = srv.GetServer()->LookupAndCreateCommand("hmset", &cmd_hmset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hsetnx;
  s = srv.GetServer()->LookupAndCreateCommand("hsetnx", &cmd_hsetnx);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hincrby;
  s = srv.GetServer()->LookupAndCreateCommand("hincrby", &cmd_hincrby);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hincrbyfloat;
  s = srv.GetServer()->LookupAndCreateCommand("hincrbyfloat", &cmd_hincrbyfloat);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hdel;
  s = srv.GetServer()->LookupAndCreateCommand("hdel", &cmd_hdel);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hmget;
  s = srv.GetServer()->LookupAndCreateCommand("hmget", &cmd_hmget);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_del;
  s = srv.GetServer()->LookupAndCreateCommand("del", &cmd_del);
  ASSERT_TRUE(s.IsOK());

  // enable cdc sync
  storage->GetConfig()->enable_cdc_sync = true;

  std::string hkey = "hkey";
  CommandTokens del_cmd_token{"del", hkey};

  // Test hset cmd
  {
    // hset hkey f1 v1 f2 v2
    // hmget hkey f1 f2
    // hset hkey f1 v1 f2 v2
    // hmget hkey f1 f2
    // hset hkey f1 v1 f2 222
    // hmget hkey f1 f2
    // del hkey
    std::string f1 = "f1";
    std::string v1 = "v1";
    std::string f2 = "f2";
    std::string v2 = "v2";
    std::string v222 = "222";
    std::string v111 = "111";
    // create command tokens
    CommandTokens hset_cmd_token{"hset", hkey, f1, v1, f2, v2};
    CommandTokens hget_cmd_token{"hmget", hkey, f1, f2};
    CommandTokens hset_cmd_token2{"hset", hkey, f1, v1, f2, v222};
    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(2));
    s = GenericExecCmd(hget_cmd_token, &output, cmd_hmget, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::MultiBulkString({v1, v2}));
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(0));
    s = GenericExecCmd(hget_cmd_token, &output, cmd_hmget, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::MultiBulkString({v1, v2}));
    s = GenericExecCmd(hset_cmd_token2, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(0));
    s = GenericExecCmd(hget_cmd_token, &output, cmd_hmget, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::MultiBulkString({v1, v222}));
    // del hkey
    s = GenericExecCmd(del_cmd_token, &output, cmd_del, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(1));
  }

  // Test hsetnx
  {
    // hset hkey f1 v1 f2 v2
    // hsetnx hkey f1 v11 f3 v3
    // hmget hkey f1 f3
    // del hkey
    std::string f1 = "f1";
    std::string v1 = "v1";
    std::string f2 = "f2";
    std::string v2 = "v2";
    std::string f3 = "f3";
    std::string v3 = "v3";
    std::string v11 = "11";
    // create command tokens
    CommandTokens hset_cmd_token{"hset", hkey, f1, v1, f2, v2};
    CommandTokens hsetnx_cmd_token{"hsetnx", hkey, f1, v11, f3, v3};
    CommandTokens hget_cmd_token{"hmget", hkey, f1, f3};
    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(2));
    s = GenericExecCmd(hsetnx_cmd_token, &output, cmd_hsetnx, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(1));
    s = GenericExecCmd(hget_cmd_token, &output, cmd_hmget, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::MultiBulkString({v1, v3}));
    // del hkey
    s = GenericExecCmd(del_cmd_token, &output, cmd_del, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(1));
  }

  // Test hincrby/hincrbyfloat
  {
    // hincrby hkey f4 4
    // hincrby hkey f4 4
    // hincrbyfloat hkey f4 0.11
    // del hkey
    std::string f4 = "f4";
    std::string v4 = "4";
    std::string v44 = "8";
    std::string v444 = "8.11";
    // create command tokens
    CommandTokens hincrby_cmd_token{"hincrby", hkey, f4, v4};
    CommandTokens hincrbyfloat_cmd_token{"hincrbyfloat", hkey, f4, "0.11"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(hincrby_cmd_token, &output, cmd_hincrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::Integer(4));
    s = GenericExecCmd(hincrby_cmd_token, &output, cmd_hincrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(output, redis::Integer(8));
    s = GenericExecCmd(hincrbyfloat_cmd_token, &output, cmd_hincrbyfloat, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::BulkString(util::Float2String(8.11)));
    // del hkey
    s = GenericExecCmd(del_cmd_token, &output, cmd_del, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
  }

  // Test hmset
  {
    // hmset hkey f5 v5 f6 v6
    // hmget hkey f5 f6
    // del hkey
    std::string f5 = "f5";
    std::string v5 = "v5";
    std::string f6 = "f6";
    std::string v6 = "v6";
    // create command tokens
    CommandTokens hmset_cmd_token{"hmset", hkey, f5, v5, f6, v6};
    CommandTokens hget_cmd_token{"hmget", hkey, f5, f6};
    // exec cmds
    std::string output;
    s = GenericExecCmd(hmset_cmd_token, &output, cmd_hmset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::SimpleString("OK"));
    s = GenericExecCmd(hget_cmd_token, &output, cmd_hmget, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::MultiBulkString({v5, v6}));
    // del hkey
    s = GenericExecCmd(del_cmd_token, &output, cmd_del, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
  }

  // Test hdel
  {
    // hset hkey f1 v1 f2 v2 f3 v3 f4 v4
    // hmget hkey f1 f2 f3 f4
    // hdel f1 f2
    // hmget hkey f1 f2 f3 f4
    // del hkey
    std::string f1 = "f1";
    std::string f2 = "f2";
    std::string f3 = "f3";
    std::string f4 = "f4";
    std::string v1 = "v1";
    std::string v2 = "v2";
    std::string v3 = "v3";
    std::string v4 = "v4";
    // create command tokens
    CommandTokens hset_cmd_token{"hset", hkey, f1, v1, f2, v2, f3, v3, f4, v4};
    CommandTokens hget_cmd_token{"hmget", hkey, f1, f2, f3, f4};
    CommandTokens hdel_cmd_token{"hdel", hkey, f1, f2};
    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::Integer(4));
    s = GenericExecCmd(hget_cmd_token, &output, cmd_hmget, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::MultiBulkString({v1, v2, v3, v4}));
    s = GenericExecCmd(hdel_cmd_token, &output, cmd_hdel, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::Integer(2));
    s = GenericExecCmd(hget_cmd_token, &output, cmd_hmget, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::MultiBulkString({"", "", v3, v4}));
    // del hkey
    s = GenericExecCmd(del_cmd_token, &output, cmd_del, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
  }
}

}  // namespace redis
