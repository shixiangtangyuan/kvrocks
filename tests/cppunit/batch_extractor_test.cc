#include "storage/batch_extractor.h"

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>
#include <rocksdb/transaction_log.h>

#include <cstddef>
#include <memory>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "commands/cmd_test_util.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "server/redis_connection.h"

namespace redis {

Status GenerateCmdsFromBatch(rocksdb::BatchResult &batch, std::vector<std::vector<std::string>> *output) {
  WriteBatchExtractor write_batch_extractor;
  auto status = batch.writeBatchPtr->Iterate(&write_batch_extractor);
  if (!status.ok()) {
    return {Status::NotOK, status.ToString()};
  }

  for (auto &token : write_batch_extractor.GetCommands()) {
    output->emplace_back(token);
  }
  return Status::OK();
}

Status ParseWAL(engine::Storage *storage, uint64_t *next_seq, std::vector<CommandTokens> *output_cmds) {
  uint64_t latest_seq = storage->GetDB()->GetLatestSequenceNumber();
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
  auto s = storage->GetWALIter(*next_seq, &iter);
  if (!s.IsOK()) {
    std::cout << "[ParseWal error] failed to create wal iterator, err: " << s.Msg() << std::endl;
    return s;
  }

  while (iter->Valid()) {
    auto batch = iter->GetBatch();
    s = GenerateCmdsFromBatch(batch, output_cmds);
    if (!s.IsOK()) {
      std::cout << "[ParseWal error] failed to parse writebatch, err:  " << s.Msg() << std::endl;
      return s;
    }

    *next_seq = batch.sequence + batch.writeBatchPtr->Count();
    if (*next_seq > latest_seq) {
      break;
    }
    iter->Next();
  }
  return Status::OK();
}

class BatchExtractorTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override { enable_cdc_ = GetParam(); }
  void TearDown() override { enable_cdc_ = false; }

  bool enable_cdc_ = false;
};

TEST_P(BatchExtractorTest, AllWriteCmdTest) {
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
  // set cdc
  storage->GetConfig()->enable_cdc_sync = enable_cdc_;

  // create commands
  // create string cmds
  std::unique_ptr<Commander> cmd_mset;
  auto s = srv.GetServer()->LookupAndCreateCommand("mset", &cmd_mset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_setex;
  s = srv.GetServer()->LookupAndCreateCommand("setex", &cmd_setex);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_setnx;
  s = srv.GetServer()->LookupAndCreateCommand("setnx", &cmd_setnx);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_incrby;
  s = srv.GetServer()->LookupAndCreateCommand("incrby", &cmd_incrby);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_incr;
  s = srv.GetServer()->LookupAndCreateCommand("incr", &cmd_incr);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_decrby;
  s = srv.GetServer()->LookupAndCreateCommand("decrby", &cmd_decrby);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_decr;
  s = srv.GetServer()->LookupAndCreateCommand("decr", &cmd_decr);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_incrbyfloat;
  s = srv.GetServer()->LookupAndCreateCommand("incrbyfloat", &cmd_incrbyfloat);
  ASSERT_TRUE(s.IsOK());
  // create hash cmds
  std::unique_ptr<Commander> cmd_hset;
  s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd_hset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hsetnx;
  s = srv.GetServer()->LookupAndCreateCommand("hsetnx", &cmd_hsetnx);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hmset;
  s = srv.GetServer()->LookupAndCreateCommand("hmset", &cmd_hmset);
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
  // create list cmds
  std::unique_ptr<Commander> cmd_linsert;
  s = srv.GetServer()->LookupAndCreateCommand("linsert", &cmd_linsert);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_lset;
  s = srv.GetServer()->LookupAndCreateCommand("lset", &cmd_lset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_lpush;
  s = srv.GetServer()->LookupAndCreateCommand("lpush", &cmd_lpush);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_rpush;
  s = srv.GetServer()->LookupAndCreateCommand("rpush", &cmd_rpush);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_lpushx;
  s = srv.GetServer()->LookupAndCreateCommand("lpushx", &cmd_lpushx);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_rpushx;
  s = srv.GetServer()->LookupAndCreateCommand("rpushx", &cmd_rpushx);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_lpop;
  s = srv.GetServer()->LookupAndCreateCommand("lpop", &cmd_lpop);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_rpop;
  s = srv.GetServer()->LookupAndCreateCommand("rpop", &cmd_rpop);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_lrem;
  s = srv.GetServer()->LookupAndCreateCommand("lrem", &cmd_lrem);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_ltrim;
  s = srv.GetServer()->LookupAndCreateCommand("ltrim", &cmd_ltrim);
  ASSERT_TRUE(s.IsOK());
  // create set cmds
  std::unique_ptr<Commander> cmd_sadd;
  s = srv.GetServer()->LookupAndCreateCommand("sadd", &cmd_sadd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_srem;
  s = srv.GetServer()->LookupAndCreateCommand("srem", &cmd_srem);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_spop;
  s = srv.GetServer()->LookupAndCreateCommand("spop", &cmd_spop);
  ASSERT_TRUE(s.IsOK());
  // create zset cmds
  std::unique_ptr<Commander> cmd_zadd;
  s = srv.GetServer()->LookupAndCreateCommand("zadd", &cmd_zadd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_zpopmax;
  s = srv.GetServer()->LookupAndCreateCommand("zpopmax", &cmd_zpopmax);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_zrem;
  s = srv.GetServer()->LookupAndCreateCommand("zrem", &cmd_zrem);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_zremrangebyscore;
  s = srv.GetServer()->LookupAndCreateCommand("zremrangebyscore", &cmd_zremrangebyscore);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_zremrangebyrank;
  s = srv.GetServer()->LookupAndCreateCommand("zremrangebyrank", &cmd_zremrangebyrank);
  ASSERT_TRUE(s.IsOK());
  // create common cmds
  std::unique_ptr<Commander> cmd_del;
  s = srv.GetServer()->LookupAndCreateCommand("del", &cmd_del);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_expire;
  s = srv.GetServer()->LookupAndCreateCommand("expire", &cmd_expire);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_pexpire;
  s = srv.GetServer()->LookupAndCreateCommand("pexpire", &cmd_pexpire);
  ASSERT_TRUE(s.IsOK());
  // script cmd
  std::unique_ptr<Commander> cmd_script;
  s = srv.GetServer()->LookupAndCreateCommand("script", &cmd_script);
  ASSERT_TRUE(s.IsOK());

  // next sequence
  uint64_t next_seq = 0;

  // Test string type
  {
    // construct commands tokens
    CommandTokens mset_cmd_token{"mset", "string_key1", "mset_val1", "string_key2", "mset_val2"};
    CommandTokens setnx_cmd_token{"setnx", "string_key", "setnx_val"};
    CommandTokens setex_cmd_token{"setex", "string_key", "10", "setex_val"};
    CommandTokens incr_cmd_token{"incr", "string_key_num"};
    CommandTokens incrby_cmd_token{"incrby", "string_key_num", "10"};
    CommandTokens incrbyfloat_cmd_token{"incrbyfloat", "string_key_float", "10.0"};
    CommandTokens decr_cmd_token{"decr", "string_key_num"};
    CommandTokens decrby_cmd_token{"decrby", "string_key_num", "4"};

    // exec cmds
    std::string output;
    s = GenericExecCmd(mset_cmd_token, &output, cmd_mset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(setnx_cmd_token, &output, cmd_setnx, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(setex_cmd_token, &output, cmd_setex, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(incr_cmd_token, &output, cmd_incr, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(incrby_cmd_token, &output, cmd_incrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(incrbyfloat_cmd_token, &output, cmd_incrbyfloat, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(decr_cmd_token, &output, cmd_decr, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(decrby_cmd_token, &output, cmd_decrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_cmds.size(), 8);
    EXPECT_EQ(output_cmds[0],
              std::vector<std::string>({"MSET", "string_key1", "mset_val1", "string_key2", "mset_val2"}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"SET", "string_key", "setnx_val"}));
    EXPECT_EQ(output_cmds[2][0], "SET");
    EXPECT_EQ(output_cmds[2][1], "string_key");
    EXPECT_EQ(output_cmds[2][2], "setex_val");
    EXPECT_EQ(output_cmds[2][3], "PXAT");
    EXPECT_EQ(output_cmds[3], std::vector<std::string>({"SET", "string_key_num", "1"}));
    EXPECT_EQ(output_cmds[4], std::vector<std::string>({"SET", "string_key_num", "11"}));
    EXPECT_EQ(output_cmds[5], std::vector<std::string>({"SET", "string_key_float", "10.000000"}));
    EXPECT_EQ(output_cmds[6], std::vector<std::string>({"SET", "string_key_num", "10"}));
    EXPECT_EQ(output_cmds[7], std::vector<std::string>({"SET", "string_key_num", "6"}));
  }

  // Test hash type
  {
    // construct commands tokens
    CommandTokens hset_cmd_token{"HSET", "hash_key", "f1", "v1", "f2", "v2"};
    CommandTokens hmset_cmd_token{"HMSET", "hash_key", "f1", "11", "f2", "22"};
    CommandTokens hsetnx_cmd_token{"HSETNX", "hash_key", "f3", "v3"};
    CommandTokens hset_cmd_token_repeated{"HSET", "hash_key", "f1", "11"};
    CommandTokens hsetnx_cmd_token_exists{"HSETNX", "hash_key", "f3", "333"};
    CommandTokens hincrby_cmd_token{"HINCRBY", "hash_key", "f1", "10"};
    CommandTokens hincrbyfloat_cmd_token{"HINCRBYFLOAT", "hash_key", "f1", "10.0"};
    CommandTokens hdel_cmd_token{"HDEL", "hash_key", "f1"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hmset_cmd_token, &output, cmd_hmset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hsetnx_cmd_token, &output, cmd_hsetnx, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hset_cmd_token_repeated, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hsetnx_cmd_token_exists, &output, cmd_hsetnx, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hincrby_cmd_token, &output, cmd_hincrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hincrbyfloat_cmd_token, &output, cmd_hincrbyfloat, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hdel_cmd_token, &output, cmd_hdel, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_cmds.size(), 6);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"HSET", "hash_key", "f2", "v2", "f1", "v1"}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"HSET", "hash_key", "f2", "22", "f1", "11"}));
    EXPECT_EQ(output_cmds[2], std::vector<std::string>({"HSET", "hash_key", "f3", "v3"}));
    EXPECT_EQ(output_cmds[3], std::vector<std::string>({"HSET", "hash_key", "f1", "21"}));
    EXPECT_EQ(output_cmds[4], std::vector<std::string>({"HSET", "hash_key", "f1", "31.000000"}));
    EXPECT_EQ(output_cmds[5], std::vector<std::string>({"HDEL", "hash_key", "f1"}));
  }

  // Test list type
  {
    // construct commands tokens
    CommandTokens lpush_cmd_token{"LPUSH", "list_key", "1", "2", "3"};            // list: 3 2 1
    CommandTokens rpush_cmd_token{"RPUSH", "list_key", "a", "b", "c"};            // list: 3 2 1 a b c
    CommandTokens lset_cmd_token{"LSET", "list_key", "0", "10"};                  // list: 10 2 1 a b c
    CommandTokens linsert_cmd_token1{"LINSERT", "list_key", "BEFORE", "a", "0"};  // list: 10 2 1 0 a b c
    CommandTokens linsert_cmd_token2{"LINSERT", "list_key", "AFTER", "a", "0"};   // list: 10 2 1 0 a 0 b c
    CommandTokens lpop_cmd_token{"LPOP", "list_key"};                             // list: 2 1 0 a 0 b c
    CommandTokens rpop_cmd_token{"RPOP", "list_key"};                             // list: 2 1 0 a 0 b
    CommandTokens lrem_cmd_token{"LREM", "list_key", "2", "0"};                   // list: 2 1 a b
    CommandTokens ltrim_cmd_token{"LTRIM", "list_key", "1", "2"};                 // list: 1 a
    // exec cmds
    std::string output;
    s = GenericExecCmd(lpush_cmd_token, &output, cmd_lpush, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(rpush_cmd_token, &output, cmd_rpush, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(lset_cmd_token, &output, cmd_lset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(linsert_cmd_token1, &output, cmd_linsert, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(linsert_cmd_token2, &output, cmd_linsert, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(lpop_cmd_token, &output, cmd_lpop, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(rpop_cmd_token, &output, cmd_rpop, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(lrem_cmd_token, &output, cmd_lrem, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(ltrim_cmd_token, &output, cmd_ltrim, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_cmds.size(), 9);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"LPUSH", "list_key", "1", "2", "3"}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"RPUSH", "list_key", "a", "b", "c"}));
    EXPECT_EQ(output_cmds[2], std::vector<std::string>({"LSET", "list_key", "0", "10"}));
    EXPECT_EQ(output_cmds[3], std::vector<std::string>({"LINSERT", "list_key", "BEFORE", "a", "0"}));
    EXPECT_EQ(output_cmds[4], std::vector<std::string>({"LINSERT", "list_key", "AFTER", "a", "0"}));
    EXPECT_EQ(output_cmds[5], std::vector<std::string>({"LPOP", "list_key"}));
    EXPECT_EQ(output_cmds[6], std::vector<std::string>({"RPOP", "list_key"}));
    EXPECT_EQ(output_cmds[7], std::vector<std::string>({"LREM", "list_key", "2", "0"}));
    EXPECT_EQ(output_cmds[8], std::vector<std::string>({"LTRIM", "list_key", "1", "2"}));
  }

  // Test set type
  {
    // construct commands tokens
    CommandTokens sadd_cmd_token{"SADD", "set_key", "1", "v2", "x3"};
    CommandTokens srem_cmd_token{"SREM", "set_key", "x3"};
    CommandTokens spop_cmd_token{"SPOP", "set_key", "1"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(sadd_cmd_token, &output, cmd_sadd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(srem_cmd_token, &output, cmd_srem, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(spop_cmd_token, &output, cmd_spop, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_cmds.size(), 3);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"SADD", "set_key", "1", "v2", "x3"}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"SREM", "set_key", "x3"}));
    EXPECT_EQ(output_cmds[2], std::vector<std::string>({"SREM", "set_key", "1"}));
  }

  // Test zset type
  {
    // construct commands tokens
    CommandTokens zadd_cmd_token{"ZADD", "zset_key", "1.5", "m1",  "2.512132432", "m2", "3.1415926535897932",
                                 "m3",   "4.5",      "m4",  "5.5", "m5"};
    CommandTokens zpopmax_cmd_token{"ZPOPMAX", "zset_key", "1"};
    CommandTokens zrem_cmd_token{"ZREM", "zset_key", "m3"};
    CommandTokens zremrangebyscore_cmd_token{"ZREMRANGEBYSCORE", "zset_key", "2", "3"};
    CommandTokens zremrangebyrank_cmd_token{"ZREMRANGEBYRANK", "zset_key", "0", "0"};

    // exec cmds
    std::string output;
    s = GenericExecCmd(zadd_cmd_token, &output, cmd_zadd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(zpopmax_cmd_token, &output, cmd_zpopmax, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(zrem_cmd_token, &output, cmd_zrem, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(zremrangebyscore_cmd_token, &output, cmd_zremrangebyscore, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(zremrangebyrank_cmd_token, &output, cmd_zremrangebyrank, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_cmds.size(), 5);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"ZADD", "zset_key", "5.5", "m5", "4.5", "m4",
                                                        "3.1415926535897931", "m3", "2.512132432", "m2", "1.5", "m1"}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"ZREM", "zset_key", "m5"}));
    EXPECT_EQ(output_cmds[2], std::vector<std::string>({"ZREM", "zset_key", "m3"}));
    EXPECT_EQ(output_cmds[3], std::vector<std::string>({"ZREM", "zset_key", "m2"}));
    EXPECT_EQ(output_cmds[4], std::vector<std::string>({"ZREM", "zset_key", "m1"}));
  }

  // Test common type
  {
    // construct commands tokens
    CommandTokens mset_cmd_token{"MSET", "com_string_key", "cmd_str_val", "com_string_key2", "cmd_str_val2"};
    CommandTokens hset_cmd_token{"HSET", "com_hash_key", "f1", "v1", "f2", "v2", "f3", "v3"};
    CommandTokens ex_str_cmd_token{"EXPIRE", "com_string_key", "1000"};
    CommandTokens pex_str_cmd_token{"PEXPIRE", "com_string_key", "2000000"};
    CommandTokens ex_hash_cmd_token{"EXPIRE", "com_hash_key", "1000"};
    CommandTokens pex_hash_cmd_token{"PEXPIRE", "com_hash_key", "2000000"};
    CommandTokens del_cmd_token{"DEL", "com_string_key", "com_hash_key"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(mset_cmd_token, &output, cmd_mset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(ex_str_cmd_token, &output, cmd_expire, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(pex_str_cmd_token, &output, cmd_pexpire, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(ex_hash_cmd_token, &output, cmd_expire, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(pex_hash_cmd_token, &output, cmd_pexpire, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(del_cmd_token, &output, cmd_del, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_cmds.size(), 7);
    EXPECT_EQ(output_cmds[0],
              std::vector<std::string>({"MSET", "com_string_key", "cmd_str_val", "com_string_key2", "cmd_str_val2"}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"HSET", "com_hash_key", "f3", "v3", "f2", "v2", "f1", "v1"}));
    EXPECT_EQ(output_cmds[2][0], std::string("PEXPIREAT"));
    EXPECT_EQ(output_cmds[2][1], std::string("com_string_key"));
    EXPECT_EQ(output_cmds[3][0], std::string("PEXPIREAT"));
    EXPECT_EQ(output_cmds[3][1], std::string("com_string_key"));
    EXPECT_EQ(output_cmds[4][0], std::string("PEXPIREAT"));
    EXPECT_EQ(output_cmds[4][1], std::string("com_hash_key"));
    EXPECT_EQ(output_cmds[5][0], std::string("PEXPIREAT"));
    EXPECT_EQ(output_cmds[5][1], std::string("com_hash_key"));
    EXPECT_EQ(output_cmds[6], std::vector<std::string>({"DEL", "com_string_key", "com_hash_key"}));
  }

  // Test script
  {
    uint64_t cur_seq = next_seq;
    // construct commands tokens
    CommandTokens script_cmd_token{"SCRIPT", "LOAD", "return redis.call('get', 'key')"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(script_cmd_token, &output, cmd_script, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_cmds.size(), 0);
    EXPECT_GT(next_seq, cur_seq);
  }

  // Test replid
  {
    uint64_t cur_seq = next_seq;
    // shift replId
    s = storage->ShiftReplId();
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_cmds.size(), 0);
    EXPECT_GT(next_seq, cur_seq);
  }
}

// TODO:(mingfo) Test with and without CDC after CDC is implemented for httl
// Test all cmds with enable-hfe-cmd yes
TEST(BatchExtractorTest, HttlKKVAllCmdTest) {
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
  // TODO: Test with and without cdc after cdc is implemented for httl
  storage->GetConfig()->enable_cdc_sync = false;

  // create config cmd
  std::unique_ptr<Commander> cmd_conf;
  auto s = srv.GetServer()->LookupAndCreateCommand("config", &cmd_conf);
  ASSERT_TRUE(s.IsOK());
  // create hash cmds
  std::unique_ptr<Commander> cmd_hset;
  s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd_hset);
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
  std::unique_ptr<Commander> cmd_hmset;
  s = srv.GetServer()->LookupAndCreateCommand("hmset", &cmd_hmset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hdel;
  s = srv.GetServer()->LookupAndCreateCommand("hdel", &cmd_hdel);
  ASSERT_TRUE(s.IsOK());
  // create hash httl cmds
  std::unique_ptr<Commander> cmd_hexpire;
  s = srv.GetServer()->LookupAndCreateCommand("hexpire", &cmd_hexpire);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hexpireat;
  s = srv.GetServer()->LookupAndCreateCommand("hexpireat", &cmd_hexpireat);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hpexpire;
  s = srv.GetServer()->LookupAndCreateCommand("hpexpire", &cmd_hpexpire);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hpexpireat;
  s = srv.GetServer()->LookupAndCreateCommand("hpexpireat", &cmd_hpexpireat);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hpersist;
  s = srv.GetServer()->LookupAndCreateCommand("hpersist", &cmd_hpersist);
  ASSERT_TRUE(s.IsOK());
  // create kkv cmds
  std::unique_ptr<Commander> cmd_kkvhset;
  s = srv.GetServer()->LookupAndCreateCommand("kkvhset", &cmd_kkvhset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_kkvhsetnx;
  s = srv.GetServer()->LookupAndCreateCommand("kkvhsetnx", &cmd_kkvhsetnx);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_kkvhcas;
  s = srv.GetServer()->LookupAndCreateCommand("kkvhcas", &cmd_kkvhcas);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_kkvhcad;
  s = srv.GetServer()->LookupAndCreateCommand("kkvhcad", &cmd_kkvhcad);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_kkvhremrangebylex;
  s = srv.GetServer()->LookupAndCreateCommand("kkvhremrangebylex", &cmd_kkvhremrangebylex);
  ASSERT_TRUE(s.IsOK());

  // next sequence
  uint64_t next_seq = 0;

  // config set enable-hfe-cmd
  {
    // construct commands tokens
    CommandTokens config_set_token{"CONFIG", "SET", "enable-hfe-cmd", "yes"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(config_set_token, &output, cmd_conf, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
  }

  // Test hash cmds
  {
    // construct commands tokens
    CommandTokens hset_cmd_token{"HSET", "hash_key", "f1", "v1", "f2", "v2"};
    CommandTokens hsetnx_cmd_token{"HSETNX", "hash_key", "f3", "v3"};
    CommandTokens hincrby_cmd_token{"HINCRBY", "hash_key", "f4", "100"};
    CommandTokens hincrbyfloat_cmd_token{"HINCRBYFLOAT", "hash_key", "f4", "0.5"};
    CommandTokens hmset_cmd_token{"HMSET", "hash_key", "f5", "v5"};
    CommandTokens hdel_cmd_token{"HDEL", "hash_key", "f1", "f2"};

    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hsetnx_cmd_token, &output, cmd_hsetnx, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hincrby_cmd_token, &output, cmd_hincrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hincrbyfloat_cmd_token, &output, cmd_hincrbyfloat, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hmset_cmd_token, &output, cmd_hmset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hdel_cmd_token, &output, cmd_hdel, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_cmds.size(), 6);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"HSET", "hash_key", "f2", "v2", "f1", "v1"}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"HSET", "hash_key", "f3", "v3"}));
    EXPECT_EQ(output_cmds[2], std::vector<std::string>({"HSET", "hash_key", "f4", "100"}));
    EXPECT_EQ(output_cmds[3], std::vector<std::string>({"HSET", "hash_key", "f4", "100.500000"}));
    EXPECT_EQ(output_cmds[4], std::vector<std::string>({"HSET", "hash_key", "f5", "v5"}));
    EXPECT_EQ(output_cmds[5], std::vector<std::string>({"HDEL", "hash_key", "f1", "f2"}));
  }

  // Test hash httl cmds
  {
    // construct commands tokens
    CommandTokens hset_cmd_token{"HSET", "hash_key", "f1", "v1", "f2", "v2"};
    auto cur_secs = util::GetTimeStamp();
    auto cur_msecs = util::GetTimeStampMS();
    uint64_t min_exat = cur_msecs + 10000;
    uint64_t max_exat = min_exat + 1000;
    CommandTokens hexpire_cmd_token{"HEXPIRE", "hash_key", "10", "FIELDS", "2", "f4", "f5"};
    CommandTokens hexpireat_cmd_token{"HEXPIREAT", "hash_key", std::to_string(cur_secs + 10), "FIELDS", "1", "f3"};
    CommandTokens hpexpire_cmd_token{"HPEXPIRE", "hash_key", "10000", "FIELDS", "1", "f2"};
    CommandTokens hpexpireat_cmd_token{"HPEXPIREAT", "hash_key", std::to_string(min_exat), "FIELDS", "1", "f1"};
    CommandTokens hpersist_cmd_token{"HPERSIST", "hash_key", "FIELDS", "5", "f1", "f2", "f3", "f4", "f5"};

    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hexpire_cmd_token, &output, cmd_hexpire, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hexpireat_cmd_token, &output, cmd_hexpireat, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hpexpire_cmd_token, &output, cmd_hpexpire, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hpexpireat_cmd_token, &output, cmd_hpexpireat, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hpersist_cmd_token, &output, cmd_hpersist, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_cmds.size(), 6);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"HSET", "hash_key", "f2", "v2", "f1", "v1"}));

    EXPECT_EQ(output_cmds[1].size(), 7);
    uint64_t exat = *ParseInt<uint64_t>(output_cmds[1][2], 10);
    EXPECT_TRUE(exat >= min_exat && exat < max_exat);
    EXPECT_EQ(output_cmds[1],
              std::vector<std::string>({"HPEXPIREAT", "hash_key", output_cmds[1][2], "FIELDS", "2", "f4", "f5"}));

    EXPECT_EQ(output_cmds[2].size(), 6);
    exat = *ParseInt<uint64_t>(output_cmds[2][2], 10);
    EXPECT_TRUE(exat >= static_cast<uint64_t>((cur_secs + 10) * 1000) && exat < max_exat);
    EXPECT_EQ(output_cmds[2],
              std::vector<std::string>({"HPEXPIREAT", "hash_key", output_cmds[2][2], "FIELDS", "1", "f3"}));

    EXPECT_EQ(output_cmds[3].size(), 6);
    exat = *ParseInt<uint64_t>(output_cmds[3][2], 10);
    EXPECT_TRUE(exat >= min_exat && exat < max_exat);
    EXPECT_EQ(output_cmds[3],
              std::vector<std::string>({"HPEXPIREAT", "hash_key", output_cmds[3][2], "FIELDS", "1", "f2"}));

    EXPECT_EQ(output_cmds[4].size(), 6);
    exat = *ParseInt<uint64_t>(output_cmds[4][2], 10);
    EXPECT_TRUE(exat >= min_exat && exat < max_exat);
    EXPECT_EQ(output_cmds[4],
              std::vector<std::string>({"HPEXPIREAT", "hash_key", output_cmds[4][2], "FIELDS", "1", "f1"}));

    EXPECT_EQ(output_cmds[5],
              std::vector<std::string>({"HPERSIST", "hash_key", "FIELDS", "5", "f1", "f2", "f3", "f4", "f5"}));
  }

  // Test data expired while parsed into HSET
  {
    // construct commands tokens
    CommandTokens hset_cmd_token{"HSET", "hash_key", "f10", "10", "f11", "11"};
    auto exat = util::GetTimeStampMS() + 300;
    CommandTokens hpexpireat_cmd_token{"HPEXPIREAT", "hash_key", std::to_string(exat), "FIELDS", "1", "f10"};
    CommandTokens hincrby_cmd_token1{"HINCRBY", "hash_key", "f10", "10"};
    CommandTokens hincrby_cmd_token2{"HINCRBY", "hash_key", "f11", "11"};

    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hpexpireat_cmd_token, &output, cmd_hpexpireat, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hincrby_cmd_token1, &output, cmd_hincrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hincrby_cmd_token2, &output, cmd_hincrby, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    // sleep 500ms
    std::this_thread::sleep_for(std::chrono::milliseconds(350));

    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_cmds.size(), 3);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"HSET", "hash_key", "f11", "11", "f10", "10"}));
    EXPECT_EQ(output_cmds[1], hpexpireat_cmd_token);
    EXPECT_EQ(output_cmds[2], std::vector<std::string>({"HSET", "hash_key", "f11", "22"}));
  }

  // config set enable-hfe-cmd
  {
    // construct commands tokens
    CommandTokens config_set_token{"CONFIG", "SET", "enable-kkv-cmd", "yes"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(config_set_token, &output, cmd_conf, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
  }

  // Test kkv cmds
  {
    // construct commands tokens
    std::string kkv_key{"kkv_key"};
    auto pexat = util::GetTimeStampMS() + 10000;  // ttl 10s
    auto pexat_str = std::to_string(pexat);
    CommandTokens hset_cmd_token = {"HSET", kkv_key, "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"};  // set fields
    CommandTokens kkvhset_cmd_token{"KKVHSET", kkv_key,   "k1", "v1",  "PXAT", pexat_str, "k2", "vv2",
                                    "PXAT",    pexat_str, "k3", "vv3", "PXAT", "123456"};    // set k1,k2 ttl and del k3
    CommandTokens kkvhsetnx_cmd_token{"KKVHSETNX", kkv_key, "k5", "v5", "PXAT", pexat_str};  // set k5 with ttl
    CommandTokens kkvhcas_cmd_token1{"KKVHCAS", kkv_key, "k5", "v5", "vv5", "PERSIST"};      // set k5
    CommandTokens kkvhcas_cmd_token2{"KKVHCAS", kkv_key, "k5", "vv5", "v5", "PXAT", "123456"};  // del k5
    CommandTokens kkvhcad_cmd_token{"KKVHCAD", kkv_key, "k4", "v4"};                            // del k4
    CommandTokens kkvhremrangebylex_cmd_token{"KKVHREMRANGEBYLEX", kkv_key, "[k1", "(k3"};

    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(kkvhset_cmd_token, &output, cmd_kkvhset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(kkvhsetnx_cmd_token, &output, cmd_kkvhsetnx, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(kkvhcas_cmd_token1, &output, cmd_kkvhcas, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(kkvhcas_cmd_token2, &output, cmd_kkvhcas, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(kkvhcad_cmd_token, &output, cmd_kkvhcad, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(kkvhremrangebylex_cmd_token, &output, cmd_kkvhremrangebylex, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_cmds.size(), 8);
    EXPECT_EQ(output_cmds[0],
              std::vector<std::string>({"HSET", kkv_key, "k4", "v4", "k3", "v3", "k2", "v2", "k1", "v1"}));

    // KKVHSET is parsed into KKVHSET && HDEL
    // KKVHSET kkv_key k2 vv2 PXAT pexat_str k1 vv1 PXAT pexat_str
    // HDEL kkv_key k3
    EXPECT_EQ(output_cmds[1], std::vector<std::string>(
                                  {"KKVHSET", kkv_key, "k2", "vv2", "PXAT", pexat_str, "k1", "v1", "PXAT", pexat_str}));
    EXPECT_EQ(output_cmds[2], std::vector<std::string>({"HDEL", kkv_key, "k3"}));
    EXPECT_EQ(output_cmds[3], std::vector<std::string>({"KKVHSETNX", kkv_key, "k5", "v5", "PXAT", pexat_str}));
    EXPECT_EQ(output_cmds[4], std::vector<std::string>({"KKVHSET", kkv_key, "k5", "vv5", "PERSIST"}));
    EXPECT_EQ(output_cmds[5], std::vector<std::string>({"HDEL", kkv_key, "k5"}));
    EXPECT_EQ(output_cmds[6], std::vector<std::string>({"HDEL", kkv_key, "k4"}));
    EXPECT_EQ(output_cmds[7], std::vector<std::string>({"KKVHREMRANGEBYLEX", kkv_key, "[k1", "(k3"}));
  }

  // Test expired fields will be parsed in KKVHSET
  {
    // construct commands tokens
    std::string kkv_key{"kkv_key1"};
    auto pexat = util::GetTimeStampMS() + 10000;
    auto pexat_str = std::to_string(pexat);
    CommandTokens kkvhset_cmd_token1{"KKVHSET", kkv_key, "k1", "v1", "PXAT", pexat_str, "k2", "v2", "PX", "300"};
    CommandTokens kkvhset_cmd_token2{"KKVHSET", kkv_key, "k1", "v1", "PERSIST"};

    // exec cmds
    std::string output;
    s = GenericExecCmd(kkvhset_cmd_token1, &output, cmd_kkvhset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(kkvhset_cmd_token2, &output, cmd_kkvhset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    // sleep 350ms
    std::this_thread::sleep_for(std::chrono::milliseconds(350));

    // parse wal
    std::vector<CommandTokens> output_cmds;
    s = ParseWAL(storage, &next_seq, &output_cmds);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_cmds.size(), 2);
    EXPECT_EQ(output_cmds[0], std::vector<std::string>({"KKVHSET", kkv_key, "k2", "v2", "PXAT", output_cmds[0][5], "k1",
                                                        "v1", "PXAT", pexat_str}));
    EXPECT_EQ(output_cmds[1], std::vector<std::string>({"KKVHSET", kkv_key, "k1", "v1", "PERSIST"}));
  }
}

INSTANTIATE_TEST_SUITE_P(CDCSyncTests, BatchExtractorTest, ::testing::Values(false, true));

}  // namespace redis
