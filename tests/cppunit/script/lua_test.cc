#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "script/script_util.h"
#include "server/redis_connection.h"

namespace redis {

std::vector<std::string> GetKeysInSameSlot(int16_t slotid, int key_count) {
  std::vector<std::string> keys;
  std::string hash_tag(global_slot_keys[slotid]);
  auto prefix = "{" + hash_tag + "}_";
  for (int i = 0; i < key_count; i++) {
    auto key = prefix + std::to_string(i);
    keys.emplace_back(key);
  }
  return keys;
}

Status ExecuteCmd(const std::vector<std::string> &cmd_tokens, std::string *output, std::unique_ptr<Commander> &cmd,
                  Server *srv, Connection *conn, engine::Storage *storage) {
  output->clear();

  // test keys in same slot
  auto attr = cmd->GetAttributes();
  auto first_key = attr->GetKeyRange(cmd_tokens).first_key;
  if (first_key != 0) {
    auto s = conn->CheckKeysInSameSlot(attr, cmd_tokens);
    if (!s.IsOK()) {
      LOG(WARNING) << "[Lua_test] Err: " << s.Msg();
      return s;
    }
  }

  cmd->SetArgs(cmd_tokens);
  auto s = cmd->Parse();
  if (!s.IsOK()) {
    LOG(WARNING) << "[Lua_test] Err: " << s.Msg();
    return s;
  }
  return cmd->Execute(srv, conn, output, storage);
}

TEST(ScriptTest, LuaCommandTest) {
  MockOptions opt;
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
  ASSERT_TRUE(s.IsOK());
  // create conn
  auto worker0 = srv.GetWorkerThreads()[0]->GetWorker();
  ASSERT_TRUE(worker0 != nullptr);
  redis::Connection conn{nullptr, worker0};

  std::string lua_mset{"return redis.call('mset', KEYS[1], ARGV[1], KEYS[2], ARGV[2])"};
  std::string lua_mget{"return redis.call('mget', KEYS[1], KEYS[2])"};
  char sha[41];
  lua::SHA1Hex(sha, lua_mset.data(), lua_mset.length());
  std::string lua_mset_sha(std::begin(sha), std::end(sha) - 1);
  lua::SHA1Hex(sha, lua_mget.data(), lua_mget.length());
  std::string lua_mget_sha(std::begin(sha), std::end(sha) - 1);

  // create cmds
  std::unique_ptr<Commander> eval_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("EVAL", &eval_cmd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> evalsha_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("EVALSHA", &evalsha_cmd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> script_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("SCRIPT", &script_cmd);
  ASSERT_TRUE(s.IsOK());

  // get storages
  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  auto storage1 = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id + 1).get();

  auto srv_ptr = srv.GetServer().get();

  std::string output;

  // Test cmd EVAL
  {
    // create cmd
    std::vector<std::string> keys = GetKeysInSameSlot(0, 2);
    std::vector<std::string> vals = keys;

    std::vector<std::string> lua_mset_cmd{"eval"};
    lua_mset_cmd.emplace_back(lua_mset);
    lua_mset_cmd.emplace_back(std::to_string(keys.size()));
    lua_mset_cmd.insert(lua_mset_cmd.end(), keys.begin(), keys.end());
    lua_mset_cmd.insert(lua_mset_cmd.end(), vals.begin(), vals.end());

    std::vector<std::string> lua_mget_cmd{"eval"};
    lua_mget_cmd.emplace_back(lua_mget);
    lua_mget_cmd.emplace_back(std::to_string(keys.size()));
    lua_mget_cmd.insert(lua_mget_cmd.end(), keys.begin(), keys.end());

    // exec eval mset
    {
      s = ExecuteCmd(lua_mset_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(output, redis::BulkString("OK"));
    }

    // exec eval mget
    {
      s = ExecuteCmd(lua_mget_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(output, redis::MultiBulkString(vals));
    }
  }

  // Test cmd SCRIPT
  {
    // script exists
    std::vector<std::string> cmd_script_exists{"script", "exists", lua_mset_sha, lua_mget_sha};
    // script load
    std::vector<std::string> cmd_script_load_mset{"script", "load", lua_mset};
    std::vector<std::string> cmd_script_load_mget{"script", "load", lua_mget};
    // script flush
    std::vector<std::string> cmd_script_flush{"script", "flush"};

    // check scripts not existing
    {
      s = ExecuteCmd(cmd_script_exists, &output, script_cmd, srv_ptr, &conn, nullptr);
      ASSERT_TRUE(s.IsOK());
      std::string expect_output = redis::MultiLen(2) + redis::Integer(0) + redis::Integer(0);
      EXPECT_EQ(output, expect_output);
    }

    // load scripts
    {
      // load mset
      s = ExecuteCmd(cmd_script_load_mset, &output, script_cmd, srv_ptr, &conn, nullptr);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(redis::BulkString(lua_mset_sha), output);

      // load mget
      s = ExecuteCmd(cmd_script_load_mget, &output, script_cmd, srv_ptr, &conn, nullptr);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(redis::BulkString(lua_mget_sha), output);

      // check load success
      s = ExecuteCmd(cmd_script_exists, &output, script_cmd, srv_ptr, &conn, nullptr);
      ASSERT_TRUE(s.IsOK());
      std::string expect_output = redis::MultiLen(2) + redis::Integer(1) + redis::Integer(1);
      EXPECT_EQ(output, expect_output);
    }

    // flush scripts
    {
      s = ExecuteCmd(cmd_script_flush, &output, script_cmd, srv_ptr, &conn, nullptr);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(redis::SimpleString("OK"), output);

      // check flushed by exists
      s = ExecuteCmd(cmd_script_exists, &output, script_cmd, srv_ptr, &conn, nullptr);
      ASSERT_TRUE(s.IsOK());
      std::string expect_output = redis::MultiLen(2) + redis::Integer(0) + redis::Integer(0);
      EXPECT_EQ(output, expect_output);
    }
  }

  // Test cmd EVALSHA and SCRIPT
  {
    // load script first
    std::vector<std::string> cmd_script_load_mset{"script", "load", lua_mset};
    std::vector<std::string> cmd_script_load_mget{"script", "load", lua_mget};
    // load script mset/mget
    s = ExecuteCmd(cmd_script_load_mset, &output, script_cmd, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    s = ExecuteCmd(cmd_script_load_mget, &output, script_cmd, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());

    // create cmd evalsha
    std::vector<std::string> keys = GetKeysInSameSlot(8191, 2);
    std::vector<std::string> vals = keys;

    std::vector<std::string> lua_mset_cmd_sha{"evalsha"};
    lua_mset_cmd_sha.emplace_back(lua_mset_sha);
    lua_mset_cmd_sha.emplace_back(std::to_string(keys.size()));
    lua_mset_cmd_sha.insert(lua_mset_cmd_sha.end(), keys.begin(), keys.end());
    lua_mset_cmd_sha.insert(lua_mset_cmd_sha.end(), vals.begin(), vals.end());

    std::vector<std::string> lua_mget_cmd_sha{"evalsha"};
    lua_mget_cmd_sha.emplace_back(lua_mget_sha);
    lua_mget_cmd_sha.emplace_back(std::to_string(keys.size()));
    lua_mget_cmd_sha.insert(lua_mget_cmd_sha.end(), keys.begin(), keys.end());

    // exec on storage test_db_id
    {
      // evalsha mset
      s = ExecuteCmd(lua_mset_cmd_sha, &output, evalsha_cmd, srv_ptr, &conn, storage);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(output, redis::BulkString("OK"));

      // evalsha mget
      s = ExecuteCmd(lua_mget_cmd_sha, &output, evalsha_cmd, srv_ptr, &conn, storage);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(output, redis::MultiBulkString(vals));
    }
    // exec on storage test_db_id+1
    {
      std::vector<std::string> keys1 = GetKeysInSameSlot(8192, 2);
      std::vector<std::string> vals1 = keys1;

      std::vector<std::string> lua_mset_cmd_sha1{"evalsha", lua_mset_sha};
      lua_mset_cmd_sha1.emplace_back(std::to_string(keys1.size()));
      lua_mset_cmd_sha1.insert(lua_mset_cmd_sha1.end(), keys1.begin(), keys1.end());
      lua_mset_cmd_sha1.insert(lua_mset_cmd_sha1.end(), vals1.begin(), vals1.end());

      std::vector<std::string> lua_mget_cmd_sha1{"evalsha"};
      lua_mget_cmd_sha1.emplace_back(lua_mget_sha);
      lua_mget_cmd_sha1.emplace_back(std::to_string(keys1.size()));
      lua_mget_cmd_sha1.insert(lua_mget_cmd_sha1.end(), keys1.begin(), keys1.end());

      // evalsha mset
      s = ExecuteCmd(lua_mset_cmd_sha1, &output, evalsha_cmd, srv_ptr, &conn, storage1);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(output, redis::BulkString("OK"));

      // evalsha mget
      s = ExecuteCmd(lua_mget_cmd_sha1, &output, evalsha_cmd, srv_ptr, &conn, storage1);
      ASSERT_TRUE(s.IsOK());
      EXPECT_EQ(output, redis::MultiBulkString(vals1));
    }
  }

  // Test script will be cached by EVAL in same thread
  {
    std::string key{global_slot_keys[5]};
    std::string val = key;
    std::string lua_set{"return redis.call('mset', KEYS[1], ARGV[1])"};
    std::string lua_get{"return redis.call('mget', KEYS[1])"};

    char sha[41];
    lua::SHA1Hex(sha, lua_get.data(), lua_get.length());
    std::string lua_set_sha(std::begin(sha), std::end(sha) - 1);

    // check sha not existing
    std::vector<std::string> script_exists_cmd{"SCRIPT", "EXISTS", sha};
    s = ExecuteCmd(script_exists_cmd, &output, script_cmd, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    std::string expect_output = redis::MultiLen(1) + redis::Integer(0);
    EXPECT_EQ(output, expect_output);

    // exec eval set
    std::vector<std::string> lua_set_cmd{"EVAL", lua_set, "1", key, val};
    s = ExecuteCmd(lua_set_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::BulkString("OK"));

    // exec eval get
    std::vector<std::string> lua_get_cmd{"EVAL", lua_get, "1", key};
    s = ExecuteCmd(lua_get_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::MultiBulkString(std::vector<std::string>{val}));

    // exec evalsha get
    std::vector<std::string> lua_get_cmd_sha{"EVALSHA", sha, "1", key};
    s = ExecuteCmd(lua_get_cmd_sha, &output, evalsha_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::MultiBulkString(std::vector<std::string>{val}));
    // exec failed in other thread
    std::thread t([&evalsha_cmd, &lua_get_cmd_sha, &srv, &conn, &storage]() {
      std::string output;
      auto s = ExecuteCmd(lua_get_cmd_sha, &output, evalsha_cmd, srv.GetServer().get(), &conn, storage);
      ASSERT_FALSE(s.IsOK());
      EXPECT_TRUE(s.Msg().find("NOSCRIPT") != std::string::npos);
    });
    t.join();
  }

  // Test keys both in parameters and script
  {
    std::string key1{global_slot_keys[5]};
    std::string key2{global_slot_keys[6]};
    std::string key1_tag = "{" + key1 + "}";
    std::vector<std::string> lua_set_cmd{"mset", key1, "val1"};
    std::string lua_script1 = fmt::format("redis.call('mget', {}) return redis.call('mget', KEYS[1])", key1);
    std::string lua_script2 = fmt::format("redis.call('mget', KEYS[1]) return redis.call('mget', {})", key1);
    std::string lua_script3 = fmt::format("redis.call('mget', '{}_2') return redis.call('mget', KEYS[1])", key1_tag);
    std::string lua_script4 = fmt::format("redis.call('mget', KEYS[1]) return redis.call('mget', KEYS[2])");

    // create cmds
    std::unique_ptr<Commander> mset_cmd;
    s = srv.GetServer()->LookupAndCreateCommand("MSET", &mset_cmd);
    ASSERT_TRUE(s.IsOK());

    // write keys
    s = ExecuteCmd(lua_set_cmd, &output, mset_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    // eval script1
    std::vector<std::string> lua_script_cmd1{"EVAL", lua_script1, "1", key2};
    s = ExecuteCmd(lua_script_cmd1, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("CROSSSLOT") != std::string::npos);

    // eval script2
    std::vector<std::string> lua_script_cmd2{"EVAL", lua_script2, "1", key2};
    s = ExecuteCmd(lua_script_cmd2, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("CROSSSLOT") != std::string::npos);

    // eval script3
    std::vector<std::string> lua_script_cmd3{"EVAL", lua_script3, "1", key1};
    s = ExecuteCmd(lua_script_cmd3, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("val1") != std::string::npos);

    // eval script4
    std::vector<std::string> lua_script_cmd4{"EVAL", lua_script4, "2", key1, key2};
    s = ExecuteCmd(lua_script_cmd4, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("CROSSSLOT") != std::string::npos);
  }
}

TEST(ScriptTest, CornerCaseTest) {
  MockOptions opt;
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
  ASSERT_TRUE(s.IsOK());
  // create conn
  auto worker0 = srv.GetWorkerThreads()[0]->GetWorker();
  ASSERT_TRUE(worker0 != nullptr);
  redis::Connection conn{nullptr, worker0};

  std::string lua_set{"return redis.pcall('mset', KEYS[1], ARGV[1])"};
  std::string lua_get{"return redis.pcall('mget', KEYS[1])"};

  std::unique_ptr<Commander> eval_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("EVAL", &eval_cmd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> evalsha_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("EVALSHA", &evalsha_cmd);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> script_cmd;
  s = srv.GetServer()->LookupAndCreateCommand("SCRIPT", &script_cmd);
  ASSERT_TRUE(s.IsOK());

  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  auto srv_ptr = srv.GetServer().get();

  std::string output;

  // Test redis.pcall
  {
    std::string key{global_slot_keys[5]};
    std::string val = key;
    std::vector<std::string> lua_set_cmd{"EVAL", lua_set, "1", key, val};
    std::vector<std::string> lua_get_cmd{"EVAL", lua_get, "1", key};

    s = ExecuteCmd(lua_set_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::BulkString("OK"));

    s = ExecuteCmd(lua_get_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::MultiBulkString(std::vector<std::string>{val}));
  }

  // Test EVAL cmd error
  {
    std::string key{global_slot_keys[5]};
    std::string val = key;

    // wrong num > args
    std::vector<std::string> lua_set_cmd{"EVAL", lua_set, "3", key, val};
    s = ExecuteCmd(lua_set_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("greater") != std::string::npos);

    // wrong negative num
    std::vector<std::string> lua_get_cmd{"EVAL", lua_get, "-1", key};
    s = ExecuteCmd(lua_get_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("negative") != std::string::npos);

    // args crossslot
    std::vector<std::string> lua_get_cmd_cross_slot{"EVAL", lua_get, "2", key, "key"};
    s = ExecuteCmd(lua_get_cmd_cross_slot, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("CROSSSLOT") != std::string::npos);

    // wrong arguments
    std::vector<std::string> lua_set_cmd_wrong_arg{"EVAL", lua_set, "0"};
    s = ExecuteCmd(lua_set_cmd_wrong_arg, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("strings or integers") != std::string::npos);

    // wrong sha
    std::string lua_set1{"return redis.call('mset', KEYS[1], ARGV[1])"};
    char sha[41];
    lua::SHA1Hex(sha, lua_set1.data(), lua_set1.length());
    std::string lua_set_sha(std::begin(sha), std::end(sha) - 1);
    // sha not exists
    std::vector<std::string> lua_set_cmd_sha{"EVALSHA", lua_set_sha, "1", key, val};
    s = ExecuteCmd(lua_set_cmd_sha, &output, evalsha_cmd, srv_ptr, &conn, storage);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("NOSCRIPT") != std::string::npos);

    // wrong sha length
    std::string lua_set_sha_sub = lua_set_sha.substr(0, 39);
    std::vector<std::string> lua_set_cmd_sub_sha{"EVALSHA", lua_set_sha_sub, "1", key, val};
    s = ExecuteCmd(lua_set_cmd_sub_sha, &output, evalsha_cmd, srv_ptr, &conn, storage);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("NOSCRIPT") != std::string::npos);
  }

  // Test EVAL return integer/error
  {
    std::string lua_sadd{"return redis.call('sadd', KEYS[1], ARGV[1])"};
    std::string skey{global_slot_keys[11]};
    std::string sval = skey;
    std::vector<std::string> lua_sadd_cmd{"EVAL", lua_sadd, "1", skey, sval};

    // return integer with sadd
    s = ExecuteCmd(lua_sadd_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(output, redis::Integer(1));

    // return error with get wrong type
    std::string lua_llen{"return redis.call('llen', KEYS[1])"};
    std::vector<std::string> lua_llen_cmd{"EVAL", lua_llen, "1", skey};
    s = ExecuteCmd(lua_llen_cmd, &output, eval_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("WRONGTYPE") != std::string::npos);
  }

  // Test eval/evalsha with keynum == 0
  {
    std::string script_nokey("return 'aaaa'");
    char sha[41];
    lua::SHA1Hex(sha, script_nokey.data(), script_nokey.length());
    std::string lua_script_sha(std::begin(sha), std::end(sha) - 1);
    std::vector<std::string> lua_script_cmd{"script", "load", script_nokey};
    s = ExecuteCmd(lua_script_cmd, &output, script_cmd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());

    std::vector<std::string> lua_evalsha_cmd{"evalsha", lua_script_sha, "0"};
    s = ExecuteCmd(lua_evalsha_cmd, &output, evalsha_cmd, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("aaaa") != std::string::npos);

    std::vector<std::string> lua_eval_cmd{"eval", script_nokey, "0"};
    s = ExecuteCmd(lua_eval_cmd, &output, eval_cmd, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("aaaa") != std::string::npos);
  }

  // Test lua gc
  {
    std::string key{global_slot_keys[5]};
    std::vector<std::string> lua_get_cmd{"EVAL", lua_get, "1", key};
    eval_cmd->SetArgs(lua_get_cmd);
    auto s = eval_cmd->Parse();
    ASSERT_TRUE(s.IsOK());
    constexpr int64_t LUA_GC_CYCLE_PERIOD = 50;
    for (auto i = 0; i < LUA_GC_CYCLE_PERIOD; i++) {
      std::string output;
      s = eval_cmd->Execute(srv.GetServer().get(), &conn, &output, storage);
      ASSERT_TRUE(s.IsOK());
    }
  }
}

}  // namespace redis
