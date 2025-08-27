#include "commands/cmd_scan.h"

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

TEST(NodeScanTest, CmdTest) {
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
  conn.BecomeAdmin();
  // get server
  auto srv_ptr = srv.GetServer().get();
  // get storage
  auto storage1 = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  auto storage2 = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id + 1).get();

  // create cmds
  std::unique_ptr<Commander> cmd_nodescan;
  s = srv.GetServer()->LookupAndCreateCommand("nodescan", &cmd_nodescan);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_config;
  s = srv.GetServer()->LookupAndCreateCommand("config", &cmd_config);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_mset;
  s = srv.GetServer()->LookupAndCreateCommand("mset", &cmd_mset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_mget;
  s = srv.GetServer()->LookupAndCreateCommand("mget", &cmd_mget);
  ASSERT_TRUE(s.IsOK());

  // test config lru cache
  {
    // scan-session-max-count
    // scan-session-ttl-seconds
    CommandTokens config_get_lru_cap{"config", "get", "scan-session-max-count"};
    CommandTokens config_set_lru_cap_1k{"config", "set", "scan-session-max-count", "2000"};
    CommandTokens config_get_session_ttl{"config", "get", "scan-session-ttl-seconds"};
    CommandTokens config_set_session_ttl_1k{"config", "set", "scan-session-ttl-seconds", "2000"};

    std::string output;
    s = GenericExecCmd(config_get_lru_cap, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("10000") != std::string::npos);
    s = GenericExecCmd(config_set_lru_cap_1k, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(config_get_lru_cap, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("2000") != std::string::npos);
    EXPECT_EQ(2000, srv.GetServer()->scan_lru_cache->GetCapacity());

    s = GenericExecCmd(config_get_session_ttl, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("86400") != std::string::npos);
    s = GenericExecCmd(config_set_session_ttl_1k, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(config_get_session_ttl, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("2000") != std::string::npos);
    EXPECT_EQ(2000 * 1000, srv.GetServer()->scan_lru_cache->GetTTL());

    // val out of range
    CommandTokens config_set_lru_cap_0{"config", "set", "scan-session-max-count", "0"};
    CommandTokens config_set_lru_cap_100k{"config", "set", "scan-session-max-count", "100001"};
    CommandTokens config_set_session_ttl_neg{"config", "set", "scan-session-ttl-seconds", "-1"};
    s = GenericExecCmd(config_set_lru_cap_0, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_FALSE(s.IsOK());
    s = GenericExecCmd(config_set_lru_cap_100k, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_FALSE(s.IsOK());
    s = GenericExecCmd(config_set_session_ttl_neg, &output, cmd_config, srv_ptr, &conn, nullptr);
    ASSERT_FALSE(s.IsOK());
  }

  // Test nodescan cmd parse
  {
    // test parse error
    std::vector<CommandTokens> cmd_error_params{
        // slotrange error
        {"nodescan", "100", "0"},
        {"nodescan", "-1-100", "0"},
        {"nodescan", "100,1000", "0"},
        {"nodescan", "1000-500", "0"},
        {"nodescan", "aa-1000", "0"},
        {"nodescan", "100-bb", "0"},
        {"nodescan", "10000-20000", "0"},
        {"nodescan", "20000-1000", "0"},
        // cursor error
        {"nodescan", "100-1000", "aa"},
        {"nodescan", "100-1000", "-10"},               // invalid slotid(-10)
        {"nodescan", "100-1000", "2814749767778304"},  // invalid slotid(16384)
        {"nodescan", "100-1000", "2814749767761970"},  // slotid(50) is not in slotrange
        // count error
        {"nodescan", "100-1000", "0", "count", "-1"},                              // count out of range
        {"nodescan", "100-1000", "0", "count", "0"},                               // count can't be 0
        {"nodescan", "100-1000", "0", "count", "199999999999999999999999999999"},  // count out of range
        {"nodescan", "100-1000", "0", "count", "aa"},
        // match error: none
        // type error
        {"nodescan", "100-1000", "0", "type", "aa"},
    };

    for (const auto& token : cmd_error_params) {
      cmd_nodescan->SetArgs(token);
      s = cmd_nodescan->Parse();
      EXPECT_FALSE(s.IsOK());
    }

    // test parse ok
    std::vector<CommandTokens> cmd_right_tokens{
        // slotrange & cursor
        {"nodescan", "0-16383", "0"},
        {"nodescan", "0-16383", "2814749767761970"},
        {"nodescan", "100-2000", "0"},
        {"nodescan", "100-2000", "2814749767762920"},
        // count
        {"nodescan", "0-16383", "0", "count", "1000"},
        // match
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "prefix_*"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*_suffix"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*match*"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*aa*bb*"},
        // type
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*", "type", "string"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*", "type", "hash"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*", "type", "list"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*", "type", "set"},
        {"nodescan", "0-16383", "0", "count", "1000", "match", "*", "type", "zset"}};

    for (const auto& token : cmd_right_tokens) {
      cmd_nodescan->SetArgs(token);
      s = cmd_nodescan->Parse();
      EXPECT_TRUE(s.IsOK());
    }
  }

  {
    std::string output;
    // write data to db
    std::vector<std::string> slotrange1_keys;
    std::vector<std::string> slotrange2_keys;
    std::string key_prefix("key_");
    std::string val_prefix("val_");

    for (auto i = 1; i <= 200; i += 2) {
      std::string key = key_prefix + "{" + global_slot_keys[i] + "}_" + std::to_string(i);
      std::string val = val_prefix + std::to_string(i);
      slotrange1_keys.emplace_back(key);
      slotrange1_keys.emplace_back(val);
    }

    for (auto i = 8192; i < 8192 + 200; i += 2) {
      std::string key = key_prefix + "{" + global_slot_keys[i] + "}_" + std::to_string(i);
      std::string val = val_prefix + std::to_string(i);
      slotrange2_keys.emplace_back(key);
      slotrange2_keys.emplace_back(val);
    }
    // scan and check output
    std::vector<std::string> mset_tokens1{"mset"};
    mset_tokens1.insert(mset_tokens1.end(), slotrange1_keys.begin(), slotrange1_keys.end());
    std::vector<std::string> mset_tokens2{"mset"};
    mset_tokens2.insert(mset_tokens2.end(), slotrange2_keys.begin(), slotrange2_keys.end());
    // write data
    s = GenericExecCmd(mset_tokens1, &output, cmd_mset, srv_ptr, &conn, storage1);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(mset_tokens2, &output, cmd_mset, srv_ptr, &conn, storage2);
    ASSERT_TRUE(s.IsOK());
    // get data
    std::vector<std::string> mget1_tokens{"mget", slotrange1_keys.front(), slotrange1_keys[slotrange1_keys.size() - 2]};
    s = GenericExecCmd(mget1_tokens, &output, cmd_mget, srv_ptr, &conn, storage1);
    EXPECT_TRUE(output.find(slotrange1_keys[1]) != std::string::npos);
    EXPECT_TRUE(output.find(slotrange1_keys.back()) != std::string::npos);

    std::vector<std::string> mget2_tokens{"mget", slotrange2_keys.front(), slotrange2_keys[slotrange2_keys.size() - 2]};
    s = GenericExecCmd(mget2_tokens, &output, cmd_mget, srv_ptr, &conn, storage2);
    EXPECT_TRUE(output.find(slotrange2_keys[1]) != std::string::npos);
    EXPECT_TRUE(output.find(slotrange2_keys.back()) != std::string::npos);

    // exec nodescan all
    std::vector<std::string> nodescan_tokens1{"nodescan", "0-8191", "0", "count", "100", "type", "string"};
    s = GenericExecCmd(nodescan_tokens1, &output, cmd_nodescan, srv_ptr, &conn, storage1);
    ASSERT_TRUE(s.IsOK());
    for (size_t i = 0; i < slotrange1_keys.size(); i += 2) {
      EXPECT_TRUE(output.find(slotrange1_keys[i]) != std::string::npos);
    }

    std::vector<std::string> nodescan_tokens2{"nodescan", "8192-16383", "0", "count", "100", "type", "string"};
    s = GenericExecCmd(nodescan_tokens2, &output, cmd_nodescan, srv_ptr, &conn, storage2);
    ASSERT_TRUE(s.IsOK());
    for (size_t i = 0; i < slotrange2_keys.size(); i += 2) {
      EXPECT_TRUE(output.find(slotrange2_keys[i]) != std::string::npos);
    }
  }

  // scan in lua
  {
    std::unique_ptr<Commander> eval_cmd;
    s = srv.GetServer()->LookupAndCreateCommand("EVAL", &eval_cmd);
    ASSERT_TRUE(s.IsOK());

    std::string output;
    std::vector<std::string> eval_scan_tokens{"EVAL", "return redis.call('nodescan', '0-16383', '0')", "0"};
    s = GenericExecCmd(eval_scan_tokens, &output, eval_cmd, srv_ptr, &conn, storage1);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(output.find("allowed") != std::string::npos);
  }
}

}  // namespace redis
