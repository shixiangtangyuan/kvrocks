#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <cstddef>
#include <string>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "commands/cmd_scan.h"
#include "commands/cmd_test_util.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "server/redis_connection.h"
#include "status.h"

namespace redis {

TEST(CmdHashTest, HFESetAndGetTest) {
  std::string reply_ok = "+OK\r\n";
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
  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  // TODO:(mingfo) cdc for httl is not ready
  storage->GetConfig()->enable_cdc_sync = false;

  // enable hfe cmd
  s = srv_ptr->GetConfig()->Set(nullptr, "enable-hfe-cmd", "yes");
  ASSERT_TRUE(s.IsOK());

  // set key
  std::unique_ptr<Commander> cmd_hmset;
  s = srv.GetServer()->LookupAndCreateCommand("hmset", &cmd_hmset);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hmset{"hmset", "key", "f1", "v1", "f2", "v2", "f3", "v3"};
  std::string output;
  s = GenericExecCmd(token_hmset, &output, cmd_hmset, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(output, reply_ok);

  // hmget
  std::unique_ptr<Commander> cmd_hmget;
  s = srv.GetServer()->LookupAndCreateCommand("hmget", &cmd_hmget);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hmget{"hget", "key", "f1", "f2", "f3", "f4"};
  s = GenericExecCmd(token_hmget, &output, cmd_hmget, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hmget_reply = "*4\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$-1\r\n";
  ASSERT_EQ(output, hmget_reply);

  std::unique_ptr<Commander> cmd_hexpireat;
  s = srv.GetServer()->LookupAndCreateCommand("hexpireat", &cmd_hexpireat);
  ASSERT_TRUE(s.IsOK());
  auto timestamp = util::GetTimeStamp() + 2;
  // hexpireat ok
  CommandTokens token_hexpireat{"hexpireat", "key", std::to_string(timestamp), "fields", "3", "f1", "f2", "f4"};
  s = GenericExecCmd(token_hexpireat, &output, cmd_hexpireat, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hexpireat_reply = "*3\r\n:1\r\n:1\r\n:-2\r\n";
  ASSERT_TRUE(output == hexpireat_reply);
  // hexpireat nx
  std::unique_ptr<Commander> cmd_hexpireat_nx;
  s = srv.GetServer()->LookupAndCreateCommand("hexpireat", &cmd_hexpireat_nx);
  token_hexpireat = {"hexpireat", "key", std::to_string(timestamp), "NX", "fields", "1", "f1"};
  s = GenericExecCmd(token_hexpireat, &output, cmd_hexpireat_nx, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  hexpireat_reply = "*1\r\n:0\r\n";
  ASSERT_TRUE(output == hexpireat_reply);

  // hexpiretime
  std::unique_ptr<Commander> cmd_hexpiretime;
  s = srv.GetServer()->LookupAndCreateCommand("hexpiretime", &cmd_hexpiretime);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hexpiretime{"hexpireat", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_hexpiretime, &output, cmd_hexpiretime, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hexpiretime_reply = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp);
  ASSERT_EQ(output, hexpiretime_reply);

  // hpxepiretime
  std::unique_ptr<Commander> cmd_hpexpiretime;
  s = srv.GetServer()->LookupAndCreateCommand("hpexpiretime", &cmd_hpexpiretime);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hpexpiretime{"hpexpiretime", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_hpexpiretime, &output, cmd_hpexpiretime, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpexpiretime_reply = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp * 1000);
  ASSERT_EQ(output, hpexpiretime_reply);

  // httl
  std::unique_ptr<Commander> cmd_httl;
  s = srv.GetServer()->LookupAndCreateCommand("httl", &cmd_httl);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_httl{"httl", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_httl, &output, cmd_httl, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string httl_reply1 = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", 2);
  std::string httl_reply2 = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", 1);
  ASSERT_TRUE(output == httl_reply1 || output == httl_reply2);

  // hpttl
  std::unique_ptr<Commander> cmd_hpttl;
  s = srv.GetServer()->LookupAndCreateCommand("hpttl", &cmd_hpttl);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hpttl{"hpttl", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_hpttl, &output, cmd_hpttl, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpttl_reply1 = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", 2 * 1000);
  std::string hpttl_reply2 = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", 1 * 1000);
  ASSERT_TRUE(output <= hpttl_reply1 && output >= hpttl_reply2);

  // hpersist
  std::unique_ptr<Commander> cmd_hpersist;
  s = srv.GetServer()->LookupAndCreateCommand("hpersist", &cmd_hpersist);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hpersist{"hpersist", "key", "fields", "4", "f1", "f2", "f3", "f4"};
  s = GenericExecCmd(token_hpersist, &output, cmd_hpersist, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpersist_reply = "*4\r\n:1\r\n:1\r\n:-1\r\n:-2\r\n";
  ASSERT_TRUE(output == hpersist_reply);

  // hpttl
  std::unique_ptr<Commander> cmd_hpttl_2;
  s = srv.GetServer()->LookupAndCreateCommand("hpttl", &cmd_hpttl_2);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hpttl_2{"hpttl", "key", "fields", "3", "f1", "f2", "f3"};
  s = GenericExecCmd(token_hpttl_2, &output, cmd_hpttl_2, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpttl_reply_2 = "*3\r\n:-1\r\n:-1\r\n:-1\r\n";
  ASSERT_EQ(output, hpttl_reply_2);

  // hmget
  std::unique_ptr<Commander> cmd_hmget2;
  s = srv.GetServer()->LookupAndCreateCommand("hmget", &cmd_hmget2);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hmget2{"hget", "key", "f1", "f2", "f3", "f4"};
  s = GenericExecCmd(token_hmget2, &output, cmd_hmget2, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hmget_reply2 = "*4\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$-1\r\n";
  ASSERT_EQ(output, hmget_reply2);

  // hpexpireat
  std::unique_ptr<Commander> cmd_hpexpireat_2;
  s = srv.GetServer()->LookupAndCreateCommand("hpexpireat", &cmd_hpexpireat_2);
  ASSERT_TRUE(s.IsOK());
  auto timestamp_ms = util::GetTimeStampMS() + 10;
  CommandTokens token_hpexpireat_2{"hpexpireat", "key", std::to_string(timestamp_ms), "fields", "2", "f1", "f2"};
  s = GenericExecCmd(token_hpexpireat_2, &output, cmd_hpexpireat_2, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpexpireat_reply_2 = "*2\r\n:1\r\n:1\r\n";
  ASSERT_EQ(output, hpexpireat_reply_2);

  // sleep 100ms and mget
  usleep(100 * 1000);
  std::unique_ptr<Commander> cmd_hmget_2;
  s = srv.GetServer()->LookupAndCreateCommand("hmget", &cmd_hmget_2);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hmget_2{"hget", "key", "f1", "f2", "f3", "f4"};
  s = GenericExecCmd(token_hmget_2, &output, cmd_hmget_2, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  hmget_reply = "*4\r\n$-1\r\n$-1\r\n$2\r\nv3\r\n$-1\r\n";
  ASSERT_EQ(output, hmget_reply);

  // hpttl
  std::unique_ptr<Commander> cmd_httl_2;
  s = srv.GetServer()->LookupAndCreateCommand("hpttl", &cmd_httl_2);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_httl_2{"httl", "key", "fields", "3", "f1", "f2", "f3"};
  s = GenericExecCmd(token_httl_2, &output, cmd_httl_2, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string httl_reply_2 = "*3\r\n:-2\r\n:-2\r\n:-1\r\n";
  ASSERT_EQ(output, httl_reply_2);
}

TEST(CmdHashTest, ParseHttlAndHpersistTest) {
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
  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  // TODO:(mingfo) cdc for httl is not ready
  storage->GetConfig()->enable_cdc_sync = false;

  // enable hfe cmd
  s = srv_ptr->GetConfig()->Set(nullptr, "enable-hfe-cmd", "yes");
  ASSERT_TRUE(s.IsOK());

  auto cmds = std::vector<std::string>{"httl", "hpttl", "hexpiretime", "hpexpiretime", "hpersist"};
  for (const auto& cmd : cmds) {
    // create cmds
    std::unique_ptr<Commander> cmd_httl;
    s = srv.GetServer()->LookupAndCreateCommand(cmd, &cmd_httl);
    ASSERT_TRUE(s.IsOK());
    // test parse
    {
      CommandTokens token_httl{cmd, "key", "fields", "3", "f1", "f2", "f3"};
      std::string output;
      s = cmd_httl->Parse(token_httl);

      ASSERT_TRUE(s.IsOK());
    }
    // HTTL/HEXPIRETIME... key FIELDS numfields field [field ...]
    // HPERSIST key FIELDS numfields field [field ...]
    {
      // test parse error
      std::vector<std::pair<CommandTokens, Status>> cmd_error_params;
      cmd_error_params.emplace_back(CommandTokens{cmd}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", "a"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", "Fields"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", "Fields", "3"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", "Fields", "3", "f1"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", "Fields", "1", "f1", "f2"}, Status::RedisParseErr);

      for (const auto& pair : cmd_error_params) {
        cmd_httl->SetArgs(pair.first);
        s = cmd_httl->Parse();
        EXPECT_FALSE(s.IsOK());
        EXPECT_EQ(s.GetCode(), pair.second.GetCode());
      }
      // test parse ok
      std::vector<CommandTokens> cmd_ok_params;
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", "Fields", "3", "f1", "f2", "f3"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", "Fields", "2", "f1", "f2"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", "Fields", "1", "f1"});
      for (const auto& token : cmd_ok_params) {
        cmd_httl->SetArgs(token);
        s = cmd_httl->Parse();
        EXPECT_TRUE(s.IsOK());
      }
    }
  }
}

TEST(CmdHashTest, ParseHexpireTest) {
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
  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  // TODO:(mingfo) cdc for httl is not ready
  storage->GetConfig()->enable_cdc_sync = false;

  // enable hfe cmd
  s = srv_ptr->GetConfig()->Set(nullptr, "enable-hfe-cmd", "yes");
  ASSERT_TRUE(s.IsOK());

  auto cmds = std::vector<std::string>{"hexpire", "hpexpire", "hexpireat", "hpexpireat", "hpexpireat"};
  auto timestamp =
      std::vector<std::string>{"100", "100000", std::to_string(util::GetTimeStamp()),
                               std::to_string(util::GetTimeStampMS()), std::to_string(kHashFieldMaxAbsTimeMS)};
  int idx = 0;
  for (const auto& cmd : cmds) {
    // create cmds
    std::unique_ptr<Commander> cmd_httl;
    s = srv.GetServer()->LookupAndCreateCommand(cmd, &cmd_httl);
    ASSERT_TRUE(s.IsOK());
    // test parse
    {
      CommandTokens token_httl{cmd, "key", timestamp[idx], "fields", "3", "f1", "f2", "f3"};
      std::string output;
      s = cmd_httl->Parse(token_httl);

      ASSERT_TRUE(s.IsOK());
    }
    // HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
    {
      // test parse error
      std::vector<std::pair<CommandTokens, Status>> cmd_error_params;
      cmd_error_params.emplace_back(CommandTokens{cmd}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx]}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "a"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "Fields"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "Fields", "3"}, Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "Fields", "3", "f1"},
                                    Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "Fields", "1", "f1", "f2"},
                                    Status::RedisParseErr);
      cmd_error_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "NX", "1", "f1", "f2"},
                                    Status::RedisParseErr);
      cmd_error_params.emplace_back(
          CommandTokens{cmd, "key", std::to_string(kHashFieldMaxAbsTimeMS + 1), "Fields", "3", "f1", "f2", "f3"},
          Status::RedisParseErr);

      for (const auto& pair : cmd_error_params) {
        cmd_httl->SetArgs(pair.first);
        s = cmd_httl->Parse();
        EXPECT_FALSE(s.IsOK());
        EXPECT_EQ(s.GetCode(), pair.second.GetCode());
      }
      // test parse ok
      std::vector<CommandTokens> cmd_ok_params;
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "Fields", "3", "f1", "f2", "f3"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "Fields", "2", "f1", "f2"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "Fields", "1", "f1"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "NX", "Fields", "2", "f1", "f2"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "XX", "Fields", "2", "f1", "f2"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "GT", "Fields", "2", "f1", "f2"});
      cmd_ok_params.emplace_back(CommandTokens{cmd, "key", timestamp[idx], "LT", "Fields", "2", "f1", "f2"});
      for (const auto& token : cmd_ok_params) {
        cmd_httl->SetArgs(token);
        s = cmd_httl->Parse();
        EXPECT_TRUE(s.IsOK());
      }
    }
  }
}

TEST(CmdHashTest, TimeStampTest) {
  std::string reply_ok = "+OK\r\n";
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
  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  // TODO:(mingfo) cdc for httl is not ready
  storage->GetConfig()->enable_cdc_sync = false;

  // enable hfe cmd
  s = srv_ptr->GetConfig()->Set(nullptr, "enable-hfe-cmd", "yes");
  ASSERT_TRUE(s.IsOK());

  // set key
  std::unique_ptr<Commander> cmd_hmset;
  s = srv.GetServer()->LookupAndCreateCommand("hmset", &cmd_hmset);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hmset{"hmset", "key", "f1", "v1", "f2", "v2", "f3", "v3"};
  std::string output;
  s = GenericExecCmd(token_hmset, &output, cmd_hmset, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(output, reply_ok);

  std::unique_ptr<Commander> cmd_hexpireat;
  output = "";
  s = srv.GetServer()->LookupAndCreateCommand("hexpireat", &cmd_hexpireat);
  ASSERT_TRUE(s.IsOK());
  auto timestamp = util::GetTimeStamp() + 2;
  // hexpireat
  CommandTokens token_hexpireat{"hexpireat", "key", std::to_string(timestamp), "fields", "3", "f1", "f2", "f4"};
  s = GenericExecCmd(token_hexpireat, &output, cmd_hexpireat, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hexpireat_reply = "*3\r\n:1\r\n:1\r\n:-2\r\n";
  ASSERT_TRUE(output == hexpireat_reply);

  // hpxepiretime
  std::unique_ptr<Commander> cmd_hpexpiretime;
  output = "";
  s = srv.GetServer()->LookupAndCreateCommand("hpexpiretime", &cmd_hpexpiretime);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hpexpiretime{"hpexpiretime", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_hpexpiretime, &output, cmd_hpexpiretime, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpexpiretime_reply = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp * 1000);
  ASSERT_EQ(output, hpexpiretime_reply);

  // hpexpireat
  std::unique_ptr<Commander> cmd_hpexpireat;
  output = "";
  s = srv.GetServer()->LookupAndCreateCommand("hpexpireat", &cmd_hpexpireat);
  ASSERT_TRUE(s.IsOK());
  timestamp = (util::GetTimeStamp() + 2) * 1000 + 123;
  CommandTokens token_hpexpireat{"hpexpireat", "key", std::to_string(timestamp), "fields", "3", "f1", "f2", "f4"};
  s = GenericExecCmd(token_hpexpireat, &output, cmd_hpexpireat, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpexpireat_reply = "*3\r\n:1\r\n:1\r\n:-2\r\n";
  ASSERT_TRUE(output == hexpireat_reply);

  // hpxepiretime
  cmd_hpexpiretime = std::unique_ptr<Commander>{};
  output = "";
  s = srv.GetServer()->LookupAndCreateCommand("hpexpiretime", &cmd_hpexpiretime);
  ASSERT_TRUE(s.IsOK());
  token_hpexpiretime = {"hpexpiretime", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_hpexpiretime, &output, cmd_hpexpiretime, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  hpexpiretime_reply = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp);
  ASSERT_EQ(output, hpexpiretime_reply);

  // hexpire
  std::unique_ptr<Commander> cmd_hexpire;
  output = "";
  s = srv.GetServer()->LookupAndCreateCommand("hexpire", &cmd_hexpire);
  ASSERT_TRUE(s.IsOK());
  uint64_t ttl = 5;
  CommandTokens token_hexpire{"hexpire", "key", std::to_string(ttl), "fields", "3", "f1", "f2", "f4"};
  s = GenericExecCmd(token_hexpire, &output, cmd_hexpire, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hexpire_reply = "*3\r\n:1\r\n:1\r\n:-2\r\n";
  ASSERT_TRUE(output == hexpire_reply);

  // hpxepiretime
  output = "";
  int64_t timestamp_lower = (util::GetTimeStamp() + 5) * 1000;
  int64_t timestamp_upper = (util::GetTimeStamp() + 6) * 1000;
  cmd_hpexpiretime = std::unique_ptr<Commander>{};
  s = srv.GetServer()->LookupAndCreateCommand("hpexpiretime", &cmd_hpexpiretime);
  ASSERT_TRUE(s.IsOK());
  token_hpexpiretime = {"hpexpiretime", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_hpexpiretime, &output, cmd_hpexpiretime, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpexpiretime_reply_lower = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp_lower);
  std::string hpexpiretime_reply_upper = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp_upper);
  ASSERT_GE(output, hpexpiretime_reply_lower);
  ASSERT_LE(output, hpexpiretime_reply_upper);

  // hpexpire
  std::unique_ptr<Commander> cmd_hpexpire;
  output = "";
  s = srv.GetServer()->LookupAndCreateCommand("hpexpire", &cmd_hpexpire);
  ASSERT_TRUE(s.IsOK());
  uint64_t pttl = 7000;
  CommandTokens token_hpexpire{"hpexpire", "key", std::to_string(pttl), "fields", "3", "f1", "f2", "f4"};
  s = GenericExecCmd(token_hpexpire, &output, cmd_hpexpire, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hpexpire_reply = "*3\r\n:1\r\n:1\r\n:-2\r\n";
  ASSERT_EQ(output, hpexpire_reply);

  // hpxepiretime
  output = "";
  timestamp_lower = (util::GetTimeStamp() + 7) * 1000;
  timestamp_upper = (util::GetTimeStamp() + 8) * 1000;
  cmd_hpexpiretime = std::unique_ptr<Commander>{};
  s = srv.GetServer()->LookupAndCreateCommand("hpexpiretime", &cmd_hpexpiretime);
  ASSERT_TRUE(s.IsOK());
  token_hpexpiretime = {"hpexpiretime", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_hpexpiretime, &output, cmd_hpexpiretime, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  hpexpiretime_reply_lower = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp_lower);
  hpexpiretime_reply_upper = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", timestamp_upper);
  ASSERT_GE(output, hpexpiretime_reply_lower);
  ASSERT_LE(output, hpexpiretime_reply_upper);

  // sleep 500ms
  usleep(500 * 1000);
  // httl
  std::unique_ptr<Commander> cmd_httl;
  output = "";
  s = srv.GetServer()->LookupAndCreateCommand("httl", &cmd_httl);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_httl{"httl", "key", "fields", "3", "f1", "f3", "f4"};
  s = GenericExecCmd(token_httl, &output, cmd_httl, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string httl_reply = fmt::format("*3\r\n:{}\r\n:-1\r\n:-2\r\n", 7);
  ASSERT_EQ(output, httl_reply);
}

TEST(CmdHashTest, HDelAndHMgetTest) {
  std::string reply_ok = "+OK\r\n";
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
  auto storage = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  // TODO:(mingfo) cdc for httl is not ready
  storage->GetConfig()->enable_cdc_sync = false;

  // enable hfe cmd
  s = srv_ptr->GetConfig()->Set(nullptr, "enable-hfe-cmd", "yes");
  ASSERT_TRUE(s.IsOK());

  // set key
  std::unique_ptr<Commander> cmd_hmset;
  s = srv.GetServer()->LookupAndCreateCommand("hmset", &cmd_hmset);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hmset{"hmset", "k", "f1", "v1", "f2", "v2", "f3", "v3"};
  std::string output;
  s = GenericExecCmd(token_hmset, &output, cmd_hmset, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(output, reply_ok);

  // hmget
  std::unique_ptr<Commander> cmd_hmget;
  s = srv.GetServer()->LookupAndCreateCommand("hmget", &cmd_hmget);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hmget{"hmget", "k", "f1", "f2", "f3", "f4"};
  s = GenericExecCmd(token_hmget, &output, cmd_hmget, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hmget_reply = "*4\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$-1\r\n";
  ASSERT_EQ(output, hmget_reply);

  // hdel
  std::unique_ptr<Commander> cmd_hdel;
  s = srv.GetServer()->LookupAndCreateCommand("hdel", &cmd_hdel);
  ASSERT_TRUE(s.IsOK());
  CommandTokens token_hdel{"hdel", "k", "f1"};
  s = GenericExecCmd(token_hdel, &output, cmd_hdel, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  std::string hdel_reply = ":1\r\n";
  ASSERT_EQ(output, hdel_reply);

  // hmget
  s = srv.GetServer()->LookupAndCreateCommand("hmget", &cmd_hmget);
  CommandTokens token_hmget_2{"hmget", "k", "f1"};
  s = GenericExecCmd(token_hmget_2, &output, cmd_hmget, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  hmget_reply = "*1\r\n$-1\r\n";
  ASSERT_EQ(output, hmget_reply);
}

}  // namespace redis
