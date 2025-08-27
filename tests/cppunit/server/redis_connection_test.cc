#include "server/redis_connection.h"

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "mock/mock_server.h"

namespace redis {

TEST(ConnectionTest, TestLocks) {
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
  // Test mget multiple keys
  {
    // create cmd mget
    std::vector<std::string> cmd_tokens{"mget"};
    std::vector<int16_t> keyslots{0, 2, 4096, 8191};
    auto keys = GetSlotsKeys(keyslots);
    cmd_tokens.insert(cmd_tokens.end(), keys.begin(), keys.end());
    std::unique_ptr<Commander> current_cmd;
    s = srv.GetServer()->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd);
    ASSERT_TRUE(s.IsOK());
    auto attr = current_cmd->GetAttributes();
    auto cmd_flags = attr->GenerateFlags(cmd_tokens);

    // test get locks mget
    Context ctx;
    std::vector<std::unique_ptr<KeyLock>> key_locks;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    std::string slot_range_name{"[0,8191]"};
    s = conn.getExecLocks(cmd_flags, slot_range_name, attr, cmd_tokens, &ctx, &key_locks, &sr_locks);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(key_locks.size(), 0);
    EXPECT_EQ(sr_locks.size(), 1);
  }

  // Test mset multiple keys
  {
    // create cmd mset
    std::vector<std::string> cmd_tokens{"mset"};
    std::vector<int16_t> keyslots{0, 2, 4096, 8191};
    auto keys = GetSlotsKeys(keyslots, true);
    cmd_tokens.insert(cmd_tokens.end(), keys.begin(), keys.end());
    std::unique_ptr<Commander> current_cmd;
    s = srv.GetServer()->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd);
    ASSERT_TRUE(s.IsOK());
    auto attr = current_cmd->GetAttributes();
    auto cmd_flags = attr->GenerateFlags(cmd_tokens);

    // test get locks mset
    Context ctx;
    std::vector<std::unique_ptr<KeyLock>> key_locks;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    std::string slot_range_name{"[0,8191]"};
    s = conn.getExecLocks(cmd_flags, slot_range_name, attr, cmd_tokens, &ctx, &key_locks, &sr_locks);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(key_locks.size(), keyslots.size());
    EXPECT_EQ(sr_locks.size(), 0);

    // test key locked
    std::thread thd1([&keyslots, &slot_range_name, &srv]() {
      auto keys = GetSlotsKeys(keyslots);
      for (auto &key : keys) {
        Context ctx;
        auto lk = KeyLock::AcquireKeyLock(slot_range_name, key, mgl::LockMode::LOCK_X, &ctx,
                                          srv.GetServer()->GetMGLockMgr(), 100);
        EXPECT_FALSE(lk.IsOK());
        EXPECT_TRUE(lk.Is<Status::LockTimeOut>());
        LOG(INFO) << fmt::format("Try lock key:{}, result:{}", key, lk.Msg());
      }
    });
    thd1.join();
  }

  // Test key not in same slotrange
  {
    // mget test
    std::vector<std::string> cmd_tokens{"mget"};
    std::vector<int16_t> keyslots{0, 2, 4096, 8192};
    auto keys = GetSlotsKeys(keyslots);
    cmd_tokens.insert(cmd_tokens.end(), keys.begin(), keys.end());
    std::unique_ptr<Commander> current_cmd;
    s = srv.GetServer()->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd);
    ASSERT_TRUE(s.IsOK());
    auto attr = current_cmd->GetAttributes();
    auto cmd_flags = attr->GenerateFlags(cmd_tokens);

    // test get locks mget
    Context ctx;
    std::vector<std::unique_ptr<KeyLock>> key_locks;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    std::string slot_range_name{"[0,8191]"};
    s = conn.getExecLocks(cmd_flags, slot_range_name, attr, cmd_tokens, &ctx, &key_locks, &sr_locks);
    ASSERT_FALSE(s.IsOK());
    ASSERT_TRUE(s.Msg().find("CROSSSLOTRANGE") != std::string::npos);
  }

  {
    // mset test
    std::vector<std::string> cmd_tokens{"mset"};
    std::vector<int16_t> keyslots{0, 2, 4096, 8192};
    auto keys = GetSlotsKeys(keyslots, true);
    cmd_tokens.insert(cmd_tokens.end(), keys.begin(), keys.end());
    std::unique_ptr<Commander> current_cmd;
    s = srv.GetServer()->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd);
    ASSERT_TRUE(s.IsOK());
    auto attr = current_cmd->GetAttributes();
    auto cmd_flags = attr->GenerateFlags(cmd_tokens);

    // test get locks mset
    Context ctx;
    std::vector<std::unique_ptr<KeyLock>> key_locks;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    std::string slot_range_name{"[0,8191]"};
    s = conn.getExecLocks(cmd_flags, slot_range_name, attr, cmd_tokens, &ctx, &key_locks, &sr_locks);
    ASSERT_FALSE(s.IsOK());
    ASSERT_TRUE(s.Msg().find("CROSSSLOTRANGE") != std::string::npos);
  }

  // Test mset same key
  {
    // mset test
    std::vector<std::string> cmd_tokens{"mset"};
    std::vector<int16_t> keyslots{0, 2, 4096, 4096};
    auto keys = GetSlotsKeys(keyslots, true);
    cmd_tokens.insert(cmd_tokens.end(), keys.begin(), keys.end());
    std::unique_ptr<Commander> current_cmd;
    s = srv.GetServer()->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd);
    ASSERT_TRUE(s.IsOK());
    auto attr = current_cmd->GetAttributes();
    auto cmd_flags = attr->GenerateFlags(cmd_tokens);

    // test get locks mset
    Context ctx;
    std::vector<std::unique_ptr<KeyLock>> key_locks;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    std::string slot_range_name{"[0,8191]"};
    s = conn.getExecLocks(cmd_flags, slot_range_name, attr, cmd_tokens, &ctx, &key_locks, &sr_locks);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(key_locks.size(), keyslots.size() - 1);
    EXPECT_EQ(sr_locks.size(), 0);
  }

  // Test lockAllSlotRagens
  {
    Context ctx;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    auto s = conn.lockAllSlotRanges(&ctx, mgl::LockMode::LOCK_IX, &sr_locks);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(sr_locks.size(), 2);
  }

  // Test HandleCmdScript
  {
    Context ctx;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    auto s = conn.HandleCmdScript(&ctx, &sr_locks);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(sr_locks.size(), 2);
  }

  // Test HandleCmdNodeScan
  {
    Context ctx;
    std::vector<std::unique_ptr<SlotRangeLock>> sr_locks;
    // create nodescan cmd
    std::vector<std::string> nodescan_tokens{"nodescan", "0-8191", "0"};
    std::unique_ptr<Commander> current_cmd;
    s = srv.GetServer()->LookupAndCreateCommand(nodescan_tokens.front(), &current_cmd);
    ASSERT_TRUE(s.IsOK());
    const auto attributes = current_cmd->GetAttributes();
    auto cmd_flags = attributes->GenerateFlags(nodescan_tokens);

    current_cmd->SetArgs(nodescan_tokens);
    s = current_cmd->Parse();
    ASSERT_TRUE(s.IsOK());
    auto res = conn.HandleCmdNodeScan(cmd_flags, current_cmd, &ctx, &sr_locks);
    ASSERT_TRUE(res.IsOK());
    EXPECT_EQ(sr_locks.size(), 1);
    ASSERT_TRUE(res.GetValue()->GetDBId() == test_db_id);

    std::vector<std::string> nodescan_tokens2{"nodescan", "10000-16000", "0"};
    current_cmd->SetArgs(nodescan_tokens2);
    s = current_cmd->Parse();
    ASSERT_TRUE(s.IsOK());
    auto ret = conn.HandleCmdNodeScan(cmd_flags, current_cmd, &ctx, &sr_locks);
    ASSERT_TRUE(ret.IsOK());
    EXPECT_EQ(sr_locks.size(), 2);
    ASSERT_EQ(ret.GetValue()->GetDBId(), test_db_id + 1);
  }
}

}  // namespace redis
