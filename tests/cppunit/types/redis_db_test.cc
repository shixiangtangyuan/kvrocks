#include "storage/redis_db.h"

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <cstddef>

#include "cluster/cluster_defs.h"
#include "cluster/redis_slot.h"
#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "commands/cmd_test_util.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "server/redis_connection.h"

namespace redis {

TEST(RedisDBTest, ScanTest) {
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
  // get server
  auto srv_ptr = srv.GetServer().get();
  // get storage
  auto storage1 = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id).get();
  auto storage2 = srv.GetServer()->storage_mgr->GetStorageByDBID(test_db_id + 1).get();

  // create cmd
  std::unique_ptr<Commander> cmd_mset;
  s = srv.GetServer()->LookupAndCreateCommand("mset", &cmd_mset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_mget;
  s = srv.GetServer()->LookupAndCreateCommand("mget", &cmd_mget);
  ASSERT_TRUE(s.IsOK());

  {
    std::string output;
    // write data to db
    std::vector<std::string> slotrange_keys;
    std::string key_prefix("key_");
    std::string val_prefix("val_");

    for (auto i = 0; i < 100; i++) {
      std::string key = key_prefix + "{" + global_slot_keys[i] + "}_" + std::to_string(i);
      std::string val = val_prefix + std::to_string(i);
      slotrange_keys.emplace_back(key);
      slotrange_keys.emplace_back(val);
    }

    // wirte keys
    std::vector<std::string> mset_tokens{"mset"};
    mset_tokens.insert(mset_tokens.end(), slotrange_keys.begin(), slotrange_keys.end());
    s = GenericExecCmd(mset_tokens, &output, cmd_mset, srv_ptr, &conn, storage1);
    ASSERT_TRUE(s.IsOK());
    // get keys
    std::vector<std::string> mget_tokens{"mget", slotrange_keys.front(), slotrange_keys[slotrange_keys.size() - 2]};
    s = GenericExecCmd(mget_tokens, &output, cmd_mget, srv_ptr, &conn, storage1);
    EXPECT_TRUE(output.find(slotrange_keys[1]) != std::string::npos);
    EXPECT_TRUE(output.find(slotrange_keys.back()) != std::string::npos);

    // test scan
    std::string store_cursor;
    std::string end_cursor;
    std::vector<std::string> got_keys;
    int16_t start_slot = -1;
    int16_t end_slot = 8191;
    uint64_t count = 20;
    std::string pattern;
    RedisType type(RedisType::kRedisNone);

    Database redis_db(storage1);
    while (true) {
      start_slot = static_cast<int16_t>(GetSlotIdFromKey(store_cursor));
      std::vector<std::string> keys;
      auto res = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor);
      ASSERT_TRUE(res.ok());
      EXPECT_LE(keys.size(), count);
      got_keys.insert(got_keys.end(), keys.begin(), keys.end());

      if (end_cursor.empty()) {
        break;
      }
      EXPECT_EQ(keys.back(), end_cursor);
      store_cursor = end_cursor;
    }
    EXPECT_EQ(got_keys.size(), slotrange_keys.size() / 2);
  }

  // test scan with count and match
  {
    std::string store_cursor;
    std::string end_cursor;
    int16_t start_slot = 0;
    int16_t end_slot = 8191;
    uint64_t count = 20;
    std::string pattern = "*9";
    RedisType type(RedisType::kRedisNone);

    Database redis_db(storage1);
    std::vector<std::string> keys;
    auto res = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor);
    ASSERT_TRUE(res.ok());
    EXPECT_LE(keys.size(), count);
    EXPECT_EQ(keys.back(), end_cursor);

    EXPECT_EQ(start_slot, 19);
    EXPECT_EQ(keys.size(), 2);
  }

  // test scan with count and match for copi2
  {
    std::string store_cursor;
    std::string end_cursor;
    int16_t start_slot = 0;
    int16_t end_slot = 8191;
    uint64_t count = 20;
    std::string pattern = "*9";
    RedisType type(RedisType::kRedisNone);

    Database redis_db(storage1);
    std::vector<std::string> keys;
    auto res = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor, true);
    ASSERT_TRUE(res.ok());
    EXPECT_LE(keys.size(), count);
    EXPECT_EQ("", end_cursor);

    EXPECT_EQ(start_slot, 8192);
    EXPECT_EQ(keys.size(), 10);  // got all keys match *9
  }

  // test scan by using match pattern prefix
  {
    std::string store_cursor;
    std::string end_cursor;
    int16_t start_slot = 0;
    int16_t end_slot = 8191;
    uint64_t count = 20;
    std::string pattern = "key_*_1*";
    RedisType type(RedisType::kRedisNone);

    Database redis_db(storage1);
    std::vector<std::string> keys;
    auto res = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor);
    ASSERT_TRUE(res.ok());
    ASSERT_FALSE(keys.empty());
    EXPECT_EQ(keys.back(), end_cursor);

    EXPECT_EQ(start_slot, 19);
    EXPECT_EQ(keys.size(), 11);
  }

  {
    std::string store_cursor;
    std::string end_cursor;
    int16_t start_slot = 0;
    int16_t end_slot = 8191;
    uint64_t count = 20;
    std::string pattern = "key_*_1*";
    RedisType type(RedisType::kRedisNone);

    Database redis_db(storage1);
    std::vector<std::string> keys;
    auto res = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor, true);
    ASSERT_TRUE(res.ok());
    ASSERT_FALSE(keys.empty());
    EXPECT_EQ("", end_cursor);

    EXPECT_EQ(start_slot, 8192);
    EXPECT_EQ(keys.size(), 11);
  }

  // use storage2 to test that unmatched keys will be skipped with MATCH pattern
  {
    std::string output;
    // write data to db
    std::vector<std::string> slotrange_keys;
    std::string key_prefix("key_");
    std::string val_prefix("val_");
    int16_t base_slot = 8192;

    for (auto i = base_slot; i < base_slot + 100; i++) {
      std::string key = key_prefix + std::to_string(i - base_slot) + "_{" + global_slot_keys[i] + "}";
      std::string val = val_prefix + std::to_string(i);
      slotrange_keys.emplace_back(key);
      slotrange_keys.emplace_back(val);
    }

    // wirte keys
    std::vector<std::string> mset_tokens{"mset"};
    mset_tokens.insert(mset_tokens.end(), slotrange_keys.begin(), slotrange_keys.end());
    s = GenericExecCmd(mset_tokens, &output, cmd_mset, srv_ptr, &conn, storage2);
    ASSERT_TRUE(s.IsOK());
    // get keys
    std::vector<std::string> mget_tokens{"mget", slotrange_keys.front(), slotrange_keys[slotrange_keys.size() - 2]};
    s = GenericExecCmd(mget_tokens, &output, cmd_mget, srv_ptr, &conn, storage2);
    EXPECT_TRUE(output.find(slotrange_keys[1]) != std::string::npos);
    EXPECT_TRUE(output.find(slotrange_keys.back()) != std::string::npos);

    // test scan
    std::string store_cursor;
    std::string end_cursor;
    std::vector<std::string> got_keys;
    int16_t start_slot = 8192;
    int16_t end_slot = 16383;
    uint64_t count = 20;
    std::string pattern;
    RedisType type(RedisType::kRedisNone);

    Database redis_db(storage2);
    while (true) {
      if (!store_cursor.empty()) start_slot = static_cast<int16_t>(GetSlotIdFromKey(store_cursor));
      std::vector<std::string> keys;
      auto res = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor);
      ASSERT_TRUE(res.ok());
      EXPECT_LE(keys.size(), count);
      got_keys.insert(got_keys.end(), keys.begin(), keys.end());

      if (end_cursor.empty()) {
        break;
      }
      EXPECT_EQ(keys.back(), end_cursor);
      store_cursor = end_cursor;
    }
    EXPECT_EQ(got_keys.size(), slotrange_keys.size() / 2);
  }

  {
    std::string store_cursor;
    std::string end_cursor;
    int16_t start_slot = 8192;
    int16_t end_slot = 16383;
    uint64_t count = 20;
    std::string pattern = "key_1*";
    RedisType type(RedisType::kRedisNone);

    // scan will end for no matching keys
    Database redis_db(storage2);
    std::vector<std::string> keys;
    auto res = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor);
    ASSERT_TRUE(res.ok());
    ASSERT_FALSE(keys.empty());
    EXPECT_EQ("", end_cursor);
    EXPECT_EQ(start_slot, 16384);
    EXPECT_EQ(keys.size(), 11);

    // scan will skip the unmatched keys
    start_slot = 8192;
    count = 10;
    keys.clear();
    end_cursor.clear();
    auto res1 = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor, true);
    ASSERT_TRUE(res1.ok());
    ASSERT_FALSE(keys.empty());
    EXPECT_EQ(keys.back(), end_cursor);
    EXPECT_EQ(start_slot, 8192 + 18);
    EXPECT_EQ(keys.size(), 10);

    // for copi2
    start_slot = 8192;
    count = 20;
    keys.clear();
    end_cursor.clear();
    auto res2 = redis_db.Scan(store_cursor, &start_slot, end_slot, count, pattern, type, &keys, &end_cursor, true);
    ASSERT_TRUE(res2.ok());
    ASSERT_FALSE(keys.empty());
    EXPECT_EQ("", end_cursor);
    EXPECT_EQ(start_slot, 16384);
    EXPECT_EQ(keys.size(), 11);
  }
}

TEST(RedisDBTest, ExpireTest) {
  // TODO(Yangzhou) add test cases when httl is implemented
}

}  // namespace redis
