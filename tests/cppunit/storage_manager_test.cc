#include <fcntl.h>
#include <gtest/gtest.h>
#include <rocksdb/advanced_cache.h>
#include <rocksdb/env.h>
#include <rocksdb/rate_limiter.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "commands/cmd_test_util.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "server/redis_connection.h"
#include "storage/storage.h"
#include "unique_fd.h"

namespace engine {

TEST(StorageManager, simpletest) {
  Config config;
  const char* path = "test.conf";
  unlink(path);
  std::ofstream output_file(path, std::ios::out);
  output_file << "";

  auto s = config.Load(CLIOptions(path));
  config.rocks_db.compression = rocksdb::CompressionType::kNoCompression;
  config.rocks_db.write_buffer_size = 1;
  config.rocks_db.block_size = 100;
  config.max_db_size = 1;
  config.rocks_db.write_options.sync = true;
  config.rocks_db.write_options.sync_for_receiver = false;

  std::string root_dir{"datanode-unittest.XXXXXX"};
  if (!mkdtemp(root_dir.data())) {
    std::cerr << "Create root dir failed, pattern:" << root_dir << std::endl;
    exit(1);
  }

  std::vector<int> db_ids{1, 2};
  std::vector<std::string> db_pathes;
  for (auto db_id : db_ids) {
    std::string db_path = root_dir + "/" + std::to_string(db_id);
    std::filesystem::create_directories(db_path);
    bool is_dir = false;
    rocksdb::Env::Default()->IsDirectory(db_path, &is_dir);
    EXPECT_TRUE(is_dir);
    db_pathes.emplace_back(db_path);
  }
  config.datadir_list = db_pathes;

  std::set<std::string> unshared_cf_names{kPubSubColumnFamilyName, kPropagateColumnFamilyName};
  // create storage manager with existing path
  {
    config.max_io_mb = static_cast<int>(kIORateLimitMaxMb) + 1;
    auto storage_man = std::make_shared<engine::StorageManager>();
    auto s = storage_man->CreateStorages(&config);
    EXPECT_TRUE(s.IsOK());
    EXPECT_EQ(storage_man->config_, &config);
    EXPECT_TRUE(storage_man->block_cache_);
    EXPECT_TRUE(storage_man->rate_limiter_);
    EXPECT_EQ(storage_man->rate_limiter_->GetBytesPerSecond(), config.max_io_mb * MiB);
    int max_io_mb = ++config.max_io_mb;
    int block_cache_size = ++config.rocks_db.block_cache_size;
    // check block cache and io rate limiter
    for (auto db_id : db_ids) {
      auto storage = storage_man->GetStorageByDBID(db_id);
      EXPECT_TRUE(storage);
      EXPECT_EQ(storage->storage_mgr_.lock().get(), storage_man.get());
      EXPECT_EQ(storage->DefaultWriteOptions().sync, true);
      EXPECT_EQ(storage->DefaultWriteOptionsForReceiver().sync, false);
      auto db_options = storage->db_->GetDBOptions();
      // row cache is disabled by default
      EXPECT_EQ(db_options.row_cache, nullptr);
      EXPECT_EQ(storage->rate_limiter_, db_options.rate_limiter);
      EXPECT_EQ(storage->rate_limiter_, storage_man->rate_limiter_);
      for (auto& cf_handler : storage->cf_handles_) {
        auto cf_options = storage->db_->GetOptions(cf_handler);
        // row cache is disabled by default
        EXPECT_EQ(cf_options.row_cache, nullptr);
        auto block_cache = cf_options.table_factory->GetOptionsPtr(rocksdb::TableFactory::kBlockCacheOpts());
        if (unshared_cf_names.count(cf_handler->GetName())) {
          // blob cache is disabled for pubsub and propagate cf
          EXPECT_EQ(cf_options.blob_cache, nullptr);
          // block cache is not shared for pubsub and propagate cf
          EXPECT_NE(block_cache, storage_man->block_cache_.get());
        } else {
          // blob cache is enabled by default
          EXPECT_EQ(cf_options.blob_cache, storage_man->block_cache_);
          // block cache is shared for other cf
          EXPECT_EQ(block_cache, storage_man->block_cache_.get());
        }
        EXPECT_TRUE(cf_options.disable_auto_compactions);
      }
      // enable auto compactions
      EXPECT_TRUE(storage->resetDisableAutoCompactionsOption().IsOK());
      for (auto& cf_handler : storage->cf_handles_) {
        auto cf_options = storage->db_->GetOptions(cf_handler);
        EXPECT_FALSE(cf_options.disable_auto_compactions);
      }
      for (auto val : {false, true, true, false}) {
        EXPECT_TRUE(storage->setDisableAutoCompactionsOption(val).IsOK());
        for (auto& cf_handler : storage->cf_handles_) {
          auto cf_options = storage->db_->GetOptions(cf_handler);
          if (val) {
            EXPECT_TRUE(cf_options.disable_auto_compactions);
          } else {
            EXPECT_FALSE(cf_options.disable_auto_compactions);
          }
        }
      }
      // check db size limit parallelly
      std::thread t1{[&]() { storage->CheckDBSizeLimit(); }};
      std::thread t2{[&]() { storage_man->CheckDBSizeLimit(); }};
      t1.join();
      t2.join();
      EXPECT_FALSE(storage->db_size_limit_reached_);
      // update io rate limit
      for (int i = -1; i <= 0; ++i) {
        storage_man->SetIORateLimit(i);
        EXPECT_EQ(storage_man->rate_limiter_->GetBytesPerSecond(), kIORateLimitMaxMb * MiB);
      }
      storage->SetIORateLimit(++max_io_mb);
      EXPECT_EQ(storage_man->rate_limiter_->GetBytesPerSecond(), max_io_mb * MiB);
      storage_man->SetIORateLimit(++max_io_mb);
      EXPECT_EQ(storage_man->rate_limiter_->GetBytesPerSecond(), max_io_mb * MiB);
      // update block cache size
      EXPECT_FALSE(storage_man->SetBlockCacheSize(-1));
      EXPECT_TRUE(storage_man->SetBlockCacheSize(++block_cache_size));
      EXPECT_EQ(storage_man->block_cache_->GetCapacity(), block_cache_size * MiB);
    }
    for (auto db_id : db_ids) {
      auto storage = storage_man->GetStorageByDBID(db_id);
      EXPECT_TRUE(storage);
      EXPECT_EQ(storage->storage_mgr_.lock().get(), storage_man.get());
      // remove storage explictly
      std::thread t1{[&]() { storage->CheckDBSizeLimit(); }};
      std::thread t2{[&]() { storage_man->CheckDBSizeLimit(); }};
      std::thread t3([&]() { storage_man->RemoveStorage(db_id); });
      t1.join();
      t2.join();
      t3.join();
      EXPECT_FALSE(storage_man->GetStorageByDBID(db_id));
      EXPECT_FALSE(storage->db_size_limit_reached_);
      EXPECT_FALSE(storage->storage_mgr_.lock());
      storage->CheckDBSizeLimit();
      EXPECT_FALSE(storage->db_size_limit_reached_);
    }
  }
  {
    config.max_io_mb = 0;
    config.rocks_db.enable_row_cache = true;
    config.rocks_db.enable_blob_cache = false;
    config.rocks_db.write_options.sync = false;
    config.rocks_db.write_options.sync_for_receiver = true;
    config.disable_auto_compactions_before_serving = false;
    auto storage_man = std::make_shared<engine::StorageManager>();
    auto s = storage_man->CreateStorages(&config);
    EXPECT_TRUE(s.IsOK());
    EXPECT_TRUE(storage_man->block_cache_);
    EXPECT_TRUE(storage_man->rate_limiter_);
    EXPECT_EQ(storage_man->rate_limiter_->GetBytesPerSecond(), kIORateLimitMaxMb * MiB);
    for (auto db_id : db_ids) {
      auto storage = storage_man->GetStorageByDBID(db_id);
      EXPECT_TRUE(storage);
      EXPECT_EQ(storage->DefaultWriteOptions().sync, false);
      EXPECT_EQ(storage->DefaultWriteOptionsForReceiver().sync, true);
      auto db_options = storage->db_->GetDBOptions();
      // row cache is enabled
      EXPECT_EQ(db_options.row_cache, storage_man->block_cache_);
      EXPECT_EQ(storage->rate_limiter_, db_options.rate_limiter);
      EXPECT_EQ(storage->rate_limiter_, storage_man->rate_limiter_);
      for (auto& cf_handler : storage->cf_handles_) {
        auto cf_options = storage->db_->GetOptions(cf_handler);
        // blob cache is disabled
        EXPECT_EQ(cf_options.blob_cache, nullptr);
        // row cache is enabled
        EXPECT_EQ(cf_options.row_cache, storage_man->block_cache_);
        auto block_cache = cf_options.table_factory->GetOptionsPtr(rocksdb::TableFactory::kBlockCacheOpts());
        if (unshared_cf_names.count(cf_handler->GetName())) {
          // block cache is not shared for pubsub and propagate cf
          EXPECT_NE(block_cache, storage_man->block_cache_.get());
        } else {
          // block cache is shared for other cf
          EXPECT_EQ(block_cache, storage_man->block_cache_.get());
        }
        EXPECT_FALSE(cf_options.disable_auto_compactions);
      }
      // reset disable auto compactions option
      EXPECT_TRUE(storage->resetDisableAutoCompactionsOption().IsOK());
      for (auto& cf_handler : storage->cf_handles_) {
        auto cf_options = storage->db_->GetOptions(cf_handler);
        EXPECT_FALSE(cf_options.disable_auto_compactions);
      }
    }
    config.rocks_db.enable_row_cache = false;
    config.rocks_db.enable_blob_cache = true;
  }

  // failed to create storage manager when path is missing
  {
    std::error_code ec;
    std::filesystem::remove_all(db_pathes[0], ec);
    if (ec) {
      EXPECT_TRUE(false);
      std::cout << "storage_manager: Encounter filesystem error: " << ec << std::endl;
    }
    auto storage_man = std::make_shared<engine::StorageManager>();
    auto s = storage_man->CreateStorages(&config);
    EXPECT_FALSE(s.IsOK());
  }

  // failed to create storage manager when path is not a dir
  {
    {
      // create file with same name of db_path
      auto fd = UniqueFD(open(db_pathes[0].data(), O_RDWR | O_CREAT, 0660));
      if (!fd) {
        ASSERT_TRUE(false);
        std::cout << "Failed to create file: " << db_pathes[0] << ", Err: " << strerror(errno);
      }
    }
    EXPECT_TRUE(rocksdb::Env::Default()->FileExists(db_pathes[0]).ok());
    auto storage_man = std::make_shared<engine::StorageManager>();
    auto s = storage_man->CreateStorages(&config);
    EXPECT_FALSE(s.IsOK());
    // remove file
    std::filesystem::remove_all(root_dir);
    EXPECT_FALSE(rocksdb::Env::Default()->FileExists(root_dir).ok());
  }
}

TEST(StorageManager, IterateWal) {
  MockOptions opt;
  opt.cluster_id = redis::test_active_cluster_id;
  opt.datanode_id = redis::test_active_datanode_id;
  opt.pool = redis::test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {1, 2};
  // create server
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  // set topo
  auto s = redis::SetTopo(srv);
  encode_hash_sub_flag.store(false);
  ASSERT_TRUE(s.IsOK());
  // create conn
  auto worker0 = srv.GetWorkerThreads()[0]->GetWorker();
  ASSERT_TRUE(worker0 != nullptr);
  redis::Connection conn{nullptr, worker0};

  // get storages
  auto storage1 = srv.GetServer()->storage_mgr->GetStorageByDBID(1).get();
  auto storage2 = srv.GetServer()->storage_mgr->GetStorageByDBID(2).get();
  // get server ptr
  auto srv_ptr = srv.GetServer().get();

  // create cmds
  std::unique_ptr<redis::Commander> cmd_mset;
  s = srv.GetServer()->LookupAndCreateCommand("mset", &cmd_mset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<redis::Commander> cmd_hset;
  s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd_hset);
  ASSERT_TRUE(s.IsOK());

  // before witing data
  {
    // Test get latest points
    std::vector<uint64_t> wrong_db_ids{0, 1};
    std::vector<uint64_t> db_ids{1, 2};
    // 1. test wrong dbid
    std::vector<kv::datanode::v1::LatestPoint> results;
    s = srv_ptr->storage_mgr->GetLatestPoints(wrong_db_ids, &results);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("belongings") != std::string::npos);
    // 2. test right dbids
    s = srv_ptr->storage_mgr->GetLatestPoints(db_ids, &results);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(results.size(), 2);
    // check db1
    EXPECT_EQ(results[0].db_id(), db_ids[0]);
    EXPECT_EQ(results[0].seq_id(), 1);
    EXPECT_EQ(results[0].repl_id(), storage1->GetReplIdFromDbEngine().GetValue());
    // check db2
    EXPECT_EQ(results[1].db_id(), db_ids[1]);
    EXPECT_EQ(results[1].seq_id(), 1);
    EXPECT_EQ(results[1].repl_id(), storage2->GetReplIdFromDbEngine().GetValue());
  }

  {
    // Test get wal data
    uint64_t db_id = 1;
    uint64_t next_seq = 0;
    std::vector<redis::CommandTokens> results;
    bool is_finished = false;
    // 1. wrong parameters
    s = srv_ptr->storage_mgr->GetWalDataWithCmd(0, &next_seq, &results, &is_finished);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("db_id") != std::string::npos);
    // wrong seq
    next_seq = 3;
    s = srv_ptr->storage_mgr->GetWalDataWithCmd(db_id, &next_seq, &results, &is_finished);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("sequence") != std::string::npos);

    // 2. get data from wal
    next_seq = 1;
    s = srv_ptr->storage_mgr->GetWalDataWithCmd(db_id, &next_seq, &results, &is_finished);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(results.empty());
    EXPECT_TRUE(is_finished);
  }

  // Test after writing data on storage1
  {
    // construct cmd tokens for storage1
    std::string tag1{redis::global_slot_keys[10]};
    std::string tag2{redis::global_slot_keys[100]};
    std::string tag3{redis::global_slot_keys[100]};
    std::string string_key1 = "key1_{" + tag1 + "}";
    std::string string_key2 = "key2_{" + tag2 + "}";
    std::string hash_key = "hkey_{" + tag3 + "}";

    redis::CommandTokens mset_cmd_token{"MSET", string_key1, "str_val1", string_key2, "str_val2"};
    redis::CommandTokens hset_cmd_token{"HSET", hash_key, "f1", "v1", "f2", "v2"};

    // exec cmds
    std::string output;
    s = redis::GenericExecCmd(mset_cmd_token, &output, cmd_mset, srv_ptr, &conn, storage1);
    ASSERT_TRUE(s.IsOK());
    s = redis::GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage1);
    ASSERT_TRUE(s.IsOK());

    // get data from wal
    uint64_t db_id = 1;
    uint64_t next_seq = 1;
    std::vector<redis::CommandTokens> results;
    bool is_finished = false;
    s = srv_ptr->storage_mgr->GetWalDataWithCmd(db_id, &next_seq, &results, &is_finished);
    ASSERT_TRUE(s.IsOK());
    // check data
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(next_seq, 7);
    EXPECT_TRUE(is_finished);
    EXPECT_EQ(results[0], std::vector<std::string>({"MSET", string_key1, "str_val1", string_key2, "str_val2"}));
    EXPECT_EQ(results[1], std::vector<std::string>({"HSET", hash_key, "f2", "v2", "f1", "v1"}));

    // test finished
    results.clear();
    is_finished = false;
    s = srv_ptr->storage_mgr->GetWalDataWithCmd(db_id, &next_seq, &results, &is_finished);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(results.size(), 0);
    EXPECT_TRUE(is_finished);

    // test get partial data
    results.clear();
    next_seq = 4;
    is_finished = false;
    s = srv_ptr->storage_mgr->GetWalDataWithCmd(db_id, &next_seq, &results, &is_finished);
    ASSERT_TRUE(s.IsOK());
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(next_seq, 7);
    EXPECT_TRUE(is_finished);
    EXPECT_EQ(results[0], std::vector<std::string>({"HSET", hash_key, "f2", "v2", "f1", "v1"}));

    // test get with middle sequence id
    results.clear();
    next_seq = 3;
    is_finished = false;
    s = srv_ptr->storage_mgr->GetWalDataWithCmd(db_id, &next_seq, &results, &is_finished);
    ASSERT_FALSE(s.IsOK());
    EXPECT_TRUE(s.Msg().find("Unmatched") != std::string::npos);
    EXPECT_EQ(results.size(), 0);
  }
}

}  // namespace engine
