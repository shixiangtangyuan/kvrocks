/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <vector>

#include "commands/commander.h"
#include "config/config_util.h"
#include "server/server.h"

void SetDatanodeConfItems(const char *path) {
  std::ofstream output_file(path, std::ios::app);
  output_file << "datadir-list test_datadirlist"
              << "\n"
              << "cluster-id test_clusterid"
              << "\n"
              << "datanode-id tset_datanodeid"
              << "\n"
              << "controller-addr test_addr "
              << "\n"
              << "pool test_pool"
              << "\n";
  output_file.close();
}

TEST(Config, GetAndSet) {
  const char *path = "test.conf";
  Config config;

  auto s = config.Load(CLIOptions(path));
  EXPECT_FALSE(s.IsOK());

  std::map<std::string, std::string> mutable_cases = {
      // this option will change the workers size without lock protection.
      // It may result in concurrent issue.
      // {"workers", "4"},
      {"log-level", "info"},
      {"timeout", "1000"},
      {"maxclients", "2000"},
      {"max-backup-to-keep", "1"},
      {"max-backup-keep-hours", "4000"},
      {"requirepass", "mytest_requirepass"},
      {"masterauth", "mytest_masterauth"},
      {"max-io-mb", "5000"},
      {"max-db-size", "6000"},
      {"max-replication-mb", "7000"},
      {"slave-serve-stale-data", "no"},
      {"slave-read-only", "no"},
      {"slave-priority", "101"},
      {"slow-req-record-threshold-us", "1234"},
      {"slow-req-record-max-len", "123"},
      {"slow-req-log-threshold-us", "2345"},
      {"slow-req-log-period-us", "234"},
      {"profiling-sample-ratio", "50"},
      {"profiling-sample-record-max-len", "1"},
      {"profiling-sample-record-threshold-us", "50"},
      {"profiling-sample-commands", "get,set"},
      {"rocksdb.compression", "no"},
      {"rocksdb.max_open_files", "1234"},
      {"rocksdb.write_buffer_size", "1234"},
      {"rocksdb.max_write_buffer_number", "1"},
      {"rocksdb.target_file_size_base", "100"},
      {"rocksdb.max_subcompactions", "3"},
      {"rocksdb.delayed_write_rate", "1234"},
      {"rocksdb.stats_dump_period_sec", "600"},
      {"rocksdb.compaction_readahead_size", "1024"},
      {"rocksdb.level0_slowdown_writes_trigger", "50"},
      {"rocksdb.level0_stop_writes_trigger", "100"},
      {"rocksdb.enable_blob_files", "no"},
      {"rocksdb.min_blob_size", "4096"},
      {"rocksdb.blob_file_size", "268435456"},
      {"rocksdb.enable_blob_garbage_collection", "yes"},
      {"rocksdb.blob_garbage_collection_age_cutoff", "25"},
      {"rocksdb.blob_garbage_collection_force_threshold", "75"},
      {"rocksdb.max_bytes_for_level_base", "268435456"},
      {"rocksdb.max_bytes_for_level_multiplier", "10"},
      {"rocksdb.level_compaction_dynamic_level_bytes", "yes"},
      {"rocksdb.max_background_jobs", "4"},
      {"rocksdb.target_file_size_multiplier", "4096"},
      {"rocksdb.arena_block_size", "4096"},
      {"rocksdb.soft_pending_compaction_bytes_limit", "26843545600"},
      {"rocksdb.hard_pending_compaction_bytes_limit", "268435456000"},
      {"rocksdb.max_compaction_bytes", "26843545600"},
      {"rocksdb.max_sequential_skip_in_iterations", "8"},
      {"rocksdb.paranoid_file_checks", "yes"},
      {"rocksdb.report_bg_io_stats", "yes"},
      {"rocksdb.sample_for_compression", "16"},
      {"rocksdb.delete_obsolete_files_period_micros", "28800"},
      {"rocksdb.writable_file_max_buffer_size", "8192"},
      {"rocksdb.bytes_per_sync", "16"},
      {"rocksdb.wal_bytes_per_sync", "16"},
      {"rocksdb.avoid_flush_during_shutdown", "yes"},
      {"rocksdb.stats_persist_period_sec", "3600"},
      {"rocksdb.stats_history_buffer_size", "4096"},
      {"rocksdb.bottommost_file_compaction_delay", "1"},
      {"rocksdb.periodic_compaction_seconds", "2592001"},
      {"rocksdb.ttl", "2592001"}};
  std::vector<std::string> values;
  for (const auto &iter : mutable_cases) {
    s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_TRUE(s.IsOK()) << "key:" << iter.first << ", value:" << iter.second;
    config.Get(iter.first, &values);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], iter.first);
    if (iter.first == "rocksdb.max_write_buffer_number") {
      auto ret = ParseInt(iter.second);
      ASSERT_TRUE(ret.IsOK());
      if (auto num = ret.GetValue(); num <= kMinWriteBufferNumberToMerge) {
        EXPECT_EQ(values[1], std::to_string(kMinWriteBufferNumberToMerge + 1));
        continue;
      }
    }
    EXPECT_EQ(values[1], iter.second);
  }
  ASSERT_TRUE(config.Rewrite({}).IsOK());
  SetDatanodeConfItems(path);
  s = config.Load(CLIOptions(path));
  EXPECT_TRUE(s.IsOK());
  for (const auto &iter : mutable_cases) {
    s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_TRUE(s.IsOK());
    config.Get(iter.first, &values);
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], iter.first);
    if (iter.first == "rocksdb.max_write_buffer_number") {
      auto ret = ParseInt(iter.second);
      ASSERT_TRUE(ret.IsOK());
      if (auto num = ret.GetValue(); num <= kMinWriteBufferNumberToMerge) {
        EXPECT_EQ(values[1], std::to_string(kMinWriteBufferNumberToMerge + 1));
        continue;
      }
    }
    EXPECT_EQ(values[1], iter.second);
  }
  unlink(path);

  std::map<std::string, std::string> immutable_cases = {
      {"daemonize", "yes"},
      {"bind", "0.0.0.0"},
      {"repl-bind", "0.0.0.0"},
      {"repl-workers", "8"},
      {"tcp-backlog", "500"},
      {"slaveof", "no one"},
      {"db-name", "test_dbname"},
      {"dir", "test_dir"},
      {"pidfile", "test.pid"},
      {"supervised", "no"},
      {"rocksdb.block_size", "1234"},
      {"rocksdb.wal_size_limit_mb", "16"},
      {"rocksdb.enable_pipelined_write", "no"},
      {"rocksdb.cache_index_and_filter_blocks", "no"},
      {"rocksdb.metadata_block_cache_size", "100"},
      {"rocksdb.subkey_block_cache_size", "100"},
      {"rocksdb.row_cache_size", "100"},
      {"rocksdb.rate_limiter_auto_tuned", "yes"},
  };
  for (const auto &iter : immutable_cases) {
    s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_FALSE(s.IsOK());
  }
}

TEST(Config, GetRenameCommand) {
  const char *path = "test.conf";
  unlink(path);

  std::ofstream output_file(path, std::ios::out);
  output_file << "rename-command AUTH AUTH_NEW"
              << "\n";
  output_file << "rename-command GET GET_NEW"
              << "\n";
  output_file << "rename-command SET SET_NEW"
              << "\n";
  output_file.close();
  SetDatanodeConfItems(path);
  redis::CommandTable::Reset();
  Config config;
  auto s = config.Load(CLIOptions(path));
  ASSERT_TRUE(s.IsOK());
  std::vector<std::string> values;
  config.Get("rename-command", &values);
  ASSERT_EQ(values[1], "AUTH AUTH_NEW");
  ASSERT_EQ(values[3], "GET GET_NEW");
  ASSERT_EQ(values[5], "SET SET_NEW");
  ASSERT_EQ(values[0], "rename-command");
  ASSERT_EQ(values[2], "rename-command");
  ASSERT_EQ(values[4], "rename-command");
}

TEST(Config, Rewrite) {
  const char *path = "test.conf";
  unlink(path);

  std::ofstream output_file(path, std::ios::out);
  output_file << "rename-command AUTH AUTH_NEW"
              << "\n";
  output_file << "rename-command GET GET_NEW"
              << "\n";
  output_file << "rename-command SET SET_NEW"
              << "\n";
  output_file.close();
  SetDatanodeConfItems(path);

  redis::CommandTable::Reset();
  Config config;
  auto s = config.Load(CLIOptions(path));
  ASSERT_TRUE(s.IsOK());
  ASSERT_TRUE(config.Rewrite({}).IsOK());
  // Need to re-populate the command table since it has renamed by the previous
  redis::CommandTable::Reset();
  Config new_config;
  ASSERT_TRUE(new_config.Load(CLIOptions(path)).IsOK());
  unlink(path);
}

TEST(Config, ParseConfigLine) {
  ASSERT_EQ(*ParseConfigLine(""), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("# hello"), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("       #x y z "), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("key value  "), (ConfigKV{"key", "value"}));
  ASSERT_EQ(*ParseConfigLine("key value#x"), (ConfigKV{"key", "value"}));
  ASSERT_EQ(*ParseConfigLine("key"), (ConfigKV{"key", ""}));
  ASSERT_EQ(*ParseConfigLine("    key    value1   value2   "), (ConfigKV{"key", "value1   value2"}));
  ASSERT_EQ(*ParseConfigLine(" #"), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("  key val ue #h e l l o"), (ConfigKV{"key", "val ue"}));
  ASSERT_EQ(*ParseConfigLine("key 'val ue'"), (ConfigKV{"key", "val ue"}));
  ASSERT_EQ(*ParseConfigLine(R"(key ' value\'\'v a l ')"), (ConfigKV{"key", " value''v a l "}));
  ASSERT_EQ(*ParseConfigLine(R"( key "val # hi" # hello!)"), (ConfigKV{"key", "val # hi"}));
  ASSERT_EQ(*ParseConfigLine(R"(key "\n \r \t ")"), (ConfigKV{"key", "\n \r \t "}));
  ASSERT_EQ(*ParseConfigLine("key ''"), (ConfigKV{"key", ""}));
  ASSERT_FALSE(ParseConfigLine("key \"hello "));
  ASSERT_FALSE(ParseConfigLine("key \'\\"));
  ASSERT_FALSE(ParseConfigLine("key \"hello'"));
  ASSERT_FALSE(ParseConfigLine("key \""));
  ASSERT_FALSE(ParseConfigLine("key '' ''"));
  ASSERT_FALSE(ParseConfigLine("key '' x"));
}

TEST(Config, DumpConfigLine) {
  ASSERT_EQ(DumpConfigLine({"key", "value"}), "key value");
  ASSERT_EQ(DumpConfigLine({"key", " v a l "}), R"(key " v a l ")");
  ASSERT_EQ(DumpConfigLine({"a", "'b"}), "a \"\\'b\"");
  ASSERT_EQ(DumpConfigLine({"a", "x#y"}), "a \"x#y\"");
  ASSERT_EQ(DumpConfigLine({"a", "x y"}), "a \"x y\"");
  ASSERT_EQ(DumpConfigLine({"a", "xy"}), "a xy");
  ASSERT_EQ(DumpConfigLine({"a", "x\n"}), "a \"x\\n\"");
}
