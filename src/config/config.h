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

#pragma once

#include <grpc++/grpc++.h>
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>
#include <sys/resource.h>

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "config_type.h"
#include "cron.h"
#include "grpcpp/support/channel_arguments.h"
#include "status.h"
#include "storage/redis_metadata.h"

// forward declaration
class Server;
namespace engine {
class Storage;
}

constexpr const uint32_t PORT_LIMIT = 65535;

enum SupervisedMode { kSupervisedNone = 0, kSupervisedAutoDetect, kSupervisedSystemd, kSupervisedUpStart };

constexpr const char *TLS_AUTH_CLIENTS_NO = "no";
constexpr const char *TLS_AUTH_CLIENTS_OPTIONAL = "optional";

constexpr const size_t KiB = 1024L;
constexpr const size_t MiB = 1024L * KiB;
constexpr const size_t GiB = 1024L * MiB;
constexpr const uint32_t kDefaultPort = 6666;

constexpr const char *kDefaultNamespace = "__namespace";

constexpr int kMinWriteBufferNumberToMerge = 2;

constexpr uint64_t kHashFieldMaxAbsTimeMS = ((uint64_t)0x0000FFFFFFFFFFFF) >> 2;

struct CompactionCheckerRange {
 public:
  int start;
  int stop;

  bool Enabled() const { return start != -1 || stop != -1; }
};

using CompactionCheckerFull = CompactionCheckerRange;

struct NewCronFullCompaction {
 public:
  int start_hour;
  int end_hour;
  std::vector<int> days;

  bool Enabled() const { return (start_hour != -1) && days.size() != 0; }
  bool IsInCompactionDays(int day) const { return std::find(days.begin(), days.end(), day) != days.end(); }
};

struct CLIOptions {
  std::string conf_file;
  std::vector<std::pair<std::string, std::string>> cli_options;

  CLIOptions() = default;
  explicit CLIOptions(std::string_view file) : conf_file(file) {}
};

struct Config {
 public:
  Config();
  ~Config() = default;
  uint32_t port = 0;
  uint32_t tls_port = 0;
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_key_file_pass;
  std::string tls_ca_cert_file;
  std::string tls_ca_cert_dir;
  std::string tls_auth_clients;
  bool tls_prefer_server_ciphers = false;
  std::string tls_ciphers;
  std::string tls_ciphersuites;
  std::string tls_protocols;
  bool tls_session_caching = true;
  int tls_session_cache_size = 1024 * 20;
  int tls_session_cache_timeout = 300;
  bool tls_replication = false;

  int workers = 0;
  bool worker_load_balance = false;  // Enable worker load balancing based on connection count
  int timeout = 0;
  int log_level = 0;
  int vlog_level = 0;
  int backlog = 511;
  int maxclients = 10000;
  uint32_t client_buffer_limit_mb = 4096;
  int max_backup_to_keep = 1;
  int max_backup_keep_hours = 24;
  int slow_req_record_threshold_us = 200000;
  int slow_req_record_max_len = 0;
  int slow_req_log_threshold_us = 200000;
  int slow_req_log_period_us = 100000;
  bool daemonize = false;
  SupervisedMode supervised_mode = kSupervisedNone;
  bool slave_readonly = true;
  bool slave_serve_stale_data = true;
  bool slave_empty_db_before_fullsync = false;
  int slave_priority = 100;
  int max_db_size = 0;
  int max_replication_mb = 0;
  int max_io_mb = 0;
  int max_bitmap_to_string_mb = 16;
  bool master_use_repl_port = false;
  bool purge_backup_on_fullsync = false;
  bool auto_resize_block_and_sst = true;
  int fullsync_recv_file_delay = 0;
  bool use_rsid_psync = true;
  std::vector<std::string> binds;
  std::vector<std::string> datadir_list;  // datanode option
  std::string log_dir;
  std::string db_name;
  std::string masterauth;
  std::string requirepass;
  std::string master_host;
  std::string unixsocket;
  int unixsocketperm = 0777;
  uint32_t master_port = 0;
  Cron compact_cron;
  Cron bgsave_cron;
  bool enable_compact_full;
  bool enable_compact_range;
  int max_io_mb_full_compact = 0;
  CompactionCheckerRange compaction_checker_range{-1, -1};
  CompactionCheckerFull compaction_checker_full{-1, -1};
  NewCronFullCompaction new_cron_full_compaction{-1, -1, {}};
  int64_t force_compact_file_age;
  int force_compact_file_min_deleted_percentage;
  bool repl_namespace_enabled = false;
  std::string replica_announce_ip;
  uint32_t replica_announce_port = 0;

  bool persist_cluster_nodes_enabled = true;
  bool slot_id_encoded = true;
  bool cluster_enabled = true;
  std::string cluster_id;
  std::string datanode_id;
  std::string controller_addr;
  std::string pool;
  int controller_heartbeat_interval_milliseconds;
  std::string controller_server_config_json;
  int controller_client_wait_connect_ready_timeout_ms;

  bool redis_cursor_compatible = false;
  int log_retention_days;

  // load_tokens is used to buffer the tokens when loading,
  // don't use it to authenticate or rewrite the configuration file.
  std::map<std::string, std::string> load_tokens;

  // profiling
  std::set<std::string> profiling_sample_commands;
  bool profiling_sample_all_commands = false;
  int profiling_sample_ratio = 0;
  int profiling_sample_record_threshold_us = 100000;
  int profiling_sample_record_max_len = 0;

  // json
  int json_max_nesting_depth = 1024;
  JsonStorageFormat json_storage_format = JsonStorageFormat::JSON;

  // scan
  uint32_t scan_session_max_count = 10000;
  int64_t scan_session_ttl_sec = 24 * 60 * 60;  // 1 day
  // xscan
  uint32_t xscan_session_max_count = 10000;
  int64_t xscan_session_ttl_sec = 24 * 60 * 60;  // 1 day
  bool scan_copi2_cluster = false;

  // hfe
  bool enable_hfe_cmd = false;

  // kkv
  bool enable_kkv_cmd = false;

  // cdc
  bool enable_cdc_sync = false;

  // grpc connect backoff configs
  int32_t grpc_client_initial_reconnect_backoff_ms = 1000;
  int32_t grpc_client_min_reconnect_backoff_ms = 1000;
  int32_t grpc_client_max_reconnect_backoff_ms = 3000;
  // grpc keepalive configs
  int32_t grpc_client_keepalive_time_ms = 20000;
  int32_t grpc_client_keepalive_timeout_ms = 10000;
  bool grpc_client_keepalive_permit_without_calls = true;
  int32_t grpc_client_max_pings_without_data = 2;
  int32_t grpc_server_keepalive_time_ms = 20000;
  int32_t grpc_server_keepalive_timeout_ms = 10000;
  bool grpc_server_keepalive_permit_without_calls = true;
  int32_t grpc_server_max_pings_without_data = 2;
  int32_t grpc_server_min_recv_ping_interval_ms = 8000;
  int32_t grpc_server_max_ping_strikes = 2;
  // grpc max send and receiver message length configs
  int32_t grpc_datanode_service_max_message_length = 256 * MiB;
  int32_t grpc_controller_service_max_message_length = 16 * MiB;
  // migration configs
  uint32_t stop_write_log_gap = 10000;
  uint32_t stop_write_try_lock_timeout_ms = 100;
  // max duration in ms between stop write and replicate done
  uint32_t stop_write_timeout_ms = 5000;
  // max duration in ms between replicate done and clean running status
  uint32_t stop_write_wait_topo_timeout_ms = 5000;

  bool compaction_with_filter = true;
  std::string metadata_filter_levels = "3,4,5,6";  // Skip L0/L1/L2 by default for performance
  std::string subkey_filter_levels = "3,4,5,6";    // Skip L0/L1/L2 by default for performance

  // Cached bitmasks for fast level checking (bit N = 1 means level N is enabled)
  mutable std::atomic<uint32_t> metadata_filter_mask{0b1111000};  // levels 3-6
  mutable std::atomic<uint32_t> subkey_filter_mask{0b1111000};    // levels 3-6

  uint64_t worker_blocked_threshold_seconds = 120;
  int bufferevent_write_priority = 1;
  int max_dispatch_interval_ms = 0;
  int bufferevent_max_single_write = 65536;  // 64KB
  int bufferevent_max_single_read = 0;       // 0 means use libevent default
  bool duration_ingest_close_auto_compact = false;
  bool disable_auto_compactions_before_serving = true;

  struct RocksDB {
    int block_size;
    bool cache_index_and_filter_blocks;
    int block_cache_size;
    bool enable_blob_cache;
    bool enable_row_cache;
    int max_open_files;
    int write_buffer_size;
    int max_write_buffer_number;
    int max_sub_compactions;
    int stats_dump_period_sec;
    rocksdb::StatsLevel stats_level;
    bool enable_pipelined_write;
    int64_t delayed_write_rate;
    int compaction_readahead_size;
    int target_file_size_base;
    int wal_size_limit_mb;
    bool wal_compression;
    int max_total_wal_size;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    int level0_file_num_compaction_trigger;
    rocksdb::CompressionType compression;
    bool disable_auto_compactions;
    bool enable_blob_files;
    int min_blob_size;
    int blob_file_size;
    bool enable_blob_garbage_collection;
    int blob_garbage_collection_age_cutoff;
    int blob_garbage_collection_force_threshold;
    rocksdb::CompressionType blob_compression_type;
    int max_bytes_for_level_base;
    int max_bytes_for_level_multiplier;
    bool level_compaction_dynamic_level_bytes;
    int max_background_jobs;
    bool rate_limiter_auto_tuned;
    bool avoid_unnecessary_blocking_io = true;

    // Dynamically changeable through SetOptions() API
    // Configuration can refer to
    // https://github.com/facebook/rocksdb/blob/main/include/rocksdb/advanced_options.h
    int target_file_size_multiplier = 1;
    uint32_t arena_block_size = 0;
    int64_t soft_pending_compaction_bytes_limit = 64 * 1073741824ull;
    int64_t hard_pending_compaction_bytes_limit = 256 * 1073741824ull;
    int64_t max_compaction_bytes = 0;
    int64_t max_sequential_skip_in_iterations = 8;
    bool paranoid_file_checks = false;
    bool report_bg_io_stats = false;
    int64_t sample_for_compression = 0;
    uint32_t bottommost_file_compaction_delay = 0;
    int64_t periodic_compaction_seconds = 2592000;
    int64_t ttl = 2592000;
    // Dynamically changeable through SetDBOptions() API
    // Configuration can refer to
    // https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h
    int64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;
    int writable_file_max_buffer_size = 1024 * 1024;
    int64_t bytes_per_sync = 0;
    int64_t wal_bytes_per_sync = 0;
    bool avoid_flush_during_shutdown = false;
    uint32_t stats_persist_period_sec = 600;
    int stats_history_buffer_size = 1024 * 1024;

    struct WriteOptions {
      bool sync;
      bool disable_wal;
      bool no_slowdown;
      bool low_pri;
      bool memtable_insert_hint_per_batch;
      bool sync_for_receiver;
    } write_options;

    struct ReadOptions {
      bool async_io;
      bool fill_cache;
    } read_options;
  } rocks_db;

  mutable std::mutex filter_mu;
  std::atomic_bool compact_with_filter_flag{true};

  // Helper methods to check if filtering should be enabled for a specific level (fast bit check)
  bool ShouldMetadataFilterAtLevel(int level) const {
    return level >= 0 && level < 32 && (metadata_filter_mask.load() & (1u << level)) != 0;
  }
  bool ShouldSubkeyFilterAtLevel(int level) const {
    return level >= 0 && level < 32 && (subkey_filter_mask.load() & (1u << level)) != 0;
  }

  // Update the cached bitmasks when configuration changes
  void UpdateFilterLevelMasks();

  Status Rewrite(const std::map<std::string, std::string> &tokens);
  Status Load(const CLIOptions &path, std::string *output = nullptr);
  void Get(const std::string &key, std::vector<std::string> *values) const;
  Status Set(Server *srv, std::string key, const std::string &value);
  void SetMaster(const std::string &host, uint32_t port);
  void ClearMaster();
  bool IsSlave() const { return !master_host.empty(); }
  bool HasConfigFile() const { return !path_.empty(); }
  std::string GetPidFile() const { return pidfile_; }
  static uint32_t GetGrpcPort(uint32_t port) { return port + 1000; };
  uint32_t GetGrpcPort() const { return GetGrpcPort(port); };
  grpc::ChannelArguments BuildDatanodeChannelArgs() const;
  grpc::ChannelArguments BuildControllerChannelArgs() const;
  void SetCompactWithFilter();
  void SetCompactWithoutFilter();

 private:
  std::string path_;
  std::string datadir_list_;
  std::string pidfile_;
  std::string binds_str_;
  std::string profiling_sample_commands_str_;
  std::map<std::string, std::unique_ptr<ConfigField>> fields_;
  std::vector<std::string> rename_command_;
  std::string compaction_checker_range_str_;
  std::string bgsave_cron_str_;
  std::string compaction_checker_full_str_;
  std::string new_cron_full_compaction_str_;

  void initFieldValidator();
  void initFieldCallback();
  Status parseConfigFromPair(const std::pair<std::string, std::string> &input, int line_number, std::string *output);
  Status parseConfigFromString(const std::string &input, int line_number, std::string *output);
  bool checkFieldValueIsDefault(const std::string &key, const std::string &value) const;
  Status finish() const;
  grpc::ChannelArguments buildBaseChannelArgs() const;
};
