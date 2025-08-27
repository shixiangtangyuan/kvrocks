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

#include "config.h"

#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/statistics.h>
#include <strings.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "commands/commander.h"
#include "config_type.h"
#include "config_util.h"
#include "grpcpp/support/channel_arguments.h"
#include "parse_util.h"
#include "rocksdb/compression_type.h"
#include "server/server.h"
#include "status.h"
#include "storage/redis_metadata.h"

constexpr const char *errBlobDbNotEnabled = "Must set rocksdb.enable_blob_files to yes first.";
constexpr const char *errLevelCompactionDynamicLevelBytesNotSet =
    "Must set rocksdb.level_compaction_dynamic_level_bytes yes first.";

const std::vector<ConfigEnum<SupervisedMode>> supervised_modes{
    {"no", kSupervisedNone},
    {"auto", kSupervisedAutoDetect},
    {"upstart", kSupervisedUpStart},
    {"systemd", kSupervisedSystemd},
};

const std::vector<ConfigEnum<int>> log_levels{
    {"info", google::INFO},
    {"warning", google::WARNING},
    {"error", google::ERROR},
    {"fatal", google::FATAL},
};

const std::vector<ConfigEnum<JsonStorageFormat>> json_storage_formats{{"json", JsonStorageFormat::JSON},
                                                                      {"cbor", JsonStorageFormat::CBOR}};

const std::vector<ConfigEnum<rocksdb::CompressionType>> compression_types{[] {
  std::vector<ConfigEnum<rocksdb::CompressionType>> res;
  res.reserve(engine::CompressionOptions.size());
  for (const auto &e : engine::CompressionOptions) {
    res.push_back({e.name, e.type});
  }
  return res;
}()};

const std::vector<ConfigEnum<rocksdb::StatsLevel>> stats_levels{
    {"kDisableAll", rocksdb::StatsLevel::kDisableAll},
    {"kExceptHistogramOrTimers", rocksdb::StatsLevel::kExceptHistogramOrTimers},
    {"kExceptTimers", rocksdb::StatsLevel::kExceptTimers},
    {"kExceptDetailedTimers", rocksdb::StatsLevel::kExceptDetailedTimers},
    {"kExceptTimeForMutex", rocksdb::StatsLevel::kExceptTimeForMutex},
    {"kAll", rocksdb::StatsLevel::kAll}};

// Forward declaration for ValidateLevelConfig
static Status ValidateLevelConfig(const std::string &config);

std::string TrimRocksDbPrefix(std::string s) {
  if (strncasecmp(s.data(), "rocksdb.", 8) != 0) return s;
  return s.substr(8, s.size() - 8);
}

Config::Config() {
  struct FieldWrapper {
    std::string name;
    bool readonly;
    std::unique_ptr<ConfigField> field;

    FieldWrapper(std::string name, bool readonly, ConfigField *field)
        : name(std::move(name)), readonly(readonly), field(field) {}
  };

  FieldWrapper fields[] = {
      {"daemonize", true, new YesNoField(&daemonize, false)},
      {"bind", true, new StringField(&binds_str_, "0.0.0.0")},
      {"port", true, new UInt32Field(&port, kDefaultPort, 1, PORT_LIMIT)},
#ifdef ENABLE_OPENSSL
      {"tls-port", true, new UInt32Field(&tls_port, 0, 0, PORT_LIMIT)},
      {"tls-cert-file", false, new StringField(&tls_cert_file, "")},
      {"tls-key-file", false, new StringField(&tls_key_file, "")},
      {"tls-key-file-pass", false, new StringField(&tls_key_file_pass, "")},
      {"tls-ca-cert-file", false, new StringField(&tls_ca_cert_file, "")},
      {"tls-ca-cert-dir", false, new StringField(&tls_ca_cert_dir, "")},
      {"tls-protocols", false, new StringField(&tls_protocols, "")},
      {"tls-auth-clients", false, new StringField(&tls_auth_clients, "")},
      {"tls-ciphers", false, new StringField(&tls_ciphers, "")},
      {"tls-ciphersuites", false, new StringField(&tls_ciphersuites, "")},
      {"tls-prefer-server-ciphers", false, new YesNoField(&tls_prefer_server_ciphers, false)},
      {"tls-session-caching", false, new YesNoField(&tls_session_caching, true)},
      {"tls-session-cache-size", false, new IntField(&tls_session_cache_size, 1024 * 20, 0, INT_MAX)},
      {"tls-session-cache-timeout", false, new IntField(&tls_session_cache_timeout, 300, 0, INT_MAX)},
      {"tls-replication", true, new YesNoField(&tls_replication, false)},
#endif
      {"workers", true, new IntField(&workers, 8, 1, 256)},
      {"worker-load-balance", true, new YesNoField(&worker_load_balance, true)},
      {"timeout", false, new IntField(&timeout, 0, 0, INT_MAX)},
      {"tcp-backlog", true, new IntField(&backlog, 511, 0, INT_MAX)},
      {"maxclients", false, new IntField(&maxclients, 10000, 0, INT_MAX)},
      {"client-buffer-limit-mb", false, new UInt32Field(&client_buffer_limit_mb, 4096, 0, UINT32_MAX)},
      {"max-backup-to-keep", false, new IntField(&max_backup_to_keep, 1, 0, 1)},
      {"max-backup-keep-hours", false, new IntField(&max_backup_keep_hours, 0, 0, INT_MAX)},
      {"master-use-repl-port", false, new YesNoField(&master_use_repl_port, false)},
      {"requirepass", false, new StringField(&requirepass, "")},
      {"masterauth", false, new StringField(&masterauth, "")},
      {"replica-announce-ip", false, new StringField(&replica_announce_ip, "")},
      {"replica-announce-port", false, new UInt32Field(&replica_announce_port, 0, 0, PORT_LIMIT)},
      {"compaction-checker-range", false, new StringField(&compaction_checker_range_str_, "")},
      {"enable-compact-range", false, new YesNoField(&enable_compact_range, true)},
      {"enable-compact-full", false, new YesNoField(&enable_compact_full, false)},
      {"compaction-checker-full", false, new StringField(&compaction_checker_full_str_, "")},
      {"cron-full-compaction", false, new StringField(&new_cron_full_compaction_str_, "")},
      {"max-io-mb-full-compact", false, new IntField(&max_io_mb_full_compact, 100, 0, INT_MAX)},
      {"bgsave-cron", false, new StringField(&bgsave_cron_str_, "")},
      {"force-compact-file-age", false, new Int64Field(&force_compact_file_age, 2 * 24 * 3600, 60, INT64_MAX)},
      {"force-compact-file-min-deleted-percentage", false,
       new IntField(&force_compact_file_min_deleted_percentage, 10, 1, 100)},
      {"db-name", true, new StringField(&db_name, "datanode.db")},
      {"log-dir", true, new StringField(&log_dir, "/data/log/kv-datanode")},
      {"log-level", false, new EnumField<int>(&log_level, log_levels, google::INFO)},
      {"vlog-level", false, new IntField(&vlog_level, 0, 0, INT_MAX)},
      {"pidfile", true, new StringField(&pidfile_, "/data/kv-datanode/datanode.pid")},
      {"max-io-mb", false, new IntField(&max_io_mb, 0, 0, INT_MAX)},
      {"max-bitmap-to-string-mb", false, new IntField(&max_bitmap_to_string_mb, 16, 0, INT_MAX)},
      {"max-db-size", false, new IntField(&max_db_size, 0, 0, INT_MAX)},
      {"max-replication-mb", false, new IntField(&max_replication_mb, 0, 0, INT_MAX)},
      {"supervised", true, new EnumField<SupervisedMode>(&supervised_mode, supervised_modes, kSupervisedNone)},
      {"slave-serve-stale-data", false, new YesNoField(&slave_serve_stale_data, true)},
      {"slave-empty-db-before-fullsync", false, new YesNoField(&slave_empty_db_before_fullsync, false)},
      {"slave-priority", false, new IntField(&slave_priority, 100, 0, INT_MAX)},
      {"slave-read-only", false, new YesNoField(&slave_readonly, true)},
      {"use-rsid-psync", true, new YesNoField(&use_rsid_psync, true)},
      {"profiling-sample-commands", false, new StringField(&profiling_sample_commands_str_, "")},
      {"profiling-sample-ratio", false, new IntField(&profiling_sample_ratio, 0, 0, 100)},
      {"profiling-sample-record-threshold-us", false,
       new IntField(&profiling_sample_record_threshold_us, 100000, -1, INT_MAX)},
      {"profiling-sample-record-max-len", false, new IntField(&profiling_sample_record_max_len, 0, 0, INT_MAX)},
      {"slow-req-record-threshold-us", false, new IntField(&slow_req_record_threshold_us, 200000, -1, INT_MAX)},
      {"slow-req-record-max-len", false, new IntField(&slow_req_record_max_len, 0, 0, INT_MAX)},
      {"slow-req-log-threshold-us", false, new IntField(&slow_req_log_threshold_us, 200000, -1, INT_MAX)},
      {"slow-req-log-period-us", false, new IntField(&slow_req_log_period_us, 100000, -1, INT_MAX)},
      {"purge-backup-on-fullsync", false, new YesNoField(&purge_backup_on_fullsync, false)},
      {"rename-command", true, new MultiStringField(&rename_command_, std::vector<std::string>{})},
      {"auto-resize-block-and-sst", false, new YesNoField(&auto_resize_block_and_sst, true)},
      {"fullsync-recv-file-delay", false, new IntField(&fullsync_recv_file_delay, 0, 0, INT_MAX)},
      {"cluster-enabled", true, new YesNoField(&cluster_enabled, true)},
      {"unixsocket", true, new StringField(&unixsocket, "")},
      {"unixsocketperm", true, new OctalField(&unixsocketperm, 0777, 1, INT_MAX)},
      {"log-retention-days", false, new IntField(&log_retention_days, 3, -1, INT_MAX)},
      {"redis-cursor-compatible", false, new YesNoField(&redis_cursor_compatible, false)},
      {"repl-namespace-enabled", false, new YesNoField(&repl_namespace_enabled, false)},
      {"json-max-nesting-depth", false, new IntField(&json_max_nesting_depth, 1024, 0, INT_MAX)},
      {"json-storage-format", false,
       new EnumField<JsonStorageFormat>(&json_storage_format, json_storage_formats, JsonStorageFormat::JSON)},

      // datanode options
      {"datadir-list", true, new StringField(&datadir_list_, "")},
      {"cluster-id", true, new StringField(&cluster_id, "")},
      {"datanode-id", true, new StringField(&datanode_id, "")},
      {"controller-addr", true, new StringField(&controller_addr, "")},
      {"pool", true, new StringField(&pool, "")},
      {"controller-heartbeat-interval-milliseconds", false,
       new IntField(&controller_heartbeat_interval_milliseconds, 2000, 1, 3600000)},
      {"controller-server-config-json", false,
       new StringField(
           &controller_server_config_json,
           "{\"loadBalancingPolicy\":\"round_robin\", "
           "\"healthCheckConfig\":{\"serviceName\":\"kv.controller.v1.ControllerApiService\"}, "
           "\"methodConfig\":[{\"name\":[{}], \"retryPolicy\":{\"maxAttempts\":5, \"initialBackoff\": \"0.1s\", "
           "\"maxBackoff\":\"3s\", \"backoffMultiplier\":2, \"retryableStatusCodes\":[\"UNAVAILABLE\"]}}]}")},
      {"controller-client-wait-connect-ready-timeout-ms", false,
       new IntField(&controller_client_wait_connect_ready_timeout_ms, 5000, 5000, INT_MAX)},
      {"scan-session-max-count", false, new UInt32Field(&scan_session_max_count, 10000, 1, 100000)},
      {"scan-session-ttl-seconds", false, new Int64Field(&scan_session_ttl_sec, 86400, 0, LONG_MAX)},
      {"xscan-session-max-count", false, new UInt32Field(&xscan_session_max_count, 10000, 1, 100000)},
      {"xscan-session-ttl-seconds", false, new Int64Field(&xscan_session_ttl_sec, 86400, 0, LONG_MAX)},
      {"scan-copi2-cluster", false, new YesNoField(&scan_copi2_cluster, false)},
      {"enable-hfe-cmd", false, new YesNoField(&enable_hfe_cmd, false)},
      {"enable-kkv-cmd", false, new YesNoField(&enable_kkv_cmd, false)},
      // TODO(ying.qiu): set default value to false
      {"enable-cdc-sync", true, new YesNoField(&enable_cdc_sync, false)},

      // grpc connect backoff configs
      // refer to https://grpc.github.io/grpc/core/md_doc_connection-backoff.html
      {"grpc-client-initial-reconnect-backoff-ms", false,
       new IntField(&grpc_client_initial_reconnect_backoff_ms, 1000, 1, INT32_MAX)},
      // grpc_client_min_reconnect_backoff_ms shuld be greater than or equal to 5000ms
      // refer to https://github.com/grpc/grpc/issues/33593
      {"grpc-client-min-reconnect-backoff-ms", false,
       new IntField(&grpc_client_min_reconnect_backoff_ms, 1000, 1, INT32_MAX)},
      {"grpc-client-max-reconnect-backoff-ms", false,
       new IntField(&grpc_client_max_reconnect_backoff_ms, 3000, 1, INT32_MAX)},
      // grpc keepalive configs
      // refer to https://grpc.github.io/grpc/core/md_doc_keepalive.html
      {"grpc-client-keepalive-time-ms", false, new IntField(&grpc_client_keepalive_time_ms, 20000, 1, INT32_MAX)},
      {"grpc-client-keepalive-timeout-ms", false, new IntField(&grpc_client_keepalive_timeout_ms, 10000, 1, INT32_MAX)},
      {"grpc-client-keepalive-permit-without-calls", false,
       new YesNoField(&grpc_client_keepalive_permit_without_calls, true)},
      {"grpc-client-max-pings-without-data", false, new IntField(&grpc_client_max_pings_without_data, 2, 0, INT32_MAX)},
      {"grpc-server-keepalive-time-ms", true, new IntField(&grpc_server_keepalive_time_ms, 20000, 1, INT32_MAX)},
      {"grpc-server-keepalive-timeout-ms", true, new IntField(&grpc_server_keepalive_timeout_ms, 10000, 1, INT32_MAX)},
      {"grpc-server-keepalive-permit-without-calls", true,
       new YesNoField(&grpc_server_keepalive_permit_without_calls, true)},
      {"grpc-server-max-pings-without-data", true, new IntField(&grpc_server_max_pings_without_data, 2, 0, INT32_MAX)},
      {"grpc-server-min-recv-ping-interval-ms", true,
       new IntField(&grpc_server_min_recv_ping_interval_ms, 8000, 1, INT32_MAX)},
      {"grpc-server-max-ping-strikes", true, new IntField(&grpc_server_max_ping_strikes, 2, 0, INT32_MAX)},
      // grpc max send and receive message length configs
      {"grpc-datanode-service-max-message-length", false,
       new IntField(&grpc_datanode_service_max_message_length, 256 * MiB, 16 * MiB, INT32_MAX)},
      {"grpc-controller-service-max-message-length", true,
       new IntField(&grpc_controller_service_max_message_length, 16 * MiB, 4 * MiB, INT32_MAX)},
      // migration configs
      {"stop-write-log-gap", false, new UInt32Field(&stop_write_log_gap, 10000, 1, UINT32_MAX)},
      {"stop-write-try-lock-timeout-ms", false, new UInt32Field(&stop_write_try_lock_timeout_ms, 100, 1, UINT32_MAX)},
      {"stop-write-timeout-ms", false, new UInt32Field(&stop_write_timeout_ms, 5000, 1, UINT32_MAX)},
      {"stop-write-wait-topo-timeout-ms", false,
       new UInt32Field(&stop_write_wait_topo_timeout_ms, 11000, 1, UINT32_MAX)},
      // compaction configs
      {"compaction-with-filter", false, new YesNoField(&compaction_with_filter, true)},
      // Control which compaction levels should apply metadata filtering (expired key cleanup)
      // Format: "level1,level2,level3" - comma separated level numbers (input level from compaction)
      // Examples:
      //   "2,3,4,5,6" - Apply filter on levels 2,3,4,5,6
      //   "0,1,2" - Apply filter on levels 0,1,2
      //   "" - Disable filter on all levels (empty string)
      // Note: level here refers to the input level parameter passed to the compaction filter
      // Default: "3,4,5,6" - Skip filtering on L0/L1/L2 for better write performance
      {"metadata-filter-levels", false, new StringField(&metadata_filter_levels, "3,4,5,6")},
      // Control which compaction levels should apply subkey filtering (expired subkey cleanup)
      // Same format as metadata-filter-levels
      // Note: level here refers to the input level parameter passed to the compaction filter
      // Default: "3,4,5,6" - Skip filtering on L0/L1/L2 for better write performance
      {"subkey-filter-levels", false, new StringField(&subkey_filter_levels, "3,4,5,6")},
      // ingest configs
      {"duration-ingest-close-auto-compact", false, new YesNoField(&duration_ingest_close_auto_compact, false)},
      {"disable-auto-compactions-before-serveing", true,
       new YesNoField(&disable_auto_compactions_before_serving, true)},

      // worker
      {"worker-blocked-threshold-seconds", false,
       new UInt64Field(&worker_blocked_threshold_seconds, 120, 0, UINT64_MAX)},
      {"bufferevent-write-priority", false, new IntField(&bufferevent_write_priority, 1, 0, 2)},
      {"max-dispatch-interval-ms", true, new IntField(&max_dispatch_interval_ms, 0, 0, INT_MAX)},
      {"bufferevent-max-single-write", true, new IntField(&bufferevent_max_single_write, 65536, 0, INT_MAX)},
      {"bufferevent-max-single-read", true, new IntField(&bufferevent_max_single_read, 0, 0, INT_MAX)},

      /* rocksdb options */
      {"rocksdb.compression", false,
       new EnumField<rocksdb::CompressionType>(&rocks_db.compression, compression_types,
                                               rocksdb::CompressionType::kNoCompression)},
      {"rocksdb.block_size", true, new IntField(&rocks_db.block_size, 16384, 0, INT_MAX)},
      {"rocksdb.max_open_files", false, new IntField(&rocks_db.max_open_files, 8096, -1, INT_MAX)},
      {"rocksdb.write_buffer_size", false, new IntField(&rocks_db.write_buffer_size, 64, 0, 4096)},
      {"rocksdb.max_write_buffer_number", false, new IntField(&rocks_db.max_write_buffer_number, 4, 0, 256)},
      {"rocksdb.target_file_size_base", false, new IntField(&rocks_db.target_file_size_base, 128, 1, 1024)},
      {"rocksdb.max_subcompactions", false, new IntField(&rocks_db.max_sub_compactions, 2, 0, 16)},
      {"rocksdb.delayed_write_rate", false, new Int64Field(&rocks_db.delayed_write_rate, 0, 0, INT64_MAX)},
      {"rocksdb.wal_size_limit_mb", true, new IntField(&rocks_db.wal_size_limit_mb, 16384, 0, INT_MAX)},
      {"rocksdb.wal_compression", true, new YesNoField(&rocks_db.wal_compression, false)},
      {"rocksdb.max_total_wal_size", false, new IntField(&rocks_db.max_total_wal_size, 64 * 4 * 2, 0, INT_MAX)},
      {"rocksdb.disable_auto_compactions", false, new YesNoField(&rocks_db.disable_auto_compactions, false)},
      {"rocksdb.enable_pipelined_write", true, new YesNoField(&rocks_db.enable_pipelined_write, false)},
      {"rocksdb.stats_dump_period_sec", false, new IntField(&rocks_db.stats_dump_period_sec, 600, 0, INT_MAX)},
      {"rocksdb.stats_level", false,
       new EnumField<rocksdb::StatsLevel>(&rocks_db.stats_level, stats_levels,
                                          rocksdb::StatsLevel::kExceptDetailedTimers)},
      {"rocksdb.cache_index_and_filter_blocks", true, new YesNoField(&rocks_db.cache_index_and_filter_blocks, true)},
      {"rocksdb.block_cache_size", false, new IntField(&rocks_db.block_cache_size, 4096, 0, INT_MAX)},
      {"rocksdb.enable_blob_cache", true, new YesNoField(&rocks_db.enable_blob_cache, true)},
      {"rocksdb.enable_row_cache", true, new YesNoField(&rocks_db.enable_row_cache, false)},
      {"rocksdb.compaction_readahead_size", false,
       new IntField(&rocks_db.compaction_readahead_size, 2 * MiB, 0, 64 * MiB)},
      {"rocksdb.level0_slowdown_writes_trigger", false,
       new IntField(&rocks_db.level0_slowdown_writes_trigger, 20, 1, 1024)},
      {"rocksdb.level0_stop_writes_trigger", false, new IntField(&rocks_db.level0_stop_writes_trigger, 40, 1, 1024)},
      {"rocksdb.level0_file_num_compaction_trigger", false,
       new IntField(&rocks_db.level0_file_num_compaction_trigger, 4, 1, 1024)},
      {"rocksdb.enable_blob_files", false, new YesNoField(&rocks_db.enable_blob_files, false)},
      {"rocksdb.min_blob_size", false, new IntField(&rocks_db.min_blob_size, 4096, 0, INT_MAX)},
      {"rocksdb.blob_file_size", false, new IntField(&rocks_db.blob_file_size, 268435456, 0, INT_MAX)},
      {"rocksdb.enable_blob_garbage_collection", false, new YesNoField(&rocks_db.enable_blob_garbage_collection, true)},
      {"rocksdb.blob_garbage_collection_age_cutoff", false,
       new IntField(&rocks_db.blob_garbage_collection_age_cutoff, 25, 0, 100)},
      {"rocksdb.blob_garbage_collection_force_threshold", false,
       new IntField(&rocks_db.blob_garbage_collection_force_threshold, 75, 0, 100)},
      {"rocksdb.blob_compression_type", false,
       new EnumField<rocksdb::CompressionType>(&rocks_db.blob_compression_type, compression_types,
                                               rocksdb::CompressionType::kNoCompression)},
      {"rocksdb.max_bytes_for_level_base", false,
       new IntField(&rocks_db.max_bytes_for_level_base, 268435456, 0, INT_MAX)},
      {"rocksdb.max_bytes_for_level_multiplier", false,
       new IntField(&rocks_db.max_bytes_for_level_multiplier, 10, 1, 100)},
      {"rocksdb.level_compaction_dynamic_level_bytes", false,
       new YesNoField(&rocks_db.level_compaction_dynamic_level_bytes, true)},
      {"rocksdb.max_background_jobs", false, new IntField(&rocks_db.max_background_jobs, 4, 0, 32)},
      {"rocksdb.rate_limiter_auto_tuned", true, new YesNoField(&rocks_db.rate_limiter_auto_tuned, false)},
      {"rocksdb.avoid_unnecessary_blocking_io", true, new YesNoField(&rocks_db.avoid_unnecessary_blocking_io, true)},
      {"rocksdb.target_file_size_multiplier", false,
       new IntField(&rocks_db.target_file_size_multiplier, 1, 0, INT_MAX)},
      {"rocksdb.arena_block_size", false, new UInt32Field(&rocks_db.arena_block_size, 0, 4096, 2 << 30)},
      {"rocksdb.soft_pending_compaction_bytes_limit", false,
       new Int64Field(&rocks_db.soft_pending_compaction_bytes_limit, 64 * 1073741824ull, 0, INT64_MAX)},
      {"rocksdb.hard_pending_compaction_bytes_limit", false,
       new Int64Field(&rocks_db.hard_pending_compaction_bytes_limit, 256 * 1073741824ull, 0, INT64_MAX)},
      {"rocksdb.max_compaction_bytes", false, new Int64Field(&rocks_db.max_compaction_bytes, 0, 0, INT64_MAX)},
      {"rocksdb.max_sequential_skip_in_iterations", false,
       new Int64Field(&rocks_db.max_sequential_skip_in_iterations, 8, 0, INT64_MAX)},
      {"rocksdb.paranoid_file_checks", false, new YesNoField(&rocks_db.paranoid_file_checks, false)},
      {"rocksdb.report_bg_io_stats", false, new YesNoField(&rocks_db.report_bg_io_stats, false)},
      {"rocksdb.sample_for_compression", false, new Int64Field(&rocks_db.sample_for_compression, 0, 0, INT64_MAX)},
      {"rocksdb.bottommost_file_compaction_delay", false,
       new UInt32Field(&rocks_db.bottommost_file_compaction_delay, 0, 0, 2 << 30)},
      // Rocksdb Original definitions and default values
      // uint64_t periodic_compaction_seconds = 0xfffffffffffffffe;
      // Default value: 30 days (2592000 seconds)if using block based table format + compaction filter +
      // leveled compaction or block based table format + universal compaction. 0 (disabled) otherwise
      // Datanode using block based table format + compaction filter + leveled compaction, so
      // default value is 2592000 seconds
      {"rocksdb.periodic_compaction_seconds", false,
       new Int64Field(&rocks_db.periodic_compaction_seconds, 2592000, 0, INT64_MAX)},
      // Rocksdb Original definitions and default values
      // uint64_t ttl = 0xfffffffffffffffe;
      // Default value: 30 days (2592000 seconds)if using block based table. 0 (disable) otherwise.
      // Datanode using block based table. so ttl default valuse is 2592000 seconds
      {"rocksdb.ttl", false, new Int64Field(&rocks_db.ttl, 2592000, 0, INT64_MAX)},
      {"rocksdb.delete_obsolete_files_period_micros", false,
       new Int64Field(&rocks_db.delete_obsolete_files_period_micros, 6ULL * 60 * 60 * 1000000, 0, INT64_MAX)},
      {"rocksdb.writable_file_max_buffer_size", false,
       new IntField(&rocks_db.writable_file_max_buffer_size, 1024 * 1024, 0, INT_MAX)},
      {"rocksdb.bytes_per_sync", false, new Int64Field(&rocks_db.bytes_per_sync, 0, 0, INT64_MAX)},
      {"rocksdb.wal_bytes_per_sync", false, new Int64Field(&rocks_db.wal_bytes_per_sync, 0, 0, INT64_MAX)},
      {"rocksdb.avoid_flush_during_shutdown", false, new YesNoField(&rocks_db.avoid_flush_during_shutdown, false)},
      {"rocksdb.stats_persist_period_sec", false,
       new UInt32Field(&rocks_db.stats_persist_period_sec, 0, 0, UINT32_MAX)},
      {"rocksdb.stats_history_buffer_size", false,
       new IntField(&rocks_db.stats_history_buffer_size, 1024 * 1024, 0, INT32_MAX)},
      /* rocksdb write options */
      {"rocksdb.write_options.sync", true, new YesNoField(&rocks_db.write_options.sync, false)},
      {"rocksdb.write_options.disable_wal", true, new YesNoField(&rocks_db.write_options.disable_wal, false)},
      {"rocksdb.write_options.no_slowdown", true, new YesNoField(&rocks_db.write_options.no_slowdown, false)},
      {"rocksdb.write_options.low_pri", true, new YesNoField(&rocks_db.write_options.low_pri, false)},
      {"rocksdb.write_options.memtable_insert_hint_per_batch", true,
       new YesNoField(&rocks_db.write_options.memtable_insert_hint_per_batch, false)},
      {"rocksdb.write_options.sync_for_receiver", true,
       new YesNoField(&rocks_db.write_options.sync_for_receiver, false)},

      /* rocksdb read options */
      {"rocksdb.read_options.async_io", false, new YesNoField(&rocks_db.read_options.async_io, false)},
      {"rocksdb.read_options.fill_cache", false, new YesNoField(&rocks_db.read_options.fill_cache, true)},
  };
  for (auto &wrapper : fields) {
    auto &field = wrapper.field;
    field->readonly = wrapper.readonly;
    fields_.emplace(std::move(wrapper.name), std::move(field));
  }
  initFieldValidator();
  initFieldCallback();
}

// The validate function would be invoked before the field was set,
// to make sure that new value is valid.
void Config::initFieldValidator() {
  std::map<std::string, ValidateFn> validators = {
      {"requirepass",
       [this](const std::string &k, const std::string &v) -> Status {
         if (v.empty() && !load_tokens.empty()) {
           return {Status::NotOK, "requirepass empty not allowed while the namespace exists"};
         }
         if (load_tokens.find(v) != load_tokens.end()) {
           return {Status::NotOK, "requirepass is duplicated with namespace tokens"};
         }
         return Status::OK();
       }},
      {"masterauth",
       [this](const std::string &k, const std::string &v) -> Status {
         if (load_tokens.find(v) != load_tokens.end()) {
           return {Status::NotOK, "masterauth is duplicated with namespace tokens"};
         }
         return Status::OK();
       }},
      //[start, end] mean start:00:00 <= time <= end:59:59 can execute
      {"compaction-checker-range",
       [this](const std::string &k, const std::string &v) -> Status {
         if (v.empty()) {
           compaction_checker_range.start = -1;
           compaction_checker_range.stop = -1;
           return Status::OK();
         }
         std::vector<std::string> args = util::Split(v, "-");
         if (args.size() != 2) {
           return {Status::NotOK, "invalid range format, the range should be between 0 and 24"};
         }
         auto start = GET_OR_RET(ParseInt<int>(args[0], {0, 24}, 10)),
              stop = GET_OR_RET(ParseInt<int>(args[1], {0, 24}, 10));
         if (start > stop) return {Status::NotOK, "invalid range format, start should be smaller than stop"};
         compaction_checker_range.start = start;
         compaction_checker_range.stop = stop;
         return Status::OK();
       }},
      {"compaction-checker-full",
       [this](const std::string &k, const std::string &v) -> Status {
         if (v.empty()) {
           compaction_checker_full.start = -1;
           compaction_checker_full.stop = -1;
           return Status::OK();
         }
         std::vector<std::string> args = util::Split(v, "-");
         if (args.size() != 2) {
           return {Status::NotOK, "invalid range format, the range should be between 0 and 24"};
         }
         auto start = GET_OR_RET(ParseInt<int>(args[0], {0, 24}, 10)),
              stop = GET_OR_RET(ParseInt<int>(args[1], {0, 24}, 10));
         if (start > stop) return {Status::NotOK, "invalid range format, start should be smaller than stop"};
         compaction_checker_full.start = start;
         compaction_checker_full.stop = stop;
         return Status::OK();
       }},
      {"cron-full-compaction",
       [this](const std::string &k, const std::string &v) -> Status {
         if (v.empty()) {
           new_cron_full_compaction.start_hour = -1;
           new_cron_full_compaction.end_hour = -1;
           new_cron_full_compaction.days.clear();
           return Status::OK();
         }

         // cron-full-compaction format: "0 6-8 * * *"
         std::vector<std::string> args = util::Split(v, " ");
         if (args.size() != 5) {
           return {Status::NotOK, "invalid cron-full-compaction format, should be like \"0 6-8 * * *\""};
         }

         // args[0] present the minute of hour, only support 0 now
         if (args[0] != "0") {
           return {Status::NotOK, "invalid cron-full-compaction format, the first argument should be 0"};
         }

         // args[3] present month of year, only support * now
         if (args[3] != "*") {
           return {Status::NotOK, "invalid cron-full-compaction format, the fourth argument should be *"};
         }

         // args[4] present the days of week, only support * now
         if (args[4] != "*") {
           return {Status::NotOK, "invalid cron-full-compaction format, the fifth argument should be *"};
         }

         // args[1] present the hour of day, it may be 6-8, or single hour
         int start_hour = -1;
         int end_hour = -1;
         std::vector<std::string> hour_args = util::Split(args[1], "-");
         if (hour_args.size() == 1) {
           start_hour = GET_OR_RET(ParseInt<int>(hour_args[0], {0, 23}, 10));
         } else if (hour_args.size() == 2) {
           start_hour = GET_OR_RET(ParseInt<int>(hour_args[0], {0, 23}, 10));
           end_hour = GET_OR_RET(ParseInt<int>(hour_args[1], {0, 23}, 10));
           if (start_hour > end_hour) {
             return {Status::NotOK, "invalid cron-full-compaction format, start hour should be smaller than end hour"};
           }
         } else {
           return {Status::NotOK, "invalid cron-full-compaction format, the second argument should like 6-8 or 6"};
         }

         // args[2] present the day of month, it may be *, sigle number or comma separated numbers
         std::vector<int> days;
         if (args[2] == "*") {
           days = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                   17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
         } else {
           std::vector<std::string> day_args = util::Split(args[2], ",");
           for (auto &day_arg : day_args) {
             int day = GET_OR_RET(ParseInt<int>(day_arg, {1, 31}, 10));
             days.push_back(day);
           }
         }

         // param check success, so set the new_cron_full_compaction
         new_cron_full_compaction.start_hour = start_hour;
         new_cron_full_compaction.end_hour = end_hour;
         new_cron_full_compaction.days = days;
         return Status::OK();
       }},
      {"bgsave-cron",
       [this](const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> args = util::Split(v, " \t");
         return bgsave_cron.SetScheduleTime(args);
       }},
      {"enable-hfe-cmd",
       [this](const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> args = util::Split(v, " \t");
         if (args.size() != 1) {
           return {Status::NotOK, "Invalid enable-hfe-cmd format"};
         }
         if (args[0] != "yes" && args[0] != "no") {
           return {Status::NotOK, "Invalid enable-hfe-cmd format"};
         }
         bool enable_hfe = false;
         args[0] == "yes" ? enable_hfe = true : enable_hfe = false;
         if (!enable_hfe && enable_hfe_cmd) {
           return {Status::NotOK, "Can't disable hfe-cmd while it's enabled"};
         }
         if (enable_hfe && enable_cdc_sync) {
           return {Status::NotOK, "Can't enable hfe-cmd while cdc-sync is enabled"};
         }
         return Status::OK();
       }},
      {"enable-cdc-sync",
       [this](const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> args = util::Split(v, " \t");
         if (args.size() != 1) {
           return {Status::NotOK, "Invalid enable-cdc-sync format"};
         }
         if (args[0] != "yes" && args[0] != "no") {
           return {Status::NotOK, "Invalid enable-cdc-sync format"};
         }
         bool enable_cdc = false;
         args[0] == "yes" ? enable_cdc = true : enable_cdc = false;
         if (enable_cdc && enable_hfe_cmd) {
           return {Status::NotOK, "Can't enable cdc-sync while hfe-cmd is enabled"};
         }
         return Status::OK();
       }},
      {"metadata-filter-levels",
       [](const std::string &k, const std::string &v) -> Status { return ValidateLevelConfig(v); }},
      {"subkey-filter-levels",
       [](const std::string &k, const std::string &v) -> Status { return ValidateLevelConfig(v); }},
      {"rename-command",
       [](const std::string &k, const std::string &v) -> Status {
         std::vector<std::string> all_args = util::Split(v, "\n");
         for (auto &p : all_args) {
           std::vector<std::string> args = util::Split(p, " \t");
           if (args.size() != 2) {
             return {Status::NotOK, "Invalid rename-command format"};
           }
           auto commands = redis::CommandTable::Get();
           auto cmd_iter = commands->find(util::ToLower(args[0]));
           if (cmd_iter == commands->end()) {
             return {Status::NotOK, "No such command in rename-command"};
           }
           if (args[1] != "\"\"") {
             auto new_command_name = util::ToLower(args[1]);
             if (commands->find(new_command_name) != commands->end()) {
               return {Status::NotOK, "Target command name already exists"};
             }
             (*commands)[new_command_name] = cmd_iter->second;
           }
           commands->erase(cmd_iter);
         }
         return Status::OK();
       }},
  };
  for (const auto &iter : validators) {
    auto field_iter = fields_.find(iter.first);
    if (field_iter != fields_.end()) {
      field_iter->second->validate = iter.second;
    }
  }
}

// The callback function would be invoked after the field was set,
// it may change related fields or re-format the field. for example,
// when the 'dir' was set, the db-dir or backup-dir should be reset as well.
void Config::initFieldCallback() {
  auto set_db_option_cb = [](Server *srv, const std::string &k, const std::string &v) -> Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->SetDBOption(TrimRocksDbPrefix(k), v);
  };

  auto set_cf_option_cb = [](Server *srv, const std::string &k, const std::string &v) -> Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), v);
  };

  auto set_compression_type_cb = [](Server *srv, const std::string &k, const std::string &v) -> Status {
    if (!srv) return Status::OK();

    std::string compression_option;
    for (auto &option : engine::CompressionOptions) {
      if (option.name == v) {
        compression_option = option.val;
        break;
      }
    }
    if (compression_option.empty()) {
      return {Status::NotOK, "Invalid compression type"};
    }

    // For the first two levels, it may contain the frequently accessed data,
    // so it'd be better to use uncompressed data to save the CPU.
    auto slot_ranges = srv->cluster->LocalSlotRanges();
    for (auto &[name, slot_range] : slot_ranges) {
      std::string compression_levels = "kNoCompression:kNoCompression";
      auto db = slot_range->GetStorage()->GetDB();
      for (size_t i = 2; i < db->GetOptions().compression_per_level.size(); i++) {
        compression_levels += ":";
        compression_levels += compression_option;
      }
      auto s = srv->SetDBOptionForAllColumnFamilies("compression_per_level", compression_levels);
      if (!s.IsOK()) {
        std::string err_msg{"rocksdb.["};
        err_msg.append(std::to_string(slot_range->GetRangeStart()))
            .append("-")
            .append(std::to_string(slot_range->GetRangeEnd()))
            .append("]")
            .append(s.Msg());
        return {Status::NotOK, err_msg};
      }
    }
    return Status::OK();
  };

  auto set_blob_compression_type_cb = [this](Server *srv, const std::string &k, const std::string &v) -> Status {
    if (!srv) return Status::OK();
    if (!rocks_db.enable_blob_files) {
      return {Status::NotOK, errBlobDbNotEnabled};
    }
    std::string blob_compression_type;
    for (auto &option : engine::CompressionOptions) {
      if (option.name == v) {
        blob_compression_type = option.val;
        break;
      }
    }
    if (blob_compression_type.empty()) {
      return {Status::NotOK, "unsupported blob compression type"};
    }
    return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), blob_compression_type);
  };

#ifdef ENABLE_OPENSSL
  auto set_tls_option = [](Server *srv, const std::string &k, const std::string &v) {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    auto new_ctx = CreateSSLContext(srv->GetConfig());
    if (!new_ctx) {
      return Status(Status::NotOK, "Failed to configure SSL context, check server log for more details");
    }
    srv->ssl_ctx = std::move(new_ctx);
    return Status::OK();
  };
#endif

  std::map<std::string, CallbackFn> callbacks =
      {
          {"workers",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             srv->AdjustWorkerThreads();
             return Status::OK();
           }},
          {"datadir-list",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             std::vector<std::string> args = util::Split(v, " \t");
             if (args.empty()) {
               return {Status::NotOK, "at least one volume should be given in .conf file"};
             }
             datadir_list = std::move(args);
             return Status::OK();
           }},
          {"cluster-id",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (v.empty()) {
               return {Status::NotOK, "cluster-id should not be empty!"};
             }
             return Status::OK();
           }},
          {"datanode-id",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (v.empty()) {
               return {Status::NotOK, "datanode-id should not be empty!"};
             }
             return Status::OK();
           }},
          {"controller-addr",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (v.empty()) {
               return {Status::NotOK, "controller-addr should not be empty!"};
             }
             return Status::OK();
           }},
          {"pool",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (v.empty()) {
               return {Status::NotOK, "pool should not be empty!"};
             }
             return Status::OK();
           }},
          {"if-name",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status { return Status::OK(); }},
          {"cluster-enabled",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (cluster_enabled) slot_id_encoded = true;
             return Status::OK();
           }},
          {"bind",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             std::vector<std::string> args = util::Split(v, " \t");
             binds = std::move(args);
             return Status::OK();
           }},
          {"maxclients",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             srv->AdjustOpenFilesLimit();
             return Status::OK();
           }},
          {"slaveof",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (v.empty()) {
               return Status::OK();
             }
             std::vector<std::string> args = util::Split(v, " \t");
             if (args.size() != 2) return {Status::NotOK, "wrong number of arguments"};
             if (args[0] != "no" && args[1] != "one") {
               master_host = args[0];
               auto parse_result = ParseInt<int>(args[1], NumericRange<int>{1, PORT_LIMIT - 1}, 10);
               if (!parse_result) {
                 return {Status::NotOK, "should be between 0 and 65535"};
               }
               master_port = *parse_result;
             }
             return Status::OK();
           }},
          {"profiling-sample-commands",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             std::vector<std::string> cmds = util::Split(v, ",");
             profiling_sample_all_commands = false;
             profiling_sample_commands.clear();
             for (auto const &cmd : cmds) {
               if (cmd == "*") {
                 profiling_sample_all_commands = true;
                 profiling_sample_commands.clear();
                 return Status::OK();
               }
               if (!redis::CommandTable::IsExists(cmd)) {
                 return {Status::NotOK, cmd + " is not Datanode supported command"};
               }
               // profiling_sample_commands use command's original name, regardless of rename-command directive
               profiling_sample_commands.insert(cmd);
             }
             return Status::OK();
           }},
          {"slow-req-record-max-len",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             srv->GetSlowLog()->SetMaxEntries(slow_req_record_max_len);
             return Status::OK();
           }},
          {"max-db-size",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             srv->storage_mgr->CheckDBSizeLimit();
             return Status::OK();
           }},
          {"max-io-mb",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             srv->storage_mgr->SetIORateLimit(max_io_mb);
             return Status::OK();
           }},
          {"profiling-sample-record-max-len",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             srv->GetPerfLog()->SetMaxEntries(profiling_sample_record_max_len);
             return Status::OK();
           }},
          {"log-level",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             FLAGS_minloglevel = log_level;
             return Status::OK();
           }},
          {"vlog-level",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             FLAGS_v = vlog_level;
             return Status::OK();
           }},
          {"log-retention-days",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (util::ToLower(log_dir) == "stdout") {
               return {Status::NotOK, "can't set the 'log-retention-days' when the log dir is stdout"};
             }

             if (log_retention_days != -1) {
               google::EnableLogCleaner(log_retention_days);
             } else {
               google::DisableLogCleaner();
             }
             return Status::OK();
           }},
          {"scan-session-max-count",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (srv->scan_lru_cache == nullptr) {
               return {Status::NotOK, "session lru cache is null"};
             }
             srv->scan_lru_cache->SetCapacity(scan_session_max_count);
             return Status::OK();
           }},
          {"scan-session-ttl-seconds",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (srv->scan_lru_cache == nullptr) {
               return {Status::NotOK, "session lru cache is null"};
             }
             srv->scan_lru_cache->SetTTL(scan_session_ttl_sec * 1000);
             return Status::OK();
           }},
          {"xscan-session-max-count",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (srv->xscan_lru_cache == nullptr) {
               return {Status::NotOK, "xscan session lru cache is null"};
             }
             srv->xscan_lru_cache->SetCapacity(xscan_session_max_count);
             return Status::OK();
           }},
          {"xscan-session-ttl-seconds",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (srv->xscan_lru_cache == nullptr) {
               return {Status::NotOK, "xscan session lru cache is null"};
             }
             srv->xscan_lru_cache->SetTTL(xscan_session_ttl_sec * 1000);
             return Status::OK();
           }},
          {"enable-hfe-cmd",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             encode_hash_sub_flag.store(enable_hfe_cmd, std::memory_order::memory_order_relaxed);
             return Status::OK();
           }},
           {"enable-kkv-cmd",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             enable_kkv_cmd_flag.store(enable_kkv_cmd, std::memory_order::memory_order_relaxed);
             return Status::OK();
           }},
          {"compaction-with-filter",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             std::lock_guard<std::mutex> guard(filter_mu);
             compact_with_filter_flag.store(compaction_with_filter, std::memory_order::memory_order_relaxed);
             return Status::OK();
           }},
          {"metadata-filter-levels",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             this->UpdateFilterLevelMasks();
             return Status::OK();
           }},
          {"subkey-filter-levels",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             this->UpdateFilterLevelMasks();
             return Status::OK();
           }},
          {"rocksdb.target_file_size_base",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k),
                                                         std::to_string(rocks_db.target_file_size_base * MiB));
           }},
          {"rocksdb.block_cache_size",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             return srv->storage_mgr->SetBlockCacheSize(rocks_db.block_cache_size);
           }},
          {"rocksdb.write_buffer_size",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k),
                                                         std::to_string(rocks_db.write_buffer_size * MiB));
           }},
          {"rocksdb.disable_auto_compactions",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             std::string disable_auto_compactions = v == "yes" ? "true" : "false";
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), disable_auto_compactions);
           }},
          {"rocksdb.max_total_wal_size",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             return srv->SetDBOption(TrimRocksDbPrefix(k), std::to_string(rocks_db.max_total_wal_size * MiB));
           }},
          {"rocksdb.enable_blob_files",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             std::string enable_blob_files = rocks_db.enable_blob_files ? "true" : "false";
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), enable_blob_files);
           }},
          {"rocksdb.min_blob_size",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (!rocks_db.enable_blob_files) {
               return {Status::NotOK, errBlobDbNotEnabled};
             }
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), v);
           }},
          {"rocksdb.blob_file_size",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (!rocks_db.enable_blob_files) {
               return {Status::NotOK, errBlobDbNotEnabled};
             }
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), std::to_string(rocks_db.blob_file_size));
           }},
          {"rocksdb.enable_blob_garbage_collection",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (!rocks_db.enable_blob_files) {
               return {Status::NotOK, errBlobDbNotEnabled};
             }
             std::string enable_blob_garbage_collection = v == "yes" ? "true" : "false";
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), enable_blob_garbage_collection);
           }},
          {"rocksdb.blob_garbage_collection_age_cutoff",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (!rocks_db.enable_blob_files) {
               return {Status::NotOK, errBlobDbNotEnabled};
             }
             int val = 0;
             auto parse_result = ParseInt<int>(v, 10);
             if (!parse_result) {
               return {Status::NotOK, "Illegal blob_garbage_collection_age_cutoff value."};
             }
             val = *parse_result;
             if (val < 0 || val > 100) {
               return {Status::NotOK, "blob_garbage_collection_age_cutoff must >= 0 and <= 100."};
             }

             double cutoff = val / 100.0;
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), std::to_string(cutoff));
           }},
          {"rocksdb.blob_garbage_collection_force_threshold",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (!rocks_db.enable_blob_files) {
               return {Status::NotOK, errBlobDbNotEnabled};
             }
             int val = 0;
             auto parse_result = ParseInt<int>(v, 10);
             if (!parse_result) {
               return {Status::NotOK, "Illegal blob_garbage_collection_force_threshold value."};
             }
             val = *parse_result;
             if (val < 0 || val > 100) {
               return {Status::NotOK, "blob_garbage_collection_force_threshold must >= 0 and <= 100."};
             }

             double threshold = val / 100.0;
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), std::to_string(threshold));
           }},
          {"rocksdb.level_compaction_dynamic_level_bytes",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             std::string level_compaction_dynamic_level_bytes = v == "yes" ? "true" : "false";
             return srv->SetDBOption(TrimRocksDbPrefix(k), level_compaction_dynamic_level_bytes);
           }},
          {"rocksdb.max_bytes_for_level_base",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (!rocks_db.level_compaction_dynamic_level_bytes) {
               return {Status::NotOK, errLevelCompactionDynamicLevelBytesNotSet};
             }
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k),
                                                         std::to_string(rocks_db.max_bytes_for_level_base));
           }},
          {"rocksdb.max_bytes_for_level_multiplier",
           [this](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             if (!rocks_db.level_compaction_dynamic_level_bytes) {
               return {Status::NotOK, errLevelCompactionDynamicLevelBytesNotSet};
             }
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), v);
           }},
          {"rocksdb.stats_level",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             bool found = false;
             auto stats_level = rocksdb::StatsLevel::kExceptDetailedTimers;
             for (auto& tmp : stats_levels) {
               if (tmp.name == v) {
                 stats_level = tmp.val;
                 found = true;
                 break;
               }
             }
             if (!found) return {Status::NotOK, "unsupported stats level"};
             srv->storage_mgr->SetStatsLevel(stats_level);
             return Status::OK();
          }},
          {"rocksdb.paranoid_file_checks",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             std::string paranoid_file_checks = v == "yes" ? "true" : "false";
             return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), paranoid_file_checks);
           }},
          {"rocksdb.report_bg_io_stats",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             std::string report_bg_io_stats = v == "yes" ? "true" : "false";
            return srv->SetDBOptionForAllColumnFamilies(TrimRocksDbPrefix(k), report_bg_io_stats);
           }},
          {"rocksdb.avoid_flush_during_shutdown",
           [](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (!srv) return Status::OK();
             std::string avoid_flush_during_shutdown = v == "yes" ? "true" : "false";
             return srv->SetDBOption(TrimRocksDbPrefix(k), avoid_flush_during_shutdown);
           }},
          {"rocksdb.max_write_buffer_number",
           [this, set_cf_option_cb](Server *srv, const std::string &k, const std::string &v) -> Status {
             if (rocks_db.max_write_buffer_number <= kMinWriteBufferNumberToMerge) {
               rocks_db.max_write_buffer_number = kMinWriteBufferNumberToMerge + 1;
             }
             return set_cf_option_cb(srv, k, std::to_string(rocks_db.max_write_buffer_number));
           }},
          {"rocksdb.max_open_files", set_db_option_cb},
          {"rocksdb.stats_dump_period_sec", set_db_option_cb},
          {"rocksdb.delayed_write_rate", set_db_option_cb},
          {"rocksdb.max_subcompactions", set_db_option_cb},
          {"rocksdb.compaction_readahead_size", set_db_option_cb},
          {"rocksdb.max_background_jobs", set_db_option_cb},

          {"rocksdb.max_write_buffer_number", set_cf_option_cb},
          {"rocksdb.level0_slowdown_writes_trigger", set_cf_option_cb},
          {"rocksdb.level0_stop_writes_trigger", set_cf_option_cb},
          {"rocksdb.level0_file_num_compaction_trigger", set_cf_option_cb},
          {"rocksdb.compression", set_compression_type_cb},
          {"rocksdb.blob_compression_type", set_blob_compression_type_cb},
          {"rocksdb.target_file_size_multiplier", set_cf_option_cb},
          {"rocksdb.arena_block_size", set_cf_option_cb},
          {"rocksdb.soft_pending_compaction_bytes_limit", set_cf_option_cb},
          {"rocksdb.hard_pending_compaction_bytes_limit", set_cf_option_cb},
          {"rocksdb.max_compaction_bytes", set_cf_option_cb},
          {"rocksdb.max_sequential_skip_in_iterations", set_cf_option_cb},
          {"rocksdb.sample_for_compression", set_cf_option_cb},
          {"rocksdb.periodic_compaction_seconds", set_cf_option_cb},
          {"rocksdb.ttl", set_cf_option_cb},
          {"rocksdb.bottommost_file_compaction_delay", set_cf_option_cb},
          {"rocksdb.delete_obsolete_files_period_micros", set_db_option_cb},
          {"rocksdb.writable_file_max_buffer_size", set_db_option_cb},
          {"rocksdb.bytes_per_sync", set_db_option_cb},
          {"rocksdb.wal_bytes_per_sync", set_db_option_cb},
          {"rocksdb.stats_persist_period_sec", set_db_option_cb},
          {"rocksdb.stats_history_buffer_size", set_db_option_cb},

#ifdef ENABLE_OPENSSL
          {"tls-cert-file", set_tls_option},
          {"tls-key-file", set_tls_option},
          {"tls-key-file-pass", set_tls_option},
          {"tls-ca-cert-file", set_tls_option},
          {"tls-ca-cert-dir", set_tls_option},
          {"tls-protocols", set_tls_option},
          {"tls-auth-clients", set_tls_option},
          {"tls-ciphers", set_tls_option},
          {"tls-ciphersuites", set_tls_option},
          {"tls-prefer-server-ciphers", set_tls_option},
          {"tls-session-caching", set_tls_option},
          {"tls-session-cache-size", set_tls_option},
          {"tls-session-cache-timeout", set_tls_option},
#endif
};
  for (const auto &iter : callbacks) {
    auto field_iter = fields_.find(iter.first);
    if (field_iter != fields_.end()) {
      field_iter->second->callback = iter.second;
    }
  }
}

void Config::SetMaster(const std::string &host, uint32_t port) {
  master_host = host;
  master_port = port;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    auto s = iter->second->Set(master_host + " " + std::to_string(master_port));
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed to set the value of 'slaveof' setting: " << s.Msg();
    }
  }
}

void Config::ClearMaster() {
  master_host.clear();
  master_port = 0;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    auto s = iter->second->Set("no one");
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed to clear the value of 'slaveof' setting: " << s.Msg();
    }
  }
}

Status Config::parseConfigFromPair(const std::pair<std::string, std::string> &input, int line_number,
                                   std::string *output) {
  std::string field_key = util::ToLower(input.first);
  auto iter = fields_.find(field_key);
  if (iter != fields_.end()) {
    auto &field = iter->second;
    field->line_number = line_number;
    auto s = field->Set(input.second);
    if (!s.IsOK()) return s.Prefixed(fmt::format("failed to set value of field '{}'", field_key));
  } else {
    if (output == nullptr) {
      std::cout << fmt::format("WARNING: '{}' at line {} is not a valid configuration key.", field_key, line_number)
                << std::endl;
    } else {
      output->append(fmt::format("WARNING: '{}' at line {} is not a valid configuration key.", field_key, line_number))
          .append("\r\n");
    }
  }
  return Status::OK();
}

Status Config::parseConfigFromString(const std::string &input, int line_number, std::string *output) {
  auto parsed = ParseConfigLine(input);
  if (!parsed) return parsed.ToStatus().Prefixed("malformed line");

  auto kv = std::move(*parsed);

  if (kv.first.empty() || kv.second.empty()) return Status::OK();

  return parseConfigFromPair(kv, line_number, output);
}

Status Config::finish() const {
  if (binds.size() == 0) {
    return {Status::NotOK,
            "node is in cluster mode, but TCP listen address "
            "wasn't specified via configuration file"};
  }
  if (master_port != 0 && binds.size() == 0) {
    return {Status::NotOK, "replication doesn't support unix socket"};
  }
  return Status::OK();
}

Status Config::Load(const CLIOptions &opts, std::string *output) {
  if (!opts.conf_file.empty()) {
    std::ifstream file;
    std::istream *in = nullptr;
    if (opts.conf_file == "-") {
      in = &std::cin;
    } else {
      path_ = opts.conf_file;
      file.open(path_);
      if (!file.is_open()) {
        return {Status::NotOK, fmt::format("failed to open file '{}': {}", path_, strerror(errno))};
      }

      in = &file;
    }

    std::string line;
    int line_num = 1;
    while (in->good() && std::getline(*in, line)) {
      if (auto s = parseConfigFromString(line, line_num, output); !s.IsOK()) {
        return s.Prefixed(fmt::format("at line #L{}", line_num));
      }

      line_num++;
    }
  } else {
    if (output == nullptr) {
      std::cout << "WARNING: No config file specified, using the default configuration. "
                << "In order to specify a config file use 'datanode -c /path/to/datanode.conf'" << std::endl;
    } else {
      output->append("WARNING: No config file specified, using the default configuration. ")
          .append("In order to specify a config file use 'datanode -c /path/to/datanode.conf\r\n");
    }
  }

  for (const auto &opt : opts.cli_options) {
    GET_OR_RET(parseConfigFromPair(opt, -1, output).Prefixed("CLI config option error"));
  }

  for (const auto &iter : fields_) {
    // line_number = 0 means the user didn't specify the field value
    // on config file and would use default value, so won't validate here.
    if (iter.second->line_number != 0 && iter.second->validate) {
      auto s = iter.second->validate(iter.first, iter.second->ToString());
      if (!s.IsOK()) {
        return s.Prefixed(fmt::format("at line #L{}: {} is invalid", iter.second->line_number, iter.first));
      }
    }
  }

  for (const auto &iter : fields_) {
    if (iter.second->callback) {
      auto s = iter.second->callback(nullptr, iter.first, iter.second->ToString());
      if (!s.IsOK()) {
        return s.Prefixed(fmt::format("while changing key '{}'", iter.first));
      }
    }
  }
  return finish();
}

void Config::Get(const std::string &key, std::vector<std::string> *values) const {
  values->clear();
  for (const auto &iter : fields_) {
    if (key == "*" || util::ToLower(key) == iter.first) {
      if (iter.second->IsMultiConfig()) {
        for (const auto &p : util::Split(iter.second->ToString(), "\n")) {
          values->emplace_back(iter.first);
          values->emplace_back(p);
        }
      } else {
        values->emplace_back(iter.first);
        values->emplace_back(iter.second->ToString());
      }
    }
  }
}

Status Config::Set(Server *srv, std::string key, const std::string &value) {
  key = util::ToLower(key);
  auto iter = fields_.find(key);
  if (iter == fields_.end() || iter->second->readonly) {
    return {Status::NotOK, "Unsupported CONFIG parameter: " + key};
  }

  auto &field = iter->second;
  if (field->validate) {
    auto s = field->validate(key, value);
    if (!s.IsOK()) return s.Prefixed("invalid value");
  }

  auto s = field->Set(value);
  if (!s.IsOK()) return s.Prefixed("failed to set new value");

  if (field->callback) {
    return field->callback(srv, key, value);
  }

  return Status::OK();
}

bool Config::checkFieldValueIsDefault(const std::string &key, const std::string &value) const {
  auto iter = fields_.find(key);
  return iter != fields_.end() && iter->second->Default() == value;
}

Status Config::Rewrite(const std::map<std::string, std::string> &tokens) {
  if (!HasConfigFile()) {
    return {Status::NotOK, "the server is running without a config file"};
  }

  std::vector<std::string> lines;
  std::map<std::string, std::string> new_config;
  for (const auto &iter : fields_) {
    if (iter.second->IsMultiConfig()) {
      // We should NOT overwrite the commands which are MultiConfig since it cannot be rewritten in-flight,
      // so skip it here to avoid rewriting it as new item.
      continue;
    }
    new_config[iter.first] = iter.second->ToString();
  }

  std::string namespace_prefix = "namespace.";
  std::ifstream file(path_);
  if (file.is_open()) {
    std::string raw_line;
    while (file.good() && std::getline(file, raw_line)) {
      auto parsed = ParseConfigLine(raw_line);
      if (!parsed || parsed->first.empty()) {
        lines.emplace_back(raw_line);
        continue;
      }
      auto kv = std::move(*parsed);
      if (util::HasPrefix(kv.first, namespace_prefix)) {
        // Ignore namespace fields here since we would always rewrite them
        continue;
      }
      auto iter = new_config.find(util::ToLower(kv.first));
      if (iter != new_config.end()) {
        if (!iter->second.empty()) lines.emplace_back(DumpConfigLine({iter->first, iter->second}));
        new_config.erase(iter);
      } else {
        lines.emplace_back(raw_line);
      }
    }
  }
  file.close();

  std::string out_buf;
  for (const auto &line : lines) {
    fmt::format_to(std::back_inserter(out_buf), "{}\n", line);
  }
  for (const auto &remain : new_config) {
    if (remain.second.empty() || checkFieldValueIsDefault(remain.first, remain.second)) continue;
    fmt::format_to(std::back_inserter(out_buf), "{}\n", DumpConfigLine({remain.first, remain.second}));
  }
  std::string tmp_path = path_ + ".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);
  output_file << out_buf;
  output_file.close();
  if (rename(tmp_path.data(), path_.data()) < 0) {
    return {Status::NotOK, fmt::format("rename file encounter error: {}", strerror(errno))};
  }
  return Status::OK();
}

grpc::ChannelArguments Config::buildBaseChannelArgs() const {
  grpc::ChannelArguments args;
  // set grpc keepalive configs
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, grpc_client_keepalive_time_ms);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, grpc_client_keepalive_timeout_ms);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, grpc_client_keepalive_permit_without_calls);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, grpc_client_max_pings_without_data);
  // set grpc connect backoff configs
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, grpc_client_initial_reconnect_backoff_ms);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, grpc_client_min_reconnect_backoff_ms);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, grpc_client_max_reconnect_backoff_ms);
  return args;
}

grpc::ChannelArguments Config::BuildDatanodeChannelArgs() const {
  auto args = buildBaseChannelArgs();
  // set grpc max send and receive message length
  args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, grpc_datanode_service_max_message_length);
  args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, grpc_datanode_service_max_message_length);
  return args;
}

grpc::ChannelArguments Config::BuildControllerChannelArgs() const {
  auto args = buildBaseChannelArgs();
  // set grpc max send and receive message length
  args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, grpc_controller_service_max_message_length);
  args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, grpc_controller_service_max_message_length);
  args.SetServiceConfigJSON(controller_server_config_json);
  return args;
}

void Config::SetCompactWithFilter() {
  std::lock_guard<std::mutex> guard(filter_mu);
  compaction_with_filter = true;
  compact_with_filter_flag.store(true, std::memory_order::memory_order_relaxed);
}

void Config::SetCompactWithoutFilter() {
  std::lock_guard<std::mutex> guard(filter_mu);
  compaction_with_filter = false;
  compact_with_filter_flag.store(false, std::memory_order::memory_order_relaxed);
}

// Helper function to validate level configuration string
static Status ValidateLevelConfig(const std::string &config) {
  if (config.empty()) return Status::OK();  // Empty string means disable all

  std::vector<std::string> parts = util::Split(config, ",");
  for (const auto &part : parts) {
    std::string trimmed = util::Trim(part, " \t");
    if (trimmed.empty()) {
      return {Status::NotOK, "Empty level specification in config"};
    }

    // Only support single level numbers like "0", "3", etc.
    auto level_result = ParseInt<int>(trimmed, 10);
    if (!level_result) {
      return {Status::NotOK, "Invalid level number: " + trimmed};
    }
    int level = *level_result;
    if (level < 0 || level > 6) {
      return {Status::NotOK, "Level number must be between 0 and 6: " + trimmed};
    }
  }
  return Status::OK();
}

// Convert level configuration string to bitmask
static uint32_t ParseLevelConfigToBitmask(const std::string &config) {
  if (config.empty()) return 0;  // Empty string means disable all

  uint32_t mask = 0;
  std::vector<std::string> parts = util::Split(config, ",");
  for (const auto &part : parts) {
    std::string trimmed = util::Trim(part, " \t");
    if (!trimmed.empty()) {
      // Only support single level numbers like "0", "3", etc.
      auto level_result = ParseInt<int>(trimmed, 10);
      if (level_result && *level_result >= 0 && *level_result <= 6) {
        mask |= (1u << *level_result);
      }
    }
  }
  return mask;
}

void Config::UpdateFilterLevelMasks() {
  uint32_t new_metadata_mask = ParseLevelConfigToBitmask(metadata_filter_levels);
  uint32_t new_subkey_mask = ParseLevelConfigToBitmask(subkey_filter_levels);

  metadata_filter_mask.store(new_metadata_mask);
  subkey_filter_mask.store(new_subkey_mask);
}
