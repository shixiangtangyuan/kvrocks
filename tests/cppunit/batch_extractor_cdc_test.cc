#include "storage/batch_extractor_cdc.h"

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
#include "scope_exit.h"
#include "server/redis_connection.h"
#include "server/redis_request.h"

namespace redis {

using kv::controller::v1::SlotRangeIndex;
using kv::datanode::v1::CDCEvent;
using kv::datanode::v1::CDCGetEventsResponse;
using kv::datanode::v1::CDCPoint;
using kv::datanode::v1::EventContent;
using kv::datanode::v1::FieldData;

Status GetCDCData(const std::string& cluster_id, const SlotRangeIndex& slot_range_idx, engine::Storage* storage,
                  uint64_t* next_seq, std::vector<CDCGetEventsResponse>* output) {
  uint64_t latest_seq = storage->GetDB()->GetLatestSequenceNumber();
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;
  auto s = storage->GetWALIter(*next_seq, &iter);
  if (!s.IsOK()) {
    std::cout << "[ParseWal error] failed to create wal iterator, err: " << s.Msg() << std::endl;
    return s;
  }

  while (iter->Valid()) {
    CDCGetEventsResponse resp;
    auto batch = iter->GetBatch();
    s = cdc::GetCDCEventsResponse(cluster_id, slot_range_idx, batch, &resp);
    if (!s.IsOK()) {
      std::cout << "[ParseWal error] failed to parse writebatch, err:  " << s.Msg() << std::endl;
      return s;
    }

    // record resp
    if (!resp.events().empty()) output->emplace_back(resp);

    *next_seq = batch.sequence + batch.writeBatchPtr->Count();
    if (*next_seq > latest_seq) {
      break;
    }
    iter->Next();
  }

  return Status::OK();
}

std::string CDCRespToString(const CDCGetEventsResponse& resp) {
  auto& cdc_point = resp.point();
  auto& cdc_events = resp.events();

  std::stringstream stream;
  stream << "# CDCPoint:\n"
         << "data_size: " << cdc_point.ByteSizeLong() << "\n";
  stream << "next_seq_id: " << cdc_point.next_seq_id() << "\n";
  stream << "prev_rep_id: " << cdc_point.prev_rep_id() << "\n";
  stream << "prev_log_ts: " << cdc_point.prev_log_ts() << "\n";

  stream << "# CDCEvetns:\n";
  for (const auto& event : cdc_events) {
    stream << "event_data_size: " << event.ByteSizeLong() << "\n";
    stream << "timestamp: " << event.timestamp() << "\n";
    stream << "uuid: " << event.uuid() << "\n";
    stream << "data_type: " << event.data_type() << "\n";
    stream << "org_command: " << event.org_command() << "\n";
    stream << "eq_command: " << event.eq_command() << "\n";
    stream << "key: " << event.key() << "\n";

    stream << "Old:\n";
    const auto& old = event.old();
    if (old.has_ttl()) stream << "key_ttl: " << old.ttl() << "\n";
    const auto& old_fields = old.fields();
    for (const auto& field : old_fields) {
      stream << "  field: " << field.field();
      stream << ", ttl: " << static_cast<int64_t>(field.has_ttl() ? field.ttl() : -1);
      stream << ", val: " << (field.has_value() ? field.value() : "NULL");
      stream << "\n";
    }

    stream << "New:\n";
    const auto& new_data = event.new_();
    if (new_data.has_ttl()) stream << "key_ttl: " << new_data.ttl() << "\n";
    const auto& new_fields = new_data.fields();
    for (const auto& field : new_fields) {
      stream << "  field: " << field.field();
      stream << ", ttl: " << static_cast<int64_t>(field.has_ttl() ? field.ttl() : -1);
      stream << ", val: " << (field.has_value() ? field.value() : "NULL");
      stream << "\n";
    }
  }
  return stream.str();
}

TEST(CDCBatchExtractor, CmdTest) {
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

  // create commands
  // hash commands
  std::unique_ptr<Commander> cmd_hset;
  auto s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd_hset);
  ASSERT_TRUE(s.IsOK());
  std::unique_ptr<Commander> cmd_hmset;
  s = srv.GetServer()->LookupAndCreateCommand("hmset", &cmd_hmset);
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
  std::unique_ptr<Commander> cmd_hdel;
  s = srv.GetServer()->LookupAndCreateCommand("hdel", &cmd_hdel);
  ASSERT_TRUE(s.IsOK());
  // hexpire  commands
  std::unique_ptr<Commander> cmd_hexpire;
  s = srv.GetServer()->LookupAndCreateCommand("hexpire", &cmd_hexpire);
  ASSERT_TRUE(s.IsOK());
  // string commands
  std::unique_ptr<Commander> cmd_set;
  s = srv.GetServer()->LookupAndCreateCommand("set", &cmd_set);
  ASSERT_TRUE(s.IsOK());
  // list commands
  std::unique_ptr<Commander> cmd_lpush;
  s = srv.GetServer()->LookupAndCreateCommand("lpush", &cmd_lpush);
  ASSERT_TRUE(s.IsOK());
  // set commands
  std::unique_ptr<Commander> cmd_sadd;
  s = srv.GetServer()->LookupAndCreateCommand("sadd", &cmd_sadd);
  ASSERT_TRUE(s.IsOK());
  // zset commands
  std::unique_ptr<Commander> cmd_zadd;
  s = srv.GetServer()->LookupAndCreateCommand("zadd", &cmd_zadd);
  ASSERT_TRUE(s.IsOK());
  // key commmands
  std::unique_ptr<Commander> cmd_del;
  s = srv.GetServer()->LookupAndCreateCommand("del", &cmd_del);
  ASSERT_TRUE(s.IsOK());
  // config command
  std::unique_ptr<Commander> cmd_config;
  s = srv.GetServer()->LookupAndCreateCommand("config", &cmd_config);
  ASSERT_TRUE(s.IsOK());

  // next sequence
  uint64_t next_seq = 0;

  // node info
  std::string cluster_id = test_active_cluster_id;
  SlotRangeIndex slot_range_idx;
  slot_range_idx.set_start(0);
  slot_range_idx.set_end(16383);
  std::string db_repl_id = *(storage->GetReplIdFromDbEngine());
  ASSERT_TRUE(db_repl_id.size() == kReplIdLength);

  // enable cdc sync
  storage->GetConfig()->enable_cdc_sync = true;

  // Test hash type
  {
    // construct commands tokens, commands: hset/hsetnx/hincrby/hincrbyfloat/hmset/hdel
    CommandTokens hset_cmd_token{"HSET", "hash_key", "f1", "v1", "f2", "v2", "f3", "v3"};
    CommandTokens hsetnx_cmd_token{"HSETNX", "hash_key", "f4", "4"};
    CommandTokens hset_cmd_token_repeated{"HSET", "hash_key", "f4", "4"};
    CommandTokens hsetnx_cmd_token_exists{"HSETNX", "hash_key", "f4", "444"};
    CommandTokens hincrby_cmd_token{"HINCRBY", "hash_key", "f4", "4"};
    CommandTokens hincrbyfloat_cmd_token{"HINCRBYFLOAT", "hash_key", "f4", "0.55"};
    CommandTokens hmset_cmd_token{"HMSET", "hash_key", "f5", "v5", "f6", "v6"};
    CommandTokens hdel_cmd_token{"HDEL", "hash_key", "f1", "f2"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
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
    s = GenericExecCmd(hmset_cmd_token, &output, cmd_hmset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hdel_cmd_token, &output, cmd_hdel, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // Get cdc data
    std::vector<CDCGetEventsResponse> output_resps;
    s = GetCDCData(cluster_id, slot_range_idx, storage, &next_seq, &output_resps);
    ASSERT_TRUE(s.IsOK());

    // check result
    ASSERT_EQ(output_resps.size(), 6);

    // check hset event
    {
      auto& hset_resp = output_resps[0];
      // check point
      auto& point = hset_resp.point();
      EXPECT_EQ(point.next_seq_id(), 6);
      EXPECT_EQ(point.prev_rep_id(), db_repl_id);

      // check event content
      auto& hset_event = hset_resp.events(0);
      EXPECT_EQ(hset_event.data_type(), "hash");
      EXPECT_EQ(hset_event.org_command(), "hset");
      EXPECT_EQ(hset_event.eq_command(), "hset");
      EXPECT_EQ(hset_event.key(), "hash_key");
      // check old
      ASSERT_EQ(hset_event.old().fields_size(), 3);
      EXPECT_EQ(hset_event.old().has_ttl(), false);
      EXPECT_EQ(hset_event.old().fields(2).field(), "f1");
      EXPECT_EQ(hset_event.old().fields(2).has_value(), false);
      EXPECT_EQ(hset_event.old().fields(2).has_ttl(), false);
      EXPECT_EQ(hset_event.old().fields(1).field(), "f2");
      EXPECT_EQ(hset_event.old().fields(1).has_value(), false);
      EXPECT_EQ(hset_event.old().fields(1).has_ttl(), false);
      EXPECT_EQ(hset_event.old().fields(0).field(), "f3");
      EXPECT_EQ(hset_event.old().fields(0).has_value(), false);
      EXPECT_EQ(hset_event.old().fields(0).has_ttl(), false);
      // check new
      ASSERT_EQ(hset_event.new_().fields_size(), 3);
      EXPECT_EQ(hset_event.new_().has_ttl(), false);
      EXPECT_EQ(hset_event.new_().fields(2).field(), "f1");
      EXPECT_EQ(hset_event.new_().fields(2).has_value(), true);
      EXPECT_EQ(hset_event.new_().fields(2).value(), "v1");
      EXPECT_EQ(hset_event.new_().fields(2).has_ttl(), false);
      EXPECT_EQ(hset_event.new_().fields(1).field(), "f2");
      EXPECT_EQ(hset_event.new_().fields(1).has_value(), true);
      EXPECT_EQ(hset_event.new_().fields(1).value(), "v2");
      EXPECT_EQ(hset_event.new_().fields(1).has_ttl(), false);
      EXPECT_EQ(hset_event.new_().fields(0).field(), "f3");
      EXPECT_EQ(hset_event.new_().fields(0).has_value(), true);
      EXPECT_EQ(hset_event.new_().fields(0).value(), "v3");
      EXPECT_EQ(hset_event.new_().fields(0).has_ttl(), false);
    }

    // check hsetnx event
    {
      auto& hsetnx_resp = output_resps[1];
      // check point
      auto& point = hsetnx_resp.point();
      EXPECT_EQ(point.next_seq_id(), 8);
      EXPECT_EQ(point.prev_rep_id(), db_repl_id);
      // check event content
      auto& hsetnx_event = hsetnx_resp.events(0);
      EXPECT_EQ(hsetnx_event.data_type(), "hash");
      EXPECT_EQ(hsetnx_event.org_command(), "hsetnx");
      EXPECT_EQ(hsetnx_event.eq_command(), "hset");
      EXPECT_EQ(hsetnx_event.key(), "hash_key");
      // check old
      ASSERT_EQ(hsetnx_event.old().fields_size(), 1);
      EXPECT_EQ(hsetnx_event.old().has_ttl(), false);
      EXPECT_EQ(hsetnx_event.old().fields(0).field(), "f4");
      EXPECT_EQ(hsetnx_event.old().fields(0).has_value(), false);
      EXPECT_EQ(hsetnx_event.old().fields(0).has_ttl(), false);
      // check new
      ASSERT_EQ(hsetnx_event.new_().fields_size(), 1);
      EXPECT_EQ(hsetnx_event.new_().has_ttl(), false);
      EXPECT_EQ(hsetnx_event.new_().fields(0).field(), "f4");
      EXPECT_EQ(hsetnx_event.new_().fields(0).has_value(), true);
      EXPECT_EQ(hsetnx_event.new_().fields(0).value(), "4");
      EXPECT_EQ(hsetnx_event.new_().fields(0).has_ttl(), false);
    }

    // check hincrby event
    {
      auto& hincrby_resp = output_resps[2];
      // check point
      auto& point = hincrby_resp.point();
      EXPECT_EQ(point.next_seq_id(), 9);
      EXPECT_EQ(point.prev_rep_id(), db_repl_id);
      // check event content
      auto& hincrby_event = hincrby_resp.events(0);
      EXPECT_EQ(hincrby_event.data_type(), "hash");
      EXPECT_EQ(hincrby_event.org_command(), "hincrby");
      EXPECT_EQ(hincrby_event.eq_command(), "hset");
      EXPECT_EQ(hincrby_event.key(), "hash_key");
      // check old
      ASSERT_EQ(hincrby_event.old().fields_size(), 1);
      EXPECT_EQ(hincrby_event.old().has_ttl(), false);
      EXPECT_EQ(hincrby_event.old().fields(0).field(), "f4");
      EXPECT_EQ(hincrby_event.old().fields(0).has_value(), true);
      EXPECT_EQ(hincrby_event.old().fields(0).value(), "4");
      EXPECT_EQ(hincrby_event.old().fields(0).has_ttl(), false);
      // check new
      ASSERT_EQ(hincrby_event.new_().fields_size(), 1);
      EXPECT_EQ(hincrby_event.new_().has_ttl(), false);
      EXPECT_EQ(hincrby_event.new_().fields(0).field(), "f4");
      EXPECT_EQ(hincrby_event.new_().fields(0).has_value(), true);
      EXPECT_EQ(hincrby_event.new_().fields(0).value(), "8");
      EXPECT_EQ(hincrby_event.new_().fields(0).has_ttl(), false);
    }

    // check hincrbyfloat event
    {
      auto& hincrbyfloat_resp = output_resps[3];
      // check point
      auto& point = hincrbyfloat_resp.point();
      EXPECT_EQ(point.next_seq_id(), 10);
      EXPECT_EQ(point.prev_rep_id(), db_repl_id);
      // check event content
      auto& hincrbyfloat_event = hincrbyfloat_resp.events(0);
      EXPECT_EQ(hincrbyfloat_event.data_type(), "hash");
      EXPECT_EQ(hincrbyfloat_event.org_command(), "hincrbyfloat");
      EXPECT_EQ(hincrbyfloat_event.eq_command(), "hset");
      EXPECT_EQ(hincrbyfloat_event.key(), "hash_key");
      // check old
      ASSERT_EQ(hincrbyfloat_event.old().fields_size(), 1);
      EXPECT_EQ(hincrbyfloat_event.old().has_ttl(), false);
      EXPECT_EQ(hincrbyfloat_event.old().fields(0).field(), "f4");
      EXPECT_EQ(hincrbyfloat_event.old().fields(0).has_value(), true);
      EXPECT_EQ(hincrbyfloat_event.old().fields(0).value(), "8.000000");
      EXPECT_EQ(hincrbyfloat_event.old().fields(0).has_ttl(), false);
      // check new
      ASSERT_EQ(hincrbyfloat_event.new_().fields_size(), 1);
      EXPECT_EQ(hincrbyfloat_event.new_().has_ttl(), false);
      EXPECT_EQ(hincrbyfloat_event.new_().fields(0).field(), "f4");
      EXPECT_EQ(hincrbyfloat_event.new_().fields(0).has_value(), true);
      EXPECT_EQ(hincrbyfloat_event.new_().fields(0).value(), "8.550000");
      EXPECT_EQ(hincrbyfloat_event.new_().fields(0).has_ttl(), false);
    }

    // check hmset event
    {
      auto& hmset_resp = output_resps[4];
      // check point
      auto& point = hmset_resp.point();
      EXPECT_EQ(point.next_seq_id(), 13);
      EXPECT_EQ(point.prev_rep_id(), db_repl_id);
      // check event content
      auto& hmset_event = hmset_resp.events(0);
      EXPECT_EQ(hmset_event.data_type(), "hash");
      EXPECT_EQ(hmset_event.org_command(), "hset");
      EXPECT_EQ(hmset_event.eq_command(), "hset");
      EXPECT_EQ(hmset_event.key(), "hash_key");
      // check old
      ASSERT_EQ(hmset_event.old().fields_size(), 2);
      EXPECT_EQ(hmset_event.old().has_ttl(), false);
      EXPECT_EQ(hmset_event.old().fields(1).field(), "f5");
      EXPECT_EQ(hmset_event.old().fields(1).has_value(), false);
      EXPECT_EQ(hmset_event.old().fields(1).has_ttl(), false);
      EXPECT_EQ(hmset_event.old().fields(0).field(), "f6");
      EXPECT_EQ(hmset_event.old().fields(0).has_value(), false);
      EXPECT_EQ(hmset_event.old().fields(0).has_ttl(), false);
      // check new
      ASSERT_EQ(hmset_event.new_().fields_size(), 2);
      EXPECT_EQ(hmset_event.new_().has_ttl(), false);
      EXPECT_EQ(hmset_event.new_().fields(1).field(), "f5");
      EXPECT_EQ(hmset_event.new_().fields(1).has_value(), true);
      EXPECT_EQ(hmset_event.new_().fields(1).value(), "v5");
      EXPECT_EQ(hmset_event.new_().fields(1).has_ttl(), false);
      EXPECT_EQ(hmset_event.new_().fields(0).field(), "f6");
      EXPECT_EQ(hmset_event.new_().fields(0).has_value(), true);
      EXPECT_EQ(hmset_event.new_().fields(0).value(), "v6");
      EXPECT_EQ(hmset_event.new_().fields(0).has_ttl(), false);
    }

    // check hdel event
    {
      auto& hdel_resp = output_resps[5];
      // check point
      auto& point = hdel_resp.point();
      EXPECT_EQ(point.next_seq_id(), 16);
      EXPECT_EQ(point.prev_rep_id(), db_repl_id);
      // check event content
      auto& hdel_event = hdel_resp.events(0);
      EXPECT_EQ(hdel_event.data_type(), "hash");
      EXPECT_EQ(hdel_event.org_command(), "hdel");
      EXPECT_EQ(hdel_event.eq_command(), "hdel");
      EXPECT_EQ(hdel_event.key(), "hash_key");
      // check old
      ASSERT_EQ(hdel_event.old().fields_size(), 2);
      EXPECT_EQ(hdel_event.old().has_ttl(), false);
      EXPECT_EQ(hdel_event.old().fields(0).field(), "f1");
      EXPECT_EQ(hdel_event.old().fields(0).has_value(), true);
      EXPECT_EQ(hdel_event.old().fields(0).value(), "v1");
      EXPECT_EQ(hdel_event.old().fields(0).has_ttl(), false);
      EXPECT_EQ(hdel_event.old().fields(1).field(), "f2");
      EXPECT_EQ(hdel_event.old().fields(1).has_value(), true);
      EXPECT_EQ(hdel_event.old().fields(1).value(), "v2");
      EXPECT_EQ(hdel_event.old().fields(1).has_ttl(), false);
      // check new
      ASSERT_EQ(hdel_event.new_().fields_size(), 2);
      EXPECT_EQ(hdel_event.new_().has_ttl(), false);
      EXPECT_EQ(hdel_event.new_().fields(0).field(), "f1");
      EXPECT_EQ(hdel_event.new_().fields(0).has_value(), false);
      EXPECT_EQ(hdel_event.new_().fields(0).has_ttl(), false);
      EXPECT_EQ(hdel_event.new_().fields(1).field(), "f2");
      EXPECT_EQ(hdel_event.new_().fields(1).has_value(), false);
      EXPECT_EQ(hdel_event.new_().fields(1).has_ttl(), false);
    }
  }

  // Test other type: string/list/set/zset
  {
    // construct commands tokens
    CommandTokens set_cmd_token{"SET", "string_key", "value"};
    CommandTokens lpush_cmd_token{"LPUSH", "list_key", "value1", "value2"};
    CommandTokens sadd_cmd_token{"SADD", "set_key", "value1", "value2"};
    CommandTokens zadd_cmd_token{"ZADD", "zset_key", "1", "value1", "2", "value2"};
    CommandTokens hset_cmd_token{"HSET", "hash_key_new", "f1", "v1"};
    CommandTokens del_cmd_token{"DEL", "hash_key_new"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(set_cmd_token, &output, cmd_set, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(lpush_cmd_token, &output, cmd_lpush, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(sadd_cmd_token, &output, cmd_sadd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(zadd_cmd_token, &output, cmd_zadd, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    s = GenericExecCmd(del_cmd_token, &output, cmd_del, srv_ptr, &conn, storage);
    ASSERT_TRUE(s.IsOK());
    // Get cdc data
    std::vector<CDCGetEventsResponse> output_resps;
    s = GetCDCData(cluster_id, slot_range_idx, storage, &next_seq, &output_resps);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_resps.size(), 1);

    // check hset event
    {
      auto& hset_resp = output_resps[0];
      // check point
      auto& point = hset_resp.point();
      EXPECT_EQ(point.next_seq_id(), 30);
      EXPECT_EQ(point.prev_rep_id(), db_repl_id);

      // check event content
      auto& hset_event = hset_resp.events(0);
      EXPECT_EQ(hset_event.data_type(), "hash");
      EXPECT_EQ(hset_event.org_command(), "hset");
      EXPECT_EQ(hset_event.eq_command(), "hset");
      EXPECT_EQ(hset_event.key(), "hash_key_new");
      // check old
      ASSERT_EQ(hset_event.old().fields_size(), 1);
      EXPECT_EQ(hset_event.old().has_ttl(), false);
      EXPECT_EQ(hset_event.old().fields(0).field(), "f1");
      EXPECT_EQ(hset_event.old().fields(0).has_value(), false);
      EXPECT_EQ(hset_event.old().fields(0).has_ttl(), false);
      // check new
      ASSERT_EQ(hset_event.new_().fields_size(), 1);
      EXPECT_EQ(hset_event.new_().has_ttl(), false);
      EXPECT_EQ(hset_event.new_().fields(0).field(), "f1");
      EXPECT_EQ(hset_event.new_().fields(0).has_value(), true);
      EXPECT_EQ(hset_event.new_().fields(0).value(), "v1");
      EXPECT_EQ(hset_event.new_().fields(0).has_ttl(), false);
    }
  }

  // Test cannot enbale httl while cdc-sync is enabed
  {
    // construct commands tokens
    CommandTokens config_cmd_token{"CONFIG", "SET", "enable-hfe-cmd", "yes"};
    // exec cmds
    std::string output;
    s = GenericExecCmd(config_cmd_token, &output, cmd_config, srv_ptr, &conn, storage);
    ASSERT_FALSE(s.IsOK());
  }

  // Test replid
  {
    uint64_t cur_seq = next_seq;
    // shift replId
    s = storage->ShiftReplId();
    ASSERT_TRUE(s.IsOK());
    // parse wal
    std::vector<CDCGetEventsResponse> output_resps;
    s = GetCDCData(cluster_id, slot_range_idx, storage, &next_seq, &output_resps);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_resps.size(), 0);
    EXPECT_GT(next_seq, cur_seq);
  }
}

TEST(CDCBatchExtractor, SlotrangeSplitTest) {
  // Test steps:
  // 1. set topo [0,16383]
  // 2. write keys in [0,8191] and in [8192,16383]
  // 3. get and check cdc data
  // 4. split slotrange
  // 5. write keys in [0,8191] and in [8192,16383] to [0,8191] slot range
  // 6. get and check cdc data

  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1};
  // create server
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  encode_hash_sub_flag.store(false);
  // set first topo
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
        .version = test_version - 1,
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
  // enable cdc sync
  storage->GetConfig()->enable_cdc_sync = true;

  // create commands
  // hash commands
  std::unique_ptr<Commander> cmd_hset;
  auto s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd_hset);
  ASSERT_TRUE(s.IsOK());

  // prepare keys in two slot ranges
  // keys in [0,8191]
  std::string hkey_prefix{"hkey_"};
  std::string key1 = hkey_prefix + "{" + global_slot_keys[1] + "}_1";
  // keys in [8192,16383]
  std::string key8192 = hkey_prefix + "{" + global_slot_keys[8192] + "}_8192";

  // construc cmd tokens
  CommandTokens hset_cmd_token1{"hset", key1, "f1", "v1"};
  CommandTokens hset_cmd_token1_2{"hset", key1, "f2", "v2"};
  CommandTokens hset_cmd_token8192{"hset", key8192, "f1", "v1"};
  CommandTokens hset_cmd_token8192_2{"hset", key8192, "f2", "v2"};

  // exec cmds in storage1
  std::string output;
  s = GenericExecCmd(hset_cmd_token1, &output, cmd_hset, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token8192, &output, cmd_hset, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());

  // next sequence
  uint64_t next_seq = 0;

  // get cdc data
  {
    // node info
    std::string cluster_id = test_active_cluster_id;
    SlotRangeIndex slot_range_idx;
    slot_range_idx.set_start(0);
    slot_range_idx.set_end(16383);
    std::string db_repl_id = *(storage->GetReplIdFromDbEngine());
    ASSERT_TRUE(db_repl_id.size() == kReplIdLength);

    // get cdc data
    std::vector<CDCGetEventsResponse> output_resps;
    s = GetCDCData(cluster_id, slot_range_idx, storage, &next_seq, &output_resps);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_resps.size(), 2);
    // check resp0
    auto& hset_resp = output_resps[0];
    // check point
    auto& point = hset_resp.point();
    EXPECT_EQ(point.next_seq_id(), 4);
    EXPECT_EQ(point.prev_rep_id(), db_repl_id);
    // check event content
    auto& hset_event = hset_resp.events(0);
    EXPECT_EQ(hset_event.data_type(), "hash");
    EXPECT_EQ(hset_event.org_command(), "hset");
    EXPECT_EQ(hset_event.eq_command(), "hset");
    EXPECT_EQ(hset_event.key(), key1);
    // check old
    ASSERT_EQ(hset_event.old().fields_size(), 1);
    EXPECT_EQ(hset_event.old().has_ttl(), false);
    EXPECT_EQ(hset_event.old().fields(0).field(), "f1");
    EXPECT_EQ(hset_event.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event.new_().fields_size(), 1);
    EXPECT_EQ(hset_event.new_().has_ttl(), false);
    EXPECT_EQ(hset_event.new_().fields(0).field(), "f1");
    EXPECT_EQ(hset_event.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event.new_().fields(0).value(), "v1");
    EXPECT_EQ(hset_event.new_().fields(0).has_ttl(), false);
    // check resp1
    auto& hset_resp1 = output_resps[1];
    // check point
    auto& point1 = hset_resp1.point();
    EXPECT_EQ(point1.next_seq_id(), 6);
    EXPECT_EQ(point1.prev_rep_id(), db_repl_id);
    // check event content
    auto& hset_event1 = hset_resp1.events(0);
    EXPECT_EQ(hset_event1.data_type(), "hash");
    EXPECT_EQ(hset_event1.org_command(), "hset");
    EXPECT_EQ(hset_event1.eq_command(), "hset");
    EXPECT_EQ(hset_event1.key(), key8192);
    // check old
    ASSERT_EQ(hset_event1.old().fields_size(), 1);
    EXPECT_EQ(hset_event1.old().has_ttl(), false);
    EXPECT_EQ(hset_event1.old().fields(0).field(), "f1");
    EXPECT_EQ(hset_event1.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event1.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event1.new_().fields_size(), 1);
    EXPECT_EQ(hset_event1.new_().has_ttl(), false);
    EXPECT_EQ(hset_event1.new_().fields(0).field(), "f1");
    EXPECT_EQ(hset_event1.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event1.new_().fields(0).value(), "v1");
    EXPECT_EQ(hset_event1.new_().fields(0).has_ttl(), false);
  }

  auto prev = sleepMSBeforeCleanData;
  sleepMSBeforeCleanData = 1;
  auto exit = MakeScopeExit([&prev]() { sleepMSBeforeCleanData = prev; });
  // split to two slot ranges
  {
    TestDatanode datanode_split1{
        .datanode_id = test_active_datanode_id,
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                // slotrange 0
                {
                    .db_id = test_db_id,
                    .start = 0,
                    .end = 8191,
                },
            },
    };

    TestDatanode datanode_split2{
        .datanode_id = test_active_datanode_id + "2",
        .serving_status = DATANODE_SERVING,
        .client_rw_status = DATANODE_CLIENT_RW,
        .dts_rw_status = DATANODE_DTS_RO,
        .slot_ranges =
            {
                // slotrange 2
                {
                    .db_id = test_db_id + 1,
                    .start = 8192,
                    .end = kClusterSlots - 1,
                },
            },
    };

    TestShard split_active_shard1 = {{datanode_split1}};
    TestShard split_active_shard2 = {{datanode_split2}};
    TestCluster split_active_cluster{
        .pool_name = test_active_pool,
        .cluster_id = test_active_cluster_id,
        .role = CLUSTER_ACTIVE,
        .shards = {{split_active_shard1}, {split_active_shard2}},
    };
    TestPBHACluster splitallcluster{
        .version = test_version + 1,
        .active_cluster = &split_active_cluster,
        .standby_cluster = nullptr,
    };

    s = srv.GetServer()->cluster->SetTopo(TestBuildControllerResp(splitallcluster));
    EXPECT_TRUE(s.IsOK());
  }
  usleep(100000);

  // exec cmds in storage
  s = GenericExecCmd(hset_cmd_token1_2, &output, cmd_hset, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token8192_2, &output, cmd_hset, srv_ptr, &conn, storage);
  ASSERT_TRUE(s.IsOK());
  // get cdc data
  {
    // node info
    std::string cluster_id = test_active_cluster_id;
    SlotRangeIndex slot_range_idx;
    slot_range_idx.set_start(0);
    slot_range_idx.set_end(8191);
    std::string db_repl_id = *(storage->GetReplIdFromDbEngine());
    ASSERT_TRUE(db_repl_id.size() == kReplIdLength);

    // get cdc data
    std::vector<CDCGetEventsResponse> output_resps;
    s = GetCDCData(cluster_id, slot_range_idx, storage, &next_seq, &output_resps);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_resps.size(), 1);
    // check resp0
    auto& hset_resp = output_resps[0];
    // check point
    auto& point = hset_resp.point();
    EXPECT_EQ(point.next_seq_id(), 11);
    EXPECT_EQ(point.prev_rep_id(), db_repl_id);
    // check event content
    auto& hset_event = hset_resp.events(0);
    EXPECT_EQ(hset_event.data_type(), "hash");
    EXPECT_EQ(hset_event.org_command(), "hset");
    EXPECT_EQ(hset_event.eq_command(), "hset");
    EXPECT_EQ(hset_event.key(), key1);
    // check old
    ASSERT_EQ(hset_event.old().fields_size(), 1);
    EXPECT_EQ(hset_event.old().has_ttl(), false);
    EXPECT_EQ(hset_event.old().fields(0).field(), "f2");
    EXPECT_EQ(hset_event.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event.new_().fields_size(), 1);
    EXPECT_EQ(hset_event.new_().has_ttl(), false);
    EXPECT_EQ(hset_event.new_().fields(0).field(), "f2");
    EXPECT_EQ(hset_event.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event.new_().fields(0).value(), "v2");
    EXPECT_EQ(hset_event.new_().fields(0).has_ttl(), false);
  }
}

TEST(CDCBatchExtractor, SlotrangeSplitDataFilterTest) {
  // Test steps:
  // create two slot ranges
  // write key to one slot range
  // get cdc data
  // check cdc data

  MockOptions opt;
  opt.cluster_id = test_active_cluster_id;
  opt.datanode_id = test_active_datanode_id;
  opt.pool = test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {test_db_id, test_db_id + 1};
  // create server
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  // set topo [0,8191],[8192,16383]
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

  // enable cdc sync
  storage1->GetConfig()->enable_cdc_sync = true;
  storage2->GetConfig()->enable_cdc_sync = true;

  // create commands
  // hash commands
  std::unique_ptr<Commander> cmd_hset;
  s = srv.GetServer()->LookupAndCreateCommand("hset", &cmd_hset);
  ASSERT_TRUE(s.IsOK());

  // prepare keys in two slot ranges
  // keys in [0,8191]
  std::string hkey_prefix{"hkey_"};
  std::string key1 = hkey_prefix + "{" + global_slot_keys[1] + "}_1";
  std::string key100 = hkey_prefix + "{" + global_slot_keys[100] + "}_100";
  std::string key1000 = hkey_prefix + "{" + global_slot_keys[1000] + "}_1000";
  // keys in [8192,16383]
  std::string key8192 = hkey_prefix + "{" + global_slot_keys[8192] + "}_8192";
  std::string key9192 = hkey_prefix + "{" + global_slot_keys[9192] + "}_9192";
  std::string key10192 = hkey_prefix + "{" + global_slot_keys[10192] + "}_10192";
  // fields
  std::string f1 = "f1";
  std::string f2 = "f2";
  std::string v1 = "v1";
  std::string v2 = "v2";

  // construc cmd tokens
  CommandTokens hset_cmd_token{"hset", key1, f1, v1, f2, v2};
  CommandTokens hset_cmd_token100{"hset", key100, f1, v1, f2, v2};
  CommandTokens hset_cmd_token1000{"hset", key1000, f1, v1, f2, v2};
  CommandTokens hset_cmd_token8192{"hset", key8192, f1, v1, f2, v2};
  CommandTokens hset_cmd_token9192{"hset", key9192, f1, v1, f2, v2};
  CommandTokens hset_cmd_token10192{"hset", key10192, f1, v1, f2, v2};

  // exec cmds in storage1
  std::string output;
  s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token8192, &output, cmd_hset, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token100, &output, cmd_hset, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token9192, &output, cmd_hset, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token1000, &output, cmd_hset, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token10192, &output, cmd_hset, srv_ptr, &conn, storage1);
  ASSERT_TRUE(s.IsOK());
  // exec cmds in storage2
  s = GenericExecCmd(hset_cmd_token, &output, cmd_hset, srv_ptr, &conn, storage2);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token8192, &output, cmd_hset, srv_ptr, &conn, storage2);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token100, &output, cmd_hset, srv_ptr, &conn, storage2);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token9192, &output, cmd_hset, srv_ptr, &conn, storage2);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token1000, &output, cmd_hset, srv_ptr, &conn, storage2);
  ASSERT_TRUE(s.IsOK());
  s = GenericExecCmd(hset_cmd_token10192, &output, cmd_hset, srv_ptr, &conn, storage2);
  ASSERT_TRUE(s.IsOK());

  // Test slotrange1 [0,8191]
  {
    // next sequence
    uint64_t next_seq = 0;

    // node info
    std::string cluster_id = test_active_cluster_id;
    SlotRangeIndex slot_range_idx;
    slot_range_idx.set_start(0);
    slot_range_idx.set_end(8191);
    std::string db_repl_id = *(storage1->GetReplIdFromDbEngine());
    ASSERT_TRUE(db_repl_id.size() == kReplIdLength);

    // get cdc  data
    std::vector<CDCGetEventsResponse> output_resps;
    s = GetCDCData(cluster_id, slot_range_idx, storage1, &next_seq, &output_resps);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_resps.size(), 3);
    // check resp0
    auto& hset_resp = output_resps[0];
    // check point
    auto& point = hset_resp.point();
    EXPECT_EQ(point.next_seq_id(), 5);
    EXPECT_EQ(point.prev_rep_id(), db_repl_id);
    // check event content
    auto& hset_event = hset_resp.events(0);
    EXPECT_EQ(hset_event.data_type(), "hash");
    EXPECT_EQ(hset_event.org_command(), "hset");
    EXPECT_EQ(hset_event.eq_command(), "hset");
    EXPECT_EQ(hset_event.key(), key1);
    // check old
    ASSERT_EQ(hset_event.old().fields_size(), 2);
    EXPECT_EQ(hset_event.old().has_ttl(), false);
    EXPECT_EQ(hset_event.old().fields(1).field(), "f1");
    EXPECT_EQ(hset_event.old().fields(1).has_value(), false);
    EXPECT_EQ(hset_event.old().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event.old().fields(0).field(), "f2");
    EXPECT_EQ(hset_event.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event.new_().fields_size(), 2);
    EXPECT_EQ(hset_event.new_().has_ttl(), false);
    EXPECT_EQ(hset_event.new_().fields(1).field(), "f1");
    EXPECT_EQ(hset_event.new_().fields(1).has_value(), true);
    EXPECT_EQ(hset_event.new_().fields(1).value(), "v1");
    EXPECT_EQ(hset_event.new_().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event.new_().fields(0).field(), "f2");
    EXPECT_EQ(hset_event.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event.new_().fields(0).value(), "v2");
    EXPECT_EQ(hset_event.new_().fields(0).has_ttl(), false);

    // check resp1
    auto& hset_resp1 = output_resps[1];
    // check point
    auto& point1 = hset_resp1.point();
    EXPECT_EQ(point1.next_seq_id(), 11);
    EXPECT_EQ(point1.prev_rep_id(), db_repl_id);
    // check event content
    auto& hset_event1 = hset_resp1.events(1);
    EXPECT_EQ(hset_event1.data_type(), "hash");
    EXPECT_EQ(hset_event1.org_command(), "hset");
    EXPECT_EQ(hset_event1.eq_command(), "hset");
    EXPECT_EQ(hset_event1.key(), key100);
    // check old
    ASSERT_EQ(hset_event1.old().fields_size(), 2);
    EXPECT_EQ(hset_event1.old().has_ttl(), false);
    EXPECT_EQ(hset_event1.old().fields(1).field(), "f1");
    EXPECT_EQ(hset_event1.old().fields(1).has_value(), false);
    EXPECT_EQ(hset_event1.old().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event1.old().fields(0).field(), "f2");
    EXPECT_EQ(hset_event1.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event1.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event1.new_().fields_size(), 2);
    EXPECT_EQ(hset_event1.new_().has_ttl(), false);
    EXPECT_EQ(hset_event1.new_().fields(1).field(), "f1");
    EXPECT_EQ(hset_event1.new_().fields(1).has_value(), true);
    EXPECT_EQ(hset_event1.new_().fields(1).value(), "v1");
    EXPECT_EQ(hset_event1.new_().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event1.new_().fields(0).field(), "f2");
    EXPECT_EQ(hset_event1.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event1.new_().fields(0).value(), "v2");
    EXPECT_EQ(hset_event1.new_().fields(0).has_ttl(), false);

    // check resp2
    auto& hset_resp2 = output_resps[2];
    // check point
    auto& point2 = hset_resp2.point();
    EXPECT_EQ(point2.next_seq_id(), 17);
    EXPECT_EQ(point2.prev_rep_id(), db_repl_id);
    // check event content2
    auto& hset_event2 = hset_resp2.events(2);
    EXPECT_EQ(hset_event2.data_type(), "hash");
    EXPECT_EQ(hset_event2.org_command(), "hset");
    EXPECT_EQ(hset_event2.eq_command(), "hset");
    EXPECT_EQ(hset_event2.key(), key1000);
    // check old
    ASSERT_EQ(hset_event2.old().fields_size(), 2);
    EXPECT_EQ(hset_event2.old().has_ttl(), false);
    EXPECT_EQ(hset_event2.old().fields(1).field(), "f1");
    EXPECT_EQ(hset_event2.old().fields(1).has_value(), false);
    EXPECT_EQ(hset_event2.old().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event2.old().fields(0).field(), "f2");
    EXPECT_EQ(hset_event2.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event2.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event2.new_().fields_size(), 2);
    EXPECT_EQ(hset_event2.new_().has_ttl(), false);
    EXPECT_EQ(hset_event2.new_().fields(1).field(), "f1");
    EXPECT_EQ(hset_event2.new_().fields(1).has_value(), true);
    EXPECT_EQ(hset_event2.new_().fields(1).value(), "v1");
    EXPECT_EQ(hset_event2.new_().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event2.new_().fields(0).field(), "f2");
    EXPECT_EQ(hset_event2.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event2.new_().fields(0).value(), "v2");
    EXPECT_EQ(hset_event2.new_().fields(0).has_ttl(), false);
  }

  // Test slotrange2 [8192,16383]
  {
    // next sequence
    uint64_t next_seq = 0;

    // node info
    std::string cluster_id = test_active_cluster_id;
    SlotRangeIndex slot_range_idx;
    slot_range_idx.set_start(8192);
    slot_range_idx.set_end(16383);
    std::string db_repl_id = *(storage2->GetReplIdFromDbEngine());
    ASSERT_TRUE(db_repl_id.size() == kReplIdLength);

    // get cdc data
    std::vector<CDCGetEventsResponse> output_resps;
    s = GetCDCData(cluster_id, slot_range_idx, storage2, &next_seq, &output_resps);
    ASSERT_TRUE(s.IsOK());
    // check result
    ASSERT_EQ(output_resps.size(), 3);
    // check resp0
    auto& hset_resp = output_resps[0];
    // check point
    auto& point = hset_resp.point();
    EXPECT_EQ(point.next_seq_id(), 8);
    EXPECT_EQ(point.prev_rep_id(), db_repl_id);
    // check event content
    auto& hset_event = hset_resp.events(0);
    EXPECT_EQ(hset_event.data_type(), "hash");
    EXPECT_EQ(hset_event.org_command(), "hset");
    EXPECT_EQ(hset_event.eq_command(), "hset");
    EXPECT_EQ(hset_event.key(), key8192);
    // check old
    ASSERT_EQ(hset_event.old().fields_size(), 2);
    EXPECT_EQ(hset_event.old().has_ttl(), false);
    EXPECT_EQ(hset_event.old().fields(1).field(), "f1");
    EXPECT_EQ(hset_event.old().fields(1).has_value(), false);
    EXPECT_EQ(hset_event.old().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event.old().fields(0).field(), "f2");
    EXPECT_EQ(hset_event.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event.new_().fields_size(), 2);
    EXPECT_EQ(hset_event.new_().has_ttl(), false);
    EXPECT_EQ(hset_event.new_().fields(1).field(), "f1");
    EXPECT_EQ(hset_event.new_().fields(1).has_value(), true);
    EXPECT_EQ(hset_event.new_().fields(1).value(), "v1");
    EXPECT_EQ(hset_event.new_().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event.new_().fields(0).field(), "f2");
    EXPECT_EQ(hset_event.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event.new_().fields(0).value(), "v2");
    EXPECT_EQ(hset_event.new_().fields(0).has_ttl(), false);

    // check resp1
    auto& hset_resp1 = output_resps[1];
    // check point
    auto& point1 = hset_resp1.point();
    EXPECT_EQ(point1.next_seq_id(), 14);
    EXPECT_EQ(point1.prev_rep_id(), db_repl_id);
    // check event content
    auto& hset_event1 = hset_resp1.events(1);
    EXPECT_EQ(hset_event1.data_type(), "hash");
    EXPECT_EQ(hset_event1.org_command(), "hset");
    EXPECT_EQ(hset_event1.eq_command(), "hset");
    EXPECT_EQ(hset_event1.key(), key9192);
    // check old
    ASSERT_EQ(hset_event1.old().fields_size(), 2);
    EXPECT_EQ(hset_event1.old().has_ttl(), false);
    EXPECT_EQ(hset_event1.old().fields(1).field(), "f1");
    EXPECT_EQ(hset_event1.old().fields(1).has_value(), false);
    EXPECT_EQ(hset_event1.old().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event1.old().fields(0).field(), "f2");
    EXPECT_EQ(hset_event1.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event1.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event1.new_().fields_size(), 2);
    EXPECT_EQ(hset_event1.new_().has_ttl(), false);
    EXPECT_EQ(hset_event1.new_().fields(1).field(), "f1");
    EXPECT_EQ(hset_event1.new_().fields(1).has_value(), true);
    EXPECT_EQ(hset_event1.new_().fields(1).value(), "v1");
    EXPECT_EQ(hset_event1.new_().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event1.new_().fields(0).field(), "f2");
    EXPECT_EQ(hset_event1.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event1.new_().fields(0).value(), "v2");
    EXPECT_EQ(hset_event1.new_().fields(0).has_ttl(), false);

    // check resp2
    auto& hset_resp2 = output_resps[2];
    // check point
    auto& point2 = hset_resp2.point();
    EXPECT_EQ(point2.next_seq_id(), 20);
    EXPECT_EQ(point2.prev_rep_id(), db_repl_id);
    // check event content2
    auto& hset_event2 = hset_resp2.events(2);
    EXPECT_EQ(hset_event2.data_type(), "hash");
    EXPECT_EQ(hset_event2.org_command(), "hset");
    EXPECT_EQ(hset_event2.eq_command(), "hset");
    EXPECT_EQ(hset_event2.key(), key10192);
    // check old
    ASSERT_EQ(hset_event2.old().fields_size(), 2);
    EXPECT_EQ(hset_event2.old().has_ttl(), false);
    EXPECT_EQ(hset_event2.old().fields(1).field(), "f1");
    EXPECT_EQ(hset_event2.old().fields(1).has_value(), false);
    EXPECT_EQ(hset_event2.old().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event2.old().fields(0).field(), "f2");
    EXPECT_EQ(hset_event2.old().fields(0).has_value(), false);
    EXPECT_EQ(hset_event2.old().fields(0).has_ttl(), false);
    // check new
    ASSERT_EQ(hset_event2.new_().fields_size(), 2);
    EXPECT_EQ(hset_event2.new_().has_ttl(), false);
    EXPECT_EQ(hset_event2.new_().fields(1).field(), "f1");
    EXPECT_EQ(hset_event2.new_().fields(1).has_value(), true);
    EXPECT_EQ(hset_event2.new_().fields(1).value(), "v1");
    EXPECT_EQ(hset_event2.new_().fields(1).has_ttl(), false);
    EXPECT_EQ(hset_event2.new_().fields(0).field(), "f2");
    EXPECT_EQ(hset_event2.new_().fields(0).has_value(), true);
    EXPECT_EQ(hset_event2.new_().fields(0).value(), "v2");
    EXPECT_EQ(hset_event2.new_().fields(0).has_ttl(), false);
  }
}

}  // namespace redis
