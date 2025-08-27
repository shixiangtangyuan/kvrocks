#include "ingest/ingest.h"

#include <gtest/gtest.h>
#include <rocksdb/sst_file_writer.h>

#include <filesystem>

#include "kv/datanode/v1/common.pb.h"
#include "kv/datanode/v1/ingest.pb.h"
#include "mock/mock_server.h"
#include "sync/sync_test_util.h"
#include "types/redis_string.h"

namespace redis {

namespace fs = std::filesystem;
bool GenerateSstFile(std::string &path, const std::vector<std::string> &keys) {
  path = "./sst_test";
  fs::create_directories(path);
  auto sst_path = path + "/test.sst";
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::EnvOptions env_info(options);
  rocksdb::SstFileWriter sst_writer(env_info, options);
  rocksdb::Status status = sst_writer.Open(sst_path);
  if (!status.ok()) {
    std::cout << "create sst file failed, msg: " << status.ToString();
    return false;
  }

  for (auto key : keys) {
    std::string value = "sst_value";
    auto slot = GetSlotIdFromKey(key);
    std::string ns_key;
    PutFixed16(&ns_key, slot);
    ns_key.append(key.data(), key.size());
    std::string bytes;
    Metadata metadata(kRedisString, false);
    metadata.Encode(&bytes);
    bytes.append(value.data(), value.size());
    status = sst_writer.Put(ns_key, bytes);
    if (!status.ok()) {
      std::cout << "sst write put key failed, msg: " << status.ToString();
      return false;
    }
  }
  sst_writer.Finish();
  return true;
}

std::unique_ptr<MockServer> CreateServer(uint32_t port, bool is_active_pool, bool is_serving, int16_t start,
                                         int16_t end) {
  MockOptions opt;
  opt.port = port;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto serving_node_id = "serving_node_id";
  auto importing_node_id = "importing_node_id";
  opt.datanode_id = is_serving ? serving_node_id : importing_node_id;
  auto srv = std::make_unique<MockServer>(opt);
  srv->StopCtrlClient();
  auto status = ApplyTopo(*srv.get(), db_id, is_active_pool, is_serving, start, end,
                          is_serving ? importing_node_id : serving_node_id);
  CHECK(status.IsOK());
  return srv;
}

TEST(Ingest, Ingester) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto status = ApplyTopo(srv, db_id, true, true, start, end);
  ASSERT_TRUE(status.IsOK());
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range != nullptr);
  auto storage = srv.GetStorage(db_id);
  ASSERT_TRUE(storage != nullptr);

  // mock request
  std::vector<std::string> keys;
  keys.emplace_back("sst_key");
  std::string sst_path;
  ASSERT_TRUE(GenerateSstFile(sst_path, keys));
  std::string cf_name = "metadata";
  std::string ingestion_task_name = "testIngest";

  grpc::CallbackServerContext ctx;
  kv::datanode::v1::IngestRequest req;
  kv::datanode::v1::IngestResponse reps;
  req.set_cluster_id(opt.cluster_id);
  req.set_ingestion_task_name(ingestion_task_name);
  req.mutable_slot_range()->set_start(start);
  req.mutable_slot_range()->set_end(end);
  auto cf1 = req.add_cfs_info();
  cf1->set_cf_name(cf_name);
  cf1->set_sst_files_path(sst_path);
  cf1->set_check_sum(false);

  // basic ingest
  // ingest
  auto ingester = storage->GetIngester();
  auto &task_id = req.ingestion_task_name();
  auto &slot_range_idx = req.slot_range();
  auto slot_range_idx_name = redis::SlotRangeIndexToString(slot_range_idx);
  std::vector<ingest::IngestArg> req_args;
  auto &cfs = req.cfs_info();
  for (auto &cf : cfs) {
    req_args.emplace_back(cf.cf_name(), cf.sst_files_path(), cf.check_sum());
  }

  ingest::IngestTaskInfo info(slot_range_idx_name, req_args, task_id);
  auto s = ingester->Run(info);
  ASSERT_TRUE(s.IsOK());

  std::string value;
  redis::String db(storage.get(), "string_ns");
  auto res = db.Get("sst_key", &value);
  if (!res.ok()) {
    std::cout << res.ToString() << std::endl;
  }
  ASSERT_TRUE(res.ok());

  // ingest mutually exclusive
  s = ingester->Run(info);
  ASSERT_TRUE(!s.IsOK());
  ASSERT_TRUE(s.GetCode() == Status::IngestJobRunning);
  ingester->IngestDone(info);

  // invalid args
  std::vector<ingest::IngestArg> invalid_args;
  ingest::IngestTaskInfo invalid_info(slot_range_idx_name, invalid_args, task_id);
  s = ingester->Run(invalid_info);
  ASSERT_TRUE(!s.IsOK());
  ASSERT_TRUE(s.GetCode() == Status::IngestInvalidInfo);
  ASSERT_TRUE(s.Msg() == "Invalid ingest info");
  ingester->IngestDone(invalid_info);

  invalid_args.emplace_back("invalid_name", sst_path, false);
  ingest::IngestTaskInfo invalid_info1(slot_range_idx_name, invalid_args, task_id);
  s = ingester->Run(invalid_info1);
  ASSERT_TRUE(s.GetCode() == Status::IngestInvalidInfo);
  ASSERT_TRUE(s.Msg() == "Illegal column family");
  ingester->IngestDone(invalid_info1);

  // sst path is empty failed
  std::vector<ingest::IngestArg> empty_sst_args;
  empty_sst_args.emplace_back("metadata", sst_path, false);
  ingest::IngestTaskInfo empty_sst_info(slot_range_idx_name, empty_sst_args, task_id);
  s = ingester->Run(empty_sst_info);
  ASSERT_TRUE(!s.IsOK());
  ASSERT_TRUE(s.GetCode() == Status::RedisExecErr);
  ingester->IngestDone(empty_sst_info);

  // change db config
  auto org_enable_compact_range = storage->GetConfig()->enable_compact_range;
  auto org_enable_compact_full = storage->GetConfig()->enable_compact_full;
  s = ingester->Run(info);
  ASSERT_TRUE(storage->GetConfig()->enable_compact_range == false);
  ASSERT_TRUE(storage->GetConfig()->enable_compact_full == false);
  ingester->IngestDone(info);
  ASSERT_TRUE(storage->GetConfig()->enable_compact_range == org_enable_compact_range);
  ASSERT_TRUE(storage->GetConfig()->enable_compact_full == org_enable_compact_full);

  // query db status
  auto result = ingester->GetResult("notFind");
  ASSERT_TRUE(result.code() == kv::datanode::v1::ErrorCode::ERROR_CODE_INGESTION_TASK_NOT_FOUND);

  ASSERT_TRUE(GenerateSstFile(sst_path, keys));
  std::vector<ingest::IngestArg> sucessed_sst_args;
  sucessed_sst_args.emplace_back("metadata", sst_path, false);
  ingest::IngestTaskInfo sucessed_info(slot_range_idx_name, sucessed_sst_args, task_id);
  s = ingester->Run(sucessed_info);
  ASSERT_TRUE(s.IsOK());
  result = ingester->GetResult(sucessed_info.task_id);
  ASSERT_TRUE(result.code() == kv::datanode::v1::ErrorCode::ERROR_CODE_INGESTION_TASK_FINISHED);
  ingester->IngestDone(sucessed_info);

  // sst path is empty failed
  s = ingester->Run(empty_sst_info);
  ASSERT_TRUE(!s.IsOK());
  result = ingester->GetResult(info.task_id);
  ASSERT_TRUE(result.code() == kv::datanode::v1::ErrorCode::ERROR_CODE_INGESTION_TASK_FAILED);
}

TEST(Ingest, rpctest) {
  int16_t start = 0, end = kClusterSlots - 1;
  std::string slot_range_name = CreateSlotRangeName(start, end);
  uint32_t serving_port = 1234;
  auto serving_srv = CreateServer(serving_port, true, true, start, end);
  auto serving_srv_opts = serving_srv->GetMockOptions();
  auto serving_srv_slot_range = serving_srv->GetSlotRange(start, end);
  // channel
  grpc::ChannelArguments args;
  auto addr = "127.0.0.1:" + std::to_string(Config::GetGrpcPort(serving_port));
  auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  auto stub_tmp = kv::datanode::v1::DataNodeService::NewStub(channel);
  std::shared_ptr<kv::datanode::v1::DataNodeService::Stub> stub = std::move(stub_tmp);

  // mock request
  std::vector<std::string> keys;
  keys.emplace_back("sst_key");
  std::string sst_path;
  ASSERT_TRUE(GenerateSstFile(sst_path, keys));
  std::string cf_name = "metadata";
  std::string ingestion_task_name = "testIngest";

  grpc::ClientContext ctx;
  kv::datanode::v1::IngestRequest req;
  kv::datanode::v1::IngestResponse reps;
  req.set_cluster_id(serving_srv_opts.cluster_id);
  req.set_ingestion_task_name(ingestion_task_name);
  req.mutable_slot_range()->set_start(start);
  req.mutable_slot_range()->set_end(end);
  auto cf1 = req.add_cfs_info();
  cf1->set_cf_name(cf_name);
  cf1->set_sst_files_path(sst_path);
  cf1->set_check_sum(false);

  // ingest sucess
  auto status = stub->Ingest(&ctx, req, &reps);
  ASSERT_TRUE(status.ok());
  kv::datanode::v1::GetIngestInfoRequest get_req;
  kv::datanode::v1::GetIngestInfoResponse get_resp;

  // ingest faild other task running
  {
    ASSERT_TRUE(GenerateSstFile(sst_path, keys));
    auto storage = serving_srv->GetStorage(1);
    auto ingester = storage->GetIngester();
    auto &task_id = req.ingestion_task_name();
    auto &slot_range_idx = req.slot_range();
    auto slot_range_idx_name = redis::SlotRangeIndexToString(slot_range_idx);
    std::vector<ingest::IngestArg> req_args;
    auto &cfs = req.cfs_info();
    for (auto &cf : cfs) {
      req_args.emplace_back(cf.cf_name(), cf.sst_files_path(), cf.check_sum());
    }

    ingest::IngestTaskInfo info(slot_range_idx_name, req_args, task_id);
    auto s = ingester->Run(info);
    ASSERT_TRUE(s.IsOK());

    ASSERT_TRUE(GenerateSstFile(sst_path, keys));
    grpc::ClientContext failed_ctx;
    kv::datanode::v1::IngestRequest failed_req;
    failed_req.set_cluster_id(serving_srv_opts.cluster_id);
    failed_req.set_ingestion_task_name(ingestion_task_name);
    failed_req.mutable_slot_range()->set_start(start);
    failed_req.mutable_slot_range()->set_end(end);
    auto cf1 = req.add_cfs_info();
    cf1->set_cf_name(cf_name);
    cf1->set_sst_files_path(sst_path);
    cf1->set_check_sum(false);

    kv::datanode::v1::IngestResponse failed_reps;
    status = stub->Ingest(&failed_ctx, failed_req, &failed_reps);
    ASSERT_TRUE(!status.ok());
    auto details = status.error_details();
    ASSERT_TRUE(details.find("ingest task is running") != std::string::npos);

    // GetIngestInfo
    grpc::ClientContext get_ctx;
    kv::datanode::v1::GetIngestInfoRequest get_req;
    kv::datanode::v1::GetIngestInfoResponse get_resp;
    get_req.set_cluster_id(serving_srv_opts.cluster_id);
    get_req.set_ingestion_task_name(ingestion_task_name);
    get_req.mutable_slot_range()->set_start(start);
    get_req.mutable_slot_range()->set_end(end);
    auto get_status = stub->GetIngestInfo(&get_ctx, get_req, &get_resp);
    ASSERT_TRUE(get_status.ok());
    ASSERT_TRUE(get_resp.result().code() == kv::datanode::v1::ErrorCode::ERROR_CODE_INGESTION_TASK_FINISHED);
  }

  // ingest failed, invalid cluster id
  {
    ASSERT_TRUE(GenerateSstFile(sst_path, keys));
    grpc::ClientContext failed_ctx;
    kv::datanode::v1::IngestRequest failed_req;
    failed_req.set_cluster_id("invalid_cluster_id");
    failed_req.set_ingestion_task_name(ingestion_task_name);
    failed_req.mutable_slot_range()->set_start(start);
    failed_req.mutable_slot_range()->set_end(end);
    auto cf1 = req.add_cfs_info();
    cf1->set_cf_name(cf_name);
    cf1->set_sst_files_path(sst_path);
    cf1->set_check_sum(false);

    kv::datanode::v1::IngestResponse failed_reps;
    status = stub->Ingest(&failed_ctx, failed_req, &failed_reps);
    ASSERT_TRUE(!status.ok());
    auto details = status.error_details();
    ASSERT_TRUE(details.find("cluster id mismatch") != std::string::npos);
  }

  // ingest failed, invalid slot_range
  {
    ASSERT_TRUE(GenerateSstFile(sst_path, keys));
    grpc::ClientContext failed_ctx;
    kv::datanode::v1::IngestRequest failed_req;
    failed_req.set_cluster_id(serving_srv_opts.cluster_id);
    failed_req.set_ingestion_task_name("slot_range_not_find");
    failed_req.mutable_slot_range()->set_start(0);
    failed_req.mutable_slot_range()->set_end(1);
    auto cf1 = req.add_cfs_info();
    cf1->set_cf_name(cf_name);
    cf1->set_sst_files_path(sst_path);
    cf1->set_check_sum(false);

    kv::datanode::v1::IngestResponse failed_reps;
    status = stub->Ingest(&failed_ctx, failed_req, &failed_reps);
    ASSERT_TRUE(!status.ok());
    auto details = status.error_details();
    ASSERT_TRUE(details.find("slot range not found") != std::string::npos);

    // GetIngestInfo
    grpc::ClientContext get_ctx;
    kv::datanode::v1::GetIngestInfoRequest get_req;
    kv::datanode::v1::GetIngestInfoResponse get_resp;
    get_req.set_cluster_id(serving_srv_opts.cluster_id);
    get_req.set_ingestion_task_name("slot_range_not_find");
    get_req.mutable_slot_range()->set_start(start);
    get_req.mutable_slot_range()->set_end(end);
    auto get_status = stub->GetIngestInfo(&get_ctx, get_req, &get_resp);
    ASSERT_TRUE(get_status.ok());
    ASSERT_TRUE(get_resp.result().code() == kv::datanode::v1::ErrorCode::ERROR_CODE_INGESTION_TASK_NOT_FOUND);
  }

  // ingest failed sst path is empty
  {
    grpc::ClientContext failed_ctx;
    kv::datanode::v1::IngestRequest failed_req;
    failed_req.set_cluster_id(serving_srv_opts.cluster_id);
    failed_req.set_ingestion_task_name(ingestion_task_name);
    failed_req.mutable_slot_range()->set_start(start);
    failed_req.mutable_slot_range()->set_end(end);
    auto cf1 = req.add_cfs_info();
    cf1->set_cf_name(cf_name);
    cf1->set_sst_files_path(sst_path);
    cf1->set_check_sum(false);

    kv::datanode::v1::IngestResponse failed_reps;
    status = stub->Ingest(&failed_ctx, failed_req, &failed_reps);
    ASSERT_TRUE(!status.ok());
    auto details = status.error_details();
    ASSERT_TRUE(details.find("ingest task failed") != std::string::npos);
  }

  // GetIngestInfo
  {
    grpc::ClientContext get_ctx;
    kv::datanode::v1::GetIngestInfoRequest get_req;
    kv::datanode::v1::GetIngestInfoResponse get_resp;
    get_req.set_cluster_id(serving_srv_opts.cluster_id);
    get_req.set_ingestion_task_name(ingestion_task_name);
    get_req.mutable_slot_range()->set_start(start);
    get_req.mutable_slot_range()->set_end(end);
    auto get_status = stub->GetIngestInfo(&get_ctx, get_req, &get_resp);
    ASSERT_TRUE(get_status.ok());
    ASSERT_TRUE(get_resp.result().code() == kv::datanode::v1::ErrorCode::ERROR_CODE_INGESTION_TASK_FAILED);
  }

  // StopDatanodeDts StartDatanodeDts
  {
    grpc::ClientContext ctx;
    kv::datanode::v1::StopDatanodeDtsRequest req;
    kv::datanode::v1::StopDatanodeDtsResponse resp;
    req.set_cluster_id(serving_srv_opts.cluster_id);
    req.mutable_slot_range()->set_start(start);
    req.mutable_slot_range()->set_end(end);
    auto stop_status = stub->StopDatanodeDts(&ctx, req, &resp);
    ASSERT_TRUE(stop_status.ok());

    auto slot_range = serving_srv->GetSlotRange(start, end);
    auto status = slot_range->GetDtsWriteRunningStatus();
    ASSERT_TRUE(redis::WriteStatus::WR_PROHIBITED == status);

    grpc::ClientContext start_ctx;
    kv::datanode::v1::StartDatanodeDtsRequest start_req;
    kv::datanode::v1::StartDatanodeDtsResponse start_resp;
    start_req.set_cluster_id(serving_srv_opts.cluster_id);
    start_req.mutable_slot_range()->set_start(start);
    start_req.mutable_slot_range()->set_end(end);
    auto start_status = stub->StartDatanodeDts(&start_ctx, start_req, &start_resp);
    ASSERT_TRUE(start_status.ok());
    status = slot_range->GetDtsWriteRunningStatus();
    ASSERT_TRUE(redis::WriteStatus::UNSPECIFIED == status);
  }
}

}  // namespace redis
