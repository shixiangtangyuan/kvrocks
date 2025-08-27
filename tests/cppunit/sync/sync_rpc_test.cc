#include <gtest/gtest.h>
#include <kv/datanode/v1/service.grpc.pb.h>

#include "config/config.h"
#include "mock/mock_server.h"
#include "server/grpc_interceptor.h"
#include "sync/sync_puller.h"
#include "sync_test_util.h"

class GlobalStatsHelper {
 public:
  static std::map<Attributes, Histogram, AttrCmp> CollectHistogramFromMetric(GlobalStats& stats, MetricType type) {
    return stats.collectHistogramFromMetric(type);
  }

  static std::map<Attributes, uint64_t, AttrCmp> CollectCounterFromMetric(GlobalStats& stats, MetricType type) {
    return stats.collectCounterFromMetric(type);
  }
};

uint64_t GetHistogramCount(Histogram& hist) {
  uint64_t cnt = hist.GetLast().count;
  for (auto& span : hist.GetTimeSpans()) {
    cnt += span.count;
  }
  return cnt;
}

namespace redis {

std::unique_ptr<MockServer> MakeServer(uint32_t port, bool is_active_pool, bool is_serving, int16_t start,
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

TEST(Sync, RPC) {
  for (auto& metric : thread_local_metric_array.metrics) {
    metric.reset();
  }
  for (auto& metric : GlobalStatsInstance().global_metrics) {
    metric.clear();
  }
  for (auto& metric : GlobalStatsInstance().sum_metrics) {
    metric.reset();
  }
  int16_t start = 0, end = kClusterSlots - 1;
  std::string slot_range_name = CreateSlotRangeName(start, end);
  uint32_t serving_port = 1234;
  auto serving_srv = MakeServer(serving_port, true, true, start, end);
  auto serving_srv_opts = serving_srv->GetMockOptions();
  auto serving_srv_slot_range = serving_srv->GetSlotRange(start, end);
  uint32_t importing_port = 2345;
  auto importing_srv = MakeServer(importing_port, true, false, start, end);
  auto importing_srv_opts = importing_srv->GetMockOptions();
  auto importing_srv_slot_range = importing_srv->GetSlotRange(start, end);

  // channel
  grpc::ChannelArguments args = serving_srv->GetServer()->GetConfig()->BuildDatanodeChannelArgs();
  auto addr = "127.0.0.1:" + std::to_string(Config::GetGrpcPort(serving_port));
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptor_creators;
  interceptor_creators.push_back(std::make_unique<ClientStatsInterceptorFactory>());
  auto channel = grpc::experimental::CreateCustomChannelWithInterceptors(addr, grpc::InsecureChannelCredentials(), args,
                                                                         std::move(interceptor_creators));
  auto stub_tmp = kv::datanode::v1::DataNodeService::NewStub(channel);
  std::shared_ptr<kv::datanode::v1::DataNodeService::Stub> stub = std::move(stub_tmp);
  // callback
  bool stop_cb_flag = false, done_cb_flag = false;
  SlotRangeWriteStoppableCB write_stoppable_cb = [&](const std::string&) { stop_cb_flag = true; };
  SlotRangeReplicationDoneCB cb = [&]() { done_cb_flag = true; };
  // puller & sender
  auto new_puller = [&]() {
    auto ret = importing_srv_slot_range->GetSyncPoint();
    EXPECT_TRUE(ret.IsOK());
    return new SyncPuller(serving_srv_opts.cluster_id, importing_srv_opts.datanode_id, serving_srv_opts.datanode_id,
                          importing_srv->GetServer(), importing_srv_slot_range, ret.GetValue(), stub,
                          write_stoppable_cb, cb);
  };
  auto get_sender = [&]() { return serving_srv->GetServer()->sync_manager->getReplSender(slot_range_name); };

  {
    LOG(INFO) << "======== stop write and finish ========";
    // init
    stop_cb_flag = false, done_cb_flag = false;
    auto pull = new_puller();
    usleep(50000);
    auto send = get_sender();
    ASSERT_TRUE(send);
    ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::Syncing);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::WriteStop);
    auto t = std::thread([&]() {
      int i = 0;
      rocksdb::WriteBatch wb;
      std::string name = CreateSlotRangeName(0, 16383);
      auto storage = serving_srv_slot_range->GetStorage();
      while (true) {
        Context ctx;
        auto key = std::to_string(i);
        auto res =
            KeyLock::AcquireKeyLock(name, key, mgl::LockMode::LOCK_X, &ctx, serving_srv->GetServer()->GetMGLockMgr());
        if (!res.IsOK()) {
          LOG(ERROR) << "Failed to get key lock, key: " << key << ", Err: " << res.Msg();
          break;
        }
        auto ret = serving_srv->GetServer()->cluster->CanExecByMySelf(kCmdWrite, slot_range_name, key);
        if (!ret.IsOK()) {
          LOG(ERROR) << "Failed to check executable, key: " << key << ", Err: " << ret.Msg();
          break;
        }
        wb.Clear();
        wb.Put(key, key);
        auto s = storage->Write(storage->DefaultWriteOptions(), &wb);
        if (!s.ok()) {
          LOG(ERROR) << "Failed to write storage, Err:" << s.ToString();
        } else {
          ++i;
        }
      }
      LOG(INFO) << "Write thread exist, total write:" << i;
    });
    // caught up
    stop_cb_flag = false, done_cb_flag = false;
    ASSERT_TRUE(pull->StopWrite());
    usleep(100000);
    ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::CaughtUp);
    ASSERT_EQ(send->GetSyncStatus(), SyncStatusEnum::CaughtUp);
    // finish
    stop_cb_flag = false, done_cb_flag = false;
    send->MarkFinished(kSyncStopWriteTimeoutError);
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
    t.join();
  }
  {
    LOG(INFO) << "======== finish and stop write ========";
    // init
    stop_cb_flag = false, done_cb_flag = false;
    auto pull = new_puller();
    usleep(50000);
    auto send = get_sender();
    ASSERT_TRUE(send);
    ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::WriteStop);
    // finish
    stop_cb_flag = false, done_cb_flag = false;
    send->MarkFinished(kSyncStopWriteTimeoutError);
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
    // stop write
    ASSERT_FALSE(pull->StopWrite());  // already destructed
  }
  {
    LOG(INFO) << "======== stop write and cancel ========";
    // init
    stop_cb_flag = false, done_cb_flag = false;
    auto pull = new_puller();
    usleep(50000);
    auto send = get_sender();
    ASSERT_TRUE(send);
    ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::WriteStop);
    // stop write
    stop_cb_flag = false, done_cb_flag = false;
    ASSERT_TRUE(pull->StopWrite());
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::CaughtUp);
    // cancel
    stop_cb_flag = false, done_cb_flag = false;
    ASSERT_TRUE(pull->MarkFinished(kSyncStopWriteTimeoutError));
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
  }
  {
    LOG(INFO) << "======== cancel and stop write ========";
    // init
    stop_cb_flag = false, done_cb_flag = false;
    auto pull = new_puller();
    usleep(50000);
    auto send = get_sender();
    ASSERT_TRUE(send);
    ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::WriteStop);
    // cancel
    stop_cb_flag = false, done_cb_flag = false;
    ASSERT_TRUE(pull->MarkFinished(kSyncStopWriteTimeoutError));
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
    // stop write
    ASSERT_FALSE(pull->StopWrite());  // already destructed
  }
  {
    LOG(INFO) << "======== cancel and finish ========";
    // init
    stop_cb_flag = false, done_cb_flag = false;
    auto pull = new_puller();
    usleep(50000);
    auto send = get_sender();
    ASSERT_TRUE(send);
    ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::WriteStop);
    // cancel
    stop_cb_flag = false, done_cb_flag = false;
    ASSERT_TRUE(pull->MarkFinished(kSyncStopWriteTimeoutError));
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
    // finish
    stop_cb_flag = false, done_cb_flag = false;
    send->MarkFinished(kSyncStopWriteTimeoutError);
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
  }
  {
    LOG(INFO) << "======== finish and cancel ========";
    // init
    stop_cb_flag = false, done_cb_flag = false;
    auto pull = new_puller();
    usleep(50000);
    auto send = get_sender();
    ASSERT_TRUE(send);
    ASSERT_TRUE(stop_cb_flag && !done_cb_flag);
    ASSERT_EQ(pull->getSyncStatus(), SyncStatusEnum::WriteStop);
    // finish
    stop_cb_flag = false, done_cb_flag = false;
    send->MarkFinished(kSyncStopWriteTimeoutError);
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && done_cb_flag);
    // cancel
    stop_cb_flag = false, done_cb_flag = false;
    ASSERT_FALSE(pull->MarkFinished(kSyncStopWriteTimeoutError));
    usleep(10000);
    ASSERT_TRUE(!stop_cb_flag && !done_cb_flag);
  }
  // check grpc client and server metrics
  auto& stats = GlobalStatsInstance();
  GlobalStatsHelper stats_helper;
  auto req_num_stats =
      stats_helper.CollectCounterFromMetric(GlobalStatsInstance(), MetricType::GRPC_CLIENT_REQUEST_NUM);
  EXPECT_GT(req_num_stats.size(), 0);
  for (auto& [_, num] : req_num_stats) {
    EXPECT_GT(num, 0);
  }
  req_num_stats = stats_helper.CollectCounterFromMetric(stats, MetricType::GRPC_SERVER_REQUEST_NUM);
  EXPECT_GT(req_num_stats.size(), 0);
  for (auto& [_, num] : req_num_stats) {
    EXPECT_GT(num, 0);
  }
  auto req_latency_stats = stats_helper.CollectHistogramFromMetric(stats, MetricType::GRPC_CLIENT_REQUEST_LATENCY);
  EXPECT_GT(req_latency_stats.size(), 0);
  for (auto& [_, hist] : req_latency_stats) {
    EXPECT_GT(GetHistogramCount(hist), 0);
  }
  req_latency_stats = stats_helper.CollectHistogramFromMetric(stats, MetricType::GRPC_SERVER_REQUEST_LATENCY);
  EXPECT_GT(req_latency_stats.size(), 0);
  for (auto& [_, hist] : req_latency_stats) {
    EXPECT_GT(GetHistogramCount(hist), 0);
  }
  auto send_msg_latency_stats =
      stats_helper.CollectHistogramFromMetric(stats, MetricType::GRPC_CLIENT_SEND_MESSAGE_LATENCY);
  EXPECT_GT(send_msg_latency_stats.size(), 0);
  for (auto& [_, hist] : send_msg_latency_stats) {
    EXPECT_GT(GetHistogramCount(hist), 0);
  }
  send_msg_latency_stats = stats_helper.CollectHistogramFromMetric(stats, MetricType::GRPC_SERVER_SEND_MESSAGE_LATENCY);
  EXPECT_GT(send_msg_latency_stats.size(), 0);
  for (auto& [_, hist] : send_msg_latency_stats) {
    EXPECT_GT(GetHistogramCount(hist), 0);
  }
  auto recv_msg_interval_stats =
      stats_helper.CollectHistogramFromMetric(stats, MetricType::GRPC_CLIENT_RECV_MESSAGE_INTERVAL);
  EXPECT_GT(recv_msg_interval_stats.size(), 0);
  for (auto& [_, hist] : recv_msg_interval_stats) {
    EXPECT_GT(GetHistogramCount(hist), 0);
  }
  recv_msg_interval_stats =
      stats_helper.CollectHistogramFromMetric(stats, MetricType::GRPC_SERVER_RECV_MESSAGE_INTERVAL);
  EXPECT_GT(recv_msg_interval_stats.size(), 0);
  for (auto& [_, hist] : recv_msg_interval_stats) {
    EXPECT_GT(GetHistogramCount(hist), 0);
  }
};

}  // namespace redis
