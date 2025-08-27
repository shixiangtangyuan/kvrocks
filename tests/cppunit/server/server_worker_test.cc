#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cluster/set_topo_util.h"
#include "mock/mock_server.h"
#include "server/server.h"
#include "server/worker.h"

namespace engine {

TEST(ServerTest, GetWorkersBlockedDuration) {
  // 创建MockServer来测试GetWorkersBlockedDuration方法
  MockOptions opt;
  opt.workers = 3;  // 设置3个工作线程
  opt.cluster_id = redis::test_active_cluster_id;
  opt.datanode_id = redis::test_active_datanode_id;
  opt.pool = redis::test_active_pool;
  opt.db_ids.clear();
  opt.db_ids = {1, 2};

  // 创建服务器
  auto srv = MockServer(opt);
  srv.StopCtrlClient();

  // 设置topo
  auto s = redis::SetTopo(srv);
  ASSERT_TRUE(s.IsOK());

  // 获取Server指针
  auto server = srv.GetServer();

  // 测试场景1: 所有工作线程都没有阻塞
  auto duration = server->GetWorkersBlockedDuration();
  EXPECT_EQ(duration.count(), 0);

  // 测试场景2: 设置一个工作线程的阻塞时间
  auto& worker_threads = srv.GetWorkerThreads();
  ASSERT_GE(worker_threads.size(), 1);

  // 设置第一个worker的阻塞时间为当前时间
  auto now = std::chrono::steady_clock::now();
  auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
  worker_threads[0]->GetWorker()->blocked_worker_start_block_time.store(now_ns);

  // 验证返回的阻塞时间大于0
  duration = server->GetWorkersBlockedDuration();
  EXPECT_GT(duration.count(), 0);

  // 睡眠一段时间
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // 再次验证阻塞时间增加了
  auto new_duration = server->GetWorkersBlockedDuration();
  EXPECT_GT(new_duration.count(), duration.count());

  // 测试场景3: 设置多个工作线程的阻塞时间，应返回最早阻塞的时间
  if (worker_threads.size() >= 2) {
    // 将第二个worker设置为更早的阻塞时间
    auto earlier_time = now_ns - std::chrono::nanoseconds(100000000);  // 100ms earlier
    worker_threads[1]->GetWorker()->blocked_worker_start_block_time.store(earlier_time);

    // 验证返回的是更早阻塞的线程的持续时间
    duration = server->GetWorkersBlockedDuration();
    EXPECT_GT(duration.count(), new_duration.count());

    // 将第一个工作线程恢复未阻塞状态
    worker_threads[0]->GetWorker()->blocked_worker_start_block_time.store(std::chrono::nanoseconds(0));

    // 验证仍返回第二个工作线程的阻塞时间
    duration = server->GetWorkersBlockedDuration();
    EXPECT_GT(duration.count(), 0);

    // 将所有工作线程恢复未阻塞状态
    for (auto& thread : worker_threads) {
      thread->GetWorker()->blocked_worker_start_block_time.store(std::chrono::nanoseconds(0));
    }

    // 验证所有工作线程都未阻塞
    duration = server->GetWorkersBlockedDuration();
    EXPECT_EQ(duration.count(), 0);
  }
}

}  // namespace engine