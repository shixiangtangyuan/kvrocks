#include "common/timeout_manager.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <bitset>

TEST(TimeoutManagerTest, Basic) {
  auto mgr = std::make_unique<util::TimeoutManager>();
  mgr->Run();

  struct Arg {
    explicit Arg(int aa) : a(aa) {}
    int a;
  };

  Arg a(10);
  auto task = mgr->AddTimeoutTask(
      10,
      [](TaskCallbackArg arg) {
        auto a = static_cast<Arg*>(arg);
        LOG(INFO) << "arg.a = " << a->a;
        a->a = 20;
      },
      &a);

  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(a.a, 20);
  task.reset();

  mgr->Stop();
}

TEST(TimeoutManagerTest, ResetTaskBeforeTimeout) {
  auto mgr = std::make_unique<util::TimeoutManager>();
  mgr->Run();

  auto task = mgr->AddTimeoutTask(
      20,
      [](TaskCallbackArg arg) {
        EXPECT_TRUE(false);  // task has reset(delete)
      },
      nullptr);

  task.reset();

  mgr->Stop();
}

TEST(TimeoutManager, AddAndRegisterTasks) {
  auto mgr = std::make_unique<util::TimeoutManager>();
  mgr->Run();

  const int cnt = 6;
  const int interval_ms = 20;
  std::bitset<cnt> add_flags(0);
  std::bitset<cnt> reg_flags(0);
  std::vector<std::shared_ptr<util::TimeoutTask>> added_tasks;
  for (int i = 0; i < cnt; ++i) {
    // add task
    auto add_func = [i, &add_flags](void*) mutable { add_flags.set(i, true); };
    auto task = mgr->AddTimeoutTask((i * interval_ms), add_func, nullptr);
    added_tasks.emplace_back(task);
    // register task
    auto reg_func = [i, &reg_flags](void*) mutable { reg_flags.set(i, true); };
    mgr->RegisterTimeoutTask(i * interval_ms, reg_func, nullptr);
  }

  auto total = cnt * interval_ms, partial = total / 2;
  std::this_thread::sleep_for(std::chrono::milliseconds(partial + interval_ms / 2));
  mgr->Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(total - partial));
  for (int i = 0; i < cnt; ++i) {
    EXPECT_EQ(add_flags.test(i), i <= cnt / 2);
    EXPECT_EQ(reg_flags.test(i), i <= cnt / 2);
  }
}
