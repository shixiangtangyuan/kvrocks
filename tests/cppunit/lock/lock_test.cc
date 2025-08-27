#include "lock/lock.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <thread>

#include "lock/invariant.h"
#include "lock/lock_defines.h"
#include "lock/mgl.h"
#include "lock/mgl_mgr.h"

namespace redis {

class TestILock : public ILock {
 public:
  TestILock(mgl::LockMode mode, Context* ctx, mgl::MGLockMgr* mgr, uint64_t timeout_ms = 60000);
  ~TestILock() final = default;

 private:
  std::string target_{"TestILock_Target"};
};

TestILock::TestILock(mgl::LockMode mode, Context* ctx, mgl::MGLockMgr* mgr, uint64_t timeout_ms)
    : ILock(nullptr, new mgl::MGLock(mgr), ctx, false) {
  lock_result_ = mgl_->Lock(target_, mode, timeout_ms);
}

TEST(Lock, Common) {
  std::atomic<bool> run_flag1{true}, run_flag2{true};
  std::atomic<bool> locked1{false}, locked2{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();

  std::thread thd1([&run_flag1, &locked1, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_IS, nullptr, mgr.get());
    LOG(INFO) << "thd1 LOCK_IS OK";
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd2([&run_flag2, &locked2, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_X, nullptr, mgr.get());
    LOG(INFO) << "thd2 LOCK_X OK";
    locked2 = true;
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  run_flag1 = false;
  thd1.join();

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked2);
  run_flag2 = false;
  thd2.join();
}

TEST(Lock, DiffMgr) {
  std::atomic<bool> run_flag1{true}, run_flag2{true}, run_flag3{true};
  std::atomic<bool> locked1{false}, locked2{false}, locked3{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  auto mgr1 = std::make_unique<mgl::MGLockMgr>();

  std::thread thd1([&run_flag1, &locked1, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_IS, nullptr, mgr.get());
    LOG(INFO) << "thd1 LOCK_IS OK";
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  // Lock same object in same mgr will fail
  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::thread thd2([&run_flag2, &locked2, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_X, nullptr, mgr.get());
    LOG(INFO) << "thd2 LOCK_X OK";
    locked2 = true;
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  // Lock same object in different mgr will succeed
  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::thread thd3([&run_flag3, &locked3, &mgr1]() {
    TestILock v(mgl::LockMode::LOCK_X, nullptr, mgr1.get());
    LOG(INFO) << "thd3 LOCK_X OK";
    locked3 = true;
    while (run_flag3) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_TRUE(locked3);  // diff mgr
  run_flag1 = false;
  thd1.join();

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked2);
  run_flag2 = false;
  thd2.join();

  run_flag3 = false;
  thd3.join();
}

TEST(Lock, Complicated) {
  // Lock X->IS->IX->S->IX->IX
  // 1. X locked, others will fail
  // 2. IS,IX can be locked at same time
  // 3. IX will exclude S
  // 4. S will exclude IX
  std::atomic<bool> run_flag1{true}, run_flag2{true};
  std::atomic<bool> run_flag3{true}, run_flag4{true};
  std::atomic<bool> run_flag5{true}, run_flag6{true};
  std::atomic<bool> locked1{false}, locked2{false};
  std::atomic<bool> locked3{false}, locked4{false};
  std::atomic<bool> locked5{false}, locked6{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  std::thread thd1([&run_flag1, &locked1, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_X, nullptr, mgr.get());
    locked1 = true;
    LOG(INFO) << "thd1 LOCK_X OK";
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd2([&run_flag2, &locked2, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_IS, nullptr, mgr.get());
    locked2 = true;
    LOG(INFO) << "thd2 LOCK_IS OK";
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd3([&run_flag3, &locked3, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_IX, nullptr, mgr.get());
    locked3 = true;
    LOG(INFO) << "thd3 LOCK_IX OK";
    while (run_flag3) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd4([&run_flag4, &locked4, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_S, nullptr, mgr.get());
    locked4 = true;
    LOG(INFO) << "thd4 LOCK_S OK";
    while (run_flag4) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd5([&run_flag5, &locked5, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_IX, nullptr, mgr.get());
    locked5 = true;
    LOG(INFO) << "thd5 LOCK_IX OK";
    while (run_flag5) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  // X locked, others failed
  EXPECT_TRUE(locked1);   // X
  EXPECT_FALSE(locked2);  // IS
  EXPECT_FALSE(locked3);  // IX
  EXPECT_FALSE(locked4);  // S
  EXPECT_FALSE(locked5);  // IX

  run_flag1 = false;
  thd1.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_TRUE(locked2);   // IS
  EXPECT_TRUE(locked3);   // IX
  EXPECT_FALSE(locked4);  // S
  EXPECT_FALSE(locked5);  // IX

  LOG(INFO) << "thd6 LOCK_IX new, should be waiting";
  std::thread thd6([&run_flag6, &locked6, &mgr]() {
    TestILock v(mgl::LockMode::LOCK_IX, nullptr, mgr.get());
    locked6 = true;
    LOG(INFO) << "thd6 LOCK_IX OK";
    while (run_flag6) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_TRUE(locked2);   // IS
  EXPECT_TRUE(locked3);   // IX
  EXPECT_FALSE(locked4);  // S
  EXPECT_FALSE(locked5);  // IX
  EXPECT_FALSE(locked6);  // IX

  run_flag2 = false;
  thd2.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_TRUE(locked2);   // IS
  EXPECT_TRUE(locked3);   // IX
  EXPECT_FALSE(locked4);  // S
  EXPECT_FALSE(locked5);  // IX
  EXPECT_FALSE(locked6);  // IX

  run_flag3 = false;
  thd3.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_TRUE(locked2);   // IS
  EXPECT_TRUE(locked3);   // IX
  EXPECT_TRUE(locked4);   // S
  EXPECT_FALSE(locked5);  // IX
  EXPECT_FALSE(locked6);  // IX

  run_flag4 = false;
  thd4.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_TRUE(locked2);  // IS
  EXPECT_TRUE(locked3);  // IX
  EXPECT_TRUE(locked4);  // S
  EXPECT_TRUE(locked5);  // IX
  EXPECT_TRUE(locked6);  // IX

  run_flag5 = false;
  thd5.join();
  run_flag6 = false;
  thd6.join();
}

TEST(Lock, KeyLockSimple) {
  std::atomic<bool> run_flag1{true}, run_flag2{true};
  std::atomic<bool> locked1{false}, locked2{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  auto ctx = std::make_shared<Context>();
  std::thread thd1([&run_flag1, &locked1, ctx, &mgr]() {
    KeyLock v("slotrange_1", "a", mgl::LockMode::LOCK_IS, ctx.get(), mgr.get());
    LOG(INFO) << "th1 KeyLock LOCK_IS OK";
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked1);

  auto ctx1 = std::make_shared<Context>();
  std::thread thd2([&run_flag2, &locked2, ctx1, &mgr]() {
    KeyLock v("slotrange_1", "a", mgl::LockMode::LOCK_X, ctx1.get(), mgr.get());
    LOG(INFO) << "th2 KeyLock LOCK_X OK";
    locked2 = true;
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_FALSE(locked2);
  run_flag1 = false;
  thd1.join();

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked2);
  run_flag2 = false;
  thd2.join();
}

TEST(Lock, SlotRangeLock) {
  std::atomic<bool> run_flag1{true}, run_flag2{true};
  std::atomic<bool> locked1{false}, locked2{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();

  auto ctx = std::make_shared<Context>();
  std::thread thd1([&run_flag1, &locked1, ctx, &mgr]() {
    SlotRangeLock v("slotrange_1", mgl::LockMode::LOCK_IS, ctx.get(), mgr.get());
    SlotRangeLock v1("slotrange_2", mgl::LockMode::LOCK_X, ctx.get(), mgr.get());
    locked1 = true;
    LOG(INFO) << "th1 lock SlotRangeLock LOCK_IS OK";
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd2([&run_flag2, &locked2, ctx, &mgr]() {
    SlotRangeLock v("slotrange_1", mgl::LockMode::LOCK_X, ctx.get(), mgr.get());
    LOG(INFO) << "th2 lock TestLock LOCK_X OK";
    locked2 = true;
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  run_flag1 = false;
  thd1.join();

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked2);
  run_flag2 = false;
  thd2.join();
}

TEST(Lock, SlotRangeLockTimeout) {
  std::atomic<bool> run_flag1{true}, run_flag2{true};
  std::atomic<bool> locked1{false}, locked2{false};
  std::atomic<bool> timeout{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();

  std::thread thd1([&run_flag1, &locked1, &mgr]() {
    SlotRangeLock v("slotrange_1", mgl::LockMode::LOCK_IS, nullptr, mgr.get());
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd2([&run_flag2, &locked2, &timeout, &mgr]() {
    SlotRangeLock v("slotrange_1", mgl::LockMode::LOCK_X, nullptr, mgr.get(), 1000);
    if (v.GetLockResult() == mgl::LockRes::LOCKRES_OK) {
      locked2 = true;
    } else {
      EXPECT_TRUE(v.GetLockResult() == mgl::LockRes::LOCKRES_TIMEOUT);
      LOG(INFO) << "thd2 lock slotrange LOCKRES_TIMEOUT";
      timeout = true;
    }
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_FALSE(timeout);
  LOG(INFO) << mgr->ToString();
  std::this_thread::sleep_for(std::chrono::seconds(2));

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_TRUE(timeout);
  LOG(INFO) << mgr->ToString();

  run_flag1 = false;
  thd1.join();

  run_flag2 = false;
  thd2.join();
}

TEST(Lock, KeyLockedByMyself) {
  std::atomic<bool> run_flag1{true}, run_flag2{true}, run_flag3{true};
  std::atomic<bool> locked1{false}, locked2{false}, locked3{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  auto ctx = std::make_shared<Context>();
  std::thread thd1([&run_flag1, &locked1, ctx, &mgr]() {
    auto lk = KeyLock::AcquireKeyLock("slotrange_1", "key", mgl::LockMode::LOCK_X, ctx.get(), mgr.get());
    EXPECT_TRUE(lk.IsOK());
    locked1 = true;
    LOG(INFO) << "thd1 lock key LOCK_X OK";
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked1);

  std::thread thd2([&run_flag2, &locked2, ctx, &mgr]() {
    auto lk = KeyLock::AcquireKeyLock("slotrange_1", "key", mgl::LockMode::LOCK_X, ctx.get(), mgr.get());
    EXPECT_TRUE(lk.IsOK());
    locked2 = true;
    LOG(INFO) << "thd2 lock key LOCK_X OK";
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::thread thd3([&run_flag3, &locked3, ctx, &mgr]() {
    auto lk = KeyLock::AcquireKeyLock("slotrange_1", "key", mgl::LockMode::LOCK_S, ctx.get(), mgr.get());
    EXPECT_TRUE(lk.IsOK());
    EXPECT_TRUE(lk.GetValue() == nullptr);
    locked3 = true;
    LOG(INFO) << "thd3 lock key LOCK_S OK";
    while (run_flag3) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked2);
  EXPECT_TRUE(locked3);

  run_flag1 = false;
  thd1.join();

  run_flag2 = false;
  thd2.join();

  run_flag3 = false;
  thd3.join();
}

TEST(Lock, KeyLockTimeout) {
  std::atomic<bool> run_flag1{true}, run_flag2{true};
  std::atomic<bool> locked1{false}, locked2{false};
  std::atomic<bool> timeout{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  auto ctx = std::make_shared<Context>();
  std::thread thd1([&run_flag1, &locked1, ctx, &mgr]() {
    KeyLock v("slotrange_1", "a", mgl::LockMode::LOCK_IS, ctx.get(), mgr.get());
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto ctx1 = std::make_shared<Context>();
  std::thread thd2([&run_flag2, &timeout, &locked2, ctx1, &mgr]() {
    auto elk = KeyLock::AcquireKeyLock("slotrange_1", "a", mgl::LockMode::LOCK_X, ctx1.get(), mgr.get(), 1000);
    if (elk.IsOK()) {
      locked2 = true;
    } else {
      LOG(INFO) << "thd2 lock key LOCKRES_TIMEOUT";
      timeout = true;
    }
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_FALSE(timeout);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_TRUE(timeout);

  run_flag1 = false;
  thd1.join();

  run_flag2 = false;
  thd2.join();
}

TEST(Lock, SlotRangeAndKeyLock) {
  std::atomic<bool> run_flag1{true}, run_flag2{true}, run_flag3{true};
  std::atomic<bool> locked1{false}, locked2{false}, locked3{false};
  std::atomic<bool> timeout{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  auto ctx = std::make_shared<Context>();
  std::thread thd1([&run_flag1, &locked1, ctx, &mgr]() {
    SlotRangeLock v("slotrange_1", mgl::LockMode::LOCK_S, ctx.get(), mgr.get());
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked1);

  auto ctx1 = std::make_shared<Context>();
  std::thread thd2([&run_flag2, &timeout, &locked2, ctx1, &mgr]() {
    auto elk = KeyLock::AcquireKeyLock("slotrange_1", "a", mgl::LockMode::LOCK_X, ctx1.get(), mgr.get(), 1000);
    if (elk.IsOK()) {
      locked2 = true;
    } else {
      timeout = true;
    }
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_FALSE(timeout);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_TRUE(timeout);

  run_flag2 = false;
  thd2.join();

  auto ctx2 = std::make_shared<Context>();
  std::thread thd3([&run_flag3, &locked3, ctx2, &mgr]() {
    auto elk = KeyLock::AcquireKeyLock("slotrange_1", "a", mgl::LockMode::LOCK_X, ctx2.get(), mgr.get());
    locked3 = true;
    while (run_flag3) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked3);

  run_flag1 = false;
  thd1.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked3);

  run_flag3 = false;
  thd3.join();
}

TEST(Lock, KeyLockAndSlotRangeLock) {
  std::atomic<bool> run_flag1{true}, run_flag2{true}, run_flag3{true};
  std::atomic<bool> locked1{false}, locked2{false}, locked3{false};
  std::atomic<bool> timeout{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  auto ctx1 = std::make_shared<Context>();
  std::thread thd1([&run_flag1, &locked1, ctx1, &mgr]() {
    KeyLock v("slotrange_1", "a", mgl::LockMode::LOCK_IS, ctx1.get(), mgr.get());
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto ctx2 = std::make_shared<Context>();
  std::thread thd2([&run_flag2, &timeout, &locked2, ctx2, &mgr]() {
    auto elk = SlotRangeLock::AcquireSlotRangeLock("slotrange_1", mgl::LockMode::LOCK_X, ctx2.get(), mgr.get(), 1000);
    if (elk.IsOK()) {
      locked2 = true;
    } else {
      LOG(INFO) << "thd2 lock slotrange LOCKRES_TIMEOUT";
      timeout = true;
    }
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_FALSE(timeout);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked2);
  EXPECT_TRUE(timeout);

  auto ctx3 = std::make_shared<Context>();
  std::thread thd3([&run_flag3, &timeout, &locked3, ctx3, &mgr]() {
    auto elk = SlotRangeLock::AcquireSlotRangeLock("slotrange_1", mgl::LockMode::LOCK_X, ctx3.get(), mgr.get());
    locked3 = true;
    while (run_flag3) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked1);
  EXPECT_FALSE(locked3);

  run_flag1 = false;
  thd1.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked3);

  run_flag2 = false;
  thd2.join();

  run_flag3 = false;
  thd3.join();
}

TEST(Lock, KeyLockAndSlotRangeLockShared) {
  std::atomic<bool> run_flag1{true}, run_flag2{true}, run_flag3{true}, run_flag4{false};
  std::atomic<bool> locked1{false}, locked2{false}, locked3{false}, locked4{false};
  std::atomic<bool> timeout{false};

  auto mgr = std::make_unique<mgl::MGLockMgr>();

  auto ctx1 = std::make_shared<Context>();
  std::thread thd1([&run_flag1, &locked1, ctx1, &mgr]() {
    auto elk = KeyLock::AcquireKeyLock("slotrange_1", "key", mgl::LockMode::LOCK_X, ctx1.get(), mgr.get());
    locked1 = true;
    while (run_flag1) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });
  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto ctx2 = std::make_shared<Context>();
  std::thread thd2([&run_flag2, &timeout, &locked2, ctx2, &mgr]() {
    auto elk = SlotRangeLock::AcquireSlotRangeLock("slotrange_1", mgl::LockMode::LOCK_IX, ctx2.get(), mgr.get(), 1000);
    if (elk.IsOK()) {
      locked2 = true;
    } else {
      LOG(INFO) << "thd2 lock slotrange LOCKRES_TIMEOUT";
      timeout = true;
    }
    while (run_flag2) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  auto ctx3 = std::make_shared<Context>();
  std::thread thd3([&run_flag3, &timeout, &locked3, ctx3, &mgr]() {
    auto elk = KeyLock::AcquireKeyLock("slotrange_1", "key", mgl::LockMode::LOCK_X, ctx3.get(), mgr.get(), 1000);
    if (elk.IsOK()) {
      locked3 = true;
    } else {
      LOG(INFO) << "thd3 lock key LOCKRES_TIMEOUT";
      timeout = true;
    }
    while (run_flag3) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_TRUE(locked1);
  EXPECT_TRUE(locked2);
  EXPECT_FALSE(locked3);
  EXPECT_FALSE(timeout);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked1);
  EXPECT_TRUE(locked2);
  EXPECT_FALSE(locked3);
  EXPECT_TRUE(timeout);

  auto ctx4 = std::make_shared<Context>();
  std::thread thd4([&run_flag4, &locked4, ctx4, &mgr]() {
    auto elk = KeyLock::AcquireKeyLock("slotrange_1", "key", mgl::LockMode::LOCK_X, ctx4.get(), mgr.get());
    locked4 = true;
    while (run_flag4) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_FALSE(locked4);

  run_flag1 = false;
  thd1.join();
  run_flag3 = false;
  thd3.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(locked2);
  EXPECT_TRUE(locked4);

  run_flag2 = false;
  thd2.join();

  run_flag4 = false;
  thd4.join();
}

TEST(Lock, duplicateSlotRangeLock) {
  std::atomic<bool> run_flag1{true};
  bool locked1 = false;

  auto mgr = std::make_unique<mgl::MGLockMgr>();
  auto ctx = std::make_shared<Context>();

  {
    SlotRangeLock v("slotrange_1", mgl::LockMode::LOCK_IS, ctx.get(), mgr.get());
    SlotRangeLock v2("slotrange_1", mgl::LockMode::LOCK_IX, ctx.get(), mgr.get());

    LOG(INFO) << mgr->ToString();

    std::thread thd1([&run_flag1, &locked1, ctx, &mgr]() {
      auto slk = SlotRangeLock::AcquireSlotRangeLock("slotrange_1", mgl::LockMode::LOCK_X, ctx.get(), mgr.get(), 10000);
      EXPECT_FALSE(slk.IsOK());
      LOG(INFO) << "timeout";
      while (run_flag1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    });

    std::this_thread::sleep_for(std::chrono::seconds(5));

    SlotRangeLock v3("slotrange_1", mgl::LockMode::LOCK_IX, ctx.get(), mgr.get());
    LOG(INFO) << "V3";
    SlotRangeLock v4("slotrange_1", mgl::LockMode::LOCK_IX, ctx.get(), mgr.get());

    LOG(INFO) << mgr->ToString();

    run_flag1 = false;
    thd1.join();

    LOG(INFO) << mgr->ToString();
  }

  LOG(INFO) << "duplicate";

  SlotRangeLock v("slotrange_1", mgl::LockMode::LOCK_IX, ctx.get(), mgr.get());

  LOG(INFO) << mgr->ToString();
}

}  // namespace redis
