#include "lock/mgl.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>

#include "lock/lock_defines.h"
#include "lock/mgl_mgr.h"

TEST(ConflictTable, Common) {
  uint16_t none = redis::mgl::Enum2Int(redis::mgl::LockMode::LOCK_NONE);
  EXPECT_FALSE(redis::mgl::IsConflict(none, redis::mgl::LockMode::LOCK_NONE));
  EXPECT_FALSE(redis::mgl::IsConflict(none, redis::mgl::LockMode::LOCK_IS));
  EXPECT_FALSE(redis::mgl::IsConflict(none, redis::mgl::LockMode::LOCK_IX));
  EXPECT_FALSE(redis::mgl::IsConflict(none, redis::mgl::LockMode::LOCK_S));
  EXPECT_FALSE(redis::mgl::IsConflict(none, redis::mgl::LockMode::LOCK_X));

  uint16_t isix = (1 << redis::mgl::Enum2Int(redis::mgl::LockMode::LOCK_IS)) |
                  (1 << redis::mgl::Enum2Int(redis::mgl::LockMode::LOCK_IX));
  EXPECT_FALSE(redis::mgl::IsConflict(isix, redis::mgl::LockMode::LOCK_NONE));
  EXPECT_FALSE(redis::mgl::IsConflict(isix, redis::mgl::LockMode::LOCK_IS));
  EXPECT_FALSE(redis::mgl::IsConflict(isix, redis::mgl::LockMode::LOCK_IX));
  EXPECT_TRUE(redis::mgl::IsConflict(isix, redis::mgl::LockMode::LOCK_S));
  EXPECT_TRUE(redis::mgl::IsConflict(isix, redis::mgl::LockMode::LOCK_X));

  uint16_t x = (1 << redis::mgl::Enum2Int(redis::mgl::LockMode::LOCK_X));
  EXPECT_FALSE(redis::mgl::IsConflict(x, redis::mgl::LockMode::LOCK_NONE));
  EXPECT_TRUE(redis::mgl::IsConflict(x, redis::mgl::LockMode::LOCK_IS));
  EXPECT_TRUE(redis::mgl::IsConflict(x, redis::mgl::LockMode::LOCK_IX));
  EXPECT_TRUE(redis::mgl::IsConflict(x, redis::mgl::LockMode::LOCK_S));
  EXPECT_TRUE(redis::mgl::IsConflict(x, redis::mgl::LockMode::LOCK_X));
}

TEST(LockShard, Align) { EXPECT_GE(sizeof(redis::mgl::LockShard), size_t(128)); }

TEST(MGL, OneTarget) {
  redis::mgl::MGLockMgr mgr;
  redis::mgl::MGLock l1(&mgr), l2(&mgr), l3(&mgr), l4(&mgr), l5(&mgr);
  EXPECT_EQ(l1.Lock("something", redis::mgl::LockMode::LOCK_IS, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l2.Lock("something", redis::mgl::LockMode::LOCK_IS, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l3.Lock("something", redis::mgl::LockMode::LOCK_IX, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l4.Lock("something", redis::mgl::LockMode::LOCK_IX, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l5.Lock("something", redis::mgl::LockMode::LOCK_S, 1000), redis::mgl::LockRes::LOCKRES_TIMEOUT);
  l1.Unlock();
  l2.Unlock();
  l3.Unlock();
  l4.Unlock();
  l5.Unlock();
}

TEST(MGL, MultiTarget) {
  redis::mgl::MGLockMgr mgr;
  redis::mgl::MGLock l1(&mgr), l2(&mgr);
  EXPECT_EQ(l1.Lock("something", redis::mgl::LockMode::LOCK_IS, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l2.Lock("something1", redis::mgl::LockMode::LOCK_S, 1000), redis::mgl::LockRes::LOCKRES_OK);
  l1.Unlock();
  l2.Unlock();
}

TEST(MGL, MultiThread) {
  redis::mgl::MGLockMgr mgr;
  redis::mgl::MGLock l1(&mgr), l2(&mgr), l3(&mgr), l4(&mgr), l5(&mgr);
  EXPECT_EQ(l1.Lock("something", redis::mgl::LockMode::LOCK_IS, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l2.Lock("something", redis::mgl::LockMode::LOCK_IS, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l3.Lock("something", redis::mgl::LockMode::LOCK_IX, 1000), redis::mgl::LockRes::LOCKRES_OK);
  EXPECT_EQ(l4.Lock("something", redis::mgl::LockMode::LOCK_IX, 1000), redis::mgl::LockRes::LOCKRES_OK);
  std::thread tmp([&l5]() {
    EXPECT_EQ(l5.Lock("something", redis::mgl::LockMode::LOCK_S, 10000), redis::mgl::LockRes::LOCKRES_OK);
  });
  std::this_thread::sleep_for(std::chrono::seconds(1));
  l1.Unlock();
  l2.Unlock();
  l3.Unlock();
  l4.Unlock();
  tmp.join();
  l5.Unlock();
}

TEST(MGL, Starvation) {
  redis::mgl::MGLockMgr mgr;
  redis::mgl::MGLock l1(&mgr), l2(&mgr), l3(&mgr);
  EXPECT_EQ(l1.Lock("something", redis::mgl::LockMode::LOCK_IS, 1000), redis::mgl::LockRes::LOCKRES_OK);
  std::thread tmp([&l2]() {
    EXPECT_EQ(l2.Lock("something", redis::mgl::LockMode::LOCK_X, 10000), redis::mgl::LockRes::LOCKRES_OK);
  });
  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_EQ(l3.Lock("something", redis::mgl::LockMode::LOCK_IX, 1000), redis::mgl::LockRes::LOCKRES_TIMEOUT);
  l1.Unlock();
  tmp.join();
  l2.Unlock();
  l3.Unlock();
}
