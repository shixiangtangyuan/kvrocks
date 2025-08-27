#pragma once

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <string>
#include <vector>

#include "lock_defines.h"
#include "mgl_mgr.h"

namespace redis {
namespace mgl {

class MGLockMgr;

class MGLock {
 public:
  explicit MGLock(MGLockMgr* mgr);
  MGLock(const MGLock&) = delete;
  MGLock(MGLock&&) = delete;
  MGLock& operator=(const MGLock&) const = delete;
  ~MGLock();
  LockRes Lock(const std::string& target, LockMode mode, uint64_t timeoutMs);
  void Unlock();
  uint64_t GetHash() const { return target_hash_; }
  LockMode GetMode() const { return mode_; }
  LockRes GetStatus() const;
  const std::string& GetTarget() const { return target_; }
  std::string ToString() const;
  const std::string& GetThreadId() const { return thread_id_; }

 private:
  friend class LockSchedCtx;
  void setLockResult(LockRes res, std::list<MGLock*>::iterator iter);
  void releaseLockResult();
  std::list<MGLock*>::iterator getLockIter() const;
  void notify();
  bool waitLock(uint64_t timeoutMs);

  const uint64_t id_;
  std::string target_;
  uint64_t target_hash_;
  LockMode mode_;

  // wrote by MGLockMgr
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  LockRes res_;
  std::list<MGLock*>::iterator res_iter_;
  MGLockMgr* lock_mgr_;
  std::string thread_id_;

  static std::atomic<uint64_t> id_gen_;
  static std::list<MGLock*> dummy_list_;
};

}  // namespace mgl
}  // namespace redis
