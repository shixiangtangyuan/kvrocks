#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "mgl.h"
#include "mgl_mgr.h"

namespace redis {

using SLSP = std::tuple<std::string, std::string, mgl::LockMode>;
class ILock;

class Context {
 public:
  Context(const Context&) = delete;
  Context(Context&&) = delete;
  Context() = default;
  ~Context() = default;

  void SetWaitLock(const std::string& slot_range_name, const std::string& key, mgl::LockMode mode);
  SLSP GetWaitLock();

  void AddLock(ILock* lock);
  void RemoveLock(ILock* lock);
  void SetKeylock(const std::string& key, mgl::LockMode mode);
  void UnsetKeylock(const std::string& key);

  bool IsLockedByMe(const std::string& key, mgl::LockMode mode);

 private:
  std::mutex mutex_;
  std::string waiting_lock_slot_range_;
  std::string waiting_lock_key_;
  mgl::LockMode waiting_lock_mode_;
  std::unordered_map<std::string, mgl::LockMode> key_lock_map_;
  std::vector<ILock*> locks_;
};

class ILock {
 public:
  ILock(ILock* parent, mgl::MGLock* lk, Context* ctx, bool isRecursive);
  virtual ~ILock();
  mgl::LockMode GetMode() const;
  mgl::LockRes GetLockResult() const;
  virtual std::string GetSlotRangeName() const;
  virtual std::string GetKey() const;

 protected:
  static mgl::LockMode GetParentMode(mgl::LockMode mode);
  mgl::LockRes lock_result_;
  std::unique_ptr<ILock> parent_;
  std::unique_ptr<mgl::MGLock> mgl_;

  Context* ctx_;
  bool is_recursive_;
};

class SlotRangeLock : public ILock {
 public:
  static StatusOr<std::unique_ptr<SlotRangeLock>> AcquireSlotRangeLock(const std::string& slot_range_name,
                                                                       mgl::LockMode mode, Context* ctx,
                                                                       mgl::MGLockMgr* mgr,
                                                                       uint64_t lockTimeoutMs = 60000);
  SlotRangeLock(const std::string& slot_range_name, mgl::LockMode mode, Context* ctx, mgl::MGLockMgr* mgr,
                uint64_t lockTimeoutMs = 60000, bool isRecursive = false);
  std::string GetSlotRangeName() const final;
  virtual ~SlotRangeLock() = default;

 private:
  std::string slot_range_name_;
};

class KeyLock : public ILock {
 public:
  static StatusOr<std::unique_ptr<KeyLock>> AcquireKeyLock(const std::string& slot_range_name, const std::string& key,
                                                           mgl::LockMode mode, Context* ctx, mgl::MGLockMgr* mgr,
                                                           uint64_t lockTimeoutMs = 60000);
  KeyLock(const std::string& slot_range_name, const std::string& key, mgl::LockMode mode, Context* ctx,
          mgl::MGLockMgr* mgr, uint64_t lockTimeoutMs = 60000);
  std::string GetSlotRangeName() const final;
  std::string GetKey() const final;

  virtual ~KeyLock();

 private:
  const std::string key_;
};

}  // namespace redis
