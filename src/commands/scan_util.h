#pragma once

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/status.h"

enum class CursorType : uint8_t {
  kTypeNone = 0,  // none
  kTypeBase = 1,  // cursor for SCAN
  kTypeHash = 2,  // cursor for HSCAN
  kTypeSet = 3,   // cursor for SSCAN
  kTypeZSet = 4,  // cursor for ZSCAN
};

namespace redis {

// cursor(64bits, low->high): slotId(16bits) + sessionId(32bits) + count(16bits)
constexpr uint8_t CURSOR_SLOTID_BITS = 16;
constexpr uint8_t CURSOR_SESSIONID_BITS = 32;

constexpr uint16_t CURSOR_SLOTID_MASK = 0xffff;
constexpr uint16_t CURSOR_COUNT_MASK = 0xffff;
constexpr uint32_t CURSOR_SESSIONID_MASK = 0xffffffff;

static const uint32_t kMaxSessionCount = 10000;           // LRU cached sessions limits, default 10000
static const uint16_t kSessionMaxHoldCursors = 4;         // max number of latest cursors of session, default 4
static const uint64_t kSessionTTL = 24 * 3600 * 1000ull;  // default 1day

struct CursorPair {
  CursorPair() : client_cursor(0) {}
  CursorPair(uint64_t client_cur, std::string store_cur)
      : client_cursor(client_cur), store_cursor(std::move(store_cur)) {}
  CursorPair(const CursorPair &pair) = default;
  CursorPair(CursorPair &&pair)
      : client_cursor(std::move(pair.client_cursor)), store_cursor(std::move(pair.store_cursor)) {}
  ~CursorPair() = default;

  CursorPair operator=(const CursorPair &pair) {
    client_cursor = pair.client_cursor;
    store_cursor = pair.store_cursor;
    return *this;
  }

  bool operator==(const CursorPair &rhs) const {
    return (client_cursor == rhs.client_cursor) && (store_cursor == rhs.store_cursor);
  }

  bool operator!=(const CursorPair &rhs) const {
    return (client_cursor != rhs.client_cursor) || (store_cursor != rhs.store_cursor);
  }

  uint64_t client_cursor;
  std::string store_cursor;
};

class ScanSession {
 public:
  ScanSession()
      : in_using_(false),
        id_(0),
        start_(-1),
        end_(-1),
        count_(ScanSession::GenRandomCount()),
        timestamp_(0),
        prev_(nullptr),
        next_(nullptr) {}
  ScanSession(uint32_t id, int16_t start, int16_t end, uint64_t timestamp)
      : in_using_(false),
        id_(id),
        start_(start),
        end_(end),
        count_(ScanSession::GenRandomCount()),
        timestamp_(timestamp),
        prev_(nullptr),
        next_(nullptr) {}
  ~ScanSession() = default;

  bool IsUsing() const { return in_using_.load(std::memory_order_acquire); }
  void SetInUsing() { in_using_.store(true, std::memory_order_release); }
  void ResetInUsing() { in_using_.store(false, std::memory_order_release); }
  uint32_t GetId() const { return id_; }
  void SetId(uint32_t id) { id_ = id; }
  void SetTimestamp(uint64_t ts) { timestamp_ = ts; }
  void SetCursorInfo(CursorType cursor_type, const std::string &meta_key) {
    cursor_type_ = cursor_type;
    meta_key_ = meta_key;
  }

  Status GetCursorPair(uint64_t client_cur, bool *is_newest, CursorPair *pair);

  uint64_t Update(const std::string &store_cursor, int16_t slot);

  uint64_t Update(const std::string &store_cursor);

  static uint16_t GenRandomCount();

  std::string ToString() {
    std::stringstream stream;
    stream << "session id: " << id_ << ", cursor pairs: ";
    for (auto &pair : cursor_pairs_) {
      stream << "[" << pair.client_cursor << "," << pair.store_cursor << "], ";
    }
    return stream.str();
  }

 private:
  friend class CursorLRUcache;
  FRIEND_TEST(ScanUtil, ScanSession);
  FRIEND_TEST(ScanUtil, CursorLRUcache);

  std::atomic<bool> in_using_;
  uint32_t id_;
  int16_t start_;  // slotrange startId
  int16_t end_;    // slotrange endId
  uint16_t count_;
  uint64_t timestamp_;
  CursorType cursor_type_ = CursorType::kTypeBase;
  std::string meta_key_;
  std::deque<CursorPair> cursor_pairs_;

  ScanSession *prev_;
  ScanSession *next_;
};

class CursorLRUcache {
 public:
  CursorLRUcache(uint32_t cap, int64_t ttl)
      : capacity_(cap), ttl_(ttl), head_(new ScanSession()), tail_(new ScanSession()) {
    head_->prev_ = nullptr;
    head_->next_ = tail_;
    tail_->prev_ = head_;
    tail_->next_ = nullptr;
  }
  ~CursorLRUcache() {
    delete head_;
    delete tail_;
    head_ = nullptr;
    tail_ = nullptr;
  }

  uint32_t GetCapacity() { return capacity_.load(); }
  void SetCapacity(uint32_t cap) { capacity_.store(cap, std::memory_order_relaxed); }
  int64_t GetTTL() { return ttl_.load(); }
  void SetTTL(int64_t ttl) { ttl_.store(ttl, std::memory_order_relaxed); }

  uint32_t CreateNewSessionId();
  StatusOr<std::shared_ptr<ScanSession>> GetScanSession(uint64_t cursor, int16_t start, int16_t end, CursorPair *pair,
                                                        CursorType cursor_type = CursorType::kTypeBase,
                                                        const std::string &meta_key = "");

 private:
  void addSession(uint64_t id, std::shared_ptr<ScanSession> &session);
  void removeSession(ScanSession *session);
  void evictSessions();
  void moveToHead(std::shared_ptr<ScanSession> &session);

  FRIEND_TEST(ScanUtil, CursorLRUcache);

  std::shared_mutex shared_mutex_;
  std::atomic<uint32_t> capacity_;
  std::atomic<int64_t> ttl_;  // ttl milliseconds
  std::unordered_map<uint16_t, std::shared_ptr<ScanSession>> sessions_map_;
  ScanSession *head_;
  ScanSession *tail_;

  // total number of created sessionIds
  uint64_t count_ = 0;
};

}  // namespace redis
