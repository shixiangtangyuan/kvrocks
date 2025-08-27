#include "scan_util.h"

#include <fmt/format.h>

#include <memory>
#include <random>

#include "cluster/redis_slot.h"
#include "common/io_util.h"
#include "common/time_util.h"

namespace redis {

Status ScanSession::GetCursorPair(uint64_t client_cur, bool *is_newest, CursorPair *pair) {
  int idx = 0;
  *is_newest = false;
  for (const auto &elem : cursor_pairs_) {
    if (elem.client_cursor == client_cur) {
      if (idx == 0) {  // newest cursor is on the head of queue
        *is_newest = true;
      }
      *pair = elem;
      return Status::OK();
    }
    idx++;
  }

  return {Status::NotFound, fmt::format("Cursor {} not found", client_cur)};
}

uint64_t ScanSession::Update(const std::string &store_cursor) {
  if (store_cursor.empty()) {
    return 0;
  }

  uint64_t new_cursor = 0;
  new_cursor |= static_cast<uint64_t>(++count_) << 48;
  new_cursor |= static_cast<uint64_t>(id_) << 16;
  new_cursor |= Crc16(store_cursor.data(), store_cursor.size());

  if (cursor_pairs_.size() >= kSessionMaxHoldCursors) {
    cursor_pairs_.pop_back();
  }
  cursor_pairs_.emplace_front(new_cursor, store_cursor);

  return new_cursor;
}

uint64_t ScanSession::Update(const std::string &store_cursor, int16_t slot) {
  // NOTE(mingfo): Lock free, the 'in_using' flag ensures session won't be concurrently updated.
  if (slot > end_) return 0;

  uint64_t new_cursor = 0;
  new_cursor |= static_cast<uint64_t>(++count_) << 48;
  new_cursor |= static_cast<uint64_t>(id_) << 16;
  new_cursor |= slot;

  if (cursor_pairs_.size() >= kSessionMaxHoldCursors) {
    cursor_pairs_.pop_back();
  }
  cursor_pairs_.emplace_front(new_cursor, store_cursor);

  return new_cursor;
}

uint16_t ScanSession::GenRandomCount() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint16_t> dis(1, std::numeric_limits<uint16_t>::max());
  return dis(gen);
}

// sessionId = hash(localIP + countStr + timstampStr)
uint32_t CursorLRUcache::CreateNewSessionId() {
  auto local_addrs = util::GetLocalIPAddresses();
  auto timestamp = util::GetTimeStampMS();

  auto hash_str = fmt::format("{}_{}_{}", local_addrs.front(), ++count_, timestamp);
  return static_cast<uint32_t>(std::hash<std::string_view>{}(hash_str));
}

StatusOr<std::shared_ptr<ScanSession>> CursorLRUcache::GetScanSession(uint64_t cursor, int16_t start, int16_t end,
                                                                      CursorPair *pair, CursorType cursor_type,
                                                                      const std::string &meta_key) {
  uint32_t session_id = 0;
  bool is_newest = false;
  bool need_create = false;
  std::shared_ptr<ScanSession> session = nullptr;

  std::lock_guard<std::shared_mutex> guard(shared_mutex_);
  if (cursor == 0) {
    need_create = true;
  } else {
    session_id = static_cast<uint32_t>((cursor >> CURSOR_SLOTID_BITS) & CURSOR_SESSIONID_MASK);
    // find session from map
    auto it = sessions_map_.find(session_id);
    if (it == sessions_map_.end()) {
      // session not found
      need_create = true;
    } else {
      // session found
      session = it->second;
      if (cursor_type != session->cursor_type_ || meta_key != session->meta_key_) {
        need_create = true;
      } else {
        auto s = session->GetCursorPair(cursor, &is_newest, pair);
        if (!s.IsOK()) {
          // cursor pair not found
          need_create = true;
        } else {
          // cursor pair is found
          if (!is_newest || session->IsUsing()) {
            // cursor is old or in-using
            need_create = true;
          }
        }
      }
    }
  }

  auto timestamp = util::GetTimeStampMS();
  if (need_create) {
    // get new sessionId
    uint32_t id = 0;
    while (true) {  // TODO: dead loop?
      id = CreateNewSessionId();
      // check hash conflict
      if ((id != 0) && (sessions_map_.find(id) == sessions_map_.end())) {
        break;
      }
    }
    // create session
    session = std::make_shared<ScanSession>(id, start, end, timestamp);
    session->SetCursorInfo(cursor_type, meta_key);
    // evict first before add
    if (sessions_map_.size() >= capacity_.load()) {
      evictSessions();
    }
    addSession(id, session);
  } else {
    session->SetTimestamp(timestamp);
    moveToHead(session);
  }

  // set session in using
  session->SetInUsing();

  return session;
}

void CursorLRUcache::addSession(uint64_t id, std::shared_ptr<ScanSession> &session) {
  sessions_map_.emplace(id, session);

  session->prev_ = head_;
  session->next_ = head_->next_;
  head_->next_->prev_ = session.get();
  head_->next_ = session.get();
}

void CursorLRUcache::removeSession(ScanSession *session) {
  session->prev_->next_ = session->next_;
  session->next_->prev_ = session->prev_;
  session->prev_ = nullptr;
  session->next_ = nullptr;

  sessions_map_.erase(session->id_);
}

void CursorLRUcache::evictSessions() {
  while (true) {
    if (sessions_map_.size() <= 0) {
      break;
    }

    auto &session = tail_->prev_;
    if (session == head_) break;

    // 1. evict when cache is full
    if (sessions_map_.size() >= capacity_.load()) {
      removeSession(session);
      continue;
    }

    // 2. keep evicting if session is expired
    auto ttl = GetTTL();
    auto eslapsed = util::GetTimeStampMS() - session->timestamp_;
    if (ttl > 0 && static_cast<int64_t>(eslapsed) > ttl) {
      // if ttl is set and session is expired
      removeSession(session);
      continue;
    }

    // end eviction
    break;
  }
}

void CursorLRUcache::moveToHead(std::shared_ptr<ScanSession> &session) {
  if (sessions_map_.size() <= 1) return;

  // take session from queue
  session->prev_->next_ = session->next_;
  session->next_->prev_ = session->prev_;
  session->prev_ = nullptr;
  session->next_ = nullptr;

  // insert session to head
  session->prev_ = head_;
  session->next_ = head_->next_;
  head_->next_->prev_ = session.get();
  head_->next_ = session.get();
}

}  // namespace redis
