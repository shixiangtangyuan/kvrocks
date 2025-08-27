#include "commands/scan_util.h"

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kv/controller/v1/api.grpc.pb.h>

#include <cstddef>
#include <memory>
#include <ostream>

#include "cluster/set_topo_util.h"
#include "cluster/slot_keys.h"
#include "commands/scan_base.h"
#include "common/time_util.h"
#include "fmt/format.h"
#include "mock/mock_server.h"
#include "server/redis_connection.h"

namespace redis {

uint64_t GetSpecCurosr(int16_t slot, uint32_t session, uint16_t count) {
  uint64_t new_cursor = 0;
  new_cursor |= static_cast<uint64_t>(count) << 48;
  new_cursor |= static_cast<uint64_t>(session) << 16;
  new_cursor |= slot;

  return new_cursor;
}

std::tuple<int16_t, uint32_t, uint16_t> ParseCursor(uint64_t cursor) {
  auto slot = static_cast<int16_t>(cursor & CURSOR_SLOTID_MASK);
  auto session_id = static_cast<uint32_t>((cursor >> CURSOR_SLOTID_BITS) & CURSOR_SESSIONID_MASK);
  auto count = static_cast<uint16_t>((cursor >> (CURSOR_SLOTID_BITS + CURSOR_SESSIONID_BITS)) & CURSOR_COUNT_MASK);

  return {slot, session_id, count};
}

TEST(ScanUtil, CursorPair) {
  CursorPair pair1;
  CursorPair pair2;
  EXPECT_EQ(pair1, pair2);

  uint64_t client_cursor1 = 100;
  uint64_t client_cursor2 = 1000;
  std::string store_cursor1("cursor1");
  std::string store_cursor2("cursor2");
  CursorPair pair3(client_cursor1, store_cursor1);
  CursorPair pair4(client_cursor2, store_cursor1);
  CursorPair pair5(client_cursor1, store_cursor2);
  CursorPair pair6(client_cursor1, store_cursor1);
  EXPECT_NE(pair3, pair4);
  EXPECT_NE(pair3, pair5);
  EXPECT_EQ(pair3, pair6);

  CursorPair pair7(std::move(pair6));
  EXPECT_EQ(pair3, pair7);
}

TEST(ScanUtil, ScanSession) {
  auto session = std::make_unique<ScanSession>();
  EXPECT_NE(session->count_, 0);
  EXPECT_FALSE(session->IsUsing());
  session->SetInUsing();
  EXPECT_TRUE(session->IsUsing());
  session->ResetInUsing();
  EXPECT_FALSE(session->IsUsing());

  {
    uint32_t id = 10000;
    int16_t start = 100;
    int16_t end = 200;
    auto ts = util::GetTimeStampMS();
    auto session1 = std::make_unique<ScanSession>(id, start, end, ts);
    EXPECT_EQ(session1->GetId(), id);
    EXPECT_EQ(ts, session1->timestamp_);

    // add cursor pairs
    std::string store_cursor1{"cursor1"};
    std::string store_cursor2{"cursor2"};
    int16_t slot = 200;
    auto client_cursor1 = session1->Update(store_cursor1, slot);
    auto client_cursor2 = session1->Update(store_cursor2, slot);

    // get cursors
    CursorPair pair;
    bool is_newest = false;
    auto s = session1->GetCursorPair(client_cursor1, &is_newest, &pair);
    ASSERT_TRUE(s.IsOK());
    EXPECT_FALSE(is_newest);
    EXPECT_EQ(pair.store_cursor, store_cursor1);

    s = session1->GetCursorPair(client_cursor2, &is_newest, &pair);
    ASSERT_TRUE(s.IsOK());
    EXPECT_TRUE(is_newest);
    EXPECT_EQ(pair.store_cursor, store_cursor2);

    uint64_t none_cursor = 123456;
    s = session1->GetCursorPair(none_cursor, &is_newest, &pair);
    ASSERT_FALSE(s.IsOK());
  }

  // test random count
  {
    uint16_t count1 = ScanSession::GenRandomCount();
    uint16_t count2 = ScanSession::GenRandomCount();
    EXPECT_NE(count1, 0);
    EXPECT_NE(count1, count2);
  }
}

TEST(ScanUtil, CursorLRUcache) {
  // create lru cache
  uint32_t init_cap = 10000;
  int64_t init_ttl = 3600 * 1000;

  // basic test
  {
    auto lru_cache = std::make_unique<CursorLRUcache>(init_cap, init_ttl);
    EXPECT_EQ(lru_cache->GetCapacity(), init_cap);
    EXPECT_EQ(lru_cache->GetTTL(), init_ttl);
    uint32_t new_cap = 1000;
    int64_t new_ttl = 1000;
    lru_cache->SetCapacity(new_cap);
    lru_cache->SetTTL(new_ttl);
    EXPECT_EQ(lru_cache->GetCapacity(), new_cap);
    EXPECT_EQ(lru_cache->GetTTL(), new_ttl);
    lru_cache->SetCapacity(init_cap);  // reset
    lru_cache->SetTTL(init_ttl);

    // test no repeated session id
    std::set<uint32_t> session_ids;
    size_t test_count = 1000;
    for (size_t i = 0; i < test_count; i++) {
      auto id = lru_cache->CreateNewSessionId();
      session_ids.emplace(id);
    }
    EXPECT_EQ(session_ids.size(), test_count);
  }

  // Test cursor cases
  // 1. cursor == 0
  // 2. cursor != 0
  //  2.1) arbitrarily created by client
  //  2.2) old deleted cursor
  //  2.3) old existing cursor
  //  2.4) cursor existing & newest
  {
    uint64_t cursor = 0;
    int16_t start = 0, end = 16383;
    auto lru_cache = std::make_unique<CursorLRUcache>(init_cap, init_ttl);
    // write a session with deleted cursor
    CursorPair org_pair;
    auto ret = lru_cache->GetScanSession(cursor, start, end, &org_pair);
    auto sess = ret.GetValue();
    sess->ResetInUsing();
    CursorPair p1(0, "store_cur1");
    CursorPair p2(0, "store_cur2");
    CursorPair p3(0, "store_cur3");
    CursorPair p4(0, "store_cur4");
    CursorPair p5(0, "store_cur5");
    int16_t org_slot = 1234;
    p1.client_cursor = sess->Update(p1.store_cursor, org_slot);
    p2.client_cursor = sess->Update(p2.store_cursor, org_slot);
    p3.client_cursor = sess->Update(p3.store_cursor, org_slot);
    p4.client_cursor = sess->Update(p4.store_cursor, org_slot);

    std::set<uint32_t> all_create_sess_ids;
    all_create_sess_ids.emplace(sess->GetId());
    {
      // cursor == 0
      CursorPair pair;
      auto sess1 = lru_cache->GetScanSession(cursor, start, end, &pair).GetValue();
      EXPECT_NE(sess->GetId(), sess1->GetId());
      EXPECT_TRUE(pair.store_cursor.empty());
      all_create_sess_ids.emplace(sess1->GetId());
    }

    {
      // arbitrarily created by client
      // 1. session match and cursor not match
      // 2. session not match
      auto cur1 = GetSpecCurosr(100, 10, 10);
      auto cur2 = GetSpecCurosr(100, sess->GetId(), 10);
      CursorPair pair;
      auto sess1 = lru_cache->GetScanSession(cur1, start, end, &pair).GetValue();
      EXPECT_NE(sess->GetId(), sess1->GetId());
      EXPECT_TRUE(pair.store_cursor.empty());
      all_create_sess_ids.emplace(sess1->GetId());

      auto sess2 = lru_cache->GetScanSession(cur2, start, end, &pair).GetValue();
      EXPECT_NE(sess->GetId(), sess2->GetId());
      EXPECT_TRUE(pair.store_cursor.empty());
      all_create_sess_ids.emplace(sess2->GetId());
    }

    {
      // old deleted cursor
      p5.client_cursor = sess->Update(p5.store_cursor, org_slot);  // write p5 & delete p1
      CursorPair pair;
      auto sess1 = lru_cache->GetScanSession(p1.client_cursor, start, end, &pair).GetValue();
      EXPECT_NE(sess->GetId(), sess1->GetId());
      EXPECT_TRUE(pair.store_cursor.empty());
      all_create_sess_ids.emplace(sess1->GetId());

      // old existing cursor
      auto sess2 = lru_cache->GetScanSession(p2.client_cursor, start, end, &pair).GetValue();
      sess2->ResetInUsing();
      EXPECT_NE(sess->GetId(), sess2->GetId());
      EXPECT_EQ(pair.store_cursor, p2.store_cursor);
      EXPECT_EQ(pair.client_cursor, p2.client_cursor);
      all_create_sess_ids.emplace(sess2->GetId());

      // existring & newest cursor
      auto sess3 = lru_cache->GetScanSession(p5.client_cursor, start, end, &pair).GetValue();
      sess3->ResetInUsing();
      EXPECT_EQ(sess->GetId(), sess3->GetId());
      EXPECT_EQ(pair.store_cursor, p5.store_cursor);
      EXPECT_EQ(pair.client_cursor, p5.client_cursor);
      all_create_sess_ids.emplace(sess2->GetId());
    }

    // created session count should match which stored in lru cache
    EXPECT_EQ(lru_cache->sessions_map_.size(), all_create_sess_ids.size());
  }

  // Test session cases
  // 0. new scan task
  // 1. session in using
  // 2. session evicted due to expiration
  // 3. session evicted due to capacity being full
  // 4. session and newest cursor are matched
  {
    uint64_t cursor = 0;
    int16_t start = 0, end = 16383;
    auto lru_cache = std::make_unique<CursorLRUcache>(init_cap, init_ttl);
    // write a session with deleted cursor
    CursorPair org_pair;
    CursorPair p1(0, "store_cur1");
    CursorPair p2(0, "store_cur2");
    CursorPair p3(0, "store_cur3");
    CursorPair p4(0, "store_cur4");
    CursorPair p5(0, "store_cur5");
    int16_t org_slot = 5678;

    // new scan tasks
    auto sess1 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p1.client_cursor = sess1->Update(p1.store_cursor, org_slot);
    sess1->ResetInUsing();
    auto sess2 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p2.client_cursor = sess2->Update(p2.store_cursor, org_slot);
    sess2->ResetInUsing();
    auto sess3 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p3.client_cursor = sess3->Update(p3.store_cursor, org_slot);
    sess3->ResetInUsing();
    EXPECT_EQ(lru_cache->sessions_map_.size(), 3);

    {
      // session in using
      CursorPair pair;
      auto ss1 = lru_cache->GetScanSession(p3.client_cursor, start, end, &pair).GetValue();
      EXPECT_EQ(ss1->GetId(), sess3->GetId());
      EXPECT_EQ(pair.store_cursor, p3.store_cursor);
      EXPECT_TRUE(ss1->IsUsing());

      auto ss2 = lru_cache->GetScanSession(p3.client_cursor, start, end, &pair).GetValue();
      EXPECT_NE(ss2->GetId(), sess3->GetId());
      EXPECT_TRUE(ss2->IsUsing());

      ss1->ResetInUsing();
      ss2->ResetInUsing();
    }

    {
      // session and newest cursor are matched
      CursorPair pair;
      auto ss1 = lru_cache->GetScanSession(p1.client_cursor, start, end, &pair).GetValue();
      EXPECT_EQ(ss1->GetId(), sess1->GetId());
      EXPECT_EQ(pair.store_cursor, p1.store_cursor);
      EXPECT_TRUE(ss1->IsUsing());

      auto ss2 = lru_cache->GetScanSession(p2.client_cursor, start, end, &pair).GetValue();
      EXPECT_EQ(ss2->GetId(), sess2->GetId());
      EXPECT_EQ(pair.store_cursor, p2.store_cursor);
      EXPECT_TRUE(ss2->IsUsing());

      auto ss3 = lru_cache->GetScanSession(p3.client_cursor, start, end, &pair).GetValue();
      EXPECT_EQ(ss3->GetId(), sess3->GetId());
      EXPECT_EQ(pair.store_cursor, p3.store_cursor);
      EXPECT_TRUE(ss3->IsUsing());

      ss1->ResetInUsing();
      ss2->ResetInUsing();
      ss3->ResetInUsing();
    }
  }

  {
    // session evicted due to capacity being full
    uint64_t cursor = 0;
    int16_t start = 0, end = 16383;
    auto lru_cache = std::make_unique<CursorLRUcache>(init_cap, init_ttl);
    // write a session with deleted cursor
    CursorPair org_pair;
    CursorPair p1(0, "store_cur1");
    CursorPair p2(0, "store_cur2");
    CursorPair p3(0, "store_cur3");
    CursorPair p4(0, "store_cur4");
    CursorPair p5(0, "store_cur5");
    int16_t org_slot = 5678;
    // set ttl
    lru_cache->SetCapacity(4);
    // write 4 sessions
    auto sess1 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p1.client_cursor = sess1->Update(p1.store_cursor, org_slot);
    sess1->ResetInUsing();

    auto sess2 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p2.client_cursor = sess2->Update(p2.store_cursor, org_slot);
    sess2->ResetInUsing();

    auto sess3 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p3.client_cursor = sess3->Update(p3.store_cursor, org_slot);
    sess3->ResetInUsing();

    auto sess4 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p4.client_cursor = sess4->Update(p4.store_cursor, org_slot);
    sess4->ResetInUsing();

    EXPECT_EQ(lru_cache->sessions_map_.size(), 4);

    // write 5th session and sess1 will be deleted
    auto sess5 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p5.client_cursor = sess5->Update(p5.store_cursor, org_slot);
    sess5->ResetInUsing();
    EXPECT_EQ(lru_cache->sessions_map_.size(), 4);

    // try to get all sessions and only sess1 will be not found
    CursorPair pair;
    auto ss2 = lru_cache->GetScanSession(p2.client_cursor, start, end, &pair).GetValue();
    EXPECT_EQ(ss2->GetId(), sess2->GetId());
    EXPECT_EQ(pair, p2);
    auto ss3 = lru_cache->GetScanSession(p3.client_cursor, start, end, &pair).GetValue();
    EXPECT_EQ(ss3->GetId(), sess3->GetId());
    EXPECT_EQ(pair, p3);
    auto ss4 = lru_cache->GetScanSession(p4.client_cursor, start, end, &pair).GetValue();
    EXPECT_EQ(ss4->GetId(), sess4->GetId());
    EXPECT_EQ(pair, p4);
    auto ss5 = lru_cache->GetScanSession(p5.client_cursor, start, end, &pair).GetValue();
    EXPECT_EQ(ss5->GetId(), sess5->GetId());
    EXPECT_EQ(pair, p5);
    auto ss1 = lru_cache->GetScanSession(p1.client_cursor, start, end, &pair).GetValue();
    EXPECT_NE(ss1->GetId(), sess1->GetId());

    // shrink capacity will evict more sessions
    lru_cache->SetCapacity(2);
    auto sess6 = lru_cache->GetScanSession(cursor, start, end, &pair).GetValue();
    sess6->ResetInUsing();
    EXPECT_EQ(lru_cache->sessions_map_.size(), 2);

    // expand capacity
    lru_cache->SetCapacity(8);
    for (int i = 0; i < 10; i++) {
      auto ss = lru_cache->GetScanSession(cursor, start, end, &pair).GetValue();
    }
    EXPECT_EQ(lru_cache->sessions_map_.size(), 8);
  }

  {
    // session evicted due to expiration
    uint64_t cursor = 0;
    int16_t start = 0, end = 16383;
    auto lru_cache = std::make_unique<CursorLRUcache>(init_cap, init_ttl);
    // write a session with deleted cursor
    CursorPair org_pair;
    CursorPair p1(0, "store_cur1");
    CursorPair p2(0, "store_cur2");
    CursorPair p3(0, "store_cur3");
    CursorPair p4(0, "store_cur4");
    CursorPair p5(0, "store_cur5");
    int16_t org_slot = 5678;

    // write 3 sessions
    auto sess1 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p1.client_cursor = sess1->Update(p1.store_cursor, org_slot);
    sess1->ResetInUsing();
    auto sess2 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p2.client_cursor = sess2->Update(p2.store_cursor, org_slot);
    sess2->ResetInUsing();
    auto sess3 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
    p3.client_cursor = sess3->Update(p3.store_cursor, org_slot);
    sess3->ResetInUsing();
    EXPECT_EQ(lru_cache->sessions_map_.size(), 3);

    // set lru ttl
    lru_cache->SetCapacity(3);  // set cap to trigger eviction
    lru_cache->SetTTL(500);     // set ttl 500ms

    {
      // make expired
      std::this_thread::sleep_for(std::chrono::milliseconds(600));
      // wirte new session
      auto sess4 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
      p4.client_cursor = sess4->Update(p4.store_cursor, org_slot);
      sess4->ResetInUsing();
      // sess1 -> sess3 are evicted
      EXPECT_EQ(lru_cache->sessions_map_.size(), 1);
      // check sessions
      EXPECT_TRUE(lru_cache->sessions_map_.find(sess1->GetId()) == lru_cache->sessions_map_.end());
      EXPECT_TRUE(lru_cache->sessions_map_.find(sess2->GetId()) == lru_cache->sessions_map_.end());
      EXPECT_TRUE(lru_cache->sessions_map_.find(sess3->GetId()) == lru_cache->sessions_map_.end());
      EXPECT_TRUE(lru_cache->sessions_map_.find(sess4->GetId()) != lru_cache->sessions_map_.end());

      // reset lru ttl
      lru_cache->SetTTL(init_ttl);
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      // write new session
      auto sess5 = lru_cache->GetScanSession(cursor, start, end, &org_pair).GetValue();
      p5.client_cursor = sess4->Update(p5.store_cursor, org_slot);
      sess5->ResetInUsing();
      EXPECT_EQ(lru_cache->sessions_map_.size(), 2);
      EXPECT_TRUE(lru_cache->sessions_map_.find(sess4->GetId()) != lru_cache->sessions_map_.end());
      EXPECT_TRUE(lru_cache->sessions_map_.find(sess5->GetId()) != lru_cache->sessions_map_.end());
    }
  }

  {
    uint64_t cursor = 0;
    CursorPair empty_cursor_pair;
    auto lru_cache = std::make_unique<CursorLRUcache>(10000, 16384);
    // insert session of hash
    CursorPair cursor_pair;
    auto session =
        lru_cache->GetScanSession(cursor, -1, -1, &cursor_pair, CursorType::kTypeHash, "meta_key").GetValue();
    EXPECT_EQ(lru_cache->sessions_map_.size(), 1);
    EXPECT_EQ(session->cursor_type_, CursorType::kTypeHash);
    EXPECT_EQ(session->meta_key_, "meta_key");
    EXPECT_EQ(session->cursor_pairs_.size(), 0);
    EXPECT_EQ(cursor_pair.client_cursor, 0);
    EXPECT_EQ(cursor_pair.store_cursor, "");
    // insert cursor in session of hash
    cursor = session->Update("sub_key");
    session->ResetInUsing();
    // insert cursor in session of hash
    EXPECT_EQ(session->cursor_pairs_.size(), 1);
    cursor_pair = empty_cursor_pair;
    session = lru_cache->GetScanSession(cursor, -1, -1, &cursor_pair, CursorType::kTypeHash, "meta_key").GetValue();
    EXPECT_EQ(lru_cache->sessions_map_.size(), 1);
    EXPECT_EQ(session->cursor_type_, CursorType::kTypeHash);
    EXPECT_EQ(session->meta_key_, "meta_key");
    EXPECT_EQ(session->cursor_pairs_.size(), 1);
    EXPECT_EQ(cursor_pair.client_cursor, cursor);
    EXPECT_EQ(cursor_pair.store_cursor, "sub_key");
    session->ResetInUsing();
    // try get session with mismatched meta key
    cursor_pair = empty_cursor_pair;
    session =
        lru_cache->GetScanSession(cursor, -1, -1, &cursor_pair, CursorType::kTypeHash, "another_meta_key").GetValue();
    EXPECT_EQ(lru_cache->sessions_map_.size(), 2);
    EXPECT_EQ(session->cursor_type_, CursorType::kTypeHash);
    EXPECT_EQ(session->meta_key_, "another_meta_key");
    EXPECT_EQ(session->cursor_pairs_.size(), 0);
    EXPECT_EQ(cursor_pair.client_cursor, 0);
    EXPECT_EQ(cursor_pair.store_cursor, "");
    session->ResetInUsing();
    // try get session with mismatched cursor type
    auto size = lru_cache->sessions_map_.size();
    for (auto cursor_type : {CursorType::kTypeSet, CursorType::kTypeZSet}) {
      session = lru_cache->GetScanSession(cursor, -1, -1, &cursor_pair, cursor_type, "meta_key").GetValue();
      EXPECT_EQ(lru_cache->sessions_map_.size(), ++size);
      EXPECT_EQ(session->cursor_pairs_.size(), 0);
      EXPECT_EQ(session->cursor_type_, cursor_type);
      EXPECT_EQ(session->meta_key_, "meta_key");
      EXPECT_EQ(cursor_pair.client_cursor, 0);
      EXPECT_EQ(cursor_pair.store_cursor, "");
      session->ResetInUsing();
    }
  }
}

}  // namespace redis
