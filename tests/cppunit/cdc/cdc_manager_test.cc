#include <gtest/gtest.h>

#include "../sync/sync_test_util.h"
#include "mock/mock_server.h"

namespace redis {

TEST(CDCManager, Base) {
  MockOptions opt;
  std::vector<uint64_t> db_ids = {1, 2};
  for (auto db_id : db_ids) {
    opt.db_ids.emplace(db_id);
  }
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  auto &cdc_manager = srv.GetServer()->cdc_manager;
  ASSERT_FALSE(cdc_manager->is_topo_updating_);
  int16_t start = 0, mid = kClusterSlots / 2, end = kClusterSlots - 1;
  std::vector<TestPBSlotRange> topo_slot_ranges = {{db_ids[0], start, mid}, {db_ids[1], mid + 1, end}};
  auto status = ApplyTopo(srv, topo_slot_ranges, true, true);
  ASSERT_TRUE(status.IsOK());
  ASSERT_FALSE(cdc_manager->is_topo_updating_);
  // init reqs
  grpc::CallbackServerContext ctx;
  std::vector<std::shared_ptr<engine::Storage>> storages;
  std::vector<kv::datanode::v1::CDCGetEventsRequest> reqs;
  std::vector<std::shared_ptr<redis::SlotRange>> slot_ranges;
  for (auto topo_slot_range : topo_slot_ranges) {
    auto slot_range = srv.GetSlotRange(topo_slot_range.start, topo_slot_range.end);
    ASSERT_TRUE(slot_range != nullptr);
    slot_ranges.emplace_back(slot_range);
    auto storage = srv.GetStorage(topo_slot_range.db_id);
    ASSERT_TRUE(storage != nullptr);
    storage->GetConfig()->enable_cdc_sync = true;
    auto ret = slot_range->GetCDCPoint();
    ASSERT_TRUE(ret.IsOK());
    auto cdc_point = ret.GetValue();
    ASSERT_FALSE(cdc_point.prev_rep_id().empty());
    ASSERT_NE(cdc_point.prev_log_ts(), 0);
    kv::datanode::v1::CDCGetEventsRequest req;
    req.set_cluster_id(opt.cluster_id);
    req.mutable_slotrange_idx()->set_start(topo_slot_range.start);
    req.mutable_slotrange_idx()->set_end(topo_slot_range.end);
    req.mutable_point()->CopyFrom(cdc_point);
    reqs.emplace_back(req);
  }
  // add cdc sender succeed
  auto send = srv.PullCDCData(&ctx, &reqs[0]);
  ASSERT_FALSE(send->IsFinished());
  auto iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  // add cdc sender failed for topo is updating
  cdc_manager->SetIsTopoUpdating(true);
  ASSERT_TRUE(cdc_manager->is_topo_updating_);
  auto send_tmp = srv.PullCDCData(&ctx, &reqs[0]);
  ASSERT_TRUE(send_tmp->IsFinished());
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  ASSERT_NE(iter->second, send_tmp);
  send_tmp->OnDone();
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  cdc_manager->SetIsTopoUpdating(false);
  ASSERT_FALSE(cdc_manager->is_topo_updating_);
  // add cdc sender failed for another sender existed
  send_tmp = srv.PullCDCData(&ctx, &reqs[0]);
  ASSERT_TRUE(send_tmp->IsFinished());
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  ASSERT_NE(iter->second, send_tmp);
  send_tmp->OnDone();
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  // add cdc sender succeed with take over
  reqs[0].set_is_take_over(true);
  send_tmp = srv.PullCDCData(&ctx, &reqs[0]);
  ASSERT_FALSE(send_tmp->IsFinished());
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_NE(iter->second, send);
  ASSERT_EQ(iter->second, send_tmp);
  send->OnDone();
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send_tmp);
  // remove cdc sender succeed
  send_tmp->MarkFinished(std::nullopt);
  send_tmp->OnDone();
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter == cdc_manager->cdc_senders_.end());
  // clear all for slot range
  send = srv.PullCDCData(&ctx, &reqs[0]);
  send_tmp = srv.PullCDCData(&ctx, &reqs[1]);
  ASSERT_EQ(cdc_manager->cdc_senders_.size(), 2);
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  iter = cdc_manager->cdc_senders_.find(slot_ranges[1]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send_tmp);
  cdc_manager->ClearAllForSlotRange("", std::nullopt);
  ASSERT_EQ(cdc_manager->cdc_senders_.size(), 2);
  cdc_manager->ClearAllForSlotRange(slot_ranges[1]->GetName(), std::nullopt);
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  iter = cdc_manager->cdc_senders_.find(slot_ranges[1]->GetName());
  ASSERT_TRUE(iter == cdc_manager->cdc_senders_.end());
  send_tmp->OnDone();
  // clear all
  send_tmp = srv.PullCDCData(&ctx, &reqs[1]);
  ASSERT_EQ(cdc_manager->cdc_senders_.size(), 2);
  iter = cdc_manager->cdc_senders_.find(slot_ranges[0]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send);
  iter = cdc_manager->cdc_senders_.find(slot_ranges[1]->GetName());
  ASSERT_TRUE(iter != cdc_manager->cdc_senders_.end());
  ASSERT_EQ(iter->second, send_tmp);
  cdc_manager->ClearAll(std::nullopt);
  ASSERT_EQ(cdc_manager->cdc_senders_.size(), 0);
  send->OnDone();
  send_tmp->OnDone();
}

}  // namespace redis
