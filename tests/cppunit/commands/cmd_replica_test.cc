#include "commands/cmd_replica.h"

#include <gtest/gtest.h>

#include "../sync/sync_test_util.h"

namespace redis {

TEST(CommandReplica, Base) {
  MockOptions opt;
  uint64_t db_id = 1;
  opt.db_ids.emplace(db_id);
  auto srv = MockServer(opt);
  srv.StopCtrlClient();
  int16_t start = 0, end = kClusterSlots - 1;
  auto status = ApplyTopo(srv, db_id, true, true, start, end);
  ASSERT_TRUE(status.IsOK());
  auto slot_range = srv.GetSlotRange(start, end);
  ASSERT_TRUE(slot_range);
  auto ret = slot_range->GetSyncPoint();
  ASSERT_TRUE(ret.IsOK());
  auto sync_point = ret.GetValue();
  auto& workers = srv.GetWorkerThreads();
  ASSERT_GT(workers.size(), 0);
  redis::Connection conn{nullptr, workers[0]->GetWorker()};
  conn.BecomeAdmin();

  std::string output;
  redis::CommandReplica cmd;
  auto reset_cmd = [&]() {
    cmd.subcommand_.clear();
    cmd.slot_id_ = -1;
    cmd.log_id_ = -1;
  };
  // invalid subcommand
  ASSERT_FALSE(cmd.Parse({"replica", "invalid subcommand", "1"}).IsOK());
  ASSERT_NE(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, -1);
  ASSERT_EQ(cmd.log_id_, -1);
  // invalid slot id
  ASSERT_FALSE(cmd.Parse({"replica", "getpoint", "invalid slot id"}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, -1);
  ASSERT_EQ(cmd.log_id_, -1);
  ASSERT_FALSE(cmd.Parse({"replica", "getpoint", "-1"}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, -1);
  ASSERT_EQ(cmd.log_id_, -1);
  ASSERT_FALSE(cmd.Parse({"replica", "getpoint", std::to_string(kClusterSlots)}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, -1);
  ASSERT_EQ(cmd.log_id_, -1);
  // invalid log id
  reset_cmd();
  ASSERT_FALSE(cmd.Parse({"replica", "getpoint", std::to_string(start), "invalid log id"}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, start);
  ASSERT_EQ(cmd.log_id_, -1);
  reset_cmd();
  ASSERT_FALSE(cmd.Parse({"replica", "getpoint", std::to_string(start), "12345678900987654321"}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, start);
  ASSERT_EQ(cmd.log_id_, -1);

  // get sync point failed with too big log id
  reset_cmd();
  ASSERT_TRUE(
      cmd.Parse({"replica", "getpoint", std::to_string(start), std::to_string(sync_point.next_seq_id())}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, start);
  ASSERT_EQ(cmd.log_id_, sync_point.next_seq_id());
  output.clear();
  ASSERT_FALSE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
  ASSERT_TRUE(output.empty());

  // get sync point succ with log id = 0
  reset_cmd();
  ASSERT_TRUE(cmd.Parse({"replica", "getpoint", std::to_string(start), "0"}).IsOK());
  ASSERT_EQ(cmd.subcommand_, "getpoint");
  ASSERT_EQ(cmd.slot_id_, start);
  ASSERT_EQ(cmd.log_id_, 0);
  output.clear();
  ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
  std::string exp_output;
  exp_output.append(redis::MultiLen(4));
  exp_output.append(redis::BulkString(slot_range->GetName()));
  exp_output.append(redis::Integer(1));
  exp_output.append(redis::Integer(0));
  exp_output.append(redis::BulkString(""));
  ASSERT_EQ(output, exp_output);

  // get sync point succ with log id > 0
  exp_output.clear();
  exp_output.append(redis::MultiLen(4));
  exp_output.append(redis::BulkString(slot_range->GetName()));
  exp_output.append(redis::Integer(sync_point.next_seq_id()));
  exp_output.append(redis::Integer(sync_point.prev_log_ts()));
  exp_output.append(redis::BulkString(sync_point.prev_rep_id()));
  uint64_t log_id = sync_point.next_seq_id() - 1;
  std::string log_id_str = std::to_string(log_id);
  std::vector<int> slot_ids{start, start + (end - start), end};
  for (auto& slot_id : slot_ids) {
    auto slot_id_str = std::to_string(slot_id);
    // latest sync point
    reset_cmd();
    ASSERT_TRUE(cmd.Parse({"replica", "getpoint", slot_id_str}).IsOK());
    ASSERT_EQ(cmd.subcommand_, "getpoint");
    ASSERT_EQ(cmd.slot_id_, slot_id);
    ASSERT_EQ(cmd.log_id_, -1);
    output.clear();
    ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
    ASSERT_EQ(output, exp_output);
    // specified sync point
    reset_cmd();
    ASSERT_TRUE(cmd.Parse({"replica", "getpoint", slot_id_str, log_id_str}).IsOK());
    ASSERT_EQ(cmd.subcommand_, "getpoint");
    ASSERT_EQ(cmd.slot_id_, slot_id);
    ASSERT_EQ(cmd.log_id_, log_id);
    output.clear();
    ASSERT_TRUE(cmd.Execute(srv.GetServer().get(), &conn, &output, nullptr).IsOK());
    ASSERT_EQ(output, exp_output);
  }
}

}  // namespace redis
