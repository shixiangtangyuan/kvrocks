#include "ingest/ingest_monitor.h"

#include <gtest/gtest.h>

#include <thread>
namespace ingest {
using Reason = ingest::IngestMonitor::Reason;
using Record = ingest::IngestMonitor::IngestStatsRecord;
TEST(IngestMonitorTest, ReasonToStringValidCases) {
  EXPECT_EQ(IngestMonitor::ReasonToString(Reason::IngestClusterIdMisMatchError), "IngestClusterIdMisMatchError");
  EXPECT_EQ(IngestMonitor::ReasonToString(Reason::IngestSlotRangeNotFindError), "IngestSlotRangeNotFindError");
  EXPECT_EQ(IngestMonitor::ReasonToString(Reason::IngestJobRunningError), "IngestJobRunningError");
  EXPECT_EQ(IngestMonitor::ReasonToString(Reason::IngestInvalidArgsError), "IngestInvalidArgsError");
  EXPECT_EQ(IngestMonitor::ReasonToString(Reason::IngestSetConfigFailedError), "IngestSetConfigFailedError");
  EXPECT_EQ(IngestMonitor::ReasonToString(Reason::IngestRockDBError), "IngestRockDBError");
  EXPECT_EQ(IngestMonitor::ReasonToString(Reason::IngestOtherError), "IngestOtherError");
}

TEST(IngestMonitorTest, ReasonToStringInvalidCase) {
  auto invalid_reason = static_cast<Reason>(999);
  EXPECT_EQ(IngestMonitor::ReasonToString(invalid_reason), "UnkownError");
}

TEST(IngestMonitorTest, RecordSuccess) {
  IngestMonitor monitor;
  Record record;
  record.success = true;
  record.sst_count = 3;
  record.sst_size = 1024;
  record.duration = 500;

  monitor.RecordIngestMonitor(record);

  EXPECT_EQ(monitor.interface_total_times, 1);
  EXPECT_EQ(monitor.ingest_task_sst_file_total_number, 3);
  EXPECT_EQ(monitor.ingest_task_sst_file_total_size, 1024);
  EXPECT_EQ(monitor.ingest_task_total_duration_time, 500);
  EXPECT_TRUE(monitor.error_counts.empty());
}

TEST(IngestMonitorTest, RecordFailure) {
  IngestMonitor monitor;
  Record record;
  record.success = false;
  record.reason = Reason::IngestRockDBError;

  monitor.RecordIngestMonitor(record);

  EXPECT_EQ(monitor.interface_total_times, 1);
  EXPECT_EQ(monitor.error_counts[Reason::IngestRockDBError], 1);
}

TEST(IngestMonitorTest, MultipleErrorCounts) {
  IngestMonitor monitor;

  Record r1{false, Reason::IngestRockDBError};
  Record r2{false, Reason::IngestRockDBError};
  Record r3{false, Reason::IngestInvalidArgsError};

  monitor.RecordIngestMonitor(r1);
  monitor.RecordIngestMonitor(r2);
  monitor.RecordIngestMonitor(r3);

  EXPECT_EQ(monitor.interface_total_times, 3);
  EXPECT_EQ(monitor.error_counts[Reason::IngestRockDBError], 2);
  EXPECT_EQ(monitor.error_counts[Reason::IngestInvalidArgsError], 1);
}

TEST(IngestMonitorTest, StartAndFinishDatanodeDtsWrProhibited) {
  using namespace std::chrono_literals;
  IngestMonitor monitor;
  EXPECT_EQ(monitor.datanode_dts_wr_prothibited_start_time, 0);
  EXPECT_EQ(monitor.datanode_dts_wr_prothibited_ms, 0);

  monitor.FinishDatanodeDtsWrProthibited();
  EXPECT_EQ(monitor.datanode_dts_wr_prothibited_start_time, 0);
  EXPECT_EQ(monitor.datanode_dts_wr_prothibited_ms, 0);

  monitor.StartDatanodeDtsWrProthibited();
  EXPECT_GT(monitor.datanode_dts_wr_prothibited_start_time, 0);
  std::this_thread::sleep_for(100ms);
  monitor.FinishDatanodeDtsWrProthibited();
  EXPECT_EQ(monitor.datanode_dts_wr_prothibited_start_time, 0);
  EXPECT_GT(monitor.datanode_dts_wr_prothibited_ms, 0);
}

TEST(IngestMonitorTest, OutputFormat) {
  IngestMonitor monitor;
  monitor.interface_total_times = 2;
  monitor.ingest_task_sst_file_total_number = 5;
  monitor.ingest_task_sst_file_total_size = 1024;
  monitor.ingest_task_total_duration_time = 2000;
  monitor.error_counts[Reason::IngestRockDBError] = 1;
  monitor.error_counts[Reason::IngestInvalidArgsError] = 2;
  monitor.datanode_dts_wr_prothibited_ms = 1500;

  std::ostringstream os;
  std::string prefix = "[Metrics] ";
  monitor.OutputToString(os, prefix);

  std::string output = os.str();

  EXPECT_NE(output.find("[Metrics] interface_count:2"), std::string::npos);
  EXPECT_NE(output.find("[Metrics] total_sst_file_number:5"), std::string::npos);
  EXPECT_NE(output.find("[Metrics] total_sst_file_size_bytes:1024"), std::string::npos);
  EXPECT_NE(output.find("[Metrics] total_ingest_duration_ms:2000"), std::string::npos);

  EXPECT_NE(output.find("IngestRockDBError:1"), std::string::npos);
  EXPECT_NE(output.find("IngestInvalidArgsError:2"), std::string::npos);

  EXPECT_NE(output.find("datanode_dts_wr_prothibited_ms:0"), std::string::npos);
}

}  // namespace ingest
