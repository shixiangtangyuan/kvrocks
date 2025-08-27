#pragma once
#include <cstdint>
#include <sstream>
#include <string>
#include <unordered_map>

#include "time_util.h"
namespace ingest {

struct IngestMonitor {
  enum Reason {
    IngestClusterIdMisMatchError = 0,
    IngestSlotRangeNotFindError = 1,
    IngestJobRunningError = 2,
    IngestInvalidArgsError = 3,
    IngestSetConfigFailedError = 4,
    IngestRockDBError = 5,
    IngestOtherError = 6,
  };

  static std::string ReasonToString(Reason reason) {
    switch (reason) {
      case IngestClusterIdMisMatchError:
        return "IngestClusterIdMisMatchError";
      case IngestSlotRangeNotFindError:
        return "IngestSlotRangeNotFindError";
      case IngestJobRunningError:
        return "IngestJobRunningError";
      case IngestInvalidArgsError:
        return "IngestInvalidArgsError";
      case IngestSetConfigFailedError:
        return "IngestSetConfigFailedError";
      case IngestRockDBError:
        return "IngestRockDBError";
      case IngestOtherError:
        return "IngestOtherError";
    }
    return "UnkownError";
  }

  struct IngestStatsRecord {
    bool success = true;
    Reason reason = IngestOtherError;
    uint64_t sst_count = 0;
    uint64_t sst_size = 0;
    uint64_t duration = 0;
    uint64_t ingest_finish_time = 0;
  };

  void StartDatanodeDtsWrProthibited() { datanode_dts_wr_prothibited_start_time = util::GetTimeStampMS(); }

  void FinishDatanodeDtsWrProthibited() {
    if (datanode_dts_wr_prothibited_start_time > 0) {
      datanode_dts_wr_prothibited_ms = util::GetTimeStampMS() - datanode_dts_wr_prothibited_start_time;
      datanode_dts_wr_prothibited_start_time = 0;
    }
  }

  void RecordIngestMonitor(const IngestStatsRecord& record) {
    interface_total_times++;
    ingest_task_sst_file_total_number += record.sst_count;
    ingest_task_sst_file_total_size += record.sst_size;
    ingest_task_total_duration_time += record.duration;
    if (record.ingest_finish_time > 0) {
      last_ingest_finish_time = record.ingest_finish_time;
    }

    if (!record.success) {
      error_counts[record.reason]++;
    }
  }

  void OutputToString(std::ostringstream& os, std::string& prefix) {
    auto format_line = [&](const std::string& name, auto value) {
      std::string line_end = "\r\n";
      os << prefix << name << std::to_string(value) << line_end;
    };

    format_line("interface_count:", interface_total_times);
    format_line("total_sst_file_number:", ingest_task_sst_file_total_number);
    format_line("total_sst_file_size_bytes:", ingest_task_sst_file_total_size);
    format_line("total_ingest_duration_ms:", ingest_task_total_duration_time);
    format_line("last_ingest_finish_time:", last_ingest_finish_time);

    for (auto& [error, count] : error_counts) {
      std::string error_name;
      error_name.append(ReasonToString(error)).append(":");
      format_line(error_name, count);
    }

    uint64_t now_dts_wr_prothibited_ms = 0;
    if (datanode_dts_wr_prothibited_start_time > 0) {
      now_dts_wr_prothibited_ms = util::GetTimeStampMS() - datanode_dts_wr_prothibited_start_time;
    }

    format_line("datanode_dts_wr_prothibited_ms:", now_dts_wr_prothibited_ms);
    format_line("datanode_dts_wr_prothibited_start_time:", datanode_dts_wr_prothibited_start_time);
  }

  uint64_t interface_total_times = 0;
  uint64_t ingest_task_sst_file_total_number = 0;
  uint64_t ingest_task_sst_file_total_size = 0;
  uint64_t ingest_task_total_duration_time = 0;
  uint64_t last_ingest_finish_time = 0;
  std::unordered_map<Reason, uint64_t> error_counts;

  uint64_t datanode_dts_wr_prothibited_ms = 0;
  uint64_t datanode_dts_wr_prothibited_start_time = 0;
};
}  // namespace ingest
