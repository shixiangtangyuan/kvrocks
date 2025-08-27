#pragma once

#include <kv/controller/v1/api.pb.h>
#include <kv/datanode/v1/cdc.pb.h>
#include <rocksdb/transaction_log.h>

#include <map>
#include <string>
#include <vector>

#include "cluster/cluster.h"
#include "redis_db.h"
#include "redis_metadata.h"
#include "status.h"
#include "storage.h"

namespace cdc {

using kv::controller::v1::SlotRangeIndex;
using kv::datanode::v1::CDCEvent;
using kv::datanode::v1::CDCGetEventsResponse;
using kv::datanode::v1::CDCPoint;
using kv::datanode::v1::EventContent;
using kv::datanode::v1::FieldData;

Status GetCDCEventsResponse(const std::string &cluster_id, const SlotRangeIndex &slot_range_idx,
                            const rocksdb::BatchResult &batch, CDCGetEventsResponse *resp);

Status GetCDCDataFromBatch(const std::string &cluster_id, const SlotRangeIndex &slot_range_idx,
                           const rocksdb::BatchResult &batch,
                           ::google::protobuf::RepeatedPtrField<::kv::datanode::v1::CDCEvent> *events,
                           uint64_t *size = nullptr, CDCPoint *point = nullptr);

// An extractor to extract cdc data from raw write batch
class CDCWriteBatchExtractor : public rocksdb::WriteBatch::Handler {
 public:
  CDCWriteBatchExtractor(std::string cluster_id, SlotRangeIndex index, uint64_t sequence)
      : cluster_id_(std::move(cluster_id)),
        slot_range_idx_(std::move(index)),
        seq_id_(sequence),
        seq_incr_(0),
        batch_ts_ns_(0) {}

  void LogData(const rocksdb::Slice &blob) override;
  rocksdb::Status PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) override;
  rocksdb::Status DeleteCF(uint32_t column_family_id, const Slice &key) override;
  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const Slice &begin_key, const Slice &end_key) override;

  bool HasData() { return !cdc_events_.empty(); }
  CDCPoint &GetCDCPoint() { return cdc_point_; }
  std::vector<CDCEvent> &GetCDCEvents() { return cdc_events_; }
  uint64_t GetCDCDataSize() const { return cdc_data_size_; }

 private:
  void constructCDCEvent();
  void constructHashCDCEvent();
  void clearPrevLogData();

  // sharding info
  std::string cluster_id_;
  SlotRangeIndex slot_range_idx_;
  uint64_t seq_id_;
  uint64_t seq_incr_;
  std::string batch_repl_id_;
  uint64_t batch_ts_ns_;

  // commom info
  redis::WriteBatchLogData log_data_;
  bool can_parse_data_ = false;
  bool has_log_data_ = false;
  std::string key_;
  int64_t key_exat_ = -1;

  // hash data
  std::vector<kv::datanode::v1::FieldData> hash_fields_;

  // output results
  CDCPoint cdc_point_;
  std::vector<CDCEvent> cdc_events_;
  uint64_t cdc_data_size_ = 0;
};

}  // namespace cdc
