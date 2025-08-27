#include "batch_extractor_cdc.h"

#include <glog/logging.h>

#include "cluster/redis_slot.h"
#include "common/string_util.h"
#include "fmt/core.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "storage/redis_logdata.h"
#include "storage/redis_metadata.h"
#include "types/redis_bitmap.h"

namespace cdc {

Status GetCDCEventsResponse(const std::string &cluster_id, const SlotRangeIndex &slot_range_idx,
                            const rocksdb::BatchResult &batch, CDCGetEventsResponse *resp) {
  CDCWriteBatchExtractor cdc_extractor(cluster_id, slot_range_idx, batch.sequence);
  auto s = batch.writeBatchPtr->Iterate(&cdc_extractor);
  if (!s.ok()) {
    LOG(WARNING) << "[cdc_extractor] Failed to get cdc data, Err: " << s.ToString();
    return {Status::NotOK, s.ToString()};
  }

  if (cdc_extractor.HasData()) {
    resp->mutable_point()->CopyFrom(cdc_extractor.GetCDCPoint());
    resp->mutable_events()->Add(cdc_extractor.GetCDCEvents().begin(), cdc_extractor.GetCDCEvents().end());
  }

  return Status::OK();
}

Status GetCDCDataFromBatch(const std::string &cluster_id, const SlotRangeIndex &slot_range_idx,
                           const rocksdb::BatchResult &batch,
                           ::google::protobuf::RepeatedPtrField<::kv::datanode::v1::CDCEvent> *events, uint64_t *size,
                           CDCPoint *point) {
  CDCWriteBatchExtractor cdc_extractor(cluster_id, slot_range_idx, batch.sequence);
  auto s = batch.writeBatchPtr->Iterate(&cdc_extractor);
  if (!s.ok()) {
    LOG(WARNING) << "[cdc_extractor] Failed to get cdc data, Err: " << s.ToString();
    return {Status::NotOK, s.ToString()};
  }

  if (point) point->CopyFrom(cdc_extractor.GetCDCPoint());
  if (cdc_extractor.HasData()) {
    if (events) {
      events->Add(cdc_extractor.GetCDCEvents().begin(), cdc_extractor.GetCDCEvents().end());
    }
    if (size) *size += cdc_extractor.GetCDCDataSize();
  }

  return Status::OK();
}

void CDCWriteBatchExtractor::LogData(const rocksdb::Slice &blob) {
  // Currently, we only have two kinds of log data
  if (ServerLogData::IsServerLogData(blob.data())) {
    ServerLogData server_log;
    if (!server_log.Decode(blob).IsOK()) {
      LOG(WARNING) << "[cdc_extractor] Failed to decode server log data";
    }
    batch_repl_id_ = server_log.GetContent();
    batch_ts_ns_ = server_log.GetTimeNanos();

    if (has_log_data_) {
      // update cdc point
      cdc_point_.set_next_seq_id(seq_id_ + seq_incr_);
      cdc_point_.set_prev_rep_id(batch_repl_id_);
      cdc_point_.set_prev_log_ts(batch_ts_ns_);
      // construct previous event
      constructCDCEvent();
    }

    // clear previous parsed info
    clearPrevLogData();

    has_log_data_ = false;
  } else {
    if (has_log_data_) {
      LOG(ERROR) << "[cdc_extractor] Wrong writebatch structure, Err: no replId";
      can_parse_data_ = false;
      return;
    }
    has_log_data_ = true;

    // cdc log data
    auto s = log_data_.Decode(blob);
    if (s.IsOK()) {
      can_parse_data_ = true;
    } else {
      LOG(ERROR) << "[cdc_extractor] Failed to decode Redis type log: " << s.Msg();
      can_parse_data_ = false;
    }
  }
}

rocksdb::Status CDCWriteBatchExtractor::PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) {
  seq_incr_++;

  if (column_family_id == kColumnFamilyIDZSetScore) {
    return rocksdb::Status::OK();
  }

  std::string ns, user_key;
  std::vector<std::string> command_args;

  if (column_family_id == kColumnFamilyIDMetadata) {
    // TODO:(mingfo) Do nothing for hash type before expire/del/xx supported
    return rocksdb::Status::OK();
  }

  if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, true);
    user_key = ikey.GetKey().ToString();
    std::string sub_key = ikey.GetSubKey().ToString();

    if (!can_parse_data_) {
      LOG(ERROR) << "[cdc_extractor] Log data err, skip key: " << user_key << ", subkey: " << sub_key;
      return rocksdb::Status::Corruption(fmt::format("PARSE log data error, key: {}, subkey: {}", user_key, sub_key));
    }
    // get key
    key_ = std::move(user_key);
    // get data
    auto redis_type = log_data_.GetRedisType();
    switch (redis_type) {
      case kRedisHash: {
        FieldData f;
        f.mutable_field()->swap(sub_key);
        f.set_value(value.ToStringView());
        hash_fields_.emplace_back(std::move(f));
      } break;
      case kRedisList:
      case kRedisSet:
      case kRedisZSet:
        break;

      default: {
        // NOTE(mingfo): currently, we only support to parse 5 types(string/hash/list/set/zset).
        // Return error for other types.
        std::string type_str;
        if (log_data_.GetRedisType() >= RedisTypeNames.size()) {
          type_str = "Unknown";
        } else {
          type_str = RedisTypeNames[log_data_.GetRedisType()];
        }
        LOG(ERROR) << "[batch_extractor] PutCf Unsupported key type: " << type_str << ", key: " << user_key
                   << ", sub_key: " << sub_key;
        return rocksdb::Status::NotSupported("Unsupported type: " + type_str + " of key: " + user_key);
      }
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status CDCWriteBatchExtractor::DeleteCF(uint32_t column_family_id, const Slice &key) {
  seq_incr_++;

  if (column_family_id == kColumnFamilyIDZSetScore) {
    return rocksdb::Status::OK();
  }

  std::vector<std::string> command_args;
  std::string ns;

  if (column_family_id == kColumnFamilyIDMetadata) {
    // TODO:(mingfo) Do nothing for hash type before cmds expire/del/... being supported
    return rocksdb::Status::OK();
  }

  if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, true);
    std::string user_key = ikey.GetKey().ToString();
    std::string sub_key = ikey.GetSubKey().ToString();

    if (!can_parse_data_) {
      LOG(ERROR) << "[cdc_extractor] Log data err, skip deleted key: " << user_key << ", subkey: " << sub_key;
      return rocksdb::Status::Corruption(fmt::format("PARSE log data error, key: {}, subkey: {}", user_key, sub_key));
    }
    // get key
    key_ = std::move(user_key);
    // get data
    auto redis_type = log_data_.GetRedisType();
    switch (redis_type) {
      case kRedisHash: {
        FieldData f;
        f.mutable_field()->swap(sub_key);
        hash_fields_.emplace_back(std::move(f));
      } break;
      case kRedisSet:
      case kRedisZSet:
      case kRedisList:
        break;

      default: {
        // Return error for other types.
        std::string type_str;
        if (log_data_.GetRedisType() >= RedisTypeNames.size()) {
          type_str = "Unknown";
        } else {
          type_str = RedisTypeNames[log_data_.GetRedisType()];
        }
        LOG(ERROR) << "[cdc_extractor] DeleteCf Unsupported key type: " << type_str << ", key: " << user_key
                   << ", sub_key: " << sub_key;
        return rocksdb::Status::NotSupported("Unsupported type: " + type_str + " of key: " + user_key);
      }
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status CDCWriteBatchExtractor::DeleteRangeCF(uint32_t column_family_id, const Slice &begin_key,
                                                      const Slice &end_key) {
  seq_incr_++;
  // Do nothing
  return rocksdb::Status::OK();
}

void CDCWriteBatchExtractor::constructCDCEvent() {
  auto data_type = log_data_.GetRedisType();

  switch (data_type) {
    case RedisType::kRedisHash:
      constructHashCDCEvent();
      break;
    // To support more data types
    default:
      break;
  }
}

void CDCWriteBatchExtractor::constructHashCDCEvent() {
  CDCEvent event;
  auto &cmd_data = log_data_.GetCmdLogDataObj();
  auto &cdc_data = log_data_.GetCDCLogDataObj();

  auto ret = cmd_data.GetHashCmdType();
  if (!ret.IsOK()) {
    LOG(WARNING) << "[cdc_extractor] Wrong hash cmd type " << cmd_data.GetCmdTypeVal();
    return;
  }
  auto cmd_type = *ret;
  if (cmd_type <= RedisHashCommand::kCmdNone || cmd_type > RedisHashCommand::kCmdHDel) {
    return;
  }

  // ignore keys not blonging to the current slotrange, for the senario that slotrange splitted
  auto slot = static_cast<int16_t>(GetSlotIdFromKey(key_));
  if (slot < slot_range_idx_.start() || slot > slot_range_idx_.end()) {
    return;
  }

  // check has data
  auto &old_fields = dynamic_cast<redis::HashCDCData *>(cdc_data.GetDataHandlerPtr())->GetFields();
  if (old_fields.empty() || hash_fields_.empty()) {
    return;
  }

  // construct uuid
  std::string uuid = fmt::format("{}-[{},{}]-{}-{}", cluster_id_, slot_range_idx_.start(), slot_range_idx_.end(),
                                 batch_repl_id_, seq_id_);

  event.set_timestamp(cdc_data.GetTs());
  event.set_data_type("hash");
  event.mutable_uuid()->swap(uuid);
  event.set_org_command(RedisHashCmdNames[static_cast<uint8_t>(cmd_type)]);
  event.set_eq_command(RedisHashCmdNames[static_cast<uint8_t>(GetHashCmdEqType(cmd_type))]);
  event.mutable_key()->swap(key_);

  // construct old content from log data
  auto old_content_ptr = event.mutable_old();
  if (cdc_data.GetKeyEXAT() > -1) old_content_ptr->set_ttl(cdc_data.GetKeyEXAT());
  old_content_ptr->mutable_fields()->Add(old_fields.begin(), old_fields.end());

  // construct new content from data
  auto new_content_ptr = event.mutable_new_();
  if (key_exat_ > -1) new_content_ptr->set_ttl(key_exat_);
  new_content_ptr->mutable_fields()->Add(hash_fields_.begin(), hash_fields_.end());

  // output results
  cdc_data_size_ += event.ByteSizeLong();
  cdc_events_.emplace_back(std::move(event));
}

void CDCWriteBatchExtractor::clearPrevLogData() {
  seq_id_ += seq_incr_;
  seq_incr_ = 0;
  batch_repl_id_.clear();
  batch_ts_ns_ = 0;

  log_data_.Clear();
  can_parse_data_ = false;
  has_log_data_ = false;
  key_.clear();
  key_exat_ = -1;

  hash_fields_.clear();
}

}  // namespace cdc
