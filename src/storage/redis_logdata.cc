#include "redis_logdata.h"

#include "storage/redis_metadata.h"

namespace redis {

// | ContentType(1byte) | size(4byte) | "cmdType | arg1 | arg2 | ... "|
std::string CmdLogData::Encode() const {
  std::string output;
  if (args_.empty()) return output;

  std::string args_str;
  args_str.append(args_[0]);
  for (size_t i = 1; i < args_.size(); i++) {
    args_str += " " + args_[i];
  }

  output.reserve(1 + 4 + args_str.size());
  PutFixed8(&output, static_cast<uint8_t>(kContentCMD));
  PutFixed32(&output, args_str.size());
  output.append(args_str);

  return output;
}

// input: | "cmdType | arg1 | arg2 | ... "|
Status CmdLogData::Decode(const std::string &input) {
  if (input.empty()) return Status::OK();
  std::vector<std::string> args = util::Split(input, " ");
  auto parse_result = ParseInt<uint8_t>(args[0], 10);
  if (!parse_result) {
    return parse_result.ToStatus();
  }
  cmd_type_ = *parse_result;
  args_ = std::vector<std::string>(args.begin() + 1, args.end());

  return Status::OK();
}

void CmdLogData::Clear() {
  cmd_type_ = 0;
  args_.clear();
}

StatusOr<RedisHashCommand> CmdLogData::GetHashCmdType() {
  if (cmd_type_ <= static_cast<uint8_t>(RedisHashCommand::kCmdNone) ||
      cmd_type_ >= static_cast<uint8_t>(RedisHashCommand::kCmdProtect)) {
    return {Status::NotOK, fmt::format("Wrong type val {} for hash command", cmd_type_)};
  }
  return static_cast<RedisHashCommand>(cmd_type_);
}

std::unique_ptr<CDCLogData::CDCDataInterface> CDCLogData::CreateDataHandler(RedisType type) {
  switch (type) {
    case kRedisHash:
      return std::make_unique<HashCDCData>();
    case kRedisString:
    case kRedisList:
    case kRedisSet:
    case kRedisZSet:
    case kRedisBitmap:
    // TODO(mingfo): To support more typs' cdc data
    default:
      break;
  }
  return nullptr;
}

std::string CDCLogData::Encode() const {
  std::string output;
  // only hash type is supported for now
  if (type_ != RedisType::kRedisHash) return output;

  std::string data_str;
  // encode key ttl
  if (key_exat_ > -1) {
    PutFixed8(&data_str, static_cast<uint8_t>(CDCDataType::KEY_TTL));
    PutFixed64(&data_str, key_exat_);
  }
  // encode data
  switch (type_) {
    case RedisType::kRedisHash: {
      auto *hash_cdcdata = dynamic_cast<HashCDCData *>(data_handler_.get());
      if (hash_cdcdata) {
        data_str.append(hash_cdcdata->Encode());
      } else {
        LOG(ERROR) << "Hash cdc data handler is null";
        return output;
      }
    } break;
    // TODO(mingfo): To support to encode data of other types
    case RedisType::kRedisString:
    case RedisType::kRedisList:
    case RedisType::kRedisSet:
    case RedisType::kRedisZSet:
    case RedisType::kRedisBitmap:
    default:
      return output;
  }

  // construct cdc log data
  output.reserve(1 + 8 + 1 + data_str.size());
  PutFixed8(&output, static_cast<uint8_t>(kContentCDC));     // ContentType
  PutFixed64(&output, 8 + 1 + data_str.size());              // size
  PutFixed8(&output, static_cast<uint8_t>(CDCVersion::V1));  // version
  PutFixed64(&output, ts_);                                  // ts
  output.append(data_str);                                   // cdc data

  return output;
}

Status CDCLogData::Decode(rocksdb::Slice *input) {
  if (input->empty()) return Status::OK();

  uint8_t version = 0;
  GetFixed8(input, &version);
  if (version != CDCVersion::V1) {
    return {Status::NotOK, fmt::format("Wrong CDC version {}", version)};
  }

  if (input->size() < 8) return {Status::NotOK, fmt::format("Wrong CDCData size {}", input->size())};
  // get ts
  GetFixed64(input, &ts_);
  // get cdc data
  while (!input->empty()) {
    uint8_t data_type = 0;
    GetFixed8(input, &data_type);
    if (data_type == CDCDataType::KEY_TTL) {
      if (input->size() < 8) return {Status::NotOK, fmt::format("Wrong CDCData size {} to get key ttl", input->size())};
      uint64_t exat = 0;
      GetFixed64(input, &exat);
      key_exat_ = static_cast<int64_t>(exat);
    } else if (data_type == CDCDataType::HASH_FIELDS_DATA) {  // hash fields data
      auto s = dynamic_cast<HashCDCData *>(data_handler_.get())->Decode(input);
      if (!s.IsOK()) {
        LOG(WARNING) << "Failed to parse hash fields data, Err: " << s.Msg();
        return s;
      }
    } else {
      return {Status::NotOK, fmt::format("Wrong CDC data type {}", data_type)};
    }
  }

  return Status::OK();
}

void CDCLogData::Clear() {
  type_ = kRedisNone;
  ts_ = 0;
  key_exat_ = -1;

  if (data_handler_) {
    data_handler_.reset(nullptr);
  }
}

std::string HashCDCData::Encode() const {
  std::string output;
  if (fields_.empty()) return output;

  // construct cdc fields data
  PutFixed8(&output, static_cast<uint8_t>(CDCDataType::HASH_FIELDS_DATA));
  PutFixed32(&output, fields_.size());
  for (const auto &f : fields_) {
    std::string field_str;
    // add field
    PutFixed8(&field_str, static_cast<uint8_t>(FieldDataType::FIELD_NAME));
    PutFixed32(&field_str, f.field().size());
    field_str.append(f.field());
    // add val
    if (f.has_value()) {
      PutFixed8(&field_str, static_cast<uint8_t>(FieldDataType::FIELD_VALUE));
      PutFixed32(&field_str, f.value().size());
      field_str.append(f.value());
    }
    // add ttl
    if (f.has_ttl()) {
      PutFixed8(&field_str, static_cast<uint8_t>(FieldDataType::FIELD_TTL));
      PutFixed64(&field_str, f.ttl());
    }
    // add to output
    PutFixed32(&output, field_str.length());
    output.append(field_str);
  }

  return output;
}

Status HashCDCData::Decode(rocksdb::Slice *input) {
  if (input->empty()) return Status::OK();

  if (input->size() < 4) {
    return {Status::NotOK, "CDC fields data is too short to get count"};
  }
  // get field count
  uint32_t field_count = 0;
  GetFixed32(input, &field_count);
  // get field data
  uint32_t i = 0;
  for (; i < field_count; i++) {
    if (input->size() < 4) {
      return {Status::NotOK, "CDC fields data is too short to get length"};
    }
    // get size
    uint32_t data_size = 0;
    GetFixed32(input, &data_size);
    if (input->size() < data_size) {
      return {Status::NotOK, fmt::format("Wrong CDC field data size {}, expected {}", input->size(), data_size)};
    }
    std::string field_data_str;
    field_data_str.append(input->data(), data_size);
    input->remove_prefix(data_size);
    // decode field data
    kv::datanode::v1::FieldData f;
    rocksdb::Slice field_slice(field_data_str);
    auto s = DecodeFieldData(&field_slice, &f);
    if (!s.IsOK()) {
      return s;
    }
    // record parsed data
    fields_.emplace_back(std::move(f));
  }

  // check field count
  if (i != field_count) {
    return {Status::NotOK, fmt::format("Wrong CDC field data count {}, expected {}", i, field_count)};
  }

  return Status::OK();
}

Status HashCDCData::DecodeFieldData(rocksdb::Slice *input, kv::datanode::v1::FieldData *f) {
  while (input->size()) {
    uint8_t field_data_type = 0;
    GetFixed8(input, &field_data_type);
    uint32_t len = 0;
    uint64_t exat = 0;
    if (field_data_type == FieldDataType::FIELD_NAME || field_data_type == FieldDataType::FIELD_VALUE) {
      if (input->size() < 4) {
        return {Status::NotOK, fmt::format("Insufficient data for length field {}",
                                           (field_data_type == FieldDataType::FIELD_NAME ? "NAME" : "VALUE"))};
      }
      GetFixed32(input, &len);
      if (len > input->size()) {
        return {Status::NotOK,
                fmt::format("Field {} length {} exceeds remaining data size {}",
                            (field_data_type == FieldDataType::FIELD_NAME ? "NAME" : "VALUE"), len, input->size())};
      }
      if (field_data_type == FieldDataType::FIELD_NAME) {
        f->set_field(input->data(), len);
      } else {
        f->set_value(input->data(), len);
      }
      input->remove_prefix(len);
    } else if (field_data_type == FieldDataType::FIELD_TTL) {
      if (input->size() < 8) {
        return {Status::NotOK, "Insufficient data for TTL field"};
      }
      GetFixed64(input, &exat);
      f->set_ttl(exat);
    } else {
      return {Status::NotOK, fmt::format("Wrong CDC field data type {}", field_data_type)};
    }
  }
  return Status::OK();
}

}  // namespace redis
