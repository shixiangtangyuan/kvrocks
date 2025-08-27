#pragma once

#include <kv/datanode/v1/cdc.pb.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "redis_metadata.h"
#include "storage.h"

namespace redis {

class CmdLogData {
 public:
  CmdLogData() = default;
  explicit CmdLogData(std::vector<std::string> &&args) : args_(std::move(args)) {}

  std::vector<std::string> &GetCmdArgs() { return args_; }
  void SetCmdArgs(std::vector<std::string> &&args) { args_ = std::move(args); }
  void Append(std::string &&arg) { args_.emplace_back(std::move(arg)); }
  std::string Encode() const;
  Status Decode(const std::string &input);
  void Clear();

  StatusOr<RedisHashCommand> GetHashCmdType();
  uint8_t GetCmdTypeVal() const { return cmd_type_; }

 private:
  uint8_t cmd_type_ = 0;
  std::vector<std::string> args_;
};

enum ContentType {
  kContentNone = 0,
  kContentCMD = 1,
  kContentCDC = 2,
};
enum CDCVersion {
  V1 = 1,
};
enum CDCDataType {
  KEY_TTL = 0,
  HASH_FIELDS_DATA = 1,
};

class CDCLogData {
 public:
  CDCLogData() = default;
  explicit CDCLogData(RedisType type) : type_(type) { data_handler_ = CreateDataHandler(type); }
  ~CDCLogData() = default;

  class CDCDataInterface {
   public:
    virtual ~CDCDataInterface() = default;
    virtual std::string Encode() const = 0;
    virtual Status Decode(rocksdb::Slice *input) = 0;
    virtual void Clear() = 0;
  };

  // cdc data factory
  static std::unique_ptr<CDCDataInterface> CreateDataHandler(RedisType type);

  // common cdc data
  void SetRedisType(RedisType type) {
    type_ = type;
    data_handler_ = CreateDataHandler(type);
  }
  RedisType GetRedisType() const { return type_; }
  void SetVersion(CDCVersion v) { version_ = v; }
  CDCVersion GetVersion() { return version_; }
  void SetTs(uint64_t ts) { ts_ = ts; }
  void SetTs() { ts_ = util::GetTimeStampMS(); }
  uint64_t GetTs() const { return ts_; }
  void SetKeyEXAT(int64_t exat) { key_exat_ = exat; }
  int64_t GetKeyEXAT() const { return key_exat_; }

  // cdc data codec
  std::string Encode() const;
  Status Decode(rocksdb::Slice *input);
  void Clear();

  // get data handler ptr
  CDCDataInterface *GetDataHandlerPtr() { return data_handler_.get(); }

 private:
  RedisType type_ = kRedisNone;
  CDCVersion version_ = V1;
  uint64_t ts_ = 0;
  int64_t key_exat_ = -1;

  std::unique_ptr<CDCDataInterface> data_handler_ = nullptr;
};

// Hash cdc data
class HashCDCData : public CDCLogData::CDCDataInterface {
 public:
  enum FieldDataType {
    FIELD_NAME = 0,
    FIELD_VALUE = 1,
    FIELD_TTL = 2,
  };

  void Append(kv::datanode::v1::FieldData &&field) { fields_.emplace_back(std::move(field)); }
  void SetFileds(std::vector<kv::datanode::v1::FieldData> &&fields) { fields_ = std::move(fields); }
  const std::vector<kv::datanode::v1::FieldData> &GetFields() const { return fields_; }
  static Status DecodeFieldData(rocksdb::Slice *input, kv::datanode::v1::FieldData *f);

  std::string Encode() const override;
  Status Decode(rocksdb::Slice *input) override;
  void Clear() override { fields_.clear(); }

 private:
  std::vector<kv::datanode::v1::FieldData> fields_;
};

}  // namespace redis
