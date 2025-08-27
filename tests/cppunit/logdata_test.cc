#include <gtest/gtest.h>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

// Test Case 1: FieldData only has field, no value and ttl
TEST(CDCLogDataTest, EncodeDecodeOnlyFields) {
  CDCLogData cdc_data;
  cdc_data.SetRedisType(kRedisHash);

  // Set version and timestamp
  cdc_data.SetVersion(CDCVersion::V1);
  uint64_t ts = 1234567890;
  cdc_data.SetTs(ts);

  // Add data containing only fields
  std::vector<std::string> fields = {"f1", "f2", "f3"};
  for (const auto& field : fields) {
    kv::datanode::v1::FieldData field_data;
    field_data.set_field(field);
    dynamic_cast<redis::HashCDCData*>(cdc_data.GetDataHandlerPtr())->Append(std::move(field_data));
  }

  // Encode
  std::string encoded = cdc_data.Encode();

  // Get ContentType and length
  rocksdb::Slice input(encoded);
  uint8_t content_type = 0;
  GetFixed8(&input, &content_type);
  uint64_t length = 0;
  GetFixed64(&input, &length);
  // Verify content type and length
  ASSERT_EQ(content_type, static_cast<uint8_t>(ContentType::kContentCDC));
  ASSERT_EQ(length, input.size());
  // Decode
  CDCLogData decoded_data;
  decoded_data.SetRedisType(kRedisHash);
  ASSERT_TRUE(decoded_data.Decode(&input).IsOK());

  // Verify decoded data
  ASSERT_EQ(decoded_data.GetVersion(), CDCVersion::V1);
  ASSERT_EQ(decoded_data.GetTs(), ts);
  ASSERT_EQ(decoded_data.GetKeyEXAT(), -1);  // Default value

  const auto& decoded_fields = dynamic_cast<redis::HashCDCData*>(decoded_data.GetDataHandlerPtr())->GetFields();
  ASSERT_EQ(decoded_fields.size(), 3);
  for (size_t i = 0; i < fields.size(); i++) {
    EXPECT_EQ(decoded_fields[i].field(), fields[i]);
    EXPECT_FALSE(decoded_fields[i].has_value());
    EXPECT_FALSE(decoded_fields[i].has_ttl());
  }
}

// Test Case 2: FieldData has field and value, no ttl
TEST(CDCLogDataTest, EncodeDecodeFieldsAndValues) {
  CDCLogData cdc_data;
  cdc_data.SetRedisType(kRedisHash);
  cdc_data.SetVersion(CDCVersion::V1);
  uint64_t ts = 1234567890;
  cdc_data.SetTs(ts);

  // Add data containing fields and values
  std::vector<std::pair<std::string, std::string>> field_values = {{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}};

  for (const auto& [field, value] : field_values) {
    kv::datanode::v1::FieldData field_data;
    field_data.set_field(field);
    field_data.set_value(value);
    dynamic_cast<redis::HashCDCData*>(cdc_data.GetDataHandlerPtr())->Append(std::move(field_data));
  }

  // Encode
  std::string encoded = cdc_data.Encode();

  // Get ContentType and length
  rocksdb::Slice input(encoded);
  uint8_t content_type = 0;
  GetFixed8(&input, &content_type);
  uint64_t length = 0;
  GetFixed64(&input, &length);
  // Verify content type and length
  ASSERT_EQ(content_type, static_cast<uint8_t>(ContentType::kContentCDC));
  ASSERT_EQ(length, input.size());
  // Decode
  CDCLogData decoded_data;
  decoded_data.SetRedisType(kRedisHash);
  ASSERT_TRUE(decoded_data.Decode(&input).IsOK());

  // Verify decoded data
  ASSERT_EQ(decoded_data.GetVersion(), CDCVersion::V1);
  ASSERT_EQ(decoded_data.GetTs(), ts);
  ASSERT_EQ(decoded_data.GetKeyEXAT(), -1);

  const auto& decoded_fields = dynamic_cast<redis::HashCDCData*>(decoded_data.GetDataHandlerPtr())->GetFields();
  ASSERT_EQ(decoded_fields.size(), 3);
  for (size_t i = 0; i < field_values.size(); i++) {
    EXPECT_EQ(decoded_fields[i].field(), field_values[i].first);
    EXPECT_TRUE(decoded_fields[i].has_value());
    EXPECT_EQ(decoded_fields[i].value(), field_values[i].second);
    EXPECT_FALSE(decoded_fields[i].has_ttl());
  }
}

// Test Case 3: FieldData has field, value and ttl
TEST(CDCLogDataTest, EncodeDecodeFieldsValuesAndTTL) {
  CDCLogData cdc_data;
  cdc_data.SetRedisType(kRedisHash);
  cdc_data.SetVersion(CDCVersion::V1);
  uint64_t ts = 1234567890;
  cdc_data.SetTs(ts);

  // Set key expiration time
  int64_t key_exat = 9876543210;
  cdc_data.SetKeyEXAT(key_exat);

  // Add data containing field, value and ttl
  struct FieldInfo {
    std::string field;
    std::string value;
    uint64_t ttl;
  };

  std::vector<FieldInfo> field_infos = {{"f1", "v1", 12345},
                                        {"f2", "", 23456},  // Empty value
                                        {"f3", "v3", 34567}};

  for (const auto& info : field_infos) {
    kv::datanode::v1::FieldData field_data;
    field_data.set_field(info.field);
    if (!info.value.empty()) {
      field_data.set_value(info.value);
    }
    field_data.set_ttl(info.ttl);
    dynamic_cast<redis::HashCDCData*>(cdc_data.GetDataHandlerPtr())->Append(std::move(field_data));
  }

  // Encode
  std::string encoded = cdc_data.Encode();

  // get ContentType and length
  uint8_t content_type = 0;
  rocksdb::Slice input(encoded);
  GetFixed8(&input, &content_type);
  uint64_t length = 0;
  GetFixed64(&input, &length);
  // Verify content type and length
  ASSERT_EQ(content_type, static_cast<uint8_t>(ContentType::kContentCDC));
  ASSERT_EQ(length, input.size());
  // Decode
  CDCLogData decoded_data;
  decoded_data.SetRedisType(kRedisHash);
  ASSERT_TRUE(decoded_data.Decode(&input).IsOK());

  // Verify decoded data
  ASSERT_EQ(decoded_data.GetVersion(), CDCVersion::V1);
  ASSERT_EQ(decoded_data.GetTs(), ts);
  ASSERT_EQ(decoded_data.GetKeyEXAT(), key_exat);

  const auto& decoded_fields = dynamic_cast<redis::HashCDCData*>(decoded_data.GetDataHandlerPtr())->GetFields();
  ASSERT_EQ(decoded_fields.size(), 3);
  for (size_t i = 0; i < field_infos.size(); i++) {
    EXPECT_EQ(decoded_fields[i].field(), field_infos[i].field);
    if (!field_infos[i].value.empty()) {
      EXPECT_TRUE(decoded_fields[i].has_value());
      EXPECT_EQ(decoded_fields[i].value(), field_infos[i].value);
    } else {
      EXPECT_FALSE(decoded_fields[i].has_value());
    }
    EXPECT_TRUE(decoded_fields[i].has_ttl());
    EXPECT_EQ(decoded_fields[i].ttl(), field_infos[i].ttl);
  }
}

}  // namespace redis
