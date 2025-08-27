/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "config/config.h"
#include "storage/compact_filter.h"
#include "storage/storage.h"
#include "types/redis_hash.h"

class FilterLevelConfigTest : public testing::Test {
 protected:
  void SetUp() override {
    config_ = std::make_unique<Config>();
    config_->slot_id_encoded = false;
    db_dir_ = "test_filter_level_db";

    // Clean up any existing test directory
    std::error_code ec;
    std::filesystem::remove_all(db_dir_, ec);
  }

  void TearDown() override {
    if (storage_) {
      storage_.reset();
    }

    // Clean up test directory
    std::error_code ec;
    std::filesystem::remove_all(db_dir_, ec);
  }

  void InitStorage() {
    storage_ = std::make_unique<engine::Storage>(config_.get());
    auto s = storage_->Open(db_dir_);
    ASSERT_TRUE(s.IsOK());
  }

  std::unique_ptr<Config> config_;
  std::unique_ptr<engine::Storage> storage_;
  std::string db_dir_;
};

TEST_F(FilterLevelConfigTest, DefaultConfiguration) {
  // Test default "3,4,5,6" configuration (skip L0/L1/L2 for performance)
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(1));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(2));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(3));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(4));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(5));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(6));

  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(0));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(1));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(2));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(3));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(4));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(5));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(6));
}

TEST_F(FilterLevelConfigTest, EmptyConfiguration) {
  config_->metadata_filter_levels = "";
  config_->subkey_filter_levels = "";
  config_->UpdateFilterLevelMasks();  // Update cached bitmasks

  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(3));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(6));

  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(0));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(3));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(6));
}

TEST_F(FilterLevelConfigTest, SpecificLevels) {
  config_->metadata_filter_levels = "0,2,4";
  config_->subkey_filter_levels = "1,3,5";
  config_->UpdateFilterLevelMasks();  // Update cached bitmasks

  // Test metadata filter levels
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(1));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(2));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(3));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(4));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(5));

  // Test subkey filter levels
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(0));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(1));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(2));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(3));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(4));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(5));
}

TEST_F(FilterLevelConfigTest, CommaListConfiguration) {
  config_->metadata_filter_levels = "2,3,4,5";
  config_->subkey_filter_levels = "0,1,2";
  config_->UpdateFilterLevelMasks();  // Update cached bitmasks

  // Test metadata filter levels
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(1));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(2));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(3));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(4));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(5));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(6));

  // Test subkey filter levels
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(0));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(1));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(2));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(3));
}

TEST_F(FilterLevelConfigTest, SingleLevelConfiguration) {
  config_->metadata_filter_levels = "3";
  config_->UpdateFilterLevelMasks();  // Update cached bitmasks

  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(1));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(2));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(3));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(4));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(5));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(6));
}

TEST_F(FilterLevelConfigTest, ConfigValidation) {
  Config config;

  // Test valid configurations
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "").IsOK());  // Empty string
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "0,1,2").IsOK());
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "2,3,4,5,6").IsOK());
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "0").IsOK());  // Single level
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "6").IsOK());  // Max level

  // Test invalid configurations
  EXPECT_FALSE(config.Set(nullptr, "metadata-filter-levels", "invalid").IsOK());
  EXPECT_FALSE(config.Set(nullptr, "metadata-filter-levels", "-1").IsOK());     // negative level
  EXPECT_FALSE(config.Set(nullptr, "metadata-filter-levels", "7").IsOK());      // level > 6
  EXPECT_FALSE(config.Set(nullptr, "metadata-filter-levels", "1-5").IsOK());    // range format not supported
  EXPECT_FALSE(config.Set(nullptr, "metadata-filter-levels", "a,b,c").IsOK());  // non-numeric
}

TEST_F(FilterLevelConfigTest, FilterBehaviorWithLevelControl) {
  // Set metadata filter to only work on levels 3+
  config_->metadata_filter_levels = "3,4,5,6";
  // Set subkey filter to only work on levels 0-2
  config_->subkey_filter_levels = "0,1,2";
  config_->UpdateFilterLevelMasks();  // Update cached bitmasks

  InitStorage();

  // Create filters
  engine::MetadataFilter metadata_filter(storage_.get());
  engine::SubKeyFilter subkey_filter(storage_.get());

  // For this test, we'll verify the configuration is working correctly
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(1));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(2));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(3));
  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(4));

  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(0));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(1));
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(2));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(3));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(4));
}

TEST_F(FilterLevelConfigTest, ConfigFileLoading) {
  const char *config_file = "test_filter_config.conf";

  // Create a config file with filter level settings
  std::ofstream output_file(config_file, std::ios::out);
  output_file << "metadata-filter-levels 2,3,4,5\n";
  output_file << "subkey-filter-levels 0,3,6\n";
  output_file << "compaction-with-filter yes\n";
  // Add required datanode config items
  output_file << "datadir-list test_datadirlist\n";
  output_file << "cluster-id test_clusterid\n";
  output_file << "datanode-id test_datanodeid\n";
  output_file << "controller-addr test_addr\n";
  output_file << "pool test_pool\n";
  output_file.close();

  Config config;
  auto s = config.Load(CLIOptions(config_file));
  if (!s.IsOK()) {
    std::cout << "Config load error: " << s.Msg() << std::endl;
  }
  ASSERT_TRUE(s.IsOK());

  // Verify the configuration was loaded correctly
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(1));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(2));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(3));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(4));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(5));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(6));

  EXPECT_TRUE(config.ShouldSubkeyFilterAtLevel(0));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(1));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(2));
  EXPECT_TRUE(config.ShouldSubkeyFilterAtLevel(3));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(4));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(5));
  EXPECT_TRUE(config.ShouldSubkeyFilterAtLevel(6));

  // Clean up
  unlink(config_file);
}

TEST_F(FilterLevelConfigTest, Level6SpecificTesting) {
  // Test level 6 (maximum level) specifically
  config_->metadata_filter_levels = "6";
  config_->subkey_filter_levels = "6";
  config_->UpdateFilterLevelMasks();

  // Only level 6 should be enabled
  for (int level = 0; level < 6; ++level) {
    EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(level))
        << "Level " << level << " should not have metadata filtering enabled";
    EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(level))
        << "Level " << level << " should not have subkey filtering enabled";
  }

  EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(6)) << "Level 6 should have metadata filtering enabled";
  EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(6)) << "Level 6 should have subkey filtering enabled";

  // Test edge case: out of bounds access
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(-1));
  EXPECT_FALSE(config_->ShouldMetadataFilterAtLevel(7));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(-1));
  EXPECT_FALSE(config_->ShouldSubkeyFilterAtLevel(7));
}

TEST_F(FilterLevelConfigTest, AllLevelsIncludingLevel6) {
  // Test configuration with all levels including level 6
  config_->metadata_filter_levels = "0,1,2,3,4,5,6";
  config_->subkey_filter_levels = "0,1,2,3,4,5,6";
  config_->UpdateFilterLevelMasks();

  // All levels 0-6 should be enabled
  for (int level = 0; level <= 6; ++level) {
    EXPECT_TRUE(config_->ShouldMetadataFilterAtLevel(level))
        << "Level " << level << " should have metadata filtering enabled";
    EXPECT_TRUE(config_->ShouldSubkeyFilterAtLevel(level))
        << "Level " << level << " should have subkey filtering enabled";
  }
}

TEST_F(FilterLevelConfigTest, Level6ValidationBoundaryTesting) {
  Config config;

  // Test level 6 boundary cases
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "6").IsOK()) << "Level 6 should be valid";
  EXPECT_TRUE(config.Set(nullptr, "subkey-filter-levels", "6").IsOK()) << "Level 6 should be valid";

  // Test level 6 in combination with other levels
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "0,6").IsOK()) << "Levels 0,6 should be valid";
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "3,4,5,6").IsOK()) << "Levels 3,4,5,6 should be valid";
  EXPECT_TRUE(config.Set(nullptr, "metadata-filter-levels", "6,0,3").IsOK())
      << "Levels 6,0,3 (order shouldn't matter) should be valid";

  // Test invalid: level 7 (beyond maximum)
  EXPECT_FALSE(config.Set(nullptr, "metadata-filter-levels", "7").IsOK()) << "Level 7 should be invalid";
  EXPECT_FALSE(config.Set(nullptr, "metadata-filter-levels", "6,7").IsOK())
      << "Level 6,7 should be invalid due to level 7";
}

TEST_F(FilterLevelConfigTest, ConfigCallbackMechanismTesting) {
  // Test that the callback mechanism properly updates the bitmasks
  Config config;

  // Initially should use default values ("3,4,5,6" -> levels 3-6 enabled)
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(3));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(6));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(1));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(2));

  // Use Config::Set which should trigger both validation and callback
  auto status = config.Set(nullptr, "metadata-filter-levels", "6");
  EXPECT_TRUE(status.IsOK()) << "Setting metadata-filter-levels should succeed: " << status.Msg();

  // After setting via Config::Set, only level 6 should be enabled
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(1));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(2));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(3));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(4));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(5));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(6));

  // Test subkey filter callback as well
  status = config.Set(nullptr, "subkey-filter-levels", "0,6");
  EXPECT_TRUE(status.IsOK()) << "Setting subkey-filter-levels should succeed: " << status.Msg();

  // After setting subkey-filter-levels to "0,6", only levels 0 and 6 should be enabled
  EXPECT_TRUE(config.ShouldSubkeyFilterAtLevel(0));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(1));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(2));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(3));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(4));
  EXPECT_FALSE(config.ShouldSubkeyFilterAtLevel(5));
  EXPECT_TRUE(config.ShouldSubkeyFilterAtLevel(6));
}

TEST_F(FilterLevelConfigTest, ValidationAndCallbackSeparation) {
  // Test that validation and callback are properly separated
  Config config;

  // First test: invalid value should fail validation and not trigger callback
  uint32_t original_mask = config.metadata_filter_mask.load();
  auto status = config.Set(nullptr, "metadata-filter-levels", "invalid_value");
  EXPECT_FALSE(status.IsOK()) << "Invalid value should fail validation";

  // The bitmask should remain unchanged because validation failed
  uint32_t current_mask = config.metadata_filter_mask.load();
  EXPECT_EQ(original_mask, current_mask) << "Bitmask should not change when validation fails";

  // Second test: valid value should pass validation and trigger callback
  status = config.Set(nullptr, "metadata-filter-levels", "1,5");
  EXPECT_TRUE(status.IsOK()) << "Valid value should pass validation";

  // The bitmask should be updated by the callback
  current_mask = config.metadata_filter_mask.load();
  EXPECT_NE(original_mask, current_mask) << "Bitmask should change when validation passes and callback executes";

  // Verify the actual filtering behavior matches the new configuration
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(0));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(1));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(2));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(3));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(4));
  EXPECT_TRUE(config.ShouldMetadataFilterAtLevel(5));
  EXPECT_FALSE(config.ShouldMetadataFilterAtLevel(6));
}

TEST_F(FilterLevelConfigTest, InitializationWithoutExplicitCall) {
  // Test that the configuration system works properly without explicit UpdateFilterLevelMasks calls
  // This verifies that our fix (removing UpdateFilterLevelMasks from constructor) is working

  Config fresh_config;

  // Default configuration should work immediately without any explicit calls
  // Default is "3,4,5,6" from header file, with corresponding bitmask 0b1111000
  EXPECT_FALSE(fresh_config.ShouldMetadataFilterAtLevel(0));
  EXPECT_FALSE(fresh_config.ShouldMetadataFilterAtLevel(1));
  EXPECT_FALSE(fresh_config.ShouldMetadataFilterAtLevel(2));
  EXPECT_TRUE(fresh_config.ShouldMetadataFilterAtLevel(3));
  EXPECT_TRUE(fresh_config.ShouldMetadataFilterAtLevel(4));
  EXPECT_TRUE(fresh_config.ShouldMetadataFilterAtLevel(5));
  EXPECT_TRUE(fresh_config.ShouldMetadataFilterAtLevel(6));

  // Same for subkey filter
  EXPECT_FALSE(fresh_config.ShouldSubkeyFilterAtLevel(0));
  EXPECT_FALSE(fresh_config.ShouldSubkeyFilterAtLevel(1));
  EXPECT_FALSE(fresh_config.ShouldSubkeyFilterAtLevel(2));
  EXPECT_TRUE(fresh_config.ShouldSubkeyFilterAtLevel(3));
  EXPECT_TRUE(fresh_config.ShouldSubkeyFilterAtLevel(4));
  EXPECT_TRUE(fresh_config.ShouldSubkeyFilterAtLevel(5));
  EXPECT_TRUE(fresh_config.ShouldSubkeyFilterAtLevel(6));
}