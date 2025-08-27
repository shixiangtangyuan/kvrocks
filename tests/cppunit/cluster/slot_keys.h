#pragma once

#include <string>
#include <vector>

#include "cluster/cluster_defs.h"

namespace redis {

extern const char* global_slot_keys[kClusterSlots];

std::vector<std::string> GetSlotsKeys(std::vector<int16_t> slots, bool with_val = false);

}  // namespace redis
