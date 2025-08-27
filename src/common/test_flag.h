#pragma once

namespace redis {

inline bool is_in_test = false;

inline void SetInTest() { is_in_test = true; }
inline void SetOutTest() { is_in_test = false; }

}  // namespace redis
