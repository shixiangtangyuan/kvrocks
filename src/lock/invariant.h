#pragma once

#include <glog/logging.h>

namespace redis {

#define INVARIANT(e)                                                                 \
  do {                                                                               \
    if (!__builtin_expect(e, 1)) {                                                   \
      LOG(FATAL) << "INVARIANT failed:" << #e << ' ' << __FILE__ << ' ' << __LINE__; \
    }                                                                                \
  } while (0)

#define INVARIANT_COMPARE(e1, op, e2)                                                                                \
  do {                                                                                                               \
    if (!((e1)op(e2))) {                                                                                             \
      LOG(FATAL) << "INVARIANT failed:" << #e1 << #op << #e2 << ' ' << (e1) << #op << (e2) << ' ' << __FILE__ << ' ' \
                 << __LINE__;                                                                                        \
    }                                                                                                                \
  } while (0)

#define INVARIANT_LOG(e)                                                             \
  do {                                                                               \
    if (!__builtin_expect(e, 1)) {                                                   \
      LOG(ERROR) << "INVARIANT failed:" << #e << ' ' << __FILE__ << ' ' << __LINE__; \
    }                                                                                \
  } while (0)

#define INVARIANT_COMPARE_LOG(e1, op, e2)                                                                            \
  do {                                                                                                               \
    if (!((e1)op(e2))) {                                                                                             \
      LOG(ERROR) << "INVARIANT failed:" << #e1 << #op << #e2 << ' ' << (e1) << #op << (e2) << ' ' << __FILE__ << ' ' \
                 << __LINE__;                                                                                        \
    }                                                                                                                \
  } while (0)

#define INVARIANT_D(e) INVARIANT_LOG(e)
#define INVARIANT_COMPARE_D(e1, op, e2) INVARIANT_COMPARE_LOG(e1, op, e2)

}  // namespace redis
