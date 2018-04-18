// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <jni.h>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"

#include "include/org_apache_kafka_streams_state_internals_WindowKeySchema_NativeWindowKeyBytesComparatorWrapper.h"

namespace rocksdb {

class NativeWindowKeyBytesComparator : public Comparator {
  const int SUFFIX_SIZE = 12;

  const char* Name() const {
    return "NativeWindowKeyBytesComparator";
  }

  int Compare(const Slice& a, const Slice& b) const {
    assert(a.data() != nullptr && b.data() != nullptr);
    const size_t akey_len = a.size() - SUFFIX_SIZE;
    const size_t bkey_len = b.size() - SUFFIX_SIZE;
    const size_t min_len = (akey_len < bkey_len) ? akey_len : bkey_len;
    int r = memcmp(a.data(), b.data(), min_len);
    if (r == 0) {
      r = memcmp(a.data() + akey_len, b.data() + bkey_len, SUFFIX_SIZE);
    }
    return r;
  }

  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const {
    return;
  }

  void FindShortSuccessor(std::string* /*key*/) const { return; }
};
}  // namespace rocksdb

/*
 * Class: org_apache_kafka_streams_state_internals_WindowKeySchema_NativeWindowKeyBytesComparatorWrapper
 * Method:    newComparator
 * Signature: ()J
 */
jlong Java_org_apache_kafka_streams_state_internals_WindowKeySchema_NativeWindowKeyBytesComparatorWrapper_newComparator(
    JNIEnv* /*env*/, jobject /*jobj*/) {
  auto* comparator = new rocksdb::NativeWindowKeyBytesComparator();
  return reinterpret_cast<jlong>(comparator);
}