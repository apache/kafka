/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

/**
 * Describes a key in producer payload
 */
public enum PayloadKeyType {
    // null key
    KEY_NULL(0),
    // fixed size key containing a long integer representing a message index (i.e., position of
    // the payload generator)
    KEY_MESSAGE_INDEX(8);

    private final int maxSizeInBytes;

    PayloadKeyType(int maxSizeInBytes) {
        this.maxSizeInBytes = maxSizeInBytes;
    }

    public int maxSizeInBytes() {
        return maxSizeInBytes;
    }
}
