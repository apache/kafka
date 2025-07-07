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
package org.apache.kafka.coordinator.group;

import java.util.function.Function;

public class OffsetExpirationConditionImpl implements OffsetExpirationCondition {

    /**
     * Given an offset and metadata, obtain the base timestamp that should be used
     * as the start of the offsets retention period.
     */
    private final Function<OffsetAndMetadata, Long> baseTimestamp;

    public OffsetExpirationConditionImpl(Function<OffsetAndMetadata, Long> baseTimestamp) {
        this.baseTimestamp = baseTimestamp;
    }

    /**
     * Determine whether an offset is expired. Older versions have an expire timestamp per partition. If this
     * exists, compare against the current timestamp. Otherwise, use the base timestamp (either commit timestamp
     * or current state timestamp if group is empty for classic groups) and check whether the offset has
     * exceeded the offset retention.
     *
     * @param offset              The offset and metadata.
     * @param currentTimestampMs  The current timestamp.
     * @param offsetsRetentionMs  The offsets retention in milliseconds.
     *
     * @return Whether the given offset is expired or not.
     */
    @Override
    public boolean isOffsetExpired(OffsetAndMetadata offset, long currentTimestampMs, long offsetsRetentionMs) {
        if (offset.expireTimestampMs.isPresent()) {
            // Older versions with explicit expire_timestamp field => old expiration semantics is used
            return currentTimestampMs >= offset.expireTimestampMs.getAsLong();
        } else {
            // Current version with no per partition retention
            return currentTimestampMs - baseTimestamp.apply(offset) >= offsetsRetentionMs;
        }
    }

    /**
     * @return The base timestamp.
     */
    public Function<OffsetAndMetadata, Long> baseTimestamp() {
        return this.baseTimestamp;
    }
}
