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

import org.junit.jupiter.api.Test;

import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetExpirationConditionImplTest {

    @Test
    public void testIsOffsetExpired() {
        long currentTimestamp = 1500L;
        long commitTimestamp = 500L;
        OptionalLong expireTimestampMs = OptionalLong.of(1500);
        long offsetsRetentionMs = 500L;

        OffsetExpirationConditionImpl condition = new OffsetExpirationConditionImpl(__ -> commitTimestamp);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
            100,
            OptionalInt.of(1),
            "metadata",
            commitTimestamp,
            expireTimestampMs
        );

        // Test when expire timestamp exists (older versions with per partition retention)
        // 1. Current timestamp >= expire timestamp => should expire
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));

        // 2. Current timestamp < expire timestamp => should not expire
        currentTimestamp = 499;
        assertFalse(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));

        // Test when expire timestamp does not exist (current version with no per partition retention)
        offsetAndMetadata = new OffsetAndMetadata(
            100,
            OptionalInt.of(1),
            "metadata",
            commitTimestamp,
            OptionalLong.empty()
        );

        // 3. Current timestamp - base timestamp >= offsets retention => should expire
        currentTimestamp = 1000L;
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));

        // 4. Current timestamp - base timestamp < offsets retention => should not expire
        currentTimestamp = 999L;
        assertFalse(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));
    }
}
