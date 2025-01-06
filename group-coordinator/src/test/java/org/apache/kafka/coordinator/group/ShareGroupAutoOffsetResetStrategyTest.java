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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.ListOffsetsRequest;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareGroupAutoOffsetResetStrategyTest {

    @Test
    public void testFromString() {
        assertEquals(ShareGroupAutoOffsetResetStrategy.EARLIEST, ShareGroupAutoOffsetResetStrategy.fromString("earliest"));
        assertEquals(ShareGroupAutoOffsetResetStrategy.LATEST, ShareGroupAutoOffsetResetStrategy.fromString("latest"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("invalid"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("by_duration:invalid"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("by_duration:-PT1H"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("by_duration:"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("by_duration"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("LATEST"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("EARLIEST"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString("NONE"));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString(""));
        assertThrows(IllegalArgumentException.class, () -> ShareGroupAutoOffsetResetStrategy.fromString(null));

        ShareGroupAutoOffsetResetStrategy strategy = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:PT1H");
        assertEquals("by_duration", strategy.name());
    }

    @Test
    public void testValidator() {
        ShareGroupAutoOffsetResetStrategy.Validator validator = new ShareGroupAutoOffsetResetStrategy.Validator();
        assertDoesNotThrow(() -> validator.ensureValid("test", "earliest"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "latest"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "by_duration:PT1H"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "invalid"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "by_duration:invalid"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "by_duration:-PT1H"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "by_duration:"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "by_duration"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "LATEST"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "EARLIEST"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "NONE"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", ""));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", null));
    }

    @Test
    public void testEqualsAndHashCode() {
        ShareGroupAutoOffsetResetStrategy earliest1 = ShareGroupAutoOffsetResetStrategy.fromString("earliest");
        ShareGroupAutoOffsetResetStrategy earliest2 = ShareGroupAutoOffsetResetStrategy.fromString("earliest");
        ShareGroupAutoOffsetResetStrategy latest1 = ShareGroupAutoOffsetResetStrategy.fromString("latest");

        ShareGroupAutoOffsetResetStrategy duration1 = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:P2D");
        ShareGroupAutoOffsetResetStrategy duration2 = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:P2D");

        assertEquals(earliest1, earliest2);
        assertNotEquals(earliest1, latest1);
        assertEquals(earliest1.hashCode(), earliest2.hashCode());
        assertNotEquals(earliest1.hashCode(), latest1.hashCode());

        assertNotEquals(latest1, duration2);
        assertEquals(duration1, duration2);
    }

    @Test
    public void testTimestamp() {
        ShareGroupAutoOffsetResetStrategy earliest1 = ShareGroupAutoOffsetResetStrategy.fromString("earliest");
        ShareGroupAutoOffsetResetStrategy earliest2 = ShareGroupAutoOffsetResetStrategy.fromString("earliest");
        assertEquals(ListOffsetsRequest.EARLIEST_TIMESTAMP, earliest1.timestamp());
        assertEquals(earliest1, earliest2);

        ShareGroupAutoOffsetResetStrategy latest1 = ShareGroupAutoOffsetResetStrategy.fromString("latest");
        ShareGroupAutoOffsetResetStrategy latest2 = ShareGroupAutoOffsetResetStrategy.fromString("latest");
        assertEquals(ListOffsetsRequest.LATEST_TIMESTAMP, latest1.timestamp());
        assertEquals(latest1, latest2);

        ShareGroupAutoOffsetResetStrategy byDuration1 = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:PT1H");
        Long timestamp = byDuration1.timestamp();
        assertTrue(timestamp <= Instant.now().toEpochMilli() - Duration.ofHours(1).toMillis());

        ShareGroupAutoOffsetResetStrategy byDuration2 = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:PT1H");
        ShareGroupAutoOffsetResetStrategy byDuration3 = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:PT2H");

        assertEquals(byDuration1, byDuration2);
        assertNotEquals(byDuration1, byDuration3);
    }
}
