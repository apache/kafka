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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.ListOffsetsRequest;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AutoOffsetResetStrategyTest {

    @Test
    public void testFromString() {
        assertEquals(AutoOffsetResetStrategy.EARLIEST, AutoOffsetResetStrategy.fromString("earliest"));
        assertEquals(AutoOffsetResetStrategy.LATEST, AutoOffsetResetStrategy.fromString("latest"));
        assertEquals(AutoOffsetResetStrategy.NONE, AutoOffsetResetStrategy.fromString("none"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("invalid"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("by_duration:invalid"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("by_duration:-PT1H"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("by_duration:"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("by_duration"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("LATEST"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("EARLIEST"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("NONE"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString(""));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString(null));

        AutoOffsetResetStrategy strategy = AutoOffsetResetStrategy.fromString("by_duration:PT1H");
        assertEquals("by_duration", strategy.name());
    }

    @Test
    public void testValidator() {
        AutoOffsetResetStrategy.Validator validator = new AutoOffsetResetStrategy.Validator();
        assertDoesNotThrow(() -> validator.ensureValid("test", "earliest"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "latest"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "none"));
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
        AutoOffsetResetStrategy earliest1 = AutoOffsetResetStrategy.fromString("earliest");
        AutoOffsetResetStrategy earliest2 = AutoOffsetResetStrategy.fromString("earliest");
        AutoOffsetResetStrategy latest1 = AutoOffsetResetStrategy.fromString("latest");

        AutoOffsetResetStrategy duration1 = AutoOffsetResetStrategy.fromString("by_duration:P2D");
        AutoOffsetResetStrategy duration2 = AutoOffsetResetStrategy.fromString("by_duration:P2D");

        assertEquals(earliest1, earliest2);
        assertNotEquals(earliest1, latest1);
        assertEquals(earliest1.hashCode(), earliest2.hashCode());
        assertNotEquals(earliest1.hashCode(), latest1.hashCode());

        assertNotEquals(latest1, duration2);
        assertEquals(duration1, duration2);
    }

    @Test
    public void testTimestamp() {
        AutoOffsetResetStrategy earliest1 = AutoOffsetResetStrategy.fromString("earliest");
        AutoOffsetResetStrategy earliest2 = AutoOffsetResetStrategy.fromString("earliest");
        assertEquals(Optional.of(ListOffsetsRequest.EARLIEST_TIMESTAMP), earliest1.timestamp());
        assertEquals(earliest1, earliest2);

        AutoOffsetResetStrategy latest1 = AutoOffsetResetStrategy.fromString("latest");
        AutoOffsetResetStrategy latest2 = AutoOffsetResetStrategy.fromString("latest");
        assertEquals(Optional.of(ListOffsetsRequest.LATEST_TIMESTAMP), latest1.timestamp());
        assertEquals(latest1, latest2);

        AutoOffsetResetStrategy none1 = AutoOffsetResetStrategy.fromString("none");
        AutoOffsetResetStrategy none2 = AutoOffsetResetStrategy.fromString("none");
        assertFalse(none1.timestamp().isPresent());
        assertEquals(none1, none2);

        AutoOffsetResetStrategy byDuration1 = AutoOffsetResetStrategy.fromString("by_duration:PT1H");
        Optional<Long> timestamp = byDuration1.timestamp();
        assertTrue(timestamp.isPresent());
        assertTrue(timestamp.get() <= Instant.now().toEpochMilli() - Duration.ofHours(1).toMillis());

        AutoOffsetResetStrategy byDuration2 = AutoOffsetResetStrategy.fromString("by_duration:PT1H");
        AutoOffsetResetStrategy byDuration3 = AutoOffsetResetStrategy.fromString("by_duration:PT2H");

        assertEquals(byDuration1, byDuration2);
        assertNotEquals(byDuration1, byDuration3);
    }
}