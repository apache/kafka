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
package org.apache.kafka.streams.kstream;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EventCountRangeTest {

    @Test
    public void shouldReturnGracePeriodForNoGrace() {
        final EventCountRange<?, ?> range = EventCountRange.ofCountBoundsWithNoGrace(3, 1, Duration.ofHours(1));
        assertEquals(0L, range.gracePeriodMs());
    }

    @Test
    public void shouldReturnGracePeriod() {
        final EventCountRange<?, ?> range = EventCountRange.ofCountBoundsAndGrace(3, 1, Duration.ofHours(1), Duration.ofSeconds(5));
        assertEquals(5000L, range.gracePeriodMs());
    }

    @Test
    public void rangeRetentionMsShouldEqualMaxTimeBefore() {
        final long maxTimeBeforeMs = Duration.ofHours(2).toMillis();
        final EventCountRange<?, ?> range = EventCountRange.ofCountBoundsWithNoGrace(3, 0, Duration.ofMillis(maxTimeBeforeMs));
        assertEquals(maxTimeBeforeMs, range.rangeRetentionMs());
    }

    @Test
    public void retentionMsShouldEqualMaxTimeBeforePlusGrace() {
        final long maxTimeBeforeMs = Duration.ofHours(1).toMillis();
        final long gracePeriodMs = Duration.ofSeconds(5).toMillis();
        final EventCountRange<?, ?> range = EventCountRange.ofCountBoundsAndGrace(3, 0, Duration.ofMillis(maxTimeBeforeMs), Duration.ofMillis(gracePeriodMs));
        assertEquals(maxTimeBeforeMs, range.rangeRetentionMs());
        assertEquals(maxTimeBeforeMs + gracePeriodMs, range.retentionMs());
    }

    @Test
    public void beforeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsWithNoGrace(-1, 1, Duration.ofHours(1)));
    }

    @Test
    public void afterMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsWithNoGrace(3, -1, Duration.ofHours(1)));
    }

    @Test
    public void maxTimeBeforeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsWithNoGrace(3, 0, Duration.ofMillis(-1)));
    }

    @Test
    public void gracePeriodMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsAndGrace(3, 0, Duration.ofHours(1), Duration.ofMillis(-1)));
    }

    @Test
    public void shouldThrowOnOverflowingMaxTimeBeforeDuration() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsWithNoGrace(3, 0, Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    public void shouldThrowOnOverflowingGraceDuration() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsAndGrace(3, 0, Duration.ofHours(1), Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    public void withMaxTimeAfterShouldReturnConcreteType() {
        final EventCountRange<String, String> range = EventCountRange.<String, String>ofCountBoundsWithNoGrace(3, 0, Duration.ofHours(1))
            .withMaxTimeAfter(Duration.ofSeconds(30));
        assertInstanceOf(EventCountRange.class, range);
    }

    @Test
    public void maxTimeAfterMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsWithNoGrace(3, 0, Duration.ofHours(1)).withMaxTimeAfter(Duration.ofMillis(-1)));
    }

    @Test
    public void shouldThrowOnOverflowingMaxTimeAfterDuration() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCountRange.ofCountBoundsWithNoGrace(3, 0, Duration.ofHours(1)).withMaxTimeAfter(Duration.ofSeconds(Long.MAX_VALUE)));
    }
}
