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

public class EventTimeRangeTest {

    @Test
    public void shouldReturnGracePeriodForNoGrace() {
        final EventTimeRange<?, ?> range = EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofSeconds(10), Duration.ofSeconds(5));
        assertEquals(0L, range.gracePeriodMs());
    }

    @Test
    public void shouldReturnGracePeriod() {
        final EventTimeRange<?, ?> range = EventTimeRange.ofTimeBoundsAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5), Duration.ofSeconds(3));
        assertEquals(3000L, range.gracePeriodMs());
    }

    @Test
    public void retentionMsShouldEqualBeforePlusGrace() {
        final long beforeMs = Duration.ofSeconds(20).toMillis();
        final long gracePeriodMs = Duration.ofSeconds(5).toMillis();
        final EventTimeRange<?, ?> range = EventTimeRange.ofTimeBoundsAndGrace(
            Duration.ofMillis(beforeMs), Duration.ofSeconds(0), Duration.ofMillis(gracePeriodMs));
        assertEquals(beforeMs, range.rangeRetentionMs());
        assertEquals(beforeMs + gracePeriodMs, range.retentionMs());
    }

    @Test
    public void rangeRetentionMsShouldEqualBefore() {
        final long beforeMs = Duration.ofMinutes(5).toMillis();
        final EventTimeRange<?, ?> range = EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofMillis(beforeMs), Duration.ofSeconds(10));
        assertEquals(beforeMs, range.rangeRetentionMs());
    }

    @Test
    public void beforeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofMillis(-1), Duration.ofSeconds(5)));
    }

    @Test
    public void afterMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofSeconds(10), Duration.ofMillis(-1)));
    }

    @Test
    public void gracePeriodMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5), Duration.ofMillis(-1)));
    }

    @Test
    public void shouldThrowOnOverflowingBeforeDuration() {
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofSeconds(Long.MAX_VALUE), Duration.ofSeconds(5)));
    }

    @Test
    public void shouldThrowOnOverflowingAfterDuration() {
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofSeconds(10), Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    public void shouldThrowOnOverflowingGraceDuration() {
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5), Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    public void withMaxRecordsShouldReturnConcreteType() {
        final EventTimeRange<String, String> range = EventTimeRange.<String, String>ofTimeBoundsWithNoGrace(Duration.ofSeconds(10), Duration.ofSeconds(5))
            .withMaxRecords(100);
        assertInstanceOf(EventTimeRange.class, range);
    }

    @Test
    public void maxRecordsMustBePositive() {
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)).withMaxRecords(0));
        assertThrows(IllegalArgumentException.class,
            () -> EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)).withMaxRecords(-1));
    }

    @Test
    public void rangeAggregatorCanBeExpressedAsLambda() {
        final RangeAggregator<String, Long, Long> aggregator = (anchor, records) -> {
            long sum = 0L;
            for (final org.apache.kafka.streams.processor.api.Record<String, Long> r : records) {
                sum += r.value();
            }
            return sum;
        };
        // Just verifying it compiles as a lambda
        assertEquals(RangeAggregator.class, aggregator.getClass().getInterfaces()[0]);
    }
}
