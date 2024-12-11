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
package org.apache.kafka.streams;

import org.apache.kafka.streams.internals.AutoOffsetResetInternal;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AutoOffsetResetTest {

    @Test
    void latestShouldReturnAnEmptyDuration() {
        final AutoOffsetResetInternal latest = new AutoOffsetResetInternal(AutoOffsetReset.latest());
        assertTrue(latest.duration().isEmpty(), "Latest should have an empty duration.");
    }

    @Test
    void earliestShouldReturnAnEmptyDuration() {
        final AutoOffsetResetInternal earliest = new AutoOffsetResetInternal(AutoOffsetReset.earliest());
        assertTrue(earliest.duration().isEmpty(), "Earliest should have an empty duration.");
    }

    @Test
    void customDurationShouldMatchExpectedValue() {
        final Duration duration = Duration.ofSeconds(10L);
        final AutoOffsetResetInternal custom = new AutoOffsetResetInternal(AutoOffsetReset.byDuration(duration));
        assertEquals(10L, custom.duration().get().toSeconds(), "Duration should match the specified value in milliseconds.");
    }

    @Test
    void shouldThrowExceptionIfDurationIsNegative() {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> AutoOffsetReset.byDuration(Duration.ofSeconds(-1)),
            "Creating an AutoOffsetReset with a negative duration should throw an IllegalArgumentException."
        );
        assertEquals("Duration cannot be negative", exception.getMessage(), "Exception message should indicate the duration cannot be negative.");
    }

    @Test
    void twoInstancesCreatedAtTheSameTimeWithSameOptionsShouldBeEqual() {
        final AutoOffsetReset latest1 = AutoOffsetReset.latest();
        final AutoOffsetReset latest2 = AutoOffsetReset.latest();
        final AutoOffsetReset earliest1 = AutoOffsetReset.earliest();
        final  AutoOffsetReset earliest2 = AutoOffsetReset.earliest();
        final AutoOffsetReset custom1 = AutoOffsetReset.byDuration(Duration.ofSeconds(5));
        final AutoOffsetReset custom2 = AutoOffsetReset.byDuration(Duration.ofSeconds(5));
        final AutoOffsetReset customDifferent = AutoOffsetReset.byDuration(Duration.ofSeconds(10));

        // Equals
        assertEquals(latest1, latest2, "Two latest instances should be equal.");
        assertEquals(earliest1, earliest2, "Two earliest instances should be equal.");
        assertEquals(custom1, custom2, "Two custom instances with the same duration should be equal.");
        assertNotEquals(latest1, earliest1, "Latest and earliest should not be equal.");
        assertNotEquals(custom1, customDifferent, "Custom instances with different durations should not be equal.");

        // HashCode
        assertEquals(latest1.hashCode(), latest2.hashCode(), "HashCode for equal instances should be the same.");
        assertEquals(custom1.hashCode(), custom2.hashCode(), "HashCode for equal custom instances should be the same.");
        assertNotEquals(custom1.hashCode(), customDifferent.hashCode(), "HashCode for different custom instances should not match.");
    }
}
