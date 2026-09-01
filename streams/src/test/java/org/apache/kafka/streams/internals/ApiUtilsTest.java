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
package org.apache.kafka.streams.internals;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondInstant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiUtilsTest {
    // This is the maximum limit that Duration accepts but fails when it converts to milliseconds.
    private static final long MAX_ACCEPTABLE_DAYS_FOR_DURATION = 106751991167300L;
    // This is the maximum limit that Duration accepts and converts to milliseconds with out fail.
    private static final long MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS = 106751991167L;

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullDuration() {
        final String nullDurationPrefix = prepareMillisCheckFailMsgPrefix(null, "nullDuration");

        final IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> validateMillisecondDuration(null, nullDurationPrefix)
        );
        assertTrue(e.getMessage().contains(nullDurationPrefix));
    }

    @Test
    public void shouldThrowArithmeticExceptionForMaxDuration() {
        final Duration maxDurationInDays = Duration.ofDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION);
        final String maxDurationPrefix = prepareMillisCheckFailMsgPrefix(maxDurationInDays, "maxDuration");

        final IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> validateMillisecondDuration(maxDurationInDays, maxDurationPrefix)
        );
        assertTrue(e.getMessage().contains(maxDurationPrefix));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullInstant() {
        final String nullInstantPrefix = prepareMillisCheckFailMsgPrefix(null, "nullInstant");

        final IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> validateMillisecondInstant(null, nullInstantPrefix)
        );
        assertTrue(e.getMessage().contains(nullInstantPrefix));
    }

    @Test
    public void shouldThrowArithmeticExceptionForMaxInstant() {
        final String maxInstantPrefix = prepareMillisCheckFailMsgPrefix(Instant.MAX, "maxInstant");

        final IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> validateMillisecondInstant(Instant.MAX, maxInstantPrefix)
        );
        assertTrue(e.getMessage().contains(maxInstantPrefix));
    }

    @Test
    public void shouldReturnMillisecondsOnValidDuration() {
        final Duration sampleDuration = Duration.ofDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS);

        assertEquals(sampleDuration.toMillis(), validateMillisecondDuration(sampleDuration, "sampleDuration"));
    }

    @Test
    public void shouldReturnMillisecondsOnValidInstant() {
        final Instant sampleInstant = Instant.now();

        assertEquals(sampleInstant.toEpochMilli(), validateMillisecondInstant(sampleInstant, "sampleInstant"));
    }

    @Test
    public void shouldContainsNameAndValueInFailMsgPrefix() {
        final String failMsgPrefix = prepareMillisCheckFailMsgPrefix("someValue", "variableName");

        assertTrue(failMsgPrefix.contains("variableName"));
        assertTrue(failMsgPrefix.contains("someValue"));
    }
}
