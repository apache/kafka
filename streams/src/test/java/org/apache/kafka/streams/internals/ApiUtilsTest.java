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

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondInstant;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


public class ApiUtilsTest {

    // This is the maximum limit that Duration accepts but fails when it converts to milliseconds.
    private static final long MAX_ACCEPTABLE_DAYS_FOR_DURATION = 106751991167300L;
    // This is the maximum limit that Duration accepts and converts to milliseconds with out fail.
    private static final long MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS = 106751991167L;

    @Test
    public void shouldThrowNullPointerExceptionForNullDuration() {
        final String nullDurationPrefix = prepareMillisCheckFailMsgPrefix(null, "nullDuration");

        try {
            validateMillisecondDuration(null, nullDurationPrefix);
            fail("Expected exception when null passed to duration.");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(nullDurationPrefix));
        }
    }

    @Test
    public void shouldThrowArithmeticExceptionForMaxDuration() {
        final Duration maxDurationInDays = Duration.ofDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION);
        final String maxDurationPrefix = prepareMillisCheckFailMsgPrefix(maxDurationInDays, "maxDuration");

        try {
            validateMillisecondDuration(maxDurationInDays, maxDurationPrefix);
            fail("Expected exception when maximum days passed for duration, because of long overflow");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(maxDurationPrefix));
        }
    }

    @Test
    public void shouldThrowNullPointerExceptionForNullInstant() {
        final String nullInstantPrefix = prepareMillisCheckFailMsgPrefix(null, "nullInstant");

        try {
            validateMillisecondInstant(null, nullInstantPrefix);
            fail("Expected exception when null value passed for instant.");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(nullInstantPrefix));
        }
    }

    @Test
    public void shouldThrowArithmeticExceptionForMaxInstant() {
        final String maxInstantPrefix = prepareMillisCheckFailMsgPrefix(Instant.MAX, "maxInstant");

        try {
            validateMillisecondInstant(Instant.MAX, maxInstantPrefix);
            fail("Expected exception when maximum value passed for instant, because of long overflow.");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(maxInstantPrefix));
        }
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

        assertThat(failMsgPrefix, containsString("variableName"));
        assertThat(failMsgPrefix, containsString("someValue"));
    }
}