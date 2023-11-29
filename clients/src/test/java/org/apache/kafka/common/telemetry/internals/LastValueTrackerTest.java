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
package org.apache.kafka.common.telemetry.internals;

import org.apache.kafka.common.telemetry.internals.LastValueTracker.InstantAndValue;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LastValueTrackerTest {

    private static final MetricKey METRIC_NAME = new MetricKey("test-metric", Collections.emptyMap());

    private final Instant instant1 = Instant.now();
    private final Instant instant2 = instant1.plusMillis(1);

    @Test
    public void testGetAndSetDouble() {
        LastValueTracker<Double> lastValueTracker = new LastValueTracker<>();
        Optional<InstantAndValue<Double>> result = lastValueTracker.getAndSet(METRIC_NAME, instant1, 1d);
        assertFalse(result.isPresent());
    }

    @Test
    public void testGetAndSetDoubleWithTrackedValue() {
        LastValueTracker<Double> lastValueTracker = new LastValueTracker<>();
        lastValueTracker.getAndSet(METRIC_NAME, instant1, 1d);

        Optional<InstantAndValue<Double>> result = lastValueTracker
            .getAndSet(METRIC_NAME, instant2, 1000d);

        assertTrue(result.isPresent());
        assertEquals(instant1, result.get().getIntervalStart());
        assertEquals(1d, result.get().getValue(), 1e-6);
    }

    @Test
    public void testGetAndSetLong() {
        LastValueTracker<Long> lastValueTracker = new LastValueTracker<>();
        Optional<InstantAndValue<Long>> result = lastValueTracker.getAndSet(METRIC_NAME, instant1, 1L);
        assertFalse(result.isPresent());
    }

    @Test
    public void testGetAndSetLongWithTrackedValue() {
        LastValueTracker<Long> lastValueTracker = new LastValueTracker<>();
        lastValueTracker.getAndSet(METRIC_NAME, instant1, 2L);
        Optional<InstantAndValue<Long>> result = lastValueTracker
            .getAndSet(METRIC_NAME, instant2, 10000L);

        assertTrue(result.isPresent());
        assertEquals(instant1, result.get().getIntervalStart());
        assertEquals(2L, result.get().getValue().longValue());
    }

    @Test
    public void testRemove() {
        LastValueTracker<Double> lastValueTracker = new LastValueTracker<>();
        lastValueTracker.getAndSet(METRIC_NAME, instant1, 1d);

        assertTrue(lastValueTracker.contains(METRIC_NAME));

        InstantAndValue<Double> result = lastValueTracker.remove(METRIC_NAME);
        assertNotNull(result);
        assertEquals(instant1, result.getIntervalStart());
        assertEquals(1d, result.getValue().longValue());
    }

    @Test
    public void testRemoveWithNullKey() {
        LastValueTracker<Double> lastValueTracker = new LastValueTracker<>();
        assertThrows(NullPointerException.class, () -> lastValueTracker.remove(null));
    }

    @Test
    public void testRemoveWithInvalidKey() {
        LastValueTracker<Double> lastValueTracker = new LastValueTracker<>();
        assertFalse(lastValueTracker.contains(METRIC_NAME));

        InstantAndValue<Double> result = lastValueTracker.remove(METRIC_NAME);
        assertNull(result);
    }
}