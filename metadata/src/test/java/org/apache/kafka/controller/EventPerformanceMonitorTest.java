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

package org.apache.kafka.controller;

import org.junit.jupiter.api.Test;

import java.util.AbstractMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class EventPerformanceMonitorTest {
    @Test
    public void testDefaultPeriodNs() {
        assertEquals(SECONDS.toNanos(60),
            new EventPerformanceMonitor.Builder().build().periodNs());
    }

    @Test
    public void testSlowestEventWithNoEvents() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        assertEquals(new AbstractMap.SimpleImmutableEntry<>(null, 0L),
            monitor.slowestEvent());
    }

    @Test
    public void testSlowestEventWithThreeEvents() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        monitor.observeEvent("fastEvent", MILLISECONDS.toNanos(2));
        monitor.observeEvent("slowEvent", MILLISECONDS.toNanos(100));
        assertEquals(new AbstractMap.SimpleImmutableEntry<>("slowEvent", MILLISECONDS.toNanos(100)),
            monitor.slowestEvent());
    }

    @Test
    public void testLogSlowEvent() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        assertEquals("Exceptionally slow controller event slowEvent took 5000 ms.",
            monitor.doObserveEvent("slowEvent", SECONDS.toNanos(5)));
    }

    @Test
    public void testDoNotLogFastEvent() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        assertNull(monitor.doObserveEvent("slowEvent", MILLISECONDS.toNanos(250)));
    }

    @Test
    public void testFormatNsAsDecimalMsWithZero() {
        assertEquals("0.00",
            EventPerformanceMonitor.formatNsAsDecimalMs(0));
    }

    @Test
    public void testFormatNsAsDecimalMsWith100() {
        assertEquals("100.00",
            EventPerformanceMonitor.formatNsAsDecimalMs(MILLISECONDS.toNanos(100)));
    }

    @Test
    public void testFormatNsAsDecimalMsWith123456789() {
        assertEquals("123.46",
            EventPerformanceMonitor.formatNsAsDecimalMs(123456789));
    }

    @Test
    public void testPeriodicPerformanceMessageWithNoEvents() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        assertEquals("In the last 60000 ms period, there were no controller events completed.",
            monitor.periodicPerformanceMessage());
    }

    @Test
    public void testPeriodicPerformanceMessageWithOneEvent() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        monitor.observeEvent("myEvent", MILLISECONDS.toNanos(12));
        assertEquals("In the last 60000 ms period, 1 controller events were completed, which took an " +
            "average of 12.00 ms each. The slowest event was myEvent, which took 12.00 ms.",
                monitor.periodicPerformanceMessage());
    }

    @Test
    public void testPeriodicPerformanceMessageWithThreeEvents() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        monitor.observeEvent("myEvent", MILLISECONDS.toNanos(12));
        monitor.observeEvent("myEvent2", MILLISECONDS.toNanos(19));
        monitor.observeEvent("myEvent3", MILLISECONDS.toNanos(1));
        assertEquals("In the last 60000 ms period, 3 controller events were completed, which took an " +
            "average of 10.67 ms each. The slowest event was myEvent2, which took 19.00 ms.",
                monitor.periodicPerformanceMessage());
    }

    @Test
    public void testGeneratePeriodicPerformanceMessageResetsState() {
        EventPerformanceMonitor monitor = new EventPerformanceMonitor.Builder().build();
        monitor.observeEvent("myEvent", MILLISECONDS.toNanos(12));
        monitor.observeEvent("myEvent2", MILLISECONDS.toNanos(19));
        monitor.observeEvent("myEvent3", MILLISECONDS.toNanos(1));
        monitor.generatePeriodicPerformanceMessage();
        assertEquals("In the last 60000 ms period, there were no controller events completed.",
            monitor.periodicPerformanceMessage());
    }
}
