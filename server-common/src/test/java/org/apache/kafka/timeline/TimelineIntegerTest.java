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

package org.apache.kafka.timeline;

import java.util.Collections;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class TimelineIntegerTest {
    @Test
    public void testModifyValue() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineInteger integer = new TimelineInteger(registry);
        assertEquals(0, integer.get());
        assertEquals(0, integer.get(Long.MAX_VALUE));
        integer.set(1);
        integer.set(2);
        assertEquals(2, integer.get());
        assertEquals(2, integer.get(Long.MAX_VALUE));
    }

    @Test
    public void testToStringAndEquals() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineInteger integer = new TimelineInteger(registry);
        assertEquals("0", integer.toString());
        integer.set(1);
        TimelineInteger integer2 = new TimelineInteger(registry);
        integer2.set(1);
        assertEquals("1", integer2.toString());
        assertEquals(integer, integer2);
    }

    @Test
    public void testSnapshot() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineInteger integer = new TimelineInteger(registry);
        registry.getOrCreateSnapshot(2);
        integer.set(1);
        registry.getOrCreateSnapshot(3);
        integer.set(2);
        integer.increment();
        integer.increment();
        integer.decrement();
        registry.getOrCreateSnapshot(4);
        assertEquals(0, integer.get(2));
        assertEquals(1, integer.get(3));
        assertEquals(3, integer.get(4));
        registry.revertToSnapshot(3);
        assertEquals(1, integer.get());
        registry.revertToSnapshot(2);
        assertEquals(0, integer.get());
    }

    @Test
    public void testReset() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineInteger value = new TimelineInteger(registry);
        registry.getOrCreateSnapshot(2);
        value.set(1);
        registry.getOrCreateSnapshot(3);
        value.set(2);

        registry.reset();

        assertEquals(Collections.emptyList(), registry.epochsList());
        assertEquals(TimelineInteger.INIT, value.get());
    }
}
