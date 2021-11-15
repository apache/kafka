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
public class TimelineLongTest {
    @Test
    public void testModifyValue() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineLong value = new TimelineLong(registry);
        assertEquals(0L, value.get());
        assertEquals(0L, value.get(Long.MAX_VALUE));
        value.set(1L);
        value.set(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, value.get());
        assertEquals(Long.MAX_VALUE, value.get(Long.MAX_VALUE));
    }

    @Test
    public void testToStringAndEquals() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineLong value = new TimelineLong(registry);
        assertEquals("0", value.toString());
        value.set(1L);
        TimelineLong integer2 = new TimelineLong(registry);
        integer2.set(1);
        assertEquals("1", integer2.toString());
        assertEquals(value, integer2);
    }

    @Test
    public void testSnapshot() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineLong value = new TimelineLong(registry);
        registry.getOrCreateSnapshot(2);
        value.set(1L);
        registry.getOrCreateSnapshot(3);
        value.set(2L);
        value.increment();
        value.increment();
        value.decrement();
        registry.getOrCreateSnapshot(4);
        assertEquals(0L, value.get(2));
        assertEquals(1L, value.get(3));
        assertEquals(3L, value.get(4));
        registry.revertToSnapshot(3);
        assertEquals(1L, value.get());
        registry.revertToSnapshot(2);
        assertEquals(0L, value.get());
    }

    @Test
    public void testReset() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineLong value = new TimelineLong(registry);
        registry.getOrCreateSnapshot(2);
        value.set(1L);
        registry.getOrCreateSnapshot(3);
        value.set(2L);

        registry.reset();

        assertEquals(Collections.emptyList(), registry.epochsList());
        assertEquals(TimelineLong.INIT, value.get());
    }
}
