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

import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class SnapshotRegistryTest {
    @Test
    public void testEmptyRegistry() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        assertThrows(RuntimeException.class, () -> registry.getSnapshot(0));
        assertIteratorContains(registry.iterator());
    }

    private static void assertIteratorContains(Iterator<Snapshot> iter,
                                               Snapshot... snapshots) {
        List<Snapshot> expected = Arrays.asList(snapshots);
        List<Snapshot> actual = new ArrayList<>();
        while (iter.hasNext()) {
            Snapshot snapshot = iter.next();
            actual.add(snapshot);
        }
        assertEquals(expected, actual);
    }

    @Test
    public void testCreateSnapshots() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        Snapshot snapshot123 = registry.getOrCreateSnapshot(123);
        assertEquals(snapshot123, registry.getSnapshot(123));
        assertThrows(RuntimeException.class, () -> registry.getSnapshot(456));
        assertIteratorContains(registry.iterator(), snapshot123);
        assertEquals("Can't create a new in-memory snapshot at epoch 1 because there is already " +
            "a snapshot with epoch 123. Snapshot epochs are 123", assertThrows(RuntimeException.class,
                () -> registry.getOrCreateSnapshot(1)).getMessage());
        Snapshot snapshot456 = registry.getOrCreateSnapshot(456);
        assertIteratorContains(registry.iterator(), snapshot123, snapshot456);
    }

    @Test
    public void testCreateAndDeleteSnapshots() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        Snapshot snapshot123 = registry.getOrCreateSnapshot(123);
        Snapshot snapshot456 = registry.getOrCreateSnapshot(456);
        Snapshot snapshot789 = registry.getOrCreateSnapshot(789);
        registry.deleteSnapshot(snapshot456.epoch());
        assertIteratorContains(registry.iterator(), snapshot123, snapshot789);
    }

    @Test
    public void testDeleteSnapshotUpTo() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        registry.getOrCreateSnapshot(10);
        registry.getOrCreateSnapshot(12);
        Snapshot snapshot14 = registry.getOrCreateSnapshot(14);
        registry.deleteSnapshotsUpTo(14);
        assertIteratorContains(registry.iterator(), snapshot14);
    }

    @Test
    public void testCreateSnapshotOfLatest() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        registry.getOrCreateSnapshot(10);
        Snapshot latest = registry.getOrCreateSnapshot(12);
        Snapshot duplicate = registry.getOrCreateSnapshot(12);

        assertEquals(latest, duplicate);
    }

    @Test
    public void testScrub() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext(), 2);
        new TimelineInteger(registry).set(123);
        new TimelineInteger(registry).set(123);
        assertEquals(0, registry.numScrubs());
        new TimelineInteger(registry).set(123);
        assertEquals(1, registry.numScrubs());
        new TimelineInteger(registry).set(123);
        new TimelineInteger(registry).set(123);
        new TimelineInteger(registry).set(123);
        assertEquals(2, registry.numScrubs());
    }

    @Test
    public void testReset() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext(), 2);
        TimelineInteger integer = new TimelineInteger(registry);
        integer.set(123);
        registry.reset();
        assertEquals(0, integer.get());
        assertEquals(1, registry.numScrubs());
    }
}
