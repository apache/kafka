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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SnapshotRegistryTest {
    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testEmptyRegistry() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        Assert.assertThrows(RuntimeException.class, () -> registry.get(0));
        assertIteratorContains(registry.snapshots());
    }

    private static void assertIteratorContains(Iterator<Snapshot> iter,
                                               Snapshot... snapshots) {
        List<Snapshot> expected = new ArrayList<>();
        for (Snapshot snapshot : snapshots) {
            expected.add(snapshot);
        }
        List<Snapshot> actual = new ArrayList<>();
        while (iter.hasNext()) {
            Snapshot snapshot = iter.next();
            actual.add(snapshot);
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCreateSnapshots() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        Snapshot snapshot123 = registry.createSnapshot(123);
        Assert.assertEquals(snapshot123, registry.get(123));
        Assert.assertThrows(RuntimeException.class, () -> registry.get(456));
        assertIteratorContains(registry.snapshots(), snapshot123);
        Assert.assertEquals("Can't create a new snapshot at epoch 1 because the current " +
            "epoch is 124", Assert.assertThrows(RuntimeException.class,
                () -> registry.createSnapshot(1)).getMessage());
        Snapshot snapshot456 = registry.createSnapshot(456);
        assertIteratorContains(registry.snapshots(), snapshot123, snapshot456);
    }


    @Test
    public void testCreateAndDeleteSnapshots() {
        SnapshotRegistry registry = new SnapshotRegistry(0);
        Snapshot snapshot123 = registry.createSnapshot(123);
        Snapshot snapshot456 = registry.createSnapshot(456);
        Snapshot snapshot789 = registry.createSnapshot(789);
        registry.deleteSnapshot(snapshot456.epoch());
        assertIteratorContains(registry.snapshots(), snapshot123, snapshot789);
    }
}
