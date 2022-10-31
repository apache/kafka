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

import java.util.Arrays;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class TimelineHashSetTest {

    @Test
    public void testEmptySet() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashSet<String> set = new TimelineHashSet<>(registry, 1);
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());
        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testNullsForbidden() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashSet<String> set = new TimelineHashSet<>(registry, 1);
        assertThrows(NullPointerException.class, () -> set.add(null));
    }

    @Test
    public void testIteration() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashSet<String> set = new TimelineHashSet<>(registry, 1);
        set.add("a");
        set.add("b");
        set.add("c");
        set.add("d");
        assertTrue(set.retainAll(Arrays.asList("a", "b", "c")));
        assertFalse(set.retainAll(Arrays.asList("a", "b", "c")));
        assertFalse(set.removeAll(Arrays.asList("d")));
        registry.getOrCreateSnapshot(2);
        assertTrue(set.removeAll(Arrays.asList("c")));
        assertThat(TimelineHashMapTest.iteratorToList(set.iterator(2)),
            containsInAnyOrder("a", "b", "c"));
        assertThat(TimelineHashMapTest.iteratorToList(set.iterator()),
            containsInAnyOrder("a", "b"));
        assertEquals(2, set.size());
        assertEquals(3, set.size(2));
        set.clear();
        assertTrue(set.isEmpty());
        assertFalse(set.isEmpty(2));
    }

    @Test
    public void testToArray() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashSet<String> set = new TimelineHashSet<>(registry, 1);
        set.add("z");
        assertArrayEquals(new String[] {"z"}, set.toArray());
        assertArrayEquals(new String[] {"z", null}, set.toArray(new String[2]));
        assertArrayEquals(new String[] {"z"}, set.toArray(new String[0]));
    }

    @Test
    public void testSetMethods() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashSet<String> set = new TimelineHashSet<>(registry, 1);
        assertTrue(set.add("xyz"));
        assertFalse(set.add("xyz"));
        assertTrue(set.remove("xyz"));
        assertFalse(set.remove("xyz"));
        assertTrue(set.addAll(Arrays.asList("abc", "def", "ghi")));
        assertFalse(set.addAll(Arrays.asList("abc", "def", "ghi")));
        assertTrue(set.addAll(Arrays.asList("abc", "def", "ghi", "jkl")));
        assertTrue(set.containsAll(Arrays.asList("def", "jkl")));
        assertFalse(set.containsAll(Arrays.asList("abc", "def", "xyz")));
        assertTrue(set.removeAll(Arrays.asList("def", "ghi", "xyz")));
        registry.getOrCreateSnapshot(5);
        assertThat(TimelineHashMapTest.iteratorToList(set.iterator(5)),
            containsInAnyOrder("abc", "jkl"));
        assertThat(TimelineHashMapTest.iteratorToList(set.iterator()),
            containsInAnyOrder("abc", "jkl"));
        set.removeIf(e -> e.startsWith("a"));
        assertThat(TimelineHashMapTest.iteratorToList(set.iterator()),
            containsInAnyOrder("jkl"));
        assertThat(TimelineHashMapTest.iteratorToList(set.iterator(5)),
            containsInAnyOrder("abc", "jkl"));
    }
}
