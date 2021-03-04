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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class TimelineHashMapTest {

    @Test
    public void testEmptyMap() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map = new TimelineHashMap<>(registry, 1);
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testNullsForbidden() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<String, Boolean> map = new TimelineHashMap<>(registry, 1);
        assertThrows(NullPointerException.class, () -> map.put(null, true));
        assertThrows(NullPointerException.class, () -> map.put("abc", null));
        assertThrows(NullPointerException.class, () -> map.put(null, null));
    }

    @Test
    public void testIteration() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map = new TimelineHashMap<>(registry, 1);
        map.put(123, "abc");
        map.put(456, "def");
        assertThat(iteratorToList(map.keySet().iterator()), containsInAnyOrder(123, 456));
        assertThat(iteratorToList(map.values().iterator()), containsInAnyOrder("abc", "def"));
        assertTrue(map.containsValue("abc"));
        assertTrue(map.containsKey(456));
        assertFalse(map.isEmpty());
        registry.createSnapshot(2);
        Iterator<Map.Entry<Integer, String>> iter = map.entrySet(2).iterator();
        map.clear();
        List<String> snapshotValues = new ArrayList<>();
        snapshotValues.add(iter.next().getValue());
        snapshotValues.add(iter.next().getValue());
        assertFalse(iter.hasNext());
        assertThat(snapshotValues, containsInAnyOrder("abc", "def"));
        assertFalse(map.isEmpty(2));
        assertTrue(map.isEmpty());
    }

    static <T> List<T> iteratorToList(Iterator<T> iter) {
        List<T> list = new ArrayList<>();
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        return list;
    }

    @Test
    public void testMapMethods() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map = new TimelineHashMap<>(registry, 1);
        assertEquals(null, map.putIfAbsent(1, "xyz"));
        assertEquals("xyz", map.putIfAbsent(1, "123"));
        assertEquals("xyz", map.putIfAbsent(1, "ghi"));
        map.putAll(Collections.singletonMap(2, "b"));
        assertTrue(map.containsKey(2));
        assertEquals("xyz", map.remove(1));
        assertEquals("b", map.remove(2));
    }

    @Test
    public void testMapEquals() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map1 = new TimelineHashMap<>(registry, 1);
        assertEquals(null, map1.putIfAbsent(1, "xyz"));
        assertEquals(null, map1.putIfAbsent(2, "abc"));
        TimelineHashMap<Integer, String> map2 = new TimelineHashMap<>(registry, 1);
        assertEquals(null, map2.putIfAbsent(1, "xyz"));
        assertFalse(map1.equals(map2));
        assertEquals(null, map2.putIfAbsent(2, "abc"));
        assertEquals(map1, map2);
    }
}
