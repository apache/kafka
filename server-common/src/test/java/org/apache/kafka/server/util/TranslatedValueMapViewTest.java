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

package org.apache.kafka.server.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 60)
public class TranslatedValueMapViewTest {
    private static Map<String, Integer> createTestMap() {
        Map<String, Integer> testMap = new TreeMap<>();
        testMap.put("foo", 2);
        testMap.put("bar", 3);
        testMap.put("baz", 5);
        return testMap;
    }

    @Test
    public void testContains() {
        Map<String, Integer> underlying = createTestMap();
        TranslatedValueMapView<String, String, Integer> view =
            new TranslatedValueMapView<>(underlying, v -> v.toString());
        assertTrue(view.containsKey("foo"));
        assertTrue(view.containsKey("bar"));
        assertTrue(view.containsKey("baz"));
        assertFalse(view.containsKey("quux"));
        underlying.put("quux", 101);
        assertTrue(view.containsKey("quux"));
    }

    @Test
    public void testIsEmptyAndSize() {
        Map<String, Integer> underlying = new HashMap<>();
        TranslatedValueMapView<String, String, Integer> view =
            new TranslatedValueMapView<>(underlying, v -> v.toString());
        assertTrue(view.isEmpty());
        assertEquals(0, view.size());
        underlying.put("quux", 101);
        assertFalse(view.isEmpty());
        assertEquals(1, view.size());
    }

    @Test
    public void testGet() {
        Map<String, Integer> underlying = createTestMap();
        TranslatedValueMapView<String, String, Integer> view =
            new TranslatedValueMapView<>(underlying, v -> v.toString());
        assertEquals("2", view.get("foo"));
        assertEquals("3", view.get("bar"));
        assertEquals("5", view.get("baz"));
        assertNull(view.get("quux"));
        underlying.put("quux", 101);
        assertEquals("101", view.get("quux"));
    }

    @Test
    public void testEntrySet() {
        Map<String, Integer> underlying = createTestMap();
        TranslatedValueMapView<String, String, Integer> view =
            new TranslatedValueMapView<>(underlying, v -> v.toString());
        assertEquals(3, view.entrySet().size());
        assertFalse(view.entrySet().isEmpty());
        assertTrue(view.entrySet().contains(new SimpleImmutableEntry<>("foo", "2")));
        assertFalse(view.entrySet().contains(new SimpleImmutableEntry<>("bar", "4")));
    }

    @Test
    public void testEntrySetIterator() {
        Map<String, Integer> underlying = createTestMap();
        TranslatedValueMapView<String, String, Integer> view =
            new TranslatedValueMapView<>(underlying, v -> v.toString());
        Iterator<Entry<String, String>> iterator = view.entrySet().iterator();
        assertTrue(iterator.hasNext());
        assertEquals(new SimpleImmutableEntry<>("bar", "3"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(new SimpleImmutableEntry<>("baz", "5"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(new SimpleImmutableEntry<>("foo", "2"), iterator.next());
        assertFalse(iterator.hasNext());
    }
}
