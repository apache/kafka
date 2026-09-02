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
package org.apache.kafka.coordinator.group.util;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OverlayMapTest {

    // Test simple additions, replacements and removals. This is enough to populate the three
    // internal fields of OverlayMap for use in the rest of the tests.

    @Test
    public void testPutNewKey() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "existing", "v1"
        ));

        assertNull(map.put("new", "x"));

        assertEquals("x", map.get("new"));
    }

    @Test
    public void testPutExistingKey() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "existing", "v1"
        ));

        assertEquals("v1", map.put("existing", "v2"));

        assertEquals("v2", map.get("existing"));
    }

    @Test
    public void testRemoveExistingKey() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "existing", "v1"
        ));

        assertEquals("v1", map.remove("existing"));

        assertNull(map.get("existing"));
    }

    // Now test everything else.

    @Test
    public void testSize() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        assertEquals(3, map.size());

        // Additions increase the size.
        map.put("addition", "addition-value");
        assertEquals(4, map.size());

        // Replacements do not change the size.
        map.put("replacement", "replacement-value-2");
        assertEquals(4, map.size());

        // Removals decrease the size.
        map.remove("removal");
        assertEquals(3, map.size());
    }

    @Test
    public void testIsEmptyWithEmptyBase() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of());
        assertTrue(map.isEmpty());

        // Additions make it non-empty.
        map.put("k", "v");
        assertFalse(map.isEmpty());
    }

    @Test
    public void testIsEmptyWhenAllBaseEntriesRemoved() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "k1", "v1",
            "k2", "v2"
        ));

        map.remove("k1");
        assertFalse(map.isEmpty());

        map.remove("k2");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testContainsKey() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        map.put("addition", "addition-value");
        map.put("replacement", "replacement-value-2");
        map.remove("removal");

        assertTrue(map.containsKey("addition"));
        assertTrue(map.containsKey("replacement"));
        assertFalse(map.containsKey("removal"));
        assertTrue(map.containsKey("base"));
        assertFalse(map.containsKey("unknown"));
    }

    @Test
    public void testGet() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        map.put("addition", "addition-value");
        map.put("replacement", "replacement-value-2");
        map.remove("removal");

        assertEquals("addition-value", map.get("addition"));
        assertEquals("replacement-value-2", map.get("replacement"));
        assertNull(map.get("removal"));
        assertEquals("base-value", map.get("base"));
        assertNull(map.get("unknown"));
    }

    @Test
    public void testPut() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        map.put("addition", "addition-value");
        map.put("replacement", "replacement-value-2");
        map.remove("removal");

        assertEquals(3, map.size());

        // Replace a new value.
        assertEquals("addition-value", map.put("addition", "addition-value-2"));
        assertEquals(3, map.size());
        assertTrue(map.containsKey("addition"));
        assertEquals("addition-value-2", map.get("addition"));
        assertEquals(Map.of(
            "addition", "addition-value-2",
            "replacement", "replacement-value-2",
            "base", "base-value"
        ), map);

        // Replace an already-replaced value.
        assertEquals("replacement-value-2", map.put("replacement", "replacement-value-3"));
        assertEquals(3, map.size());
        assertTrue(map.containsKey("replacement"));
        assertEquals("replacement-value-3", map.get("replacement"));
        assertEquals(Map.of(
            "addition", "addition-value-2",
            "replacement", "replacement-value-3",
            "base", "base-value"
        ), map);

        // Replace a removed value.
        assertNull(map.put("removal", "removal-value-2"));
        assertEquals(4, map.size());
        assertTrue(map.containsKey("removal"));
        assertEquals("removal-value-2", map.get("removal"));
        assertEquals(Map.of(
            "addition", "addition-value-2",
            "replacement", "replacement-value-3",
            "removal", "removal-value-2",
            "base", "base-value"
        ), map);

        // Put over a base value and put over an unknown value are already test separately in
        // testPutExistingKey and testPutNewKey.
    }

    @Test
    public void testRemove() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        map.put("addition", "addition-value");
        map.put("replacement", "replacement-value-2");
        map.remove("removal");

        assertEquals(3, map.size());

        // Remove a new value.
        assertEquals("addition-value", map.remove("addition"));
        assertEquals(2, map.size());
        assertFalse(map.containsKey("addition"));
        assertNull(map.get("addition"));
        assertEquals(Map.of(
            "replacement", "replacement-value-2",
            "base", "base-value"
        ), map);

        // Remove a replaced value.
        assertEquals("replacement-value-2", map.remove("replacement"));
        assertEquals(1, map.size());
        assertFalse(map.containsKey("replacement"));
        assertNull(map.get("replacement"));
        assertEquals(Map.of(
            "base", "base-value"
        ), map);

        // Remove a removed value.
        assertNull(map.remove("removal"));
        assertEquals(1, map.size());
        assertFalse(map.containsKey("removal"));
        assertNull(map.get("removal"));
        assertEquals(Map.of(
            "base", "base-value"
        ), map);

        // Remove an unknown value.
        assertNull(map.remove("unknown"));
        assertEquals(1, map.size());
        assertFalse(map.containsKey("unknown"));
        assertNull(map.get("unknown"));
        assertEquals(Map.of(
            "base", "base-value"
        ), map);

        // Removing a base value is already tested separately in testRemoveExistingKey.
    }

    @Test
    public void testClear() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        map.put("addition", "addition-value");
        map.put("replacement", "replacement-value-2");
        map.remove("removal");
        map.clear();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertFalse(map.containsKey("addition"));
        assertFalse(map.containsKey("replacement"));
        assertFalse(map.containsKey("base"));
        assertNull(map.get("addition"));
        assertNull(map.get("base"));
    }

    @Test
    public void testEntrySet() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        map.put("addition", "addition-value");
        map.put("replacement", "replacement-value-2");
        map.remove("removal");

        assertEquals(
            Map.of(
                "addition", "addition-value",
                "replacement", "replacement-value-2",
                "base", "base-value"
            ),
            new HashMap<>(map)
        );
    }

    @Test
    public void testEntrySetIterator() {
        Map<String, String> base = new LinkedHashMap<>();
        base.put("base", "base-value");
        base.put("replacement", "replacement-value");
        base.put("removal", "removal-value");

        OverlayMap<String, String> map = new OverlayMap<>(base);
        map.put("addition", "addition-value");
        map.put("replacement", "replacement-value-2");
        map.remove("removal");

        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();

        assertTrue(iterator.hasNext());
        assertEquals(Map.entry("base", "base-value"), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Map.entry("replacement", "replacement-value-2"), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Map.entry("addition", "addition-value"), iterator.next());

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testEntrySetIteratorOverEmptyMap() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of());

        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testEntrySetSize() {
        OverlayMap<String, String> map = new OverlayMap<>(Map.of(
            "base", "base-value",
            "replacement", "replacement-value",
            "removal", "removal-value"
        ));
        assertEquals(3, map.entrySet().size());

        // Additions increase the size.
        map.put("addition", "addition-value");
        assertEquals(4, map.entrySet().size());

        // Replacements do not change the size.
        map.put("replacement", "replacement-value-2");
        assertEquals(4, map.entrySet().size());

        // Removals decrease the size.
        map.remove("removal");
        assertEquals(3, map.entrySet().size());
    }
}
