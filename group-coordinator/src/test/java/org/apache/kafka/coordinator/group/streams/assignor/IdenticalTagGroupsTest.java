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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IdenticalTagGroupsTest {

    private final Map<String, Double> loads = new HashMap<>();
    private final Set<String> withRoom = new HashSet<>();

    @Test
    public void shouldGroupProcessesByTheirValuesForTheTagKeysOnly() {
        // A and C share the zone and differ only in rack, which is not a tag key; D and E both have no zone.
        final IdenticalTagGroups<String> tagGroups = tagGroups(List.of("zone"), List.of("A", "B", "C", "D", "E"), Map.of(
            "A", Map.of("zone", "z1", "rack", "r1"),
            "B", Map.of("zone", "z2"),
            "C", Map.of("zone", "z1", "rack", "r2"),
            "D", Map.of(),
            "E", Map.of("rack", "r1")
        ));

        assertSame(tagGroups.groupOf("A"), tagGroups.groupOf("C"));
        assertSame(tagGroups.groupOf("D"), tagGroups.groupOf("E"));
        assertNotSame(tagGroups.groupOf("A"), tagGroups.groupOf("B"));
        assertNotSame(tagGroups.groupOf("A"), tagGroups.groupOf("D"));
        assertEquals(List.of(tagGroups.groupOf("A"), tagGroups.groupOf("B"), tagGroups.groupOf("D")), List.copyOf(tagGroups.groups()));
    }

    @Test
    public void shouldPickLeastLoadedProcessOfGroupAndBreakTiesInProcessOrder() {
        loads.putAll(Map.of("A", 2.0, "B", 1.0, "C", 1.0));
        final IdenticalTagGroups<String> tagGroups = sameZone("A", "B", "C");

        assertEquals("B", IdenticalTagGroups.leastLoaded(List.of(tagGroups.groupOf("A"))));
    }

    @Test
    public void shouldQueueProcessAgainOnceItsLoadHasGrown() {
        loads.putAll(Map.of("A", 1.0, "B", 2.0));
        final IdenticalTagGroups<String> tagGroups = sameZone("A", "B");
        final List<IdenticalTagGroups.Group<String>> group = List.of(tagGroups.groupOf("A"));
        assertEquals("A", IdenticalTagGroups.leastLoaded(group));

        loads.put("A", 3.0);

        assertEquals("B", IdenticalTagGroups.leastLoaded(group));
    }

    @Test
    public void shouldSkipProcessWithoutRoomAndHaveNoRoomOnceNoProcessHasRoom() {
        loads.putAll(Map.of("A", 1.0, "B", 2.0));
        final IdenticalTagGroups<String> tagGroups = sameZone("A", "B");
        final IdenticalTagGroups.Group<String> group = tagGroups.groupOf("A");

        withRoom.remove("A");
        assertTrue(group.hasRoom());
        assertEquals("B", IdenticalTagGroups.leastLoaded(List.of(group)));

        withRoom.remove("B");
        assertFalse(group.hasRoom());
    }

    @Test
    public void shouldPickLeastLoadedProcessAcrossGroupsAndBreakTiesInProcessOrder() {
        // B and C have the same load in different zones, and B comes first.
        loads.putAll(Map.of("A", 2.0, "B", 1.0, "C", 1.0));
        final IdenticalTagGroups<String> tagGroups = tagGroups(List.of("zone"), List.of("A", "B", "C"), Map.of(
            "A", Map.of("zone", "z1"),
            "B", Map.of("zone", "z2"),
            "C", Map.of("zone", "z1")
        ));
        assertEquals("B", IdenticalTagGroups.leastLoaded(tagGroups.groups()));

        loads.put("B", 3.0);

        assertEquals("C", IdenticalTagGroups.leastLoaded(tagGroups.groups()));
    }

    private IdenticalTagGroups<String> sameZone(final String... processes) {
        final Map<String, Map<String, String>> clientTags = new HashMap<>();
        for (final String process : processes) {
            clientTags.put(process, Map.of("zone", "z1"));
        }
        return tagGroups(List.of("zone"), List.of(processes), clientTags);
    }

    private IdenticalTagGroups<String> tagGroups(
        final List<String> tagKeys,
        final List<String> processes,
        final Map<String, Map<String, String>> clientTags
    ) {
        for (final String process : processes) {
            loads.putIfAbsent(process, 0.0);
        }
        withRoom.addAll(processes);
        return new IdenticalTagGroups<>(tagKeys, processes, clientTags::get, loads::get, withRoom::contains);
    }
}
