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
        final IdenticalTagGroups<String> identicalTagGroups = identicalTagGroups(List.of("zone"), List.of("A", "B", "C", "D", "E"), Map.of(
            "A", Map.of("zone", "z1", "rack", "r1"),
            "B", Map.of("zone", "z2"),
            "C", Map.of("zone", "z1", "rack", "r2"),
            "D", Map.of(),
            "E", Map.of("rack", "r1")
        ));

        assertSame(identicalTagGroups.tagGroupOf("A"), identicalTagGroups.tagGroupOf("C"));
        assertSame(identicalTagGroups.tagGroupOf("D"), identicalTagGroups.tagGroupOf("E"));
        assertNotSame(identicalTagGroups.tagGroupOf("A"), identicalTagGroups.tagGroupOf("B"));
        assertNotSame(identicalTagGroups.tagGroupOf("A"), identicalTagGroups.tagGroupOf("D"));
        assertEquals(3, identicalTagGroups.tagGroups().size());
    }

    @Test
    public void shouldQueueProcessAgainOnceItsLoadHasGrown() {
        loads.putAll(Map.of("A", 1.0, "B", 2.0));
        final IdenticalTagGroups<String> identicalTagGroups = sameZone("A", "B");
        final List<IdenticalTagGroups.TagGroup<String>> tagGroup = List.of(identicalTagGroups.tagGroupOf("A"));
        assertEquals("A", IdenticalTagGroups.leastLoaded(tagGroup));

        loads.put("A", 3.0);

        assertEquals("B", IdenticalTagGroups.leastLoaded(tagGroup));
    }

    @Test
    public void shouldSkipProcessWithoutRoomAndHaveNoRoomOnceNoProcessHasRoom() {
        loads.putAll(Map.of("A", 1.0, "B", 2.0));
        final IdenticalTagGroups<String> identicalTagGroups = sameZone("A", "B");
        final IdenticalTagGroups.TagGroup<String> tagGroup = identicalTagGroups.tagGroupOf("A");

        withRoom.remove("A");
        assertTrue(tagGroup.hasRoom());
        assertEquals("B", IdenticalTagGroups.leastLoaded(List.of(tagGroup)));

        withRoom.remove("B");
        assertFalse(tagGroup.hasRoom());
    }

    @Test
    public void shouldPickLeastLoadedProcessAcrossTagGroups() {
        loads.putAll(Map.of("A", 2.0, "B", 1.0));
        final IdenticalTagGroups<String> identicalTagGroups = identicalTagGroups(List.of("zone"), List.of("A", "B"), Map.of(
            "A", Map.of("zone", "z1"),
            "B", Map.of("zone", "z2")
        ));

        assertEquals("B", IdenticalTagGroups.leastLoaded(identicalTagGroups.tagGroups()));
    }

    private IdenticalTagGroups<String> sameZone(final String... processes) {
        final Map<String, Map<String, String>> clientTags = new HashMap<>();
        for (final String process : processes) {
            clientTags.put(process, Map.of("zone", "z1"));
        }
        return identicalTagGroups(List.of("zone"), List.of(processes), clientTags);
    }

    private IdenticalTagGroups<String> identicalTagGroups(
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
