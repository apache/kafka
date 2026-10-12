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

import org.apache.kafka.coordinator.group.streams.assignor.IdenticalTagGroups.QueuedProcess;
import org.apache.kafka.coordinator.group.streams.assignor.TagTree.TagKey;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TagTreeTest {

    private final Map<String, Double> loads = new HashMap<>();
    private final Set<String> processesWithoutRoom = new HashSet<>();
    private IdenticalTagGroups<String> identicalTagGroups;
    private TagTree<String> tree;

    @Test
    public void shouldCountMissingValueAsUsed() {
        // N has no zone, so it never meets a zone condition, and its missing zone is not a value that is left.
        tree(List.of("zone"), Map.of(
            "H", Map.of("zone", "z1"),
            "N", Map.of()
        ));
        hold("H");
        assertFalse(meets("N", "zone"));
        assertFalse(tree.hasUnusedValue(key("zone")));

        // A holder without a zone uses no value of it.
        tree.clearUsedValues();
        hold("N");
        assertTrue(meets("H", "zone"));
        assertTrue(tree.hasUnusedValue(key("zone")));
    }

    @Test
    public void shouldOnlyFindTagGroupsWithRoom() {
        // X is the only process in a new zone but has no room. With no conditions, only room counts.
        processesWithoutRoom.add("X");
        tree(List.of("zone"), Map.of(
            "H", Map.of("zone", "z1"),
            "X", Map.of("zone", "z2")
        ));
        hold("H");

        assertTrue(meets("X", "zone"));
        assertFalse(hasTagGroupMeeting("zone"));
        assertNull(leastLoadedMeeting("zone"));
        assertTrue(hasTagGroupMeeting());
        assertEquals("H", leastLoadedMeeting());
    }

    @Test
    public void shouldMatchAScanOverAllProcesses() {
        // Random trees and holders, with loads that only grow and room that only shrinks between picks. Every query is
        // checked against a scan over all processes in their order.
        final Random random = new Random(42);
        for (int round = 0; round < 300; round++) {
            final List<String> tagKeys = new ArrayList<>();
            for (int keyIndex = 0, numKeys = 1 + random.nextInt(3); keyIndex < numKeys; keyIndex++) {
                tagKeys.add("key" + keyIndex);
            }
            final Map<String, Map<String, String>> clientTags = randomProcesses(random, tagKeys);
            tree(tagKeys, clientTags);
            final List<String> processes = new ArrayList<>(clientTags.keySet());

            for (int task = 0; task < 3; task++) {
                tree.clearUsedValues();
                final Set<String> holders = new HashSet<>();
                for (int holder = 0, numHolders = 1 + random.nextInt(2); holder < numHolders; holder++) {
                    final String process = processes.get(random.nextInt(processes.size()));
                    holders.add(process);
                    hold(process);
                }

                for (int pick = 0; pick < 3; pick++) {
                    assertPickMatchesScan(tagKeys, clientTags, holders, "round " + round + ", task " + task + ", pick " + pick);

                    // Before the next pick, a load grows and sometimes a process runs out of room.
                    loads.merge(processes.get(random.nextInt(processes.size())), 1.0 + random.nextInt(2), Double::sum);
                    if (random.nextInt(3) == 0) {
                        processesWithoutRoom.add(processes.get(random.nextInt(processes.size())));
                    }
                }
            }
        }
    }

    /** The client tags of up to 12 processes with random loads, each key taking one of three values or none. */
    private Map<String, Map<String, String>> randomProcesses(final Random random, final List<String> tagKeys) {
        loads.clear();
        processesWithoutRoom.clear();
        final Map<String, Map<String, String>> clientTags = new TreeMap<>();
        for (int processIndex = 0, numProcesses = 1 + random.nextInt(12); processIndex < numProcesses; processIndex++) {
            final String process = String.format("p%02d", processIndex);
            final Map<String, String> tags = new HashMap<>();
            for (final String tagKey : tagKeys) {
                final int value = random.nextInt(4);
                if (value < 3) {
                    tags.put(tagKey, tagKey + "-v" + value);
                }
            }
            clientTags.put(process, tags);
            loads.put(process, (double) random.nextInt(4));
        }
        return clientTags;
    }

    /** Checks every query of a new pick, for every set of conditions, against a scan over all processes. */
    private void assertPickMatchesScan(
        final List<String> tagKeys,
        final Map<String, Map<String, String>> clientTags,
        final Set<String> holders,
        final String context
    ) {
        final List<String> processes = new ArrayList<>(clientTags.keySet());
        tree.startPick();
        for (final String tagKey : tagKeys) {
            assertEquals(scanHasUnusedValue(clientTags, holders, tagKey), tree.hasUnusedValue(key(tagKey)), context);
        }
        for (int subset = 0; subset < 1 << tagKeys.size(); subset++) {
            final List<String> conditions = new ArrayList<>();
            for (int keyIndex = 0; keyIndex < tagKeys.size(); keyIndex++) {
                if ((subset & 1 << keyIndex) != 0) {
                    conditions.add(tagKeys.get(keyIndex));
                }
            }
            final String conditionsContext = context + ", conditions " + conditions;
            for (final String process : processes) {
                assertEquals(
                    scanMeets(clientTags, holders, process, conditions),
                    tree.tagGroupMeeting(identicalTagGroups.tagGroupOf(process), keys(conditions)),
                    conditionsContext + ", process " + process
                );
            }
            final String expected = scanLeastLoadedMeeting(clientTags, holders, processes, conditions);
            final QueuedProcess<String> actual = tree.leastLoadedProcessMeeting(keys(conditions));
            assertEquals(expected != null, tree.hasTagGroupMeeting(keys(conditions)), conditionsContext);
            assertEquals(expected, actual == null ? null : actual.process, conditionsContext);
        }
    }

    private void tree(final List<String> tagKeys, final Map<String, Map<String, String>> clientTags) {
        final Map<String, Map<String, String>> clientTagsByProcess = new TreeMap<>(clientTags);
        identicalTagGroups = new IdenticalTagGroups<>(
            tagKeys,
            clientTagsByProcess.keySet(),
            clientTagsByProcess::get,
            process -> loads.getOrDefault(process, 0.0),
            process -> !processesWithoutRoom.contains(process)
        );
        tree = new TagTree<>(tagKeys, identicalTagGroups.tagGroups());
    }

    private void hold(final String process) {
        tree.markUsed(identicalTagGroups.tagGroupOf(process));
    }

    private TagKey key(final String name) {
        for (final TagKey tagKey : tree.tagKeys()) {
            if (tagKey.toString().equals(name)) {
                return tagKey;
            }
        }
        throw new IllegalArgumentException("No tag key " + name);
    }

    private List<TagKey> keys(final List<String> names) {
        final List<TagKey> keys = new ArrayList<>();
        for (final String name : names) {
            keys.add(key(name));
        }
        return keys;
    }

    private boolean meets(final String process, final String... conditions) {
        return tree.tagGroupMeeting(identicalTagGroups.tagGroupOf(process), keys(List.of(conditions)));
    }

    /** Whether a tag group with room meets the conditions, in a new pick. */
    private boolean hasTagGroupMeeting(final String... conditions) {
        tree.startPick();
        return tree.hasTagGroupMeeting(keys(List.of(conditions)));
    }

    /** The least-loaded process with room in a tag group meeting the conditions, in a new pick. */
    private String leastLoadedMeeting(final String... conditions) {
        tree.startPick();
        final QueuedProcess<String> leastLoaded = tree.leastLoadedProcessMeeting(keys(List.of(conditions)));
        return leastLoaded == null ? null : leastLoaded.process;
    }

    private static boolean scanHasUnusedValue(
        final Map<String, Map<String, String>> clientTags,
        final Set<String> holders,
        final String tagKey
    ) {
        for (final Map<String, String> tags : clientTags.values()) {
            final String value = tags.get(tagKey);
            if (value != null && !heldValue(clientTags, holders, tagKey, value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean scanMeets(
        final Map<String, Map<String, String>> clientTags,
        final Set<String> holders,
        final String process,
        final List<String> conditions
    ) {
        for (final String tagKey : conditions) {
            final String value = clientTags.get(process).get(tagKey);
            if (value == null || heldValue(clientTags, holders, tagKey, value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean heldValue(
        final Map<String, Map<String, String>> clientTags,
        final Set<String> holders,
        final String tagKey,
        final String value
    ) {
        for (final String holder : holders) {
            if (value.equals(clientTags.get(holder).get(tagKey))) {
                return true;
            }
        }
        return false;
    }

    private String scanLeastLoadedMeeting(
        final Map<String, Map<String, String>> clientTags,
        final Set<String> holders,
        final List<String> processes,
        final List<String> conditions
    ) {
        String leastLoaded = null;
        for (final String process : processes) {
            if (!processesWithoutRoom.contains(process) && scanMeets(clientTags, holders, process, conditions)
                && (leastLoaded == null || loads.get(process) < loads.get(leastLoaded))) {
                leastLoaded = process;
            }
        }
        return leastLoaded;
    }
}
