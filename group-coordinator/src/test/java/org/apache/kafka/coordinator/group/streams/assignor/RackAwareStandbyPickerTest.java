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

import org.apache.kafka.coordinator.group.streams.assignor.RackAwareStandbyPicker.QueuedProcess;
import org.apache.kafka.coordinator.group.streams.assignor.RackAwareStandbyPicker.TagKey;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RackAwareStandbyPickerTest {

    private final Map<String, Double> loads = new HashMap<>();
    private final Set<String> processesWithoutRoom = new HashSet<>();
    private List<String> processes;
    private RackAwareStandbyPicker<String> picker;
    // The conditions of the last pick by candidates().
    private List<TagKey> conditions;

    @Test
    public void shouldOnlyPickEligibleProcessesWithNewValueForPriorityKey() {
        // W is new in zone but not in cluster, N has no cluster, and I is new in both but has no room, so zone is given
        // up.
        processesWithoutRoom.add("I");
        picker(List.of("cluster", "zone"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1"),
            "I", Map.of("cluster", "c2", "zone", "z2"),
            "N", Map.of("zone", "z2"),
            "W", Map.of("cluster", "c1", "zone", "z2"),
            "X", Map.of("cluster", "c2", "zone", "z1")
        ));
        hold("H");

        assertEquals(Set.of("X"), candidates());
        assertEquals(List.of("cluster"), conditionNames());
    }

    @Test
    public void shouldRankProcessesByNewValuesForLowerPriorityKeys() {
        // W, X and Z are all in a new cluster. A new zone outranks a new rack, so W drops although it comes first, and
        // X and Z tie: the lighter Z is the least loaded.
        loads.put("X", 1.0);
        picker(List.of("cluster", "zone", "rack"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1", "rack", "r1"),
            "W", Map.of("cluster", "c2", "zone", "z1", "rack", "r2"),
            "X", Map.of("cluster", "c2", "zone", "z2", "rack", "r1"),
            "Z", Map.of("cluster", "c3", "zone", "z2", "rack", "r1")
        ));
        hold("H");

        assertEquals(Set.of("X", "Z"), candidates());
        assertEquals(List.of("cluster", "zone"), conditionNames());
        assertEquals("Z", picker.leastLoaded(conditions));
    }

    @Test
    public void shouldKeepLaterTagKeyAfterGivingUpAnEarlierOne() {
        // No process is new in both cluster and zone, so zone is given up, but rack still narrows the processes in a
        // new cluster down to X.
        picker(List.of("cluster", "zone", "rack"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1", "rack", "r1"),
            "X", Map.of("cluster", "c2", "zone", "z1", "rack", "r2"),
            "Y", Map.of("cluster", "c1", "zone", "z2", "rack", "r2"),
            "Z", Map.of("cluster", "c2", "zone", "z1", "rack", "r1")
        ));
        hold("H");

        assertEquals(Set.of("X"), candidates());
        assertEquals(List.of("cluster", "rack"), conditionNames());
    }

    @Test
    public void shouldGiveUpPriorityKeyOnceEveryValueIsUsed() {
        picker(List.of("cluster", "zone"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1"),
            "N", Map.of("zone", "z2"),
            "W", Map.of("cluster", "c1", "zone", "z3"),
            "X", Map.of("cluster", "c2", "zone", "z1")
        ));
        hold("H");
        assertEquals(Set.of("X"), candidates());
        assertEquals(List.of("cluster"), conditionNames());

        // Every cluster is used: zone becomes the priority and N and W are back. There is no lower key to order
        // them by.
        hold("X");
        assertEquals(Set.of("N", "W"), candidates());
        assertEquals(List.of("zone"), conditionNames());

        // N has no cluster to record; its zone is used now.
        hold("N");
        assertEquals(Set.of("W"), candidates());
        assertEquals(List.of("zone"), conditionNames());

        // Every zone is used too, so no process makes the task more diverse.
        hold("W");
        assertEquals(Set.of(), candidates());
        assertEquals(List.of(), conditionNames());
    }

    @Test
    public void shouldStartOverForNextTask() {
        picker(List.of("cluster", "zone"), Map.of(
            "A", Map.of("cluster", "c1", "zone", "z1"),
            "B", Map.of("cluster", "c2", "zone", "z2"),
            "C", Map.of("cluster", "c1", "zone", "z3"),
            "D", Map.of("cluster", "c2", "zone", "z1")
        ));
        hold("A");
        assertEquals(Set.of("B"), candidates());
        assertEquals(List.of("cluster", "zone"), conditionNames());
        hold("B");
        assertEquals(Set.of("C"), candidates());
        assertEquals(List.of("zone"), conditionNames());

        // The previous task gave up cluster and left C as the only candidate. The picks of this one start from cluster
        // again, over every process and with no value used.
        picker.startTask(List.of("C"));
        assertEquals(Set.of("B", "D"), candidates());
        assertEquals(List.of("cluster", "zone"), conditionNames());
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
            picker(tagKeys, clientTags);

            for (int task = 0; task < 3; task++) {
                final Set<String> holders = new HashSet<>();
                for (int holder = 0, numHolders = 1 + random.nextInt(2); holder < numHolders; holder++) {
                    holders.add(processes.get(random.nextInt(processes.size())));
                }
                picker.startTask(holders);

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

    private void picker(final List<String> tagKeys, final Map<String, Map<String, String>> clientTags) {
        final Map<String, Map<String, String>> clientTagsByProcess = new TreeMap<>(clientTags);
        processes = new ArrayList<>(clientTagsByProcess.keySet());
        picker = new RackAwareStandbyPicker<>(
            tagKeys,
            processes,
            clientTagsByProcess::get,
            process -> loads.getOrDefault(process, 0.0),
            process -> !processesWithoutRoom.contains(process)
        );
    }

    private void hold(final String process) {
        picker.markUsed(process);
    }

    /** The candidate processes of a new pick, empty when nothing is picked. */
    private Set<String> candidates() {
        conditions = picker.pick();
        final Set<String> candidates = new HashSet<>();
        if (!conditions.isEmpty()) {
            for (final String process : processes) {
                if (picker.isCandidate(process, conditions)) {
                    candidates.add(process);
                }
            }
        }
        return candidates;
    }

    /** The names of the tag keys in the conditions of the last pick by candidates(). */
    private List<String> conditionNames() {
        final List<String> names = new ArrayList<>();
        for (final TagKey tagKey : conditions) {
            names.add(tagKey.toString());
        }
        return names;
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
        picker.startPick();
        for (final String tagKey : tagKeys) {
            assertEquals(scanHasUnusedValue(clientTags, holders, tagKey), picker.hasUnusedValue(key(tagKey)), context);
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
                    picker.meets(process, keys(conditions)),
                    conditionsContext + ", process " + process
                );
            }
            final String expected = scanLeastLoadedMeeting(clientTags, holders, conditions);
            final QueuedProcess<String> found = picker.processMeeting(keys(conditions));
            assertEquals(expected != null, found != null, conditionsContext);
            if (found != null) {
                assertTrue(
                    !processesWithoutRoom.contains(found.process) && scanMeets(clientTags, holders, found.process, conditions),
                    conditionsContext
                );
                assertEquals(expected, picker.leastLoadedProcessMeeting(keys(conditions), found).process, conditionsContext);
            }
        }
    }

    private TagKey key(final String name) {
        for (final TagKey tagKey : picker.tagKeys()) {
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
