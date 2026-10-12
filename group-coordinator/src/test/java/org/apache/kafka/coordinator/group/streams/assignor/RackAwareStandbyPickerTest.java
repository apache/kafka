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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RackAwareStandbyPickerTest {

    private final Map<String, Double> loads = new HashMap<>();
    private final Set<String> processesWithoutRoom = new HashSet<>();
    private Set<String> processes;
    private IdenticalTagGroups<String> identicalTagGroups;
    private TagTree<String> tagTree;
    private RackAwareStandbyPicker<String> picker;
    // The conditions of the last pick by candidates().
    private List<TagTree.TagKey> conditions;

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

        // The previous task gave up cluster and left C as the only candidate. The picker of this one starts from cluster
        // again, over every process and with no value used.
        picker = new RackAwareStandbyPicker<>(tagTree, List.of(identicalTagGroups.tagGroupOf("C")));
        assertEquals(Set.of("B", "D"), candidates());
        assertEquals(List.of("cluster", "zone"), conditionNames());
    }

    private void picker(final List<String> tagKeys, final Map<String, Map<String, String>> clientTags) {
        final Map<String, Map<String, String>> clientTagsByProcess = new TreeMap<>(clientTags);
        processes = clientTagsByProcess.keySet();
        identicalTagGroups = new IdenticalTagGroups<>(
            tagKeys,
            processes,
            clientTagsByProcess::get,
            process -> loads.getOrDefault(process, 0.0),
            process -> !processesWithoutRoom.contains(process)
        );
        tagTree = new TagTree<>(tagKeys, identicalTagGroups.tagGroups());
        picker = new RackAwareStandbyPicker<>(tagTree, List.of());
    }

    private void hold(final String process) {
        picker.markUsed(identicalTagGroups.tagGroupOf(process));
    }

    /** The processes of the candidate tag groups of a new pick, empty when nothing is picked. */
    private Set<String> candidates() {
        conditions = picker.pick();
        final Set<String> candidates = new HashSet<>();
        if (!conditions.isEmpty()) {
            for (final String process : processes) {
                if (picker.isCandidate(identicalTagGroups.tagGroupOf(process), conditions)) {
                    candidates.add(process);
                }
            }
        }
        return candidates;
    }

    /** The names of the tag keys in the conditions of the last pick by candidates(). */
    private List<String> conditionNames() {
        final List<String> names = new ArrayList<>();
        for (final TagTree.TagKey tagKey : conditions) {
            names.add(tagKey.toString());
        }
        return names;
    }
}
