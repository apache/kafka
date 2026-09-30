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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RackAwareStandbyPickerTest {

    private final Set<String> holders = new HashSet<>();

    @Test
    public void shouldOnlyPickEligibleProcessesWithNewValueForPriorityKey() {
        // W is new in zone but not in cluster, N has no cluster, and I is new in both but not eligible.
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1"),
            "I", Map.of("cluster", "c2", "zone", "z2"),
            "N", Map.of("zone", "z2"),
            "W", Map.of("cluster", "c1", "zone", "z2"),
            "X", Map.of("cluster", "c2", "zone", "z1")
        ));
        hold(picker, "H");

        assertEquals(Set.of("X"), picker.pickCandidates(process -> isNotHolder(process) && !process.equals("I")));
    }

    @Test
    public void shouldRankProcessesByNewValuesForLowerPriorityKeys() {
        // X, Y and Z are all in a new cluster. A new zone outranks a new rack, so Y drops and X and Z tie.
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone", "rack"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1", "rack", "r1"),
            "X", Map.of("cluster", "c2", "zone", "z2", "rack", "r1"),
            "Y", Map.of("cluster", "c2", "zone", "z1", "rack", "r2"),
            "Z", Map.of("cluster", "c3", "zone", "z2", "rack", "r1")
        ));
        hold(picker, "H");

        assertEquals(Set.of("X", "Z"), picker.pickCandidates(this::isNotHolder));
    }

    @Test
    public void shouldRankMissingLowerPriorityKeyLikeUsedValue() {
        // M has no zone and U is in the used zone: neither adds a new zone, so they tie.
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1"),
            "M", Map.of("cluster", "c2"),
            "U", Map.of("cluster", "c2", "zone", "z1")
        ));
        hold(picker, "H");

        assertEquals(Set.of("M", "U"), picker.pickCandidates(this::isNotHolder));
    }

    @Test
    public void shouldExcludeValuesOfEachNewHolder() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster"), Map.of(
            "H", Map.of("cluster", "c1"),
            "X", Map.of("cluster", "c2"),
            "Y", Map.of("cluster", "c2"),
            "Z", Map.of("cluster", "c3")
        ));
        hold(picker, "H");
        assertEquals(Set.of("X", "Y", "Z"), picker.pickCandidates(this::isNotHolder));

        // X holds cluster c2 now, so Y is out.
        hold(picker, "X");
        assertEquals(Set.of("Z"), picker.pickCandidates(this::isNotHolder));
    }

    @Test
    public void shouldGiveUpPriorityKeyOnceEveryValueIsUsed() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1"),
            "N", Map.of("zone", "z2"),
            "W", Map.of("cluster", "c1", "zone", "z3"),
            "X", Map.of("cluster", "c2", "zone", "z1")
        ));
        hold(picker, "H");
        assertEquals(Set.of("X"), picker.pickCandidates(this::isNotHolder));

        // Every cluster is used: zone becomes the priority and N and W are back. There is no lower key to order
        // them by.
        hold(picker, "X");
        assertEquals(Set.of("N", "W"), picker.pickCandidates(this::isNotHolder));

        // N has no cluster to record; its zone is used now.
        hold(picker, "N");
        assertEquals(Set.of("W"), picker.pickCandidates(this::isNotHolder));
    }

    @Test
    public void shouldGiveUpPriorityKeyWhoseOnlyNewValueIsOnIneligibleProcess() {
        // Only I is in a new cluster, and it is not eligible, so zone becomes the priority.
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1"),
            "I", Map.of("cluster", "c2", "zone", "z1"),
            "W", Map.of("cluster", "c1", "zone", "z2")
        ));
        hold(picker, "H");

        assertEquals(Set.of("W"), picker.pickCandidates(process -> isNotHolder(process) && !process.equals("I")));
    }

    @Test
    public void shouldPickNothingOnceNoProcessAddsDiversity() {
        // U carries the holder's zone, so it never comes up.
        final RackAwareStandbyPicker<String> picker = picker(List.of("zone"), Map.of(
            "H", Map.of("zone", "z1"),
            "U", Map.of("zone", "z1"),
            "W", Map.of("zone", "z2")
        ));
        hold(picker, "H");
        assertEquals(Set.of("W"), picker.pickCandidates(this::isNotHolder));

        hold(picker, "W");
        assertEquals(Set.of(), picker.pickCandidates(this::isNotHolder));
        assertEquals(Set.of(), picker.pickCandidates(this::isNotHolder));
    }

    @Test
    public void shouldStartOverForNextTask() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "A", Map.of("cluster", "c1", "zone", "z1"),
            "B", Map.of("cluster", "c2", "zone", "z2"),
            "C", Map.of("cluster", "c1", "zone", "z3"),
            "D", Map.of("cluster", "c2", "zone", "z1")
        ));
        hold(picker, "A");
        assertEquals(Set.of("B"), picker.pickCandidates(this::isNotHolder));
        hold(picker, "B");
        assertEquals(Set.of("C"), picker.pickCandidates(this::isNotHolder));

        // The previous task gave up cluster and left C as the only candidate. This one starts from cluster again,
        // over every process and with no value used.
        holders.clear();
        picker.startTask();
        hold(picker, "C");
        assertEquals(Set.of("B", "D"), picker.pickCandidates(this::isNotHolder));
    }

    @Test
    public void shouldReturnCandidatesInGroupOrder() {
        // Z is only new in cluster and drops once Y, new in zone too, comes up; X ties with Y and follows it.
        final Map<String, Map<String, String>> clientTags = Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1"),
            "Z", Map.of("cluster", "c2", "zone", "z1"),
            "Y", Map.of("cluster", "c3", "zone", "z2"),
            "X", Map.of("cluster", "c4", "zone", "z3")
        );
        final RackAwareStandbyPicker<String> picker =
            new RackAwareStandbyPicker<>(List.of("cluster", "zone"), List.of("H", "Z", "Y", "X"), clientTags::get);
        picker.startTask();
        hold(picker, "H");

        assertEquals(List.of("Y", "X"), List.copyOf(picker.pickCandidates(this::isNotHolder)));
    }

    private static RackAwareStandbyPicker<String> picker(final List<String> tagKeys, final Map<String, Map<String, String>> clientTags) {
        final Map<String, Map<String, String>> processes = new TreeMap<>(clientTags);
        final RackAwareStandbyPicker<String> picker = new RackAwareStandbyPicker<>(tagKeys, processes.keySet(), processes::get);
        picker.startTask();
        return picker;
    }

    private void hold(final RackAwareStandbyPicker<String> picker, final String process) {
        holders.add(process);
        picker.markUsed(process);
    }

    private boolean isNotHolder(final String process) {
        return !holders.contains(process);
    }
}
