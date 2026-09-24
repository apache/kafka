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

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RackAwareStandbyPickerTest {

    private static final Comparator<String> BY_NAME = Comparator.naturalOrder();

    private final Set<String> holders = new HashSet<>();

    @Test
    public void shouldPickProcessDiverseInHighestPriorityKeyFirst() {
        // B differs from the holder only in cluster, C only in zone: cluster ranks first, so B wins.
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "A", Map.of("cluster", "c1", "zone", "z1"),
            "B", Map.of("cluster", "c2", "zone", "z1"),
            "C", Map.of("cluster", "c1", "zone", "z2")
        ));
        hold(picker, "A");

        assertEquals("B", picker.pickNext(this::isNotHolder, BY_NAME));
    }

    @Test
    public void shouldPreferProcessWithNewValuesOnMoreLowerPriorityKeys() {
        // The worked example of the design: C is new in both cluster and zone, then E is the only one left in a new
        // cluster, then nothing can add diversity.
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "A", Map.of("cluster", "c1", "zone", "z1"),
            "B", Map.of("cluster", "c2", "zone", "z1"),
            "C", Map.of("cluster", "c2", "zone", "z2"),
            "D", Map.of("cluster", "c1", "zone", "z2"),
            "E", Map.of("cluster", "c3", "zone", "z1")
        ));
        hold(picker, "A");

        assertEquals("C", pickAndHold(picker));
        assertEquals("E", pickAndHold(picker));
        assertNull(picker.pickNext(this::isNotHolder, BY_NAME));
    }

    @Test
    public void shouldGiveUpHighestPriorityKeyWhenNoProcessCanDiversifyIt() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone"), Map.of(
            "A", Map.of("cluster", "c1", "zone", "z1"),
            "B", Map.of("cluster", "c1", "zone", "z2"),
            "C", Map.of("cluster", "c1", "zone", "z1")
        ));
        hold(picker, "A");

        assertEquals("B", pickAndHold(picker));
        assertNull(picker.pickNext(this::isNotHolder, BY_NAME));
    }

    @Test
    public void shouldDropProcessWithoutKeyAndIneligibleProcess() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("zone"), Map.of(
            "A", Map.of("zone", "z1"),
            "B", Map.of(),
            "C", Map.of("zone", "z2")
        ));
        hold(picker, "A");

        assertNull(picker.pickNext(process -> isNotHolder(process) && !process.equals("C"), BY_NAME));

        picker.startTask();
        picker.markUsed("A");
        assertEquals("C", picker.pickNext(this::isNotHolder, BY_NAME));
    }

    @Test
    public void shouldBreakTiesWithTheGivenOrder() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("zone"), Map.of(
            "A", Map.of("zone", "z1"),
            "B", Map.of("zone", "z2"),
            "C", Map.of("zone", "z2")
        ));
        hold(picker, "A");

        assertEquals("C", picker.pickNext(this::isNotHolder, BY_NAME.reversed()));

        picker.startTask();
        picker.markUsed("A");
        assertEquals("B", picker.pickNext(this::isNotHolder, BY_NAME));
    }

    @Test
    public void shouldNotPlaceSecondStandbyOnValueOfFirstStandby() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("zone"), Map.of(
            "A", Map.of("zone", "z1"),
            "B", Map.of("zone", "z2"),
            "C", Map.of("zone", "z2"),
            "D", Map.of("zone", "z3")
        ));
        hold(picker, "A");

        assertEquals("B", pickAndHold(picker));
        assertEquals("D", pickAndHold(picker));
        assertNull(picker.pickNext(this::isNotHolder, BY_NAME));
    }

    @Test
    public void shouldStartNextTaskFromAllProcesses() {
        final RackAwareStandbyPicker<String> picker = picker(List.of("zone"), Map.of(
            "A", Map.of("zone", "z1"),
            "B", Map.of("zone", "z2")
        ));
        hold(picker, "A");
        assertEquals("B", pickAndHold(picker));
        assertNull(picker.pickNext(this::isNotHolder, BY_NAME));

        holders.clear();
        picker.startTask();
        hold(picker, "B");
        assertEquals("A", picker.pickNext(this::isNotHolder, BY_NAME));
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

    private String pickAndHold(final RackAwareStandbyPicker<String> picker) {
        final String winner = picker.pickNext(this::isNotHolder, BY_NAME);
        hold(picker, winner);
        return winner;
    }

    private boolean isNotHolder(final String process) {
        return !holders.contains(process);
    }
}
