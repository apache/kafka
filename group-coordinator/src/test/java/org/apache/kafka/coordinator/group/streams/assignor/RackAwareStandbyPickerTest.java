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
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RackAwareStandbyPickerTest {

    private final Set<String> holders = new HashSet<>();

    @Test
    public void shouldPickStandbysUntilNoProcessAddsDiversity() {
        // H holds the active. I is the most diverse process but never eligible. N has no cluster tag. U carries the
        // holder's tags exactly. The other processes are new in different subsets of the keys.
        final RackAwareStandbyPicker<String> picker = picker(List.of("cluster", "zone", "rack"), Map.of(
            "H", Map.of("cluster", "c1", "zone", "z1", "rack", "r1"),
            "I", Map.of("cluster", "c2", "zone", "z2", "rack", "r2"),
            "N", Map.of("zone", "z9", "rack", "r9"),
            "U", Map.of("cluster", "c1", "zone", "z1", "rack", "r1"),
            "V", Map.of("cluster", "c1", "zone", "z1", "rack", "r4"),
            "W", Map.of("cluster", "c1", "zone", "z3", "rack", "r3"),
            "X", Map.of("cluster", "c2", "zone", "z2", "rack", "r1"),
            "Y", Map.of("cluster", "c2", "zone", "z1", "rack", "r2"),
            "Z", Map.of("cluster", "c3", "zone", "z2", "rack", "r1")
        ));
        final Predicate<String> eligible = process -> isNotHolder(process) && !process.equals("I");
        hold(picker, "H");

        // Cluster is the priority: W is new in zone and rack but not in cluster, N has no cluster, I is not eligible.
        // Among X, Y and Z, a new zone outranks a new rack, so Y drops and X and Z are equally diverse.
        assertEquals(Set.of("X", "Z"), picker.pickCandidates(eligible));

        // X holds cluster c2 now, so Y is out and Z is the only process in a new cluster.
        hold(picker, "X");
        assertEquals(Set.of("Z"), picker.pickCandidates(eligible));

        // Every cluster is used: the key is given up and zone becomes the priority. N is back, both N and W are new
        // in rack too.
        hold(picker, "Z");
        assertEquals(Set.of("N", "W"), picker.pickCandidates(eligible));

        // N has no cluster tag to record; its zone and rack are used now.
        hold(picker, "N");
        assertEquals(Set.of("W"), picker.pickCandidates(eligible));

        // Every zone is used: rack becomes the priority, and there is no lower key to order V and Y by.
        hold(picker, "W");
        assertEquals(Set.of("V", "Y"), picker.pickCandidates(eligible));

        hold(picker, "Y");
        assertEquals(Set.of("V"), picker.pickCandidates(eligible));

        // Every rack is used too, so no process can add diversity: U never came up, and later calls stay empty.
        hold(picker, "V");
        assertEquals(Set.of(), picker.pickCandidates(eligible));
        assertEquals(Set.of(), picker.pickCandidates(eligible));

        // The next task starts over from every process and key. U holds the same tags as H did, and with I eligible
        // now it is the single most diverse process.
        holders.clear();
        picker.startTask();
        hold(picker, "U");
        assertEquals(Set.of("I"), picker.pickCandidates(this::isNotHolder));
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
