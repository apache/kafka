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

import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentConfigsImplTest {

    @Test
    void testDefault() {
        assertEquals(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT,
            AssignmentConfigsImpl.DEFAULT.numStandbyReplicas());
        assertEquals(List.of(), AssignmentConfigsImpl.DEFAULT.rackAwareAssignmentTags());
    }

    @Test
    void testFromEmptyMap() {
        // A group metadata record written before the last assignment configs were persisted replays as an empty map.
        assertEquals(AssignmentConfigsImpl.DEFAULT, AssignmentConfigsImpl.fromMap(Map.of()));
    }

    @Test
    void testFromMapWithoutRackAwareAssignmentTags() {
        // The tags are only put in the map when any are configured.
        assertEquals(
            AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(2),
            AssignmentConfigsImpl.fromMap(Map.of("num.standby.replicas", "2"))
        );
    }

    @Test
    void testFromMap() {
        assertEquals(
            AssignmentConfigsImpl.DEFAULT
                .withNumStandbyReplicas(1)
                .withRackAwareAssignmentTags(List.of("tag1", "tag2")),
            AssignmentConfigsImpl.fromMap(Map.of(
                "num.standby.replicas", "1",
                "rack.aware.assignment.tags", "tag1,tag2"
            ))
        );
    }

    @Test
    void testToMap() {
        assertEquals(
            Map.of(
                "num.standby.replicas", "1",
                "rack.aware.assignment.tags", "tag1,tag2"
            ),
            new AssignmentConfigsImpl(1, List.of("tag1", "tag2")).toMap()
        );
    }

    @Test
    void testToMapWithoutRackAwareAssignmentTags() {
        // The tags are only put in the map when any are configured, matching what fromMap expects.
        assertEquals(
            Map.of("num.standby.replicas", "2"),
            new AssignmentConfigsImpl(2, List.of()).toMap()
        );
    }

    @Test
    void testToMapFromMapRoundTrip() {
        AssignmentConfigsImpl withTags = new AssignmentConfigsImpl(1, List.of("tag1", "tag2"));
        assertEquals(withTags, AssignmentConfigsImpl.fromMap(withTags.toMap()));

        AssignmentConfigsImpl withoutTags = new AssignmentConfigsImpl(2, List.of());
        assertEquals(withoutTags, AssignmentConfigsImpl.fromMap(withoutTags.toMap()));
    }

    @Test
    void testWithers() {
        AssignmentConfigsImpl configs = new AssignmentConfigsImpl(1, List.of("tag1"));

        assertEquals(new AssignmentConfigsImpl(2, List.of("tag1")), configs.withNumStandbyReplicas(2));
        assertEquals(new AssignmentConfigsImpl(1, List.of("tag2")), configs.withRackAwareAssignmentTags(List.of("tag2")));
    }

    @Test
    void testRackAwareAssignmentTagsAreUnmodifiable() {
        List<String> tags = new ArrayList<>(List.of("tag1"));
        AssignmentConfigsImpl configs = new AssignmentConfigsImpl(0, tags);

        tags.add("tag2");
        assertEquals(List.of("tag1"), configs.rackAwareAssignmentTags());
        assertThrows(UnsupportedOperationException.class, () -> configs.rackAwareAssignmentTags().add("tag2"));
    }
}
