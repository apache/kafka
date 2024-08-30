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
package org.apache.kafka.coordinator.group.taskassignor;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class MockAssignorTest {

    private final MockAssignor assignor = new MockAssignor();


    @Test
    public void testBasicScenario() {

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                Collections.emptyMap(),
                Collections.emptyList(),
                new HashMap<>()
            ),
            new TopologyDescriberImpl(5)
        );

        assertEquals(0, result.members().size());
    }


    @Test
    public void testSingleMember() {

        final AssignmentMemberSpec memberSpec = new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "test-process",
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                Collections.singletonMap("test_member", memberSpec),
                Collections.singletonList("test-subtopology"),
                new HashMap<>()
            ),
            new TopologyDescriberImpl(4)
        );

        assertEquals(1, result.members().size());
        final MemberAssignment testMember = result.members().get("test_member");
        assertNotNull(testMember);
        assertEquals(mkMap(
            mkEntry("test-subtopology", mkSet(0, 1, 2, 3))
        ), testMember.activeTasks());
    }


    @Test
    public void testTwoMembersTwoSubtopologies() {

        final AssignmentMemberSpec memberSpec1 = new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "test-process",
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        final AssignmentMemberSpec memberSpec2 = new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "test-process",
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                mkMap(mkEntry("test_member1", memberSpec1), mkEntry("test_member2", memberSpec2)),
                Arrays.asList("test-subtopology1", "test-subtopology2"),
                new HashMap<>()
            ),
            new TopologyDescriberImpl(4)
        );

        final Map<String, Set<Integer>> expected1 = mkMap(
            mkEntry("test-subtopology1", mkSet(1, 3)),
            mkEntry("test-subtopology2", mkSet(1, 3))
        );
        final Map<String, Set<Integer>> expected2 = mkMap(
            mkEntry("test-subtopology1", mkSet(0, 2)),
            mkEntry("test-subtopology2", mkSet(0, 2))
        );

        assertEquals(2, result.members().size());
        final MemberAssignment testMember1 = result.members().get("test_member1");
        final MemberAssignment testMember2 = result.members().get("test_member2");
        assertNotNull(testMember1);
        assertNotNull(testMember2);
        assertTrue(expected1.equals(testMember1.activeTasks()) || expected2.equals(testMember1.activeTasks()));
        assertTrue(expected1.equals(testMember2.activeTasks()) || expected2.equals(testMember2.activeTasks()));
    }

    @Test
    public void testTwoMembersTwoSubtopologiesStickiness() {

        final AssignmentMemberSpec memberSpec1 = new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            mkMap(
                mkEntry("test-subtopology1", mkSet(0, 2, 3)),
                mkEntry("test-subtopology2", mkSet(0))
            ),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "test-process",
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        final AssignmentMemberSpec memberSpec2 = new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            mkMap(
                mkEntry("test-subtopology1", mkSet(1)),
                mkEntry("test-subtopology2", mkSet(3))
            ),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "test-process",
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                mkMap(mkEntry("test_member1", memberSpec1), mkEntry("test_member2", memberSpec2)),
                Arrays.asList("test-subtopology1", "test-subtopology2"),
                new HashMap<>()
            ),
            new TopologyDescriberImpl(4)
        );

        assertEquals(2, result.members().size());
        final MemberAssignment testMember1 = result.members().get("test_member1");
        final MemberAssignment testMember2 = result.members().get("test_member2");
        assertNotNull(testMember1);
        assertNotNull(testMember2);
        assertEquals(mkMap(
            mkEntry("test-subtopology1", mkSet(0, 2, 3)),
            mkEntry("test-subtopology2", mkSet(0))
        ), testMember1.activeTasks());
        assertEquals(mkMap(
            mkEntry("test-subtopology1", mkSet(1)),
            mkEntry("test-subtopology2", mkSet(1, 2, 3))
        ), testMember2.activeTasks());
    }

    static class TopologyDescriberImpl implements TopologyDescriber {
        final int numPartitions;

        TopologyDescriberImpl(int numPartitions) {
            this.numPartitions = numPartitions;
        }
        @Override
        public int numPartitions(String subtopologyId) {
            return numPartitions;
        }
        @Override
        public boolean isStateful(String subtopologyId) {
            return false;
        }
    }

}
