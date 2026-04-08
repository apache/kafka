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
import org.mockito.internal.util.collections.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StickyTaskAssignorTest {

    public static final String NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
    private final StickyTaskAssignor assignor = new StickyTaskAssignor();

    @Test
    public void testToStringReturnsName() {
        assertEquals("sticky", assignor.name());
        assertEquals(assignor.name(), assignor.toString());
    }

    @Test
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                mkMap(mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3)),
                new HashMap<>()
            ),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        assertEquals(3, result.members().size());
        Set<Integer> actualActiveTasks = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            final MemberAssignment testMember = result.members().get("member" + (i + 1));
            assertNotNull(testMember);
            assertEquals(1, testMember.activeTasks().size());
            actualActiveTasks.addAll(testMember.activeTasks().get("test-subtopology"));
        }
        assertEquals(Sets.newSet(0, 1, 2), actualActiveTasks);
    }

    @Test
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks() {
        final AssignmentMemberSpec memberSpec11 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec12 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec21 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec22 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec31 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec32 = createAssignmentMemberSpec("process3");
        final Map<String, AssignmentMemberSpec> members = mkMap(mkEntry("member1_1", memberSpec11), mkEntry("member1_2", memberSpec12),
            mkEntry("member2_1", memberSpec21), mkEntry("member2_2", memberSpec22),
            mkEntry("member3_1", memberSpec31), mkEntry("member3_2", memberSpec32));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, Arrays.asList("test-subtopology1", "test-subtopology2"))
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member1_2"));
        assertEquals(1, getAllActiveTaskCount(result, "member2_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member2_2"));
        assertEquals(1, getAllActiveTaskCount(result, "member3_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member3_2"));
        assertEquals(mkMap(mkEntry("test-subtopology1", Sets.newSet(0, 1, 2)), mkEntry("test-subtopology2", Sets.newSet(0, 1, 2))),
            mergeAllActiveTasks(result, "member1_1", "member1_2", "member2_1", "member2_2", "member3_1", "member3_2"));
    }

    @Test
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks() {
        final Map<String, Set<Integer>> tasks = mkMap(mkEntry("test-subtopology1", Sets.newSet(0, 1, 2)), mkEntry("test-subtopology2", Sets.newSet(0, 1, 2)));
        final AssignmentMemberSpec memberSpec11 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec12 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec21 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec22 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec31 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec32 = createAssignmentMemberSpec("process3");
        final Map<String, AssignmentMemberSpec> members = mkMap(mkEntry("member1_1", memberSpec11), mkEntry("member1_2", memberSpec12),
            mkEntry("member2_1", memberSpec21), mkEntry("member2_2", memberSpec22),
            mkEntry("member3_1", memberSpec31), mkEntry("member3_2", memberSpec32));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members,
                mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(3, true, Arrays.asList("test-subtopology1", "test-subtopology2"))
        );

        // active tasks
        assertEquals(1, getAllActiveTaskCount(result, "member1_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member1_2"));
        assertEquals(1, getAllActiveTaskCount(result, "member2_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member2_2"));
        assertEquals(1, getAllActiveTaskCount(result, "member3_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member3_2"));
        assertEquals(tasks,
            mergeAllActiveTasks(result, "member1_1", "member1_2", "member2_1", "member2_2", "member3_1", "member3_2"));
    }

    @Test
    public void shouldNotMigrateActiveTaskToOtherProcess() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0))), Map.of());
        AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(1))), Map.of());
        Map<String, AssignmentMemberSpec> members = mkMap(mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertTrue(testMember1.activeTasks().get("test-subtopology").contains(0));
        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertTrue(testMember2.activeTasks().get("test-subtopology").contains(1));
        assertEquals(3,
            testMember1.activeTasks().get("test-subtopology").size() + testMember2.activeTasks().get("test-subtopology").size());

        // flip the previous active tasks assignment around.
        memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(1))), Map.of());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(2))), Map.of());
        members = mkMap(mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertTrue(testMember2.activeTasks().get("test-subtopology").contains(1));
        MemberAssignment testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertTrue(testMember3.activeTasks().get("test-subtopology").contains(2));
        assertEquals(3,
            testMember2.activeTasks().get("test-subtopology").size() + testMember3.activeTasks().get("test-subtopology").size());
    }

    @Test
    public void shouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 2))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(1))), Map.of());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertTrue(testMember1.activeTasks().get("test-subtopology").contains(0) || testMember1.activeTasks().get("test-subtopology").contains(2));
        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertTrue(testMember2.activeTasks().get("test-subtopology").contains(1));
        MemberAssignment testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertTrue(testMember3.activeTasks().get("test-subtopology").contains(2) || testMember3.activeTasks().get("test-subtopology").contains(0));
        assertEquals(3,
            testMember1.activeTasks().get("test-subtopology").size() +
                testMember2.activeTasks().get("test-subtopology").size() +
                testMember3.activeTasks().get("test-subtopology").size());
    }

    @Test
    public void shouldAssignBasedOnCapacity() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec21 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec22 = createAssignmentMemberSpec("process2");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2_1", memberSpec21), mkEntry("member2_2", memberSpec22));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        MemberAssignment testMember21 = result.members().get("member2_1");
        assertNotNull(testMember21);
        assertEquals(1, testMember21.activeTasks().get("test-subtopology").size());
        MemberAssignment testMember22 = result.members().get("member2_2");
        assertNotNull(testMember22);
        assertEquals(1, testMember22.activeTasks().get("test-subtopology").size());
    }

    @Test
    public void shouldAssignTasksEvenlyWithUnequalTopicGroupSizes() {
        final Map<String, Set<Integer>> activeTasks = mkMap(
            mkEntry("test-subtopology1", Sets.newSet(0, 1, 2, 3, 4, 5)),
            mkEntry("test-subtopology2", Sets.newSet(0)));
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", activeTasks, Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl2()
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        final Set<Integer> member1Topology1 = testMember1.activeTasks().get("test-subtopology1");
        final Set<Integer> member1Topology2 = testMember1.activeTasks().getOrDefault("test-subtopology2", new HashSet<>());
        assertEquals(4, member1Topology1.size() + member1Topology2.size());
        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        final Set<Integer> member2Topology1 = testMember2.activeTasks().get("test-subtopology1");
        final Set<Integer> member2Topology2 = testMember2.activeTasks().getOrDefault("test-subtopology2", new HashSet<>());
        assertEquals(3, member2Topology1.size() + member2Topology2.size());
        assertEquals(activeTasks, mkMap(
            mkEntry("test-subtopology1", Stream.concat(member1Topology1.stream(), member2Topology1.stream()).collect(Collectors.toSet())),
            mkEntry("test-subtopology2", Stream.concat(member1Topology2.stream(), member2Topology2.stream()).collect(Collectors.toSet()))));
    }

    @Test
    public void shouldKeepActiveTaskStickinessWhenMoreClientThanActiveTasks() {
        AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0))), Map.of());
        AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(2))), Map.of());
        AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Set.of(1))), Map.of());
        AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");
        AssignmentMemberSpec memberSpec5 = createAssignmentMemberSpec("process5");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
            mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4), mkEntry("member5", memberSpec5));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(0), testMember1.activeTasks().get("test-subtopology"));
        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(2), testMember2.activeTasks().get("test-subtopology"));
        MemberAssignment testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertEquals(1, testMember3.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(1), testMember3.activeTasks().get("test-subtopology"));
        MemberAssignment testMember4 = result.members().get("member4");
        assertNotNull(testMember4);
        assertNull(testMember4.activeTasks().get("test-subtopology"));
        MemberAssignment testMember5 = result.members().get("member5");
        assertNotNull(testMember5);
        assertNull(testMember5.activeTasks().get("test-subtopology"));

        // change up the assignment and make sure it is still sticky
        memberSpec1 = createAssignmentMemberSpec("process1");
        memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(0))), Map.of());
        memberSpec3 = createAssignmentMemberSpec("process3");
        memberSpec4 = createAssignmentMemberSpec("process4", mkMap(mkEntry("test-subtopology", Set.of(2))), Map.of());
        memberSpec5 = createAssignmentMemberSpec("process5", mkMap(mkEntry("test-subtopology", Set.of(1))), Map.of());
        members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
            mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4), mkEntry("member5", memberSpec5));

        result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertNull(testMember1.activeTasks().get("test-subtopology"));
        testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(0), testMember2.activeTasks().get("test-subtopology"));
        testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertNull(testMember3.activeTasks().get("test-subtopology"));
        testMember4 = result.members().get("member4");
        assertNotNull(testMember4);
        assertEquals(1, testMember4.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(2), testMember4.activeTasks().get("test-subtopology"));
        testMember5 = result.members().get("member5");
        assertNotNull(testMember5);
        assertEquals(1, testMember5.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(1), testMember5.activeTasks().get("test-subtopology"));
    }

    @Test
    public void shouldAssignTasksToClientWithPreviousStandbyTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", Map.of(), mkMap(mkEntry("test-subtopology", Set.of(2))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", Map.of(), mkMap(mkEntry("test-subtopology", Set.of(1))));
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", Map.of(), mkMap(mkEntry("test-subtopology", Set.of(0))));
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(2), testMember1.activeTasks().get("test-subtopology"));
        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(1), testMember2.activeTasks().get("test-subtopology"));
        MemberAssignment testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertEquals(1, testMember3.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(0), testMember3.activeTasks().get("test-subtopology"));
    }

    @Test
    public void shouldNotAssignStandbyTasksToClientWithPreviousStandbyTasksAndCurrentActiveTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", Map.of(), mkMap(mkEntry("test-subtopology", Set.of(0))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", Map.of(), mkMap(mkEntry("test-subtopology", Set.of(1))));
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(2, true, List.of("test-subtopology"))
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(0), testMember1.activeTasks().get("test-subtopology"));
        assertEquals(1, testMember1.standbyTasks().get("test-subtopology").size());
        assertEquals(Set.of(1), testMember1.standbyTasks().get("test-subtopology"));
        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(1), testMember2.activeTasks().get("test-subtopology"));
        assertEquals(1, testMember2.standbyTasks().get("test-subtopology").size());
        assertEquals(Set.of(0), testMember2.standbyTasks().get("test-subtopology"));
    }

    @Test
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
            mkMap(mkEntry("test-subtopology", Set.of(0))),
            mkMap(mkEntry("test-subtopology", Set.of(1))));
        final AssignmentMemberSpec memberSpec21 = createAssignmentMemberSpec("process2",
            mkMap(mkEntry("test-subtopology", Set.of(2))),
            mkMap(mkEntry("test-subtopology", Set.of(1))));
        final AssignmentMemberSpec memberSpec22 = createAssignmentMemberSpec("process2",
            Map.of(), Map.of());
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1),
            mkEntry("member2_1", memberSpec21), mkEntry("member2_2", memberSpec22));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(0), testMember1.activeTasks().get("test-subtopology"));
        MemberAssignment testMember21 = result.members().get("member2_1");
        assertNotNull(testMember21);
        assertEquals(1, testMember21.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(2), testMember21.activeTasks().get("test-subtopology"));
        MemberAssignment testMember22 = result.members().get("member2_2");
        assertNotNull(testMember22);
        assertEquals(1, testMember22.activeTasks().get("test-subtopology").size());
        assertEquals(Set.of(1), testMember22.activeTasks().get("test-subtopology"));
    }

    @Test
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssignedTo() {
        final Map<String, Set<Integer>> tasks = mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1, 2, 3)));
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(1))), Map.of());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Set.of(2))), Map.of());
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4", mkMap(mkEntry("test-subtopology", Set.of(3))), Map.of());
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
            mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members,
                mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(4, true, List.of("test-subtopology"))
        );

        final List<Integer> member1TaskIds = getAllStandbyTaskIds(result, "member1");
        assertFalse(member1TaskIds.contains(0));
        assertTrue(member1TaskIds.size() <= 2);
        final List<Integer> member2TaskIds = getAllStandbyTaskIds(result, "member2");
        assertFalse(member2TaskIds.contains(1));
        assertTrue(member2TaskIds.size() <= 2);
        final List<Integer> member3TaskIds = getAllStandbyTaskIds(result, "member3");
        assertFalse(member3TaskIds.contains(2));
        assertTrue(member3TaskIds.size() <= 2);
        final List<Integer> member4TaskIds = getAllStandbyTaskIds(result, "member4");
        assertFalse(member4TaskIds.contains(3));
        assertTrue(member4TaskIds.size() <= 2);
        int nonEmptyStandbyTaskCount = 0;
        nonEmptyStandbyTaskCount += member1TaskIds.isEmpty() ? 0 : 1;
        nonEmptyStandbyTaskCount += member2TaskIds.isEmpty() ? 0 : 1;
        nonEmptyStandbyTaskCount += member3TaskIds.isEmpty() ? 0 : 1;
        nonEmptyStandbyTaskCount += member4TaskIds.isEmpty() ? 0 : 1;
        assertTrue(nonEmptyStandbyTaskCount >= 3);
        assertEquals(tasks, mergeAllStandbyTasks(result));
    }

    @Test
    public void shouldAssignMultipleReplicasOfStandbyTask() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(1))), Map.of());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Set.of(2))), Map.of());
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
            mkEntry("member3", memberSpec3));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members,
                mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "2"))),
            new TopologyDescriberImpl(3, true, List.of("test-subtopology"))
        );

        assertEquals(Sets.newSet(1, 2), new HashSet<>(getAllStandbyTaskIds(result, "member1")));
        assertEquals(Sets.newSet(0, 2), new HashSet<>(getAllStandbyTaskIds(result, "member2")));
        assertEquals(Sets.newSet(0, 1), new HashSet<>(getAllStandbyTaskIds(result, "member3")));
    }

    @Test
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members,
                mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(1, true, List.of("test-subtopology"))
        );

        assertTrue(getAllStandbyTasks(result, "member1").isEmpty());
    }

    @Test
    public void shouldAssignActiveAndStandbyTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1),
            mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members,
                mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(3, true, List.of("test-subtopology"))
        );

        assertEquals(Sets.newSet(0, 1, 2), new HashSet<>(getAllActiveTaskIds(result)));
        assertEquals(Sets.newSet(0, 1, 2), new HashSet<>(getAllStandbyTaskIds(result)));
    }

    @Test
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible() {
        final AssignmentMemberSpec memberSpec11 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec12 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec13 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1_1", memberSpec11), mkEntry("member1_2", memberSpec12), mkEntry("member1_3", memberSpec13),
            mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        assertEquals(1, getAllActiveTaskIds(result, "member1_1", "member1_2", "member1_3").size());
        assertEquals(1, getAllActiveTaskIds(result, "member2").size());
        assertEquals(1, getAllActiveTaskIds(result, "member3").size());
    }

    @Test
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");
        final AssignmentMemberSpec memberSpec5 = createAssignmentMemberSpec("process5");
        final AssignmentMemberSpec memberSpec6 = createAssignmentMemberSpec("process6");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3),
            mkEntry("member4", memberSpec4), mkEntry("member5", memberSpec5), mkEntry("member6", memberSpec6));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        assertEquals(3, getAllActiveTaskIds(result, "member1", "member2", "member3", "member4", "member5", "member6").size());
        assertEquals(Sets.newSet(0, 1, 2), getActiveTasks(result, "test-subtopology", "member1", "member2", "member3", "member4", "member5", "member6"));
    }

    @Test
    public void shouldBalanceActiveAndStandbyTasksAcrossAvailableClients() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");
        final AssignmentMemberSpec memberSpec5 = createAssignmentMemberSpec("process5");
        final AssignmentMemberSpec memberSpec6 = createAssignmentMemberSpec("process6");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3),
            mkEntry("member4", memberSpec4), mkEntry("member5", memberSpec5), mkEntry("member6", memberSpec6));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members,
                mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(3, true, List.of("test-subtopology"))
        );

        for (String memberId : result.members().keySet()) {
            assertEquals(1, getAllStandbyTasks(result, memberId).size() + getAllActiveTaskIds(result, memberId).size());
        }
    }

    @Test
    public void shouldAssignMoreTasksToClientWithMoreCapacity() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec21 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec22 = createAssignmentMemberSpec("process2");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2_1", memberSpec21), mkEntry("member2_2", memberSpec22));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, Arrays.asList("test-subtopology0", "test-subtopology1", "test-subtopology2", "test-subtopology3"))
        );

        assertEquals(8, getAllActiveTaskCount(result, "member2_1", "member2_2"));
        assertEquals(4, getAllActiveTaskCount(result, "member1"));
    }

    @Test
    public void shouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame() {
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1, 2, 3))), Map.of());
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(4, false, List.of("test-subtopology"))
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1"));
        assertEquals(1, getAllActiveTaskCount(result, "member2"));
        assertEquals(1, getAllActiveTaskCount(result, "member3"));
        assertEquals(1, getAllActiveTaskCount(result, "member4"));
    }

    @Test
    public void shouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount() {
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1, 2, 3))), Map.of());
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(4, false, List.of("test-subtopology"))
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1"));
        assertEquals(1, getAllActiveTaskCount(result, "member2"));
        assertEquals(2, getAllActiveTaskCount(result, "member3"));
    }

    @Test
    public void shouldRebalanceTasksToClientsBasedOnCapacity() {
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 3, 2))), Map.of());
        final AssignmentMemberSpec memberSpec31 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec32 = createAssignmentMemberSpec("process3");
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member2", memberSpec2), mkEntry("member3_1", memberSpec31), mkEntry("member3_2", memberSpec32));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(3, false, List.of("test-subtopology"))
        );

        assertEquals(1, getAllActiveTaskCount(result, "member2"));
        assertEquals(2, getAllActiveTaskCount(result, "member3_1", "member3_2"));
    }

    @Test
    public void shouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded() {
        final Set<Integer> p1PrevTasks = Sets.newSet(0, 2);
        final Set<Integer> p2PrevTasks = Sets.newSet(1, 3);
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", p1PrevTasks)), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", p2PrevTasks)), Map.of());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(4, false, List.of("test-subtopology"))
        );

        assertEquals(1, getAllActiveTaskCount(result, "member3"));
        final List<Integer> p3ActiveTasks = getAllActiveTaskIds(result, "member3");
        if (p1PrevTasks.removeAll(p3ActiveTasks)) {
            assertEquals(p2PrevTasks, new HashSet<>(getAllActiveTaskIds(result, "member2")));
        } else {
            assertEquals(p1PrevTasks, new HashSet<>(getAllActiveTaskIds(result, "member1")));
        }
    }

    @Test
    public void shouldNotMoveAnyTasksWhenNewTasksAdded() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Sets.newSet(2, 3))), Map.of());
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(6, false, List.of("test-subtopology"))
        );

        final List<Integer> mem1Tasks = getAllActiveTaskIds(result, "member1");
        assertTrue(mem1Tasks.contains(0));
        assertTrue(mem1Tasks.contains(1));
        final List<Integer> mem2Tasks = getAllActiveTaskIds(result, "member2");
        assertTrue(mem2Tasks.contains(2));
        assertTrue(mem2Tasks.contains(3));
    }

    @Test
    public void shouldAssignNewTasksToNewClientWhenPreviousTasksAssignedToOldClients() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Sets.newSet(2, 1))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 3))), Map.of());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(6, false, List.of("test-subtopology"))
        );

        final List<Integer> mem1Tasks = getAllActiveTaskIds(result, "member1");
        assertTrue(mem1Tasks.contains(2));
        assertTrue(mem1Tasks.contains(1));
        final List<Integer> mem2Tasks = getAllActiveTaskIds(result, "member2");
        assertTrue(mem2Tasks.contains(0));
        assertTrue(mem2Tasks.contains(3));
        final List<Integer> mem3Tasks = getAllActiveTaskIds(result, "member3");
        assertTrue(mem3Tasks.contains(4));
        assertTrue(mem3Tasks.contains(5));
    }

    @Test
    public void shouldAssignTasksNotPreviouslyActiveToNewClient() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
            mkMap(mkEntry("test-subtopology0", Sets.newSet(1)), mkEntry("test-subtopology1", Sets.newSet(2, 3))),
            mkMap(mkEntry("test-subtopology0", Sets.newSet(0)), mkEntry("test-subtopology1", Sets.newSet(1)), mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2",
            mkMap(mkEntry("test-subtopology0", Sets.newSet(0)), mkEntry("test-subtopology1", Sets.newSet(1)), mkEntry("test-subtopology2", Sets.newSet(2))),
            mkMap(mkEntry("test-subtopology0", Sets.newSet(1, 2, 3)), mkEntry("test-subtopology1", Sets.newSet(0, 2, 3)), mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))));
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3",
            mkMap(mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))),
            mkMap(mkEntry("test-subtopology0", Sets.newSet(2)), mkEntry("test-subtopology1", Sets.newSet(2))));
        final AssignmentMemberSpec newMemberSpec = createAssignmentMemberSpec("process4",
            Map.of(),
            mkMap(mkEntry("test-subtopology0", Sets.newSet(0, 1, 2, 3)), mkEntry("test-subtopology1", Sets.newSet(0, 1, 2, 3)), mkEntry("test-subtopology2", Sets.newSet(0, 1, 2, 3))));
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3), mkEntry("newMember", newMemberSpec));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(4, false, Arrays.asList("test-subtopology0", "test-subtopology1", "test-subtopology2"))
        );

        assertEquals(mkMap(mkEntry("test-subtopology0", Sets.newSet(1)), mkEntry("test-subtopology1", Sets.newSet(2, 3))),
            getAllActiveTasks(result, "member1"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Sets.newSet(0)), mkEntry("test-subtopology1", Sets.newSet(1)), mkEntry("test-subtopology2", Sets.newSet(2))),
            getAllActiveTasks(result, "member2"));
        assertEquals(mkMap(mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))),
            getAllActiveTasks(result, "member3"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Sets.newSet(2, 3)), mkEntry("test-subtopology1", Sets.newSet(0))),
            getAllActiveTasks(result, "newMember"));
    }

    @Test
    public void shouldAssignTasksNotPreviouslyActiveToMultipleNewClients() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
            mkMap(mkEntry("test-subtopology0", Sets.newSet(1)), mkEntry("test-subtopology1", Sets.newSet(2, 3))),
            mkMap(mkEntry("test-subtopology0", Sets.newSet(0)), mkEntry("test-subtopology1", Sets.newSet(1)), mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2",
            mkMap(mkEntry("test-subtopology0", Sets.newSet(0)), mkEntry("test-subtopology1", Sets.newSet(1)), mkEntry("test-subtopology2", Sets.newSet(2))),
            mkMap(mkEntry("test-subtopology0", Sets.newSet(1, 2, 3)), mkEntry("test-subtopology1", Sets.newSet(0, 2, 3)), mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))));
        final AssignmentMemberSpec bounce1 = createAssignmentMemberSpec("bounce1",
            Map.of(),
            mkMap(mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))));
        final AssignmentMemberSpec bounce2 = createAssignmentMemberSpec("bounce2",
            Map.of(),
            mkMap(mkEntry("test-subtopology0", Sets.newSet(2, 3)), mkEntry("test-subtopology1", Sets.newSet(0))));
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("bounce_member1", bounce1), mkEntry("bounce_member2", bounce2));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(4, false, Arrays.asList("test-subtopology0", "test-subtopology1", "test-subtopology2"))
        );

        assertEquals(mkMap(mkEntry("test-subtopology0", Sets.newSet(1)), mkEntry("test-subtopology1", Sets.newSet(2, 3))),
            getAllActiveTasks(result, "member1"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Sets.newSet(0)), mkEntry("test-subtopology1", Sets.newSet(1)), mkEntry("test-subtopology2", Sets.newSet(2))),
            getAllActiveTasks(result, "member2"));
        assertEquals(mkMap(mkEntry("test-subtopology2", Sets.newSet(0, 1, 3))),
            getAllActiveTasks(result, "bounce_member1"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Sets.newSet(2, 3)), mkEntry("test-subtopology1", Sets.newSet(0))),
            getAllActiveTasks(result, "bounce_member2"));
    }

    @Test
    public void shouldAssignTasksToNewClient() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Sets.newSet(1, 2))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(2, false, List.of("test-subtopology"))
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1"));
    }

    @Test
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingClients() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1, 2))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Sets.newSet(3, 4, 5))), Map.of());
        final AssignmentMemberSpec newMemberSpec = createAssignmentMemberSpec("process3");
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("newMember", newMemberSpec));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(6, false, List.of("test-subtopology"))
        );

        final List<Integer> mem1Tasks = getAllActiveTaskIds(result, "member1");
        assertFalse(mem1Tasks.contains(3));
        assertFalse(mem1Tasks.contains(4));
        assertFalse(mem1Tasks.contains(5));
        assertEquals(2, mem1Tasks.size());
        final List<Integer> mem2Tasks = getAllActiveTaskIds(result, "member2");
        assertFalse(mem2Tasks.contains(0));
        assertFalse(mem2Tasks.contains(1));
        assertFalse(mem2Tasks.contains(2));
        assertEquals(2, mem2Tasks.size());
        assertEquals(2, getAllActiveTaskIds(result, "newMember").size());
    }

    @Test
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingAndBouncedClients() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1, 2, 6))), Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", Map.of(), mkMap(mkEntry("test-subtopology", Sets.newSet(3, 4, 5))));
        final AssignmentMemberSpec newMemberSpec = createAssignmentMemberSpec("newProcess");
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("newMember", newMemberSpec));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, new HashMap<>()),
            new TopologyDescriberImpl(7, false, List.of("test-subtopology"))
        );

        final List<Integer> mem1Tasks = getAllActiveTaskIds(result, "member1");
        assertFalse(mem1Tasks.contains(3));
        assertFalse(mem1Tasks.contains(4));
        assertFalse(mem1Tasks.contains(5));
        assertEquals(3, mem1Tasks.size());
        final List<Integer> mem2Tasks = getAllActiveTaskIds(result, "member2");
        assertFalse(mem2Tasks.contains(0));
        assertFalse(mem2Tasks.contains(1));
        assertFalse(mem2Tasks.contains(2));
        assertEquals(2, mem2Tasks.size());
        assertEquals(2, getAllActiveTaskIds(result, "newMember").size());
    }

    @Test
    public void shouldHandleLargeNumberOfTasksWithStandbyAssignment() {
        final int numTasks = 100;
        final int numClients = 5;
        final int numStandbyReplicas = 2;
        
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        for (int i = 0; i < numClients; i++) {
            members.put("member" + i, createAssignmentMemberSpec("process" + i));
        }

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(numStandbyReplicas)))),
            new TopologyDescriberImpl(numTasks, true, List.of("test-subtopology"))
        );

        // Verify all active tasks are assigned
        Set<Integer> allActiveTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberActiveTasks = getAllActiveTaskIds(result, memberId);
            allActiveTasks.addAll(memberActiveTasks);
        }
        assertEquals(numTasks, allActiveTasks.size());

        // Verify standby tasks are assigned (should be numTasks * numStandbyReplicas total)
        Set<Integer> allStandbyTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberStandbyTasks = getAllStandbyTaskIds(result, memberId);
            allStandbyTasks.addAll(memberStandbyTasks);
        }
        // With 5 clients and 2 standby replicas, we should have at least some standby tasks
        assertTrue(allStandbyTasks.size() > 0, "Should have some standby tasks assigned");
        // Maximum possible = numTasks * min(numStandbyReplicas, numClients - 1) = 100 * 2 = 200
        int maxPossibleStandbyTasks = numTasks * Math.min(numStandbyReplicas, numClients - 1);
        assertTrue(allStandbyTasks.size() <= maxPossibleStandbyTasks, 
            "Should not exceed maximum possible standby tasks: " + maxPossibleStandbyTasks);

        // Verify no client has both active and standby for the same task
        for (String memberId : result.members().keySet()) {
            Set<Integer> memberActiveTasks = new HashSet<>(getAllActiveTaskIds(result, memberId));
            Set<Integer> memberStandbyTasks = new HashSet<>(getAllStandbyTaskIds(result, memberId));
            memberActiveTasks.retainAll(memberStandbyTasks);
            assertTrue(memberActiveTasks.isEmpty(), "Client " + memberId + " has both active and standby for same task");
        }

        // Verify load distribution is reasonable
        int minActiveTasks = Integer.MAX_VALUE;
        int maxActiveTasks = 0;
        for (String memberId : result.members().keySet()) {
            int activeTaskCount = getAllActiveTaskCount(result, memberId);
            minActiveTasks = Math.min(minActiveTasks, activeTaskCount);
            maxActiveTasks = Math.max(maxActiveTasks, activeTaskCount);
        }
        // With 100 tasks and 5 clients, each should have 20 tasks
        assertEquals(20, minActiveTasks);
        assertEquals(20, maxActiveTasks);
        
        // Verify standby task distribution is reasonable
        int minStandbyTasks = Integer.MAX_VALUE;
        int maxStandbyTasks = 0;
        for (String memberId : result.members().keySet()) {
            int standbyTaskCount = getAllStandbyTaskIds(result, memberId).size();
            minStandbyTasks = Math.min(minStandbyTasks, standbyTaskCount);
            maxStandbyTasks = Math.max(maxStandbyTasks, standbyTaskCount);
        }
        // Each client should have some standby tasks, but not necessarily equal distribution
        assertTrue(minStandbyTasks >= 0);
        assertTrue(maxStandbyTasks > 0);
    }

    @Test
    public void shouldHandleOddNumberOfClientsWithStandbyTasks() {
        // Test with odd number of clients (7) and even number of tasks (14)
        final int numTasks = 14;
        final int numClients = 7;
        final int numStandbyReplicas = 1;
        
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        for (int i = 0; i < numClients; i++) {
            members.put("member" + i, createAssignmentMemberSpec("process" + i));
        }

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(numStandbyReplicas)))),
            new TopologyDescriberImpl(numTasks, true, List.of("test-subtopology"))
        );

        // Verify all active tasks are assigned
        Set<Integer> allActiveTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberActiveTasks = getAllActiveTaskIds(result, memberId);
            allActiveTasks.addAll(memberActiveTasks);
        }
        assertEquals(numTasks, allActiveTasks.size());

        // Verify standby tasks are assigned
        Set<Integer> allStandbyTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberStandbyTasks = getAllStandbyTaskIds(result, memberId);
            allStandbyTasks.addAll(memberStandbyTasks);
        }
        assertEquals(numTasks * numStandbyReplicas, allStandbyTasks.size());

        // With 14 tasks and 7 clients, each client should have 2 active tasks
        int expectedTasksPerClient = numTasks / numClients; // 14 / 7 = 2
        int remainder = numTasks % numClients; // 14 % 7 = 0
        
        int clientsWithExpectedTasks = 0;
        int clientsWithOneMoreTask = 0;
        for (String memberId : result.members().keySet()) {
            int activeTaskCount = getAllActiveTaskCount(result, memberId);
            if (activeTaskCount == expectedTasksPerClient) {
                clientsWithExpectedTasks++;
            } else if (activeTaskCount == expectedTasksPerClient + 1) {
                clientsWithOneMoreTask++;
            }
        }
        assertEquals(numClients - remainder, clientsWithExpectedTasks); // 7 clients should have 2 tasks
        assertEquals(remainder, clientsWithOneMoreTask); // 0 clients should have 3 tasks
    }

    @Test
    public void shouldHandleHighStandbyReplicaCount() {
        // Test with high number of standby replicas (5) and limited clients (3)
        final int numTasks = 6;
        final int numClients = 3;
        final int numStandbyReplicas = 5;
        
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        for (int i = 0; i < numClients; i++) {
            members.put("member" + i, createAssignmentMemberSpec("process" + i));
        }

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(numStandbyReplicas)))),
            new TopologyDescriberImpl(numTasks, true, List.of("test-subtopology"))
        );

        // Verify all active tasks are assigned
        Set<Integer> allActiveTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberActiveTasks = getAllActiveTaskIds(result, memberId);
            allActiveTasks.addAll(memberActiveTasks);
        }
        assertEquals(numTasks, allActiveTasks.size());

        // With only 3 clients and 5 standby replicas, not all standby replicas can be assigned
        // since each client can only hold standby tasks for tasks it doesn't have active
        Set<Integer> allStandbyTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberStandbyTasks = getAllStandbyTaskIds(result, memberId);
            allStandbyTasks.addAll(memberStandbyTasks);
        }
        
        // Maximum possible = numTasks * min(numStandbyReplicas, numClients - 1) = 6 * 2 = 12
        int maxPossibleStandbyTasks = numTasks * Math.min(numStandbyReplicas, numClients - 1);
        assertTrue(allStandbyTasks.size() <= maxPossibleStandbyTasks);
        assertTrue(allStandbyTasks.size() > 0); // Should assign at least some standby tasks
    }

    @Test
    public void shouldHandleLargeNumberOfSubtopologiesWithStandbyTasks() {
        // Test with many subtopologies (10) each with different numbers of tasks
        final int numSubtopologies = 10;
        final int numClients = 4;
        final int numStandbyReplicas = 1;
        
        List<String> subtopologies = new ArrayList<>();
        for (int i = 0; i < numSubtopologies; i++) {
            subtopologies.add("subtopology-" + i);
        }
        
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        for (int i = 0; i < numClients; i++) {
            members.put("member" + i, createAssignmentMemberSpec("process" + i));
        }

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(numStandbyReplicas)))),
            new TopologyDescriberImpl(5, true, subtopologies) // 5 tasks per subtopology
        );

        // Verify all subtopologies have tasks assigned
        Set<String> subtopologiesWithTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            MemberAssignment member = result.members().get(memberId);
            subtopologiesWithTasks.addAll(member.activeTasks().keySet());
        }
        assertEquals(numSubtopologies, subtopologiesWithTasks.size());

        // Verify standby tasks are assigned across subtopologies
        Set<String> subtopologiesWithStandbyTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            MemberAssignment member = result.members().get(memberId);
            subtopologiesWithStandbyTasks.addAll(member.standbyTasks().keySet());
        }
        assertEquals(numSubtopologies, subtopologiesWithStandbyTasks.size());
    }

    @Test
    public void shouldHandleEdgeCaseWithSingleClientAndMultipleStandbyReplicas() {
        // Test edge case: single client with multiple standby replicas
        final int numTasks = 10;
        final int numStandbyReplicas = 3;
        
        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", createAssignmentMemberSpec("process1"))
        );

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(numStandbyReplicas)))),
            new TopologyDescriberImpl(numTasks, true, List.of("test-subtopology"))
        );

        // Single client should get all active tasks
        assertEquals(numTasks, getAllActiveTaskCount(result, "member1"));
        
        // No standby tasks should be assigned since there's only one client
        // (standby tasks can't be assigned to the same client as active tasks)
        assertTrue(getAllStandbyTaskIds(result, "member1").isEmpty());
    }

    @Test
    public void shouldHandleEdgeCaseWithMoreStandbyReplicasThanAvailableClients() {
        // Test edge case: more standby replicas than available clients
        final int numTasks = 4;
        final int numClients = 2;
        final int numStandbyReplicas = 5; // More than available clients
        
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        for (int i = 0; i < numClients; i++) {
            members.put("member" + i, createAssignmentMemberSpec("process" + i));
        }

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(numStandbyReplicas)))),
            new TopologyDescriberImpl(numTasks, true, List.of("test-subtopology"))
        );

        // Verify all active tasks are assigned
        Set<Integer> allActiveTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberActiveTasks = getAllActiveTaskIds(result, memberId);
            allActiveTasks.addAll(memberActiveTasks);
        }
        assertEquals(numTasks, allActiveTasks.size());

        // With only 2 clients, maximum standby tasks per task = 1 (since each client has active tasks)
        Set<Integer> allStandbyTasks = new HashSet<>();
        for (String memberId : result.members().keySet()) {
            List<Integer> memberStandbyTasks = getAllStandbyTaskIds(result, memberId);
            allStandbyTasks.addAll(memberStandbyTasks);
        }
        
        // Maximum possible = numTasks * 1 = 4
        assertEquals(numTasks, allStandbyTasks.size());
    }

    @Test
    public void shouldReassignTasksWhenNewNodeJoinsWithExistingActiveAndStandbyAssignments() {
        // Initial setup: Node 1 has active tasks 0,1 and standby tasks 2,3
        // Node 2 has active tasks 2,3 and standby tasks 0,1
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
            mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1))),
            mkMap(mkEntry("test-subtopology", Sets.newSet(2, 3))));

        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2",
            mkMap(mkEntry("test-subtopology", Sets.newSet(2, 3))),
            mkMap(mkEntry("test-subtopology", Sets.newSet(0, 1))));

        // Node 3 joins as new client
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");

        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(4, true, List.of("test-subtopology"))
        );

        // Verify all active tasks are assigned
        final Set<Integer> allAssignedActiveTasks = new HashSet<>();
        allAssignedActiveTasks.addAll(getAllActiveTaskIds(result, "member1"));
        allAssignedActiveTasks.addAll(getAllActiveTaskIds(result, "member2"));
        allAssignedActiveTasks.addAll(getAllActiveTaskIds(result, "member3"));
        assertEquals(Sets.newSet(0, 1, 2, 3), allAssignedActiveTasks);

        // Verify all standby tasks are assigned
        final Set<Integer> allAssignedStandbyTasks = new HashSet<>();
        allAssignedStandbyTasks.addAll(getAllStandbyTaskIds(result, "member1"));
        allAssignedStandbyTasks.addAll(getAllStandbyTaskIds(result, "member2"));
        allAssignedStandbyTasks.addAll(getAllStandbyTaskIds(result, "member3"));
        assertEquals(Sets.newSet(0, 1, 2, 3), allAssignedStandbyTasks);

        // Verify each member has 1-2 active tasks and at most 3 tasks total
        assertTrue(getAllActiveTaskIds(result, "member1").size() >= 1 && getAllActiveTaskIds(result, "member1").size() <= 2);
        assertTrue(getAllActiveTaskIds(result, "member1").size() + getAllStandbyTaskIds(result, "member1").size() <= 3);

        assertTrue(getAllActiveTaskIds(result, "member2").size() >= 1 && getAllActiveTaskIds(result, "member2").size() <= 2);
        assertTrue(getAllActiveTaskIds(result, "member2").size() + getAllStandbyTaskIds(result, "member2").size() <= 3);

        assertTrue(getAllActiveTaskIds(result, "member3").size() >= 1 && getAllActiveTaskIds(result, "member3").size() <= 2);
        assertTrue(getAllActiveTaskIds(result, "member3").size() + getAllStandbyTaskIds(result, "member3").size() <= 3);
    }

    @Test
    public void shouldRangeAssignTasksWhenScalingUp() {
        // Two clients, the second one is new
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
            Map.of("test-subtopology1", Set.of(0, 1), "test-subtopology2", Set.of(0, 1)),
            Map.of());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        // Two subtopologies with 2 tasks each (4 tasks total) with standby replicas enabled
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(1)))),
            new TopologyDescriberImpl(2, true, Arrays.asList("test-subtopology1", "test-subtopology2"))
        );

        // Each client should get one task from each subtopology
        final MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology1").size());
        assertEquals(1, testMember1.activeTasks().get("test-subtopology2").size());
        
        final MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology1").size());
        assertEquals(1, testMember2.activeTasks().get("test-subtopology2").size());
        
        // Verify all tasks are assigned exactly once
        final Set<Integer> allSubtopology1Tasks = new HashSet<>();
        allSubtopology1Tasks.addAll(testMember1.activeTasks().get("test-subtopology1"));
        allSubtopology1Tasks.addAll(testMember2.activeTasks().get("test-subtopology1"));
        assertEquals(Sets.newSet(0, 1), allSubtopology1Tasks);
        
        final Set<Integer> allSubtopology2Tasks = new HashSet<>();
        allSubtopology2Tasks.addAll(testMember1.activeTasks().get("test-subtopology2"));
        allSubtopology2Tasks.addAll(testMember2.activeTasks().get("test-subtopology2"));
        assertEquals(Sets.newSet(0, 1), allSubtopology2Tasks);

        // Each client should get one task from each subtopology
        assertNotNull(testMember1);
        assertEquals(1, testMember1.standbyTasks().get("test-subtopology1").size());
        assertEquals(1, testMember1.standbyTasks().get("test-subtopology2").size());

        assertNotNull(testMember2);
        assertEquals(1, testMember2.standbyTasks().get("test-subtopology1").size());
        assertEquals(1, testMember2.standbyTasks().get("test-subtopology2").size());
    }

    @Test
    public void shouldRangeAssignTasksWhenStartingEmpty() {
        // Two clients starting empty (no previous tasks)
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        // Two subtopologies with 2 tasks each (4 tasks total) with standby replicas enabled
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, String.valueOf(1)))),
            new TopologyDescriberImpl(2, true, Arrays.asList("test-subtopology1", "test-subtopology2"))
        );

        // Each client should get one task from each subtopology
        final MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology1").size());
        assertEquals(1, testMember1.activeTasks().get("test-subtopology2").size());

        final MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology1").size());
        assertEquals(1, testMember2.activeTasks().get("test-subtopology2").size());

        // Verify all tasks are assigned exactly once
        final Set<Integer> allSubtopology1Tasks = new HashSet<>();
        allSubtopology1Tasks.addAll(testMember1.activeTasks().get("test-subtopology1"));
        allSubtopology1Tasks.addAll(testMember2.activeTasks().get("test-subtopology1"));
        assertEquals(Sets.newSet(0, 1), allSubtopology1Tasks);

        final Set<Integer> allSubtopology2Tasks = new HashSet<>();
        allSubtopology2Tasks.addAll(testMember1.activeTasks().get("test-subtopology2"));
        allSubtopology2Tasks.addAll(testMember2.activeTasks().get("test-subtopology2"));
        assertEquals(Sets.newSet(0, 1), allSubtopology2Tasks);

        // Each client should get one task from each subtopology
        assertNotNull(testMember1);
        assertEquals(1, testMember1.standbyTasks().get("test-subtopology1").size());
        assertEquals(1, testMember1.standbyTasks().get("test-subtopology2").size());

        assertNotNull(testMember2);
        assertEquals(1, testMember2.standbyTasks().get("test-subtopology1").size());
        assertEquals(1, testMember2.standbyTasks().get("test-subtopology2").size());
    }

    @Test
    public void shouldAssignStandbyTaskToPreviousOwnerBasedOnBelowQuotaCondition() {
        // This test demonstrates the change from "load-balanced" to "below-quota" condition for standby assignment.
        // We create a scenario where:
        // - Process1 previously owned standby task 1 and currently has 1 active task (task 0)
        // - Process2 currently has 1 active task (task 1) 
        // - Process3 has no previous tasks (least loaded after assignment)
        // 
        // Under the old "load-balanced" algorithm: Process1 might not get standby task 1 because 
        // Process3 could be considered least loaded.
        //
        // Under the new "below-quota" algorithm: Process1 gets standby task 1 because 
        // it previously owned it AND is below quota.

        // Set up previous task assignments:
        // Process1: active=[0], standby=[1] (previously had both active and standby tasks)
        // Process2: active=[1] (had the active task that process1 had as standby)
        // Process3: no previous tasks
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", 
            mkMap(mkEntry("test-subtopology", Sets.newSet(0))), 
            mkMap(mkEntry("test-subtopology", Sets.newSet(1))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2",
            mkMap(mkEntry("test-subtopology", Sets.newSet(1))), 
            Map.of());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");

        final Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), 
            mkEntry("member2", memberSpec2), 
            mkEntry("member3", memberSpec3));

        // We have 2 active tasks + 1 standby replica = 4 total tasks
        // Quota per process = 4 tasks / 3 processes = 1.33 -> 2 tasks per process
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, mkMap(mkEntry(NUM_STANDBY_REPLICAS_CONFIG, "1"))),
            new TopologyDescriberImpl(2, true, List.of("test-subtopology"))
        );

        // Verify that process1 gets the standby task 1 that it previously owned
        // This should work under the new "below-quota" algorithm since process1 has only 1 active task
        // which is below the quota of 2 tasks per process
        final MemberAssignment member1 = result.members().get("member1");
        assertNotNull(member1);
        
        // Member1 should retain its active task 0
        assertTrue(member1.activeTasks().get("test-subtopology").contains(0));
        
        // Member1 should get standby task 1 because it previously owned it and is below quota
        assertNotNull(member1.standbyTasks().get("test-subtopology"), "Member1 should have standby tasks assigned");
        assertTrue(member1.standbyTasks().get("test-subtopology").contains(1), 
            "Member1 should have standby task 1, but has: " + member1.standbyTasks().get("test-subtopology"));
        
        // Verify that member1 doesn't have active task 1 (standby can't be same as active)
        assertFalse(member1.activeTasks().get("test-subtopology").contains(1));
        
        // Verify the process1's total task count is at or below quota
        int member1ActiveCount = member1.activeTasks().get("test-subtopology").size();
        int member1StandbyCount = member1.standbyTasks().get("test-subtopology").size();
        int member1TotalTasks = member1ActiveCount + member1StandbyCount;
        assertTrue(member1TotalTasks <= 2, "Member1 should have <= 2 total tasks (quota), but has " + member1TotalTasks);
    }


    private int getAllActiveTaskCount(GroupAssignment result, String... memberIds) {
        int size = 0;
        for (String memberId : memberIds) {
            final MemberAssignment testMember = result.members().get(memberId);
            assertNotNull(testMember);
            assertNotNull(testMember.activeTasks());
            if (!testMember.activeTasks().isEmpty()) {
                for (Map.Entry<String, Set<Integer>> entry : testMember.activeTasks().entrySet()) {
                    size += entry.getValue().size();
                }
            }
        }
        return size;
    }

    private Set<Integer> getActiveTasks(GroupAssignment result, final String topologyId, String... memberIds) {
        Set<Integer> res = new HashSet<>();
        for (String memberId : memberIds) {
            final MemberAssignment testMember = result.members().get(memberId);
            assertNotNull(testMember);
            assertNotNull(testMember.activeTasks());
            if (testMember.activeTasks().get(topologyId) != null) {
                res.addAll(testMember.activeTasks().get(topologyId));
            }
        }
        return res;
    }

    private List<Integer> getAllActiveTaskIds(GroupAssignment result, String... memberIds) {
        List<Integer> res = new ArrayList<>();
        for (String memberId : memberIds) {
            final MemberAssignment testMember = result.members().get(memberId);
            assertNotNull(testMember);
            assertNotNull(testMember.activeTasks());
            if (!testMember.activeTasks().isEmpty()) {
                for (Map.Entry<String, Set<Integer>> entry : testMember.activeTasks().entrySet()) {
                    res.addAll(entry.getValue());
                }
            }
        }
        return res;
    }

    private List<Integer> getAllActiveTaskIds(GroupAssignment result) {
        String[] memberIds = new String[result.members().size()];
        return getAllActiveTaskIds(result, result.members().keySet().toArray(memberIds));
    }

    private Map<String, Set<Integer>> getAllActiveTasks(GroupAssignment result, String memberId) {
        final MemberAssignment testMember = result.members().get(memberId);
        assertNotNull(testMember);
        assertNotNull(testMember.activeTasks());
        if (!testMember.activeTasks().isEmpty()) {
            return testMember.activeTasks();
        }
        return new HashMap<>();
    }

    private Map<String, Set<Integer>> getAllStandbyTasks(GroupAssignment result, String memberId) {
        final MemberAssignment testMember = result.members().get(memberId);
        assertNotNull(testMember);
        assertNotNull(testMember.standbyTasks());
        if (!testMember.standbyTasks().isEmpty()) {
            return testMember.standbyTasks();
        }
        return new HashMap<>();
    }

    private List<Integer> getAllStandbyTaskIds(GroupAssignment result, String... memberIds) {
        List<Integer> res = new ArrayList<>();
        for (String memberId : memberIds) {
            final MemberAssignment testMember = result.members().get(memberId);
            assertNotNull(testMember);
            assertNotNull(testMember.standbyTasks());
            if (!testMember.standbyTasks().isEmpty()) {
                for (Map.Entry<String, Set<Integer>> entry : testMember.standbyTasks().entrySet()) {
                    res.addAll(entry.getValue());
                }
            }
        }
        return res;
    }

    private List<Integer> getAllStandbyTaskIds(GroupAssignment result) {
        String[] memberIds = new String[result.members().size()];
        return getAllStandbyTaskIds(result, result.members().keySet().toArray(memberIds));
    }

    private Map<String, Set<Integer>> mergeAllActiveTasks(GroupAssignment result, String... memberIds) {
        Map<String, Set<Integer>> res = new HashMap<>();
        for (String memberId : memberIds) {
            Map<String, Set<Integer>> memberActiveTasks = getAllActiveTasks(result, memberId);
            res = Stream.of(res, memberActiveTasks)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> {
                        v1.addAll(v2);
                        return new HashSet<>(v1);
                    }));
        }
        return res;
    }

    private Map<String, Set<Integer>> mergeAllStandbyTasks(GroupAssignment result, String... memberIds) {
        Map<String, Set<Integer>> res = new HashMap<>();
        for (String memberId : memberIds) {
            Map<String, Set<Integer>> memberStandbyTasks = getAllStandbyTasks(result, memberId);
            res = Stream.of(res, memberStandbyTasks)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> {
                        v1.addAll(v2);
                        return new HashSet<>(v1);
                    }));
        }
        return res;
    }

    private Map<String, Set<Integer>> mergeAllStandbyTasks(GroupAssignment result) {
        String[] memberIds = new String[result.members().size()];
        return mergeAllStandbyTasks(result, result.members().keySet().toArray(memberIds));
    }

    private AssignmentMemberSpec createAssignmentMemberSpec(final String processId) {
        return new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Map.of(),
            Map.of(),
            Map.of(),
            processId,
            Map.of(),
            Map.of(),
            Map.of());
    }

    private AssignmentMemberSpec createAssignmentMemberSpec(final String processId, final Map<String, Set<Integer>> prevActiveTasks,
                                                            final Map<String, Set<Integer>> prevStandbyTasks) {
        return new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            prevActiveTasks,
            prevStandbyTasks,
            Map.of(),
            processId,
            Map.of(),
            Map.of(),
            Map.of());
    }

    record TopologyDescriberImpl(int numTasks, boolean isStateful, List<String> subtopologies) implements TopologyDescriber {

        @Override
        public int maxNumInputPartitions(String subtopologyId) throws NoSuchElementException {
            return numTasks;
        }

        @Override
        public boolean isStateful(String subtopologyId) {
            return isStateful;
        }
    }

    static class TopologyDescriberImpl2 implements TopologyDescriber {

        @Override
        public List<String> subtopologies() {
            return Arrays.asList("test-subtopology1", "test-subtopology2");
        }

        @Override
        public int maxNumInputPartitions(String subtopologyId) throws NoSuchElementException {
            if (subtopologyId.equals("test-subtopology1"))
                return 6;
            return 1;
        }

        @Override
        public boolean isStateful(String subtopologyId) {
            return false;
        }
    }
}
