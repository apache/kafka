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

import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StickyTaskAssignorTest {
    private final StickyTaskAssignor assignor = new StickyTaskAssignor();


    @Test
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");

        final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(
                        mkMap(mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3)),
                        Collections.singletonList("test-subtopology"),
                        new HashMap<>()
                ),
                new TopologyDescriberImpl(3, false)
        );

        assertEquals(3, result.members().size());
        Set<Integer> actualActiveTasks = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            final MemberAssignment testMember = result.members().get("member" + (i + 1));
            assertNotNull(testMember);
            assertEquals(1, testMember.activeTasks().size());
            actualActiveTasks.addAll(testMember.activeTasks().get("test-subtopology"));
        }
        assertEquals(Set.of(0, 1, 2), actualActiveTasks);
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
                new GroupSpecImpl(members, Arrays.asList("test-subtopology1", "test-subtopology2"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member1_2"));
        assertEquals(1, getAllActiveTaskCount(result, "member2_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member2_2"));
        assertEquals(1, getAllActiveTaskCount(result, "member3_1"));
        assertEquals(1, getAllActiveTaskCount(result, "member3_2"));

        assertEquals(mkMap(mkEntry("test-subtopology1", Set.of(0, 1, 2)), mkEntry("test-subtopology2", Set.of(0, 1, 2))),
                mergeAllActiveTasks(result, "member1_1", "member1_2", "member2_1", "member2_2", "member3_1", "member3_2"));
    }

    @Test
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks() {
        final Map<String, Set<Integer>> tasks = mkMap(mkEntry("test-subtopology1", Set.of(0, 1, 2)), mkEntry("test-subtopology2", Set.of(0, 1, 2)));
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
                        Arrays.asList("test-subtopology1", "test-subtopology2"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(3, true)
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
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Collections.singleton(0))), Collections.emptyMap());
        AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Collections.singleton(1))), Collections.emptyMap());
        Map<String, AssignmentMemberSpec> members = mkMap(mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
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
        memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Collections.singleton(1))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Collections.singleton(2))), Collections.emptyMap());
        members = mkMap(mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));
        result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
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
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0, 2))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Collections.singleton(1))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");

        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
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
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
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
                mkEntry("test-subtopology1", Set.of(0, 1, 2, 3, 4, 5)),
                mkEntry("test-subtopology2", Set.of(0)));
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", activeTasks, Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");


        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Arrays.asList("test-subtopology1", "test-subtopology2"), new HashMap<>()),
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
        AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Collections.singleton(0))), Collections.emptyMap());
        AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Collections.singleton(2))), Collections.emptyMap());
        AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Collections.singleton(1))), Collections.emptyMap());
        AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");
        AssignmentMemberSpec memberSpec5 = createAssignmentMemberSpec("process5");


        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
                mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4), mkEntry("member5", memberSpec5));

        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );


        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(0), testMember1.activeTasks().get("test-subtopology"));


        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(2), testMember2.activeTasks().get("test-subtopology"));


        MemberAssignment testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertEquals(1, testMember3.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(1), testMember3.activeTasks().get("test-subtopology"));

        MemberAssignment testMember4 = result.members().get("member4");
        assertNotNull(testMember4);
        assertNull(testMember4.activeTasks().get("test-subtopology"));

        MemberAssignment testMember5 = result.members().get("member5");
        assertNotNull(testMember5);
        assertNull(testMember5.activeTasks().get("test-subtopology"));


        // change up the assignment and make sure it is still sticky
        memberSpec1 = createAssignmentMemberSpec("process1");
        memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Collections.singleton(0))), Collections.emptyMap());
        memberSpec3 = createAssignmentMemberSpec("process3");
        memberSpec4 = createAssignmentMemberSpec("process4", mkMap(mkEntry("test-subtopology", Collections.singleton(2))), Collections.emptyMap());
        memberSpec5 = createAssignmentMemberSpec("process5", mkMap(mkEntry("test-subtopology", Collections.singleton(1))), Collections.emptyMap());


        members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
                mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4), mkEntry("member5", memberSpec5));
        result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );

        testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertNull(testMember1.activeTasks().get("test-subtopology"));


        testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(0), testMember2.activeTasks().get("test-subtopology"));


        testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertNull(testMember3.activeTasks().get("test-subtopology"));

        testMember4 = result.members().get("member4");
        assertNotNull(testMember4);
        assertEquals(1, testMember4.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(2), testMember4.activeTasks().get("test-subtopology"));

        testMember5 = result.members().get("member5");
        assertNotNull(testMember5);
        assertEquals(1, testMember5.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(1), testMember5.activeTasks().get("test-subtopology"));
    }

    @Test
    public void shouldAssignTasksToClientWithPreviousStandbyTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", Collections.emptyMap(), mkMap(mkEntry("test-subtopology", Collections.singleton(2))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", Collections.emptyMap(), mkMap(mkEntry("test-subtopology", Collections.singleton(1))));
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", Collections.emptyMap(), mkMap(mkEntry("test-subtopology", Collections.singleton(0))));


        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));

        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );


        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(2), testMember1.activeTasks().get("test-subtopology"));


        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(1), testMember2.activeTasks().get("test-subtopology"));


        MemberAssignment testMember3 = result.members().get("member3");
        assertNotNull(testMember3);
        assertEquals(1, testMember3.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(0), testMember3.activeTasks().get("test-subtopology"));
    }

    @Test
    public void shouldNotAssignStandbyTasksToClientWithPreviousStandbyTasksAndCurrentActiveTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", Collections.emptyMap(), mkMap(mkEntry("test-subtopology", Collections.singleton(0))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", Collections.emptyMap(), mkMap(mkEntry("test-subtopology", Collections.singleton(1))));


        Map<String, AssignmentMemberSpec> members = mkMap(
            mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));

        GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                members,
                Collections.singletonList("test-subtopology"),
                mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))
            ),
            new TopologyDescriberImpl(2, true)
        );

        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(0), testMember1.activeTasks().get("test-subtopology"));
        assertEquals(1, testMember1.standbyTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(1), testMember1.standbyTasks().get("test-subtopology"));


        MemberAssignment testMember2 = result.members().get("member2");
        assertNotNull(testMember2);
        assertEquals(1, testMember2.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(1), testMember2.activeTasks().get("test-subtopology"));
        assertEquals(1, testMember2.standbyTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(0), testMember2.standbyTasks().get("test-subtopology"));
    }

    @Test
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
                mkMap(mkEntry("test-subtopology", Collections.singleton(0))),
                        mkMap(mkEntry("test-subtopology", Collections.singleton(1))));
        final AssignmentMemberSpec memberSpec21 = createAssignmentMemberSpec("process2",
                mkMap(mkEntry("test-subtopology", Collections.singleton(2))),
                mkMap(mkEntry("test-subtopology", Collections.singleton(1))));
        final AssignmentMemberSpec memberSpec22 = createAssignmentMemberSpec("process2",
                Collections.emptyMap(), Collections.emptyMap());

        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1),
                mkEntry("member2_1", memberSpec21), mkEntry("member2_2", memberSpec22));

        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );


        MemberAssignment testMember1 = result.members().get("member1");
        assertNotNull(testMember1);
        assertEquals(1, testMember1.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(0), testMember1.activeTasks().get("test-subtopology"));

        MemberAssignment testMember21 = result.members().get("member2_1");
        assertNotNull(testMember21);
        assertEquals(1, testMember21.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(2), testMember21.activeTasks().get("test-subtopology"));

        MemberAssignment testMember22 = result.members().get("member2_2");
        assertNotNull(testMember22);
        assertEquals(1, testMember22.activeTasks().get("test-subtopology").size());
        assertEquals(Collections.singleton(1), testMember22.activeTasks().get("test-subtopology"));
    }

    @Test
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssignedTo() {
        final Map<String, Set<Integer>> tasks = mkMap(mkEntry("test-subtopology", Set.of(0, 1, 2, 3)));
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Collections.singleton(0))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Collections.singleton(1))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Collections.singleton(2))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4", mkMap(mkEntry("test-subtopology", Collections.singleton(3))), Collections.emptyMap());

        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
                mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4));


        final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members,
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(4, true)
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
        nonEmptyStandbyTaskCount += member1TaskIds.size() == 0 ? 0 : 1;
        nonEmptyStandbyTaskCount += member2TaskIds.size() == 0 ? 0 : 1;
        nonEmptyStandbyTaskCount += member3TaskIds.size() == 0 ? 0 : 1;
        nonEmptyStandbyTaskCount += member4TaskIds.size() == 0 ? 0 : 1;

        assertTrue(nonEmptyStandbyTaskCount >= 3);
        assertEquals(tasks, mergeAllStandbyTasks(result));

    }

    @Test
    public void shouldAssignMultipleReplicasOfStandbyTask() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Collections.singleton(0))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Collections.singleton(1))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Collections.singleton(2))), Collections.emptyMap());


        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2),
                mkEntry("member3", memberSpec3));
        final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members,
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "2"))),
                new TopologyDescriberImpl(3, true)
        );


        assertEquals(Set.of(1, 2), new HashSet<>(getAllStandbyTaskIds(result, "member1")));
        assertEquals(Set.of(0, 2), new HashSet<>(getAllStandbyTaskIds(result, "member2")));
        assertEquals(Set.of(0, 1), new HashSet<>(getAllStandbyTaskIds(result, "member3")));
    }

    @Test
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");


        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1));
        final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members,
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(1, true)
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
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(3, true)
        );



        assertEquals(Set.of(0, 1, 2), new HashSet<>(getAllActiveTaskIds(result)));
        assertEquals(Set.of(0, 1, 2), new HashSet<>(getAllStandbyTaskIds(result)));
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
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
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
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );

        assertEquals(3, getAllActiveTaskIds(result, "member1", "member2", "member3", "member4", "member5", "member6").size());
        assertEquals(Set.of(0, 1, 2), getActiveTasks(result, "test-subtopology", "member1", "member2", "member3", "member4", "member5", "member6"));
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
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(3, true)
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
                new GroupSpecImpl(members, Arrays.asList("test-subtopology0", "test-subtopology1", "test-subtopology2", "test-subtopology3"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );

        assertEquals(8, getAllActiveTaskCount(result, "member2_1", "member2_2"));
        assertEquals(4, getAllActiveTaskCount(result, "member1"));
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHosts() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");

        final List<String> allMemberIds = asList("member1", "member2", "member3", "member4");
        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4));
        final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members,
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(4, true)
        );


        for (final String memberId : allMemberIds) {
            final List<Integer> taskIds = getAllTaskIds(result, memberId);
            for (final String otherMemberId : allMemberIds) {
                if (!memberId.equals(otherMemberId)) {
                    assertNotEquals(taskIds, getAllTaskIds(result, otherMemberId));
                }
            }
        }
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(1, 2))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(3))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Set.of(0))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");

        final List<String> allMemberIds = asList("member1", "member2", "member3", "member4");
        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4));
        final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members,
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(4, true)
        );


        for (final String memberId : allMemberIds) {
            final List<Integer> taskIds = getAllTaskIds(result, memberId);
            for (final String otherMemberId : allMemberIds) {
                if (!memberId.equals(otherMemberId)) {
                    assertNotEquals(taskIds, getAllTaskIds(result, otherMemberId));
                }
            }
        }
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousStandbyTasks() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
                mkMap(mkEntry("test-subtopology", Set.of(1, 2))), mkMap(mkEntry("test-subtopology", Set.of(3, 0))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2",
                mkMap(mkEntry("test-subtopology", Set.of(3, 0))), mkMap(mkEntry("test-subtopology", Set.of(1, 2))));
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");


        final List<String> allMemberIds = asList("member1", "member2", "member3", "member4");
        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4));
        final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members,
                        Collections.singletonList("test-subtopology"),
                        mkMap(mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, "1"))),
                new TopologyDescriberImpl(4, true)
        );


        for (final String memberId : allMemberIds) {
            final List<Integer> taskIds = getAllTaskIds(result, memberId);
            for (final String otherMemberId : allMemberIds) {
                if (!memberId.equals(otherMemberId)) {
                    assertNotEquals(taskIds, getAllTaskIds(result, otherMemberId));
                }
            }
        }
    }

    @Test
    public void shouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame() {
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Set.of(0, 1, 2, 3))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");
        final AssignmentMemberSpec memberSpec4 = createAssignmentMemberSpec("process4");

        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3), mkEntry("member4", memberSpec4));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(4, false)
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1"));
        assertEquals(1, getAllActiveTaskCount(result, "member2"));
        assertEquals(1, getAllActiveTaskCount(result, "member3"));
        assertEquals(1, getAllActiveTaskCount(result, "member4"));
    }

    @Test
    public void shouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount() {
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3", mkMap(mkEntry("test-subtopology", Set.of(0, 1, 2, 3))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1");
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");

        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(4, false)
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1"));
        assertEquals(1, getAllActiveTaskCount(result, "member2"));
        assertEquals(2, getAllActiveTaskCount(result, "member3"));
    }

    @Test
    public void shouldRebalanceTasksToClientsBasedOnCapacity() {
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(0, 3, 2))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec31 = createAssignmentMemberSpec("process3");
        final AssignmentMemberSpec memberSpec32 = createAssignmentMemberSpec("process3");

        Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member2", memberSpec2), mkEntry("member3_1", memberSpec31), mkEntry("member3_2", memberSpec32));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(3, false)
        );

        assertEquals(1, getAllActiveTaskCount(result, "member2"));
        assertEquals(2, getAllActiveTaskCount(result, "member3_1", "member3_2"));
    }

    @Test
    public void shouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded() {
        final Set<Integer> p1PrevTasks = Set.of(0, 2);
        final Set<Integer> p2PrevTasks = Set.of(1, 3);
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", p1PrevTasks)), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", p2PrevTasks)), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");

        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(4, false)
        );

        assertEquals(1, getAllActiveTaskCount(result, "member3"));
        final List<Integer> p3ActiveTasks = getAllActiveTaskIds(result, "member3");

        if (new HashSet<>(p1PrevTasks).removeAll(p3ActiveTasks)) {
            assertEquals(p2PrevTasks, new HashSet<>(getAllActiveTaskIds(result, "member2")));
        } else {
            assertEquals(p1PrevTasks, new HashSet<>(getAllActiveTaskIds(result, "member1")));
        }
    }

    @Test
    public void shouldNotMoveAnyTasksWhenNewTasksAdded() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0, 1))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(2, 3))), Collections.emptyMap());

        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(6, false)
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

        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(2, 1))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(0, 3))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3");


        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(6, false)
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
                mkMap(mkEntry("test-subtopology0", Set.of(1)), mkEntry("test-subtopology1", Set.of(2, 3))),
                mkMap(mkEntry("test-subtopology0", Set.of(0)), mkEntry("test-subtopology1", Set.of(1)), mkEntry("test-subtopology2", Set.of(0, 1, 3))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2",
                mkMap(mkEntry("test-subtopology0", Set.of(0)), mkEntry("test-subtopology1", Set.of(1)), mkEntry("test-subtopology2", Set.of(2))),
                mkMap(mkEntry("test-subtopology0", Set.of(1, 2, 3)), mkEntry("test-subtopology1", Set.of(0, 2, 3)), mkEntry("test-subtopology2", Set.of(0, 1, 3))));
        final AssignmentMemberSpec memberSpec3 = createAssignmentMemberSpec("process3",
                mkMap(mkEntry("test-subtopology2", Set.of(0, 1, 3))),
                mkMap(mkEntry("test-subtopology0", Set.of(2)), mkEntry("test-subtopology1", Set.of(2))));
        final AssignmentMemberSpec newMemberSpec = createAssignmentMemberSpec("process4",
               Collections.emptyMap(),
                mkMap(mkEntry("test-subtopology0", Set.of(0, 1, 2, 3)), mkEntry("test-subtopology1", Set.of(0, 1, 2, 3)), mkEntry("test-subtopology2", Set.of(0, 1, 2, 3))));

        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("member3", memberSpec3), mkEntry("newMember", newMemberSpec));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Arrays.asList("test-subtopology0", "test-subtopology1", "test-subtopology2"), new HashMap<>()),
                new TopologyDescriberImpl(4, false)
        );

        assertEquals(mkMap(mkEntry("test-subtopology0", Set.of(1)), mkEntry("test-subtopology1", Set.of(2, 3))),
                getAllActiveTasks(result, "member1"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Set.of(0)), mkEntry("test-subtopology1", Set.of(1)), mkEntry("test-subtopology2", Set.of(2))),
                getAllActiveTasks(result, "member2"));
        assertEquals(mkMap(mkEntry("test-subtopology2", Set.of(0, 1, 3))),
                getAllActiveTasks(result, "member3"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Set.of(2, 3)), mkEntry("test-subtopology1", Set.of(0))),
                getAllActiveTasks(result, "newMember"));
    }

    @Test
    public void shouldAssignTasksNotPreviouslyActiveToMultipleNewClients() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1",
                mkMap(mkEntry("test-subtopology0", Set.of(1)), mkEntry("test-subtopology1", Set.of(2, 3))),
                mkMap(mkEntry("test-subtopology0", Set.of(0)), mkEntry("test-subtopology1", Set.of(1)), mkEntry("test-subtopology2", Set.of(0, 1, 3))));
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2",
                mkMap(mkEntry("test-subtopology0", Set.of(0)), mkEntry("test-subtopology1", Set.of(1)), mkEntry("test-subtopology2", Set.of(2))),
                mkMap(mkEntry("test-subtopology0", Set.of(1, 2, 3)), mkEntry("test-subtopology1", Set.of(0, 2, 3)), mkEntry("test-subtopology2", Set.of(0, 1, 3))));


        final AssignmentMemberSpec bounce1 = createAssignmentMemberSpec("bounce1",
                Collections.emptyMap(),
                mkMap(mkEntry("test-subtopology2", Set.of(0, 1, 3))));


        final AssignmentMemberSpec bounce2 = createAssignmentMemberSpec("bounce2",
                Collections.emptyMap(),
                mkMap(mkEntry("test-subtopology0", Set.of(2, 3)), mkEntry("test-subtopology1", Set.of(0))));



        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("bounce_member1", bounce1), mkEntry("bounce_member2", bounce2));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Arrays.asList("test-subtopology0", "test-subtopology1", "test-subtopology2"), new HashMap<>()),
                new TopologyDescriberImpl(4, false)
        );


        assertEquals(mkMap(mkEntry("test-subtopology0", Set.of(1)), mkEntry("test-subtopology1", Set.of(2, 3))),
                getAllActiveTasks(result, "member1"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Set.of(0)), mkEntry("test-subtopology1", Set.of(1)), mkEntry("test-subtopology2", Set.of(2))),
                getAllActiveTasks(result, "member2"));
        assertEquals(mkMap(mkEntry("test-subtopology2", Set.of(0, 1, 3))),
                getAllActiveTasks(result, "bounce_member1"));
        assertEquals(mkMap(mkEntry("test-subtopology0", Set.of(2, 3)), mkEntry("test-subtopology1", Set.of(0))),
                getAllActiveTasks(result, "bounce_member2"));
    }

    @Test
    public void shouldAssignTasksToNewClient() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(1, 2))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2");

        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(2, false)
        );

        assertEquals(1, getAllActiveTaskCount(result, "member1"));
    }

    @Test
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingClients() {
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0, 1, 2))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", mkMap(mkEntry("test-subtopology", Set.of(3, 4, 5))), Collections.emptyMap());
        final AssignmentMemberSpec newMemberSpec = createAssignmentMemberSpec("process3");

        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("newMember", newMemberSpec));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(6, false)
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
        final AssignmentMemberSpec memberSpec1 = createAssignmentMemberSpec("process1", mkMap(mkEntry("test-subtopology", Set.of(0, 1, 2, 6))), Collections.emptyMap());
        final AssignmentMemberSpec memberSpec2 = createAssignmentMemberSpec("process2", Collections.emptyMap(), mkMap(mkEntry("test-subtopology", Set.of(3, 4, 5))));
        final AssignmentMemberSpec newMemberSpec = createAssignmentMemberSpec("newProcess");

        final Map<String, AssignmentMemberSpec> members = mkMap(
                mkEntry("member1", memberSpec1), mkEntry("member2", memberSpec2), mkEntry("newMember", newMemberSpec));
        GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, Collections.singletonList("test-subtopology"), new HashMap<>()),
                new TopologyDescriberImpl(7, false)
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


    private int getAllActiveTaskCount(GroupAssignment result, String... memberIds) {
        int size = 0;
        for (String memberId : memberIds) {
            final MemberAssignment testMember = result.members().get(memberId);
            assertNotNull(testMember);
            assertNotNull(testMember.activeTasks());
            if (testMember.activeTasks().size() != 0) {
                for (Map.Entry<String, Set<Integer>> entry : testMember.activeTasks().entrySet()) {
                    size += entry.getValue().size();
                }
            }
        }
        return size;
    }

    private Set<Integer> getActiveTasks(GroupAssignment result, final String topologyEpoch, String... memberIds) {
        Set<Integer> res = new HashSet<>();
        for (String memberId : memberIds) {
            final MemberAssignment testMember = result.members().get(memberId);
            assertNotNull(testMember);
            assertNotNull(testMember.activeTasks());
            if (testMember.activeTasks().get(topologyEpoch) != null) {
                res.addAll(testMember.activeTasks().get(topologyEpoch));
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
            if (testMember.activeTasks().size() != 0) {
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
        if (testMember.activeTasks().size() != 0) {
            return testMember.activeTasks();
        }
        return new HashMap<>();
    }

    private Map<String, Set<Integer>> getAllStandbyTasks(GroupAssignment result, String memberId) {

        final MemberAssignment testMember = result.members().get(memberId);
        assertNotNull(testMember);
        assertNotNull(testMember.standbyTasks());
        if (testMember.standbyTasks().size() != 0) {
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
            if (testMember.standbyTasks().size() != 0) {
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

    private List<Integer> getAllTaskIds(GroupAssignment result, String... memberIds) {
        List<Integer> res = new ArrayList<>();
        res.addAll(getAllActiveTaskIds(result, memberIds));
        res.addAll(getAllStandbyTaskIds(result, memberIds));
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
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                processId,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    private AssignmentMemberSpec createAssignmentMemberSpec(final String processId, final Map<String, Set<Integer>> prevActiveTasks,
                                                            final Map<String, Set<Integer>> prevStandbyTasks) {
        return new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                prevActiveTasks,
                prevStandbyTasks,
                Collections.emptyMap(),
                processId,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    static class TopologyDescriberImpl implements TopologyDescriber {
        final int numPartitions;
        final boolean isStateful;

        TopologyDescriberImpl(int numPartitions, boolean isStateful) {
            this.numPartitions = numPartitions;
            this.isStateful = isStateful;
        }

        @Override
        public int numPartitions(String subtopologyId) {
            return numPartitions;
        }

        @Override
        public boolean isStateful(String subtopologyId) {
            return isStateful;
        }
    }

    static class TopologyDescriberImpl2 implements TopologyDescriber {
        @Override
        public int numPartitions(String subtopologyId) {
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
