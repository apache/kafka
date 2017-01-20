/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class ProcessTaskAssignorTest {

    private final String consumerA = "a";
    private final String consumerB = "b";
    private final String consumerC = "c";
    private final TopicPartition active1 = new TopicPartition("t", 0);
    private final TopicPartition active2 = new TopicPartition("t", 1);
    private final TopicPartition active3 = new TopicPartition("t", 2);
    private final TopicPartition standby1 = new TopicPartition("standby", 0);
    private final TopicPartition standby2 = new TopicPartition("standby", 1);
    private final TopicPartition standby3 = new TopicPartition("standby", 2);
    private final TaskId task00 = new TaskId(0, 0);
    private final TaskId task01 = new TaskId(0, 1);
    private final TaskId task02 = new TaskId(0, 2);
    private final TaskId task10 = new TaskId(1, 0);
    private final TaskId task11 = new TaskId(1, 1);
    private final TaskId task12 = new TaskId(1, 2);

    private final List<TopicPartition> activePartitions = Arrays.asList(active1, active2, active3);
    private final List<TopicPartition> standByPartitions = Arrays.asList(standby1, standby2, standby3);
    private final UUID processId = UUID.randomUUID();
    private final SubscriptionInfo emptySubscription = new SubscriptionInfo(processId, Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), null);
    private final SubscriptionInfo taskId0Subscription = new SubscriptionInfo(processId, Utils.mkSet(task00), Collections.<TaskId>emptySet(), null);
    private final Map<UUID, StreamPartitionAssignor.ClientMetadata> clientMetadata = new HashMap<>();
    private final StreamPartitionAssignor.ClientMetadata metadata = new StreamPartitionAssignor.ClientMetadata(null);
    private final Set<TaskId> activeTasks = Utils.mkSet(task00, task01, task02);
    private final Set<TaskId> standbyTasks = Utils.mkSet(task10, task11, task12);
    private final Map<TaskId, Set<TopicPartition>> partitionsForTask = new HashMap<>();
    private final StreamPartitionAssignor.ProcessTaskAssignor assignor
            = new StreamPartitionAssignor.ProcessTaskAssignor(clientMetadata,
                                                              partitionsForTask,
                                                              Collections.<HostInfo, Set<TopicPartition>>emptyMap());

    @Before
    public void before() {
        partitionsForTask.put(task00, Utils.mkSet(active1));
        partitionsForTask.put(task01, Utils.mkSet(active2));
        partitionsForTask.put(task02, Utils.mkSet(active3));
        partitionsForTask.put(task10, Utils.mkSet(standby1));
        partitionsForTask.put(task11, Utils.mkSet(standby2));
        partitionsForTask.put(task12, Utils.mkSet(standby3));
        clientMetadata.put(processId, metadata);
        metadata.processState.activeTasks.addAll(activeTasks);
        metadata.processState.standbyTasks.addAll(standbyTasks);
    }

    @Test
    public void shouldAssignActiveTasksToEachConsumerWhenNoPreviousAssignments() throws Exception {
        metadata.addConsumer(consumerA, emptySubscription);
        metadata.addConsumer(consumerB, emptySubscription);
        metadata.addConsumer(consumerC, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignments = assignor.assign();
        final List<TopicPartition> assignedPartitions = allAssignedActivePartitions(assignments);
        assertThat(assignments.get(consumerA).partitions().size(), equalTo(1));
        assertThat(assignments.get(consumerB).partitions().size(), equalTo(1));
        assertThat(assignments.get(consumerC).partitions().size(), equalTo(1));
        assertThat(assignedPartitions, equalTo(activePartitions));
    }

    @Test
    public void shouldNotMigrateActiveTaskToOtherConsumer() throws Exception {
        metadata.addConsumer(consumerA, taskId0Subscription);
        metadata.addConsumer(consumerB, new SubscriptionInfo(processId, Utils.mkSet(task01), Collections.<TaskId>emptySet(), null));

        final Map<String, PartitionAssignor.Assignment> assignments = assignor.assign();
        assertThat(assignments.get(consumerA).partitions(), hasItems(active1));
        assertThat(assignments.get(consumerB).partitions(), hasItems(active2));
        assertThat(allAssignedActivePartitions(assignments), equalTo(activePartitions));
    }

    @Test
    public void shouldAssignAllActiveTasksToOnlyConsumer() throws Exception {
        metadata.addConsumer(consumerA, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        assertThat(assignment.get(consumerA).partitions(), equalTo(activePartitions));
    }

    @Test
    public void shouldMigrateSingleActiveTaskToNewConsumer() throws Exception {
        metadata.addConsumer(consumerA, new SubscriptionInfo(processId, activeTasks, Collections.<TaskId>emptySet(), null));
        metadata.addConsumer(consumerB, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        assertThat(assignment.get(consumerA).partitions().size(), equalTo(2));
        assertThat(assignment.get(consumerB).partitions().size(), equalTo(1));
    }

    @Test
    public void shouldMigrateOneActiveTaskToNewConsumerWithoutChangingAllAssignments() throws Exception {
        metadata.addConsumer(consumerA, new SubscriptionInfo(processId, Utils.mkSet(task01, task02), Collections.<TaskId>emptySet(), null));
        metadata.addConsumer(consumerB, taskId0Subscription);
        metadata.addConsumer(consumerC, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        assertThat(assignment.get(consumerB).partitions(), hasItems(active1));
        assertThat(assignment.get(consumerA).partitions().size(), equalTo(1));
        assertThat(assignment.get(consumerC).partitions().size(), equalTo(1));
        assertThat(allAssignedActivePartitions(assignment), equalTo(activePartitions));
    }

    @Test
    public void shouldAssignAllStandbyTasksToSingleConsumer() throws Exception {
        metadata.addConsumer(consumerA, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(assignment.get(consumerA).userData());
        assertThat(assignmentInfo.standbyTasks.keySet(), hasItems(task10, task11, task12));
        assertThat(assignmentInfo.standbyTasks.size(), equalTo(3));
        assertThat(allAssignedStandbyPartitions(assignment), equalTo(standByPartitions));
    }

    @Test
    public void shouldNotMigrateStandbyTaskToOtherConsumer() throws Exception {
        metadata.addConsumer(consumerA, new SubscriptionInfo(processId, Collections.<TaskId>emptySet(), Utils.mkSet(task11), null));
        metadata.addConsumer(consumerB, new SubscriptionInfo(processId, Collections.<TaskId>emptySet(), Utils.mkSet(task10), null));

        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        final AssignmentInfo consumerAAssignment = AssignmentInfo.decode(assignment.get(consumerA).userData());
        final AssignmentInfo consumerBAssignment = AssignmentInfo.decode(assignment.get(consumerB).userData());
        assertThat(consumerAAssignment.standbyTasks.keySet(), hasItems(task11));
        assertThat(consumerBAssignment.standbyTasks.keySet(), hasItems(task10));
        assertThat(allAssignedStandbyPartitions(assignment), equalTo(standByPartitions));
    }

    @Test
    public void shouldAssignStandbyTaskToEachConsumerWhenNoPrevious() throws Exception {
        metadata.addConsumer(consumerA, emptySubscription);
        metadata.addConsumer(consumerB, emptySubscription);
        metadata.addConsumer(consumerC, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        final AssignmentInfo consumerAAssignment = AssignmentInfo.decode(assignment.get(consumerA).userData());
        final AssignmentInfo consumerBAssignment = AssignmentInfo.decode(assignment.get(consumerB).userData());
        final AssignmentInfo consumerCAssignment = AssignmentInfo.decode(assignment.get(consumerC).userData());
        assertThat(consumerAAssignment.standbyTasks.size(), equalTo(1));
        assertThat(consumerBAssignment.standbyTasks.size(), equalTo(1));
        assertThat(consumerCAssignment.standbyTasks.size(), equalTo(1));
    }

    @Test
    public void shouldMigrateSingleStandbyTaskToNewConsumer() throws Exception {
        metadata.addConsumer(consumerA, new SubscriptionInfo(processId, Collections.<TaskId>emptySet(), standbyTasks, null));
        metadata.addConsumer(consumerB, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        final AssignmentInfo consumerAAssignment = AssignmentInfo.decode(assignment.get(consumerA).userData());
        final AssignmentInfo consumerBAssignment = AssignmentInfo.decode(assignment.get(consumerB).userData());
        assertThat(consumerAAssignment.standbyTasks.size(), equalTo(2));
        assertThat(consumerBAssignment.standbyTasks.size(), equalTo(1));
    }

    @Test
    public void shouldMigrateOneStandbyTaskToNewConsumerWithoutChangingAllAssignments() throws Exception {
        metadata.addConsumer(consumerA, new SubscriptionInfo(processId, Collections.<TaskId>emptySet(), Utils.mkSet(task11, task12), null));
        metadata.addConsumer(consumerB, new SubscriptionInfo(processId, Collections.<TaskId>emptySet(), Utils.mkSet(task10), null));
        metadata.addConsumer(consumerC, emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        final AssignmentInfo consumerAAssignment = AssignmentInfo.decode(assignment.get(consumerA).userData());
        final AssignmentInfo consumerBAssignment = AssignmentInfo.decode(assignment.get(consumerB).userData());
        final AssignmentInfo consumerCAssignment = AssignmentInfo.decode(assignment.get(consumerC).userData());
        // make sure task10 from consumerB hasn't moved
        assertThat(consumerBAssignment.standbyTasks.keySet(), hasItems(task10));
        assertThat(consumerAAssignment.standbyTasks.size(), equalTo(1));
        assertThat(consumerBAssignment.standbyTasks.size(), equalTo(1));
        assertThat(consumerCAssignment.standbyTasks.size(), equalTo(1));
    }

    @Test
    public void shouldKeepActiveTaskStickynessWhenMoreConsumersThanActiveTasks() throws Exception {
        metadata.addConsumer(consumerA, new SubscriptionInfo(processId, Utils.mkSet(task00), Utils.mkSet(task10), null));
        metadata.addConsumer(consumerB, new SubscriptionInfo(processId, Utils.mkSet(task01), Utils.mkSet(task11), null));
        metadata.addConsumer(consumerC, new SubscriptionInfo(processId, Utils.mkSet(task02), Utils.mkSet(task12), null));
        metadata.addConsumer("D", emptySubscription);
        metadata.addConsumer("E", emptySubscription);
        final Map<String, PartitionAssignor.Assignment> assignment = assignor.assign();
        final AssignmentInfo consumerAAssignment = AssignmentInfo.decode(assignment.get(consumerA).userData());
        final AssignmentInfo consumerBAssignment = AssignmentInfo.decode(assignment.get(consumerB).userData());
        final AssignmentInfo consumerCAssignment = AssignmentInfo.decode(assignment.get(consumerC).userData());
        assertThat(consumerAAssignment.activeTasks, hasItems(task00));
        assertThat(consumerBAssignment.activeTasks, hasItems(task01));
        assertThat(consumerCAssignment.activeTasks, hasItems(task02));
    }

    private List<TopicPartition> allAssignedStandbyPartitions(final Map<String, PartitionAssignor.Assignment> assignments) {
        final List<TopicPartition> assignedPartitions = new ArrayList<>();
        for (final PartitionAssignor.Assignment assignment : assignments.values()) {
            final AssignmentInfo assignmentInfo = AssignmentInfo.decode(assignment.userData());
            for (final Set<TopicPartition> topicPartitions : assignmentInfo.standbyTasks.values()) {
                assignedPartitions.addAll(topicPartitions);
            }
        }
        sortPartitions(assignedPartitions);
        return assignedPartitions;
    }

    private List<TopicPartition> allAssignedActivePartitions(final Map<String, PartitionAssignor.Assignment> assignments) {
        final List<TopicPartition> assignedPartitions = new ArrayList<>();
        for (final PartitionAssignor.Assignment assignment : assignments.values()) {
            assignedPartitions.addAll(assignment.partitions());
        }
        sortPartitions(assignedPartitions);
        return assignedPartitions;
    }

    private void sortPartitions(final List<TopicPartition> assignedPartitions) {
        Collections.sort(assignedPartitions, new Comparator<TopicPartition>() {
            @Override
            public int compare(final TopicPartition left, final TopicPartition right) {
                return left.partition() - right.partition();
            }
        });
    }

}