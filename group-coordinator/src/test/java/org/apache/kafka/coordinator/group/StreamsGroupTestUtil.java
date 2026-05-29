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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.streams.MemberState;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_PROCESS_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch;

class StreamsGroupTestUtil {

    static StreamsGroupMember.Builder streamsGroupMemberBuilderWithDefaults(String memberId) {
        return streamsGroupMemberBuilderWithDefaults(memberId, null);
    }

    static StreamsGroupMember.Builder streamsGroupMemberBuilderWithDefaults(String memberId, String instanceId) {
        return new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setRackId(null)
            .setInstanceId(instanceId)
            .setRebalanceTimeoutMs(1500)
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .setTopologyEpoch(0)
            .setClientTags(Map.of())
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setProcessId(DEFAULT_PROCESS_ID)
            .setUserEndpoint(null);
    }

    /**
     * Returns the default assignment configurations that would be used by the system.
     * This matches what streamsGroupAssignmentConfigs() would return.
     */
    static Map<String, String> getDefaultAssignmentConfigs() {
        // Use the same default value as GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT
        return new TreeMap<>(Map.of(
            "num.standby.replicas", String.valueOf(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT)
        ));
    }

    static List<StreamsGroupHeartbeatResponseData.TaskIds> mkResponseTasks(
        String subtopologyId,
        Integer... partitions
    ) {
        return List.of(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(Arrays.asList(partitions))
        );
    }

    static final int DEFAULT_REBALANCE_TIMEOUT_MS = 1500;

    static StreamsTopicFixture streamsTopicFixture(String subtopologyId, String topicName, int partitions) {
        return new StreamsTopicFixture(subtopologyId, topicName, partitions);
    }

    static StreamsGroupHeartbeatRequestData staticHeartbeat(String groupId, String memberId, String instanceId, int memberEpoch) {
        return new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setInstanceId(instanceId)
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch);
    }

    static StreamsGroupHeartbeatRequestData staticJoinHeartbeat(String groupId, String memberId, String instanceId, StreamsTopicFixture topic) {
        return staticHeartbeat(groupId, memberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
            .setProcessId(DEFAULT_PROCESS_ID)
            .setRebalanceTimeoutMs(DEFAULT_REBALANCE_TIMEOUT_MS)
            .setTopology(topic.topology)
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of());
    }

    static class StreamsTopicFixture {
        private final String subtopologyId;
        private final String topicName;
        private final Uuid topicId;
        private final StreamsGroupHeartbeatRequestData.Topology topology;
        private final CoordinatorMetadataImage metadataImage;
        private final long metadataHash;

        private StreamsTopicFixture(
            String subtopologyId,
            String topicName,
            int partitions
        ) {
            this.subtopologyId = subtopologyId;
            this.topicName = topicName;
            this.topicId = Uuid.randomUuid();
            this.topology = new StreamsGroupHeartbeatRequestData.Topology()
                .setSubtopologies(List.of(
                    new StreamsGroupHeartbeatRequestData.Subtopology()
                        .setSubtopologyId(subtopologyId)
                        .setSourceTopics(List.of(topicName))
                ));
            this.metadataImage = new MetadataImageBuilder()
                .addTopic(topicId, topicName, partitions)
                .buildCoordinatorMetadataImage();
            this.metadataHash = computeGroupHash(Map.of(
                topicName,
                computeTopicHash(topicName, metadataImage)
            ));
        }

        public Map.Entry<String, Set<Integer>> tasks(Integer... partitions) {
            return TaskAssignmentTestUtil.mkTasks(subtopologyId, partitions);
        }

        public TasksTuple targetAssignment(Integer... partitions) {
            return TaskAssignmentTestUtil.mkTasksTuple(
                TaskAssignmentTestUtil.TaskRole.ACTIVE,
                tasks(partitions)
            );
        }

        public TasksTupleWithEpochs assignedTasks(
            int epoch,
            Integer... partitions
        ) {
            return mkTasksTupleWithCommonEpoch(
                TaskAssignmentTestUtil.TaskRole.ACTIVE,
                epoch,
                tasks(partitions)
            );
        }

        public CoordinatorMetadataImage metadataImage() {
            return metadataImage;
        }

        public long metadataHash() {
            return metadataHash;
        }

        public StreamsGroupHeartbeatRequestData.Topology topology() {
            return topology;
        }

        public List<StreamsGroupHeartbeatResponseData.TaskIds> responseTasks(Integer... partitions) {
            return mkResponseTasks(subtopologyId, partitions);
        }

        public List<StreamsGroupHeartbeatRequestData.TaskIds> requestTasks(List<Integer> partitions) {
            return List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopologyId)
                    .setPartitions(partitions)
            );
        }
    }

    static StreamsGroupHeartbeatRequestData staticJoinHeartbeat(
        String groupId,
        String memberId,
        String instanceId,
        String processId
    ) {
        return staticHeartbeat(groupId, memberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
            .setProcessId(processId)
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of());
    }

    static TasksTupleWithEpochs resetAssignedTasksEpochsToZero(TasksTupleWithEpochs assignedTasks) {
        if (assignedTasks.isEmpty()) {
            return assignedTasks;
        }

        if (assignedTasks.activeTasksWithEpochs().isEmpty()) {
            return assignedTasks;
        }

        Map<String, Map<Integer, Integer>> resetActiveTasks = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Integer>> entry : assignedTasks.activeTasksWithEpochs().entrySet()) {
            Map<Integer, Integer> resetActiveTaskEpochs = new HashMap<>();
            for (Integer partitionId : entry.getValue().keySet()) {
                resetActiveTaskEpochs.put(partitionId, 0);
            }
            resetActiveTasks.put(entry.getKey(), resetActiveTaskEpochs);
        }
        return new TasksTupleWithEpochs(
            resetActiveTasks,
            assignedTasks.standbyTasks(),
            assignedTasks.warmupTasks()
        );
    }

}
