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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains all information related to a member within a Streams group.
 * <p>
 * This class is immutable and is fully backed by records stored in the __consumer_offsets topic.
 *
 * @param memberId                      The ID of the member.
 * @param memberEpoch                   The current epoch of the member.
 * @param previousMemberEpoch           The previous epoch of the member.
 * @param state                         The current state of the member.
 * @param instanceId                    The instance ID of the member.
 * @param rackId                        The rack ID of the member.
 * @param clientId                      The client ID of the member.
 * @param clientHost                    The host of the member.
 * @param rebalanceTimeoutMs            The rebalance timeout in milliseconds.
 * @param topologyEpoch                 The epoch of the topology the member uses.
 * @param processId                     The ID of the Streams client that contains the member.
 * @param userEndpoint                  The user endpoint exposed for Interactive Queries by the Streams client that
 *                                      contains the member.
 * @param clientTags                    Tags of the client of the member used for rack-aware assignment.
 * @param assignedActiveTasks           Active tasks assigned to the member.
 *                                      The key of the map is the subtopology ID and the value is the set of partition IDs.
 * @param assignedStandbyTasks          Standby tasks assigned to the member.
 *                                      The key of the map is the subtopology ID and the value is the set of partition IDs.
 * @param assignedWarmupTasks           Warm-up tasks assigned to the member.
 *                                      The key of the map is the subtopology ID and the value is the set of partition IDs.
 * @param activeTasksPendingRevocation  Active tasks assigned to the member pending revocation.
 *                                      The key of the map is the subtopology ID and the value is the set of partition IDs.
 * @param standbyTasksPendingRevocation Standby tasks assigned to the member pending revocation.
 *                                      The key of the map is the subtopology ID and the value is the set of partition IDs.
 * @param warmupTasksPendingRevocation  Warm-up tasks assigned to the member pending revocation.
 *                                      The key of the map is the subtopology ID and the value is the set of partition IDs.
 */
@SuppressWarnings("checkstyle:JavaNCSS")
public record StreamsGroupMember(String memberId,
                                 int memberEpoch,
                                 int previousMemberEpoch,
                                 MemberState state,
                                 Optional<String> instanceId,
                                 Optional<String> rackId,
                                 String clientId,
                                 String clientHost,
                                 int rebalanceTimeoutMs,
                                 int topologyEpoch,
                                 String processId,
                                 Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint,
                                 Map<String, String> clientTags,
                                 Map<String, Set<Integer>> assignedActiveTasks,
                                 Map<String, Set<Integer>> assignedStandbyTasks,
                                 Map<String, Set<Integer>> assignedWarmupTasks,
                                 Map<String, Set<Integer>> activeTasksPendingRevocation,
                                 Map<String, Set<Integer>> standbyTasksPendingRevocation,
                                 Map<String, Set<Integer>> warmupTasksPendingRevocation) {

    public StreamsGroupMember {
        Objects.requireNonNull(memberId, "memberId cannot be null");
        Objects.requireNonNull(state, "state cannot be null");
        Objects.requireNonNull(instanceId, "instanceId cannot be null");
        Objects.requireNonNull(rackId, "rackId cannot be null");
        Objects.requireNonNull(clientId, "clientId cannot be null");
        Objects.requireNonNull(clientHost, "clientHost cannot be null");
        Objects.requireNonNull(processId, "processId cannot be null");
        Objects.requireNonNull(userEndpoint, "userEndpoint cannot be null");
        clientTags = Collections.unmodifiableMap(
            Objects.requireNonNull(clientTags, "clientTags cannot be null")
        );
        assignedActiveTasks = Collections.unmodifiableMap(
            Objects.requireNonNull(assignedActiveTasks, "assignedActiveTasks cannot be null")
        );
        assignedStandbyTasks = Collections.unmodifiableMap(
            Objects.requireNonNull(assignedStandbyTasks, "assignedStandbyTasks cannot be null")
        );
        assignedWarmupTasks = Collections.unmodifiableMap(
            Objects.requireNonNull(assignedWarmupTasks, "assignedWarmupTasks cannot be null")
        );
        activeTasksPendingRevocation = Collections.unmodifiableMap(
            Objects.requireNonNull(activeTasksPendingRevocation, "activeTasksPendingRevocation cannot be null")
        );
        standbyTasksPendingRevocation = Collections.unmodifiableMap(
            Objects.requireNonNull(standbyTasksPendingRevocation, "standbyTasksPendingRevocation cannot be null")
        );
        warmupTasksPendingRevocation = Collections.unmodifiableMap(
            Objects.requireNonNull(warmupTasksPendingRevocation, "warmupTasksPendingRevocation cannot be null")
        );
    }

    /**
     * A builder that facilitates the creation of a new member or the update of an existing one.
     * <p>
     * Please refer to the javadoc of {{@link StreamsGroupMember}} for the definition of the fields.
     */
    public static class Builder {

        private final String memberId;
        private int memberEpoch = 0;
        private int previousMemberEpoch = -1;
        private MemberState state = MemberState.STABLE;
        private Optional<String> instanceId = Optional.empty();
        private Optional<String> rackId = Optional.empty();
        private int rebalanceTimeoutMs = -1;
        private String clientId = "";
        private String clientHost = "";
        private int topologyEpoch = -1;
        private String processId = "";
        private Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint = Optional.empty();
        private Map<String, String> clientTags = Collections.emptyMap();
        private Map<String, Set<Integer>> assignedActiveTasks = Collections.emptyMap();
        private Map<String, Set<Integer>> assignedStandbyTasks = Collections.emptyMap();
        private Map<String, Set<Integer>> assignedWarmupTasks = Collections.emptyMap();
        private Map<String, Set<Integer>> activeTasksPendingRevocation = Collections.emptyMap();
        private Map<String, Set<Integer>> standbyTasksPendingRevocation = Collections.emptyMap();
        private Map<String, Set<Integer>> warmupTasksPendingRevocation = Collections.emptyMap();

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId, "memberId cannot be null");
        }

        public Builder(StreamsGroupMember member) {
            Objects.requireNonNull(member, "member cannot be null");

            this.memberId = member.memberId;
            this.memberEpoch = member.memberEpoch;
            this.previousMemberEpoch = member.previousMemberEpoch;
            this.instanceId = member.instanceId;
            this.rackId = member.rackId;
            this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.topologyEpoch = member.topologyEpoch;
            this.processId = member.processId;
            this.userEndpoint = member.userEndpoint;
            this.clientTags = member.clientTags;
            this.state = member.state;
            this.assignedActiveTasks = member.assignedActiveTasks;
            this.assignedStandbyTasks = member.assignedStandbyTasks;
            this.assignedWarmupTasks = member.assignedWarmupTasks;
            this.activeTasksPendingRevocation = member.activeTasksPendingRevocation;
            this.standbyTasksPendingRevocation = member.standbyTasksPendingRevocation;
            this.warmupTasksPendingRevocation = member.warmupTasksPendingRevocation;
        }

        public Builder updateMemberEpoch(int memberEpoch) {
            int currentMemberEpoch = this.memberEpoch;
            this.memberEpoch = memberEpoch;
            this.previousMemberEpoch = currentMemberEpoch;
            return this;
        }

        public Builder setMemberEpoch(int memberEpoch) {
            this.memberEpoch = memberEpoch;
            return this;
        }

        public Builder setPreviousMemberEpoch(int previousMemberEpoch) {
            this.previousMemberEpoch = previousMemberEpoch;
            return this;
        }

        public Builder setInstanceId(String instanceId) {
            this.instanceId = Optional.ofNullable(instanceId);
            return this;
        }

        public Builder maybeUpdateInstanceId(Optional<String> instanceId) {
            instanceId.ifPresent(this::setInstanceId);
            return this;
        }

        public Builder setRackId(String rackId) {
            this.rackId = Optional.ofNullable(rackId);
            return this;
        }

        public Builder maybeUpdateRackId(Optional<String> rackId) {
            rackId.ifPresent(this::setRackId);
            return this;
        }

        public Builder setRebalanceTimeoutMs(int rebalanceTimeoutMs) {
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            return this;
        }

        public Builder maybeUpdateRebalanceTimeoutMs(OptionalInt rebalanceTimeoutMs) {
            this.rebalanceTimeoutMs = rebalanceTimeoutMs.orElse(this.rebalanceTimeoutMs);
            return this;
        }

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setClientHost(String clientHost) {
            this.clientHost = clientHost;
            return this;
        }

        public Builder setState(MemberState state) {
            this.state = state;
            return this;
        }

        public Builder setTopologyEpoch(int topologyEpoch) {
            this.topologyEpoch = topologyEpoch;
            return this;
        }

        public Builder maybeUpdateTopologyEpoch(OptionalInt topologyEpoch) {
            this.topologyEpoch = topologyEpoch.orElse(this.topologyEpoch);
            return this;
        }

        public Builder setProcessId(String processId) {
            this.processId = processId;
            return this;
        }

        public Builder maybeUpdateProcessId(Optional<String> processId) {
            this.processId = processId.orElse(this.processId);
            return this;
        }

        public Builder setUserEndpoint(StreamsGroupMemberMetadataValue.Endpoint userEndpoint) {
            this.userEndpoint = Optional.ofNullable(userEndpoint);
            return this;
        }

        public Builder maybeUpdateUserEndpoint(Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint) {
            userEndpoint.ifPresent(this::setUserEndpoint);
            return this;
        }

        public Builder setClientTags(Map<String, String> clientTags) {
            this.clientTags = clientTags;
            return this;
        }

        public Builder maybeUpdateClientTags(Optional<Map<String, String>> clientTags) {
            this.clientTags = clientTags.orElse(this.clientTags);
            return this;
        }

        public Builder setAssignment(Assignment assignment) {
            this.assignedActiveTasks = assignment.activeTasks();
            this.assignedStandbyTasks = assignment.standbyTasks();
            this.assignedWarmupTasks = assignment.warmupTasks();
            return this;
        }

        public Builder setAssignedActiveTasks(Map<String, Set<Integer>> assignedActiveTasks) {
            this.assignedActiveTasks = assignedActiveTasks;
            return this;
        }

        public Builder setAssignedStandbyTasks(Map<String, Set<Integer>> assignedStandbyTasks) {
            this.assignedStandbyTasks = assignedStandbyTasks;
            return this;
        }

        public Builder setAssignedWarmupTasks(Map<String, Set<Integer>> assignedWarmupTasks) {
            this.assignedWarmupTasks = assignedWarmupTasks;
            return this;
        }

        public Builder setAssignmentPendingRevocation(Assignment assignment) {
            this.activeTasksPendingRevocation = assignment.activeTasks();
            this.standbyTasksPendingRevocation = assignment.standbyTasks();
            this.warmupTasksPendingRevocation = assignment.warmupTasks();
            return this;
        }

        public Builder setActiveTasksPendingRevocation(
            Map<String, Set<Integer>> activeTasksPendingRevocation) {
            this.activeTasksPendingRevocation = activeTasksPendingRevocation;
            return this;
        }

        public Builder setStandbyTasksPendingRevocation(
            Map<String, Set<Integer>> standbyTasksPendingRevocation) {
            this.standbyTasksPendingRevocation = standbyTasksPendingRevocation;
            return this;
        }

        public Builder setWarmupTasksPendingRevocation(
            Map<String, Set<Integer>> warmupTasksPendingRevocation) {
            this.warmupTasksPendingRevocation = warmupTasksPendingRevocation;
            return this;
        }

        public Builder updateWith(StreamsGroupMemberMetadataValue record) {
            setInstanceId(record.instanceId());
            setRackId(record.rackId());
            setClientId(record.clientId());
            setClientHost(record.clientHost());
            setRebalanceTimeoutMs(record.rebalanceTimeoutMs());
            setTopologyEpoch(record.topologyEpoch());
            setProcessId(record.processId());
            setUserEndpoint(record.userEndpoint());
            setClientTags(record.clientTags().stream().collect(Collectors.toMap(
                StreamsGroupMemberMetadataValue.KeyValue::key,
                StreamsGroupMemberMetadataValue.KeyValue::value
            )));
            return this;
        }

        public Builder updateWith(StreamsGroupCurrentMemberAssignmentValue record) {
            setMemberEpoch(record.memberEpoch());
            setPreviousMemberEpoch(record.previousMemberEpoch());
            setState(MemberState.fromValue(record.state()));
            setAssignedActiveTasks(assignmentFromTaskIds(record.activeTasks()));
            setAssignedStandbyTasks(assignmentFromTaskIds(record.standbyTasks()));
            setAssignedWarmupTasks(assignmentFromTaskIds(record.warmupTasks()));
            setActiveTasksPendingRevocation(
                assignmentFromTaskIds(record.activeTasksPendingRevocation()));
            setStandbyTasksPendingRevocation(
                assignmentFromTaskIds(record.standbyTasksPendingRevocation()));
            setWarmupTasksPendingRevocation(
                assignmentFromTaskIds(record.warmupTasksPendingRevocation()));
            return this;
        }

        private static Map<String, Set<Integer>> assignmentFromTaskIds(
            List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> topicPartitionsList
        ) {
            return topicPartitionsList.stream().collect(Collectors.toMap(
                StreamsGroupCurrentMemberAssignmentValue.TaskIds::subtopologyId,
                taskIds -> Set.copyOf(taskIds.partitions())));
        }

        public StreamsGroupMember build() {
            return new StreamsGroupMember(
                memberId,
                memberEpoch,
                previousMemberEpoch,
                state,
                instanceId,
                rackId,
                clientId,
                clientHost,
                rebalanceTimeoutMs,
                topologyEpoch,
                processId,
                userEndpoint,
                clientTags,
                assignedActiveTasks,
                assignedStandbyTasks,
                assignedWarmupTasks,
                activeTasksPendingRevocation,
                standbyTasksPendingRevocation,
                warmupTasksPendingRevocation
            );
        }
    }

    /**
     * @return The member ID.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * @return The current member epoch.
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * @return The previous member epoch.
     */
    public int previousMemberEpoch() {
        return previousMemberEpoch;
    }

    /**
     * @return The instance ID.
     */
    public Optional<String> instanceId() {
        return instanceId;
    }

    /**
     * @return The rack ID.
     */
    public Optional<String> rackId() {
        return rackId;
    }

    /**
     * @return The rebalance timeout in millis.
     */
    public int rebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }

    /**
     * @return The client ID.
     */
    public String clientId() {
        return clientId;
    }

    /**
     * @return The client host.
     */
    public String clientHost() {
        return clientHost;
    }

    /**
     * @return The topology epoch.
     */
    public int topologyEpoch() {
        return topologyEpoch;
    }

    /**
     * @return The process ID
     */
    public String processId() {
        return processId;
    }

    /**
     * @return The user endpoint
     */
    public Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint() {
        return userEndpoint;
    }

    /**
     * @return The client tags
     */
    public Map<String, String> clientTags() {
        return clientTags;
    }

    /**
     * @return The current state.
     */
    public MemberState state() {
        return state;
    }

    /**
     * @return True if the member is in the Stable state and at the desired epoch.
     */
    public boolean isReconciledTo(int targetAssignmentEpoch) {
        return state == MemberState.STABLE && memberEpoch == targetAssignmentEpoch;
    }

    /**
     * @return The set of assigned active tasks.
     */
    public Map<String, Set<Integer>> assignedActiveTasks() {
        return assignedActiveTasks;
    }

    /**
     * @return The set of assigned standby tasks.
     */
    public Map<String, Set<Integer>> assignedStandbyTasks() {
        return assignedStandbyTasks;
    }

    /**
     * @return The set of assigned warm-up tasks.
     */
    public Map<String, Set<Integer>> assignedWarmupTasks() {
        return assignedWarmupTasks;
    }

    /**
     * @return The set of active tasks awaiting revocation from the member.
     */
    public Map<String, Set<Integer>> activeTasksPendingRevocation() {
        return activeTasksPendingRevocation;
    }

    /**
     * @return The set of standby tasks awaiting revocation from the member.
     */
    public Map<String, Set<Integer>> standbyTasksPendingRevocation() {
        return standbyTasksPendingRevocation;
    }

    /**
     * @return The set of warmup tasks awaiting revocation from the member.
     */
    public Map<String, Set<Integer>> warmupTasksPendingRevocation() {
        return warmupTasksPendingRevocation;
    }

    /**
     * Creates a member description for the Streams group describe response from this member.
     *
     * @param targetAssignment The target assignment of this member in the corresponding group.
     *
     * @return The StreamsGroupMember mapped as StreamsGroupDescribeResponseData.Member.
     */
    public StreamsGroupDescribeResponseData.Member asStreamsGroupDescribeMember(
        Assignment targetAssignment
    ) {
        final StreamsGroupDescribeResponseData.Assignment describedTargetAssignment =
            new StreamsGroupDescribeResponseData.Assignment();

        if (targetAssignment != null) {
            describedTargetAssignment
                .setActiveTasks(taskIdsFromMap(targetAssignment.activeTasks()))
                .setStandbyTasks(taskIdsFromMap(targetAssignment.standbyTasks()))
                .setWarmupTasks(taskIdsFromMap(targetAssignment.warmupTasks()));
        }

        return new StreamsGroupDescribeResponseData.Member()
            .setMemberEpoch(memberEpoch)
            .setMemberId(memberId)
            .setAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(taskIdsFromMap(assignedActiveTasks))
                    .setStandbyTasks(taskIdsFromMap(assignedStandbyTasks))
                    .setWarmupTasks(taskIdsFromMap(assignedWarmupTasks)))
            .setTargetAssignment(describedTargetAssignment)
            .setClientHost(clientHost)
            .setClientId(clientId)
            .setInstanceId(instanceId.orElse(null))
            .setRackId(rackId.orElse(null))
            .setClientTags(clientTags.entrySet().stream().map(
                entry -> new StreamsGroupDescribeResponseData.KeyValue()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue())
            ).collect(Collectors.toList()))
            .setProcessId(processId)
            .setTopologyEpoch(topologyEpoch)
            .setUserEndpoint(
                userEndpoint.map(
                    endpoint -> new StreamsGroupDescribeResponseData.Endpoint()
                        .setHost(endpoint.host())
                        .setPort(endpoint.port())
                    ).orElse(null)
            );
    }

    private static List<StreamsGroupDescribeResponseData.TaskIds> taskIdsFromMap(
        Map<String, Set<Integer>> tasks
    ) {
        List<StreamsGroupDescribeResponseData.TaskIds> taskIds = new ArrayList<>();
        tasks.forEach((subtopologyId, partitionSet) -> {
            taskIds.add(new StreamsGroupDescribeResponseData.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(new ArrayList<>(partitionSet)));
        });
        return taskIds;
    }

    /**
     * @return True if the two provided members have different assigned active tasks.
     */
    public static boolean hasAssignedActiveTasksChanged(
        StreamsGroupMember member1,
        StreamsGroupMember member2
    ) {
        return !member1.assignedActiveTasks().equals(member2.assignedActiveTasks());
    }

    /**
     * @return True if the two provided members have different assigned active tasks.
     */
    public static boolean hasAssignedStandbyTasksChanged(
        StreamsGroupMember member1,
        StreamsGroupMember member2
    ) {
        return !member1.assignedStandbyTasks().equals(member2.assignedStandbyTasks());
    }

    /**
     * @return True if the two provided members have different assigned active tasks.
     */
    public static boolean hasAssignedWarmupTasksChanged(
        StreamsGroupMember member1,
        StreamsGroupMember member2
    ) {
        return !member1.assignedWarmupTasks().equals(member2.assignedWarmupTasks());
    }
}
