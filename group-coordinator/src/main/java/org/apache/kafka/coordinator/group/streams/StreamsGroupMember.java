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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * StreamsGroupMember contains all the information related to a member within a Streams group. This class is immutable and is fully backed
 * by records stored in the __consumer_offsets topic.
 */
public class StreamsGroupMember {

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
        private String instanceId = null;
        private String rackId = null;
        private int rebalanceTimeoutMs = -1;
        private String clientId = "";
        private String clientHost = "";
        private String topologyId;
        private String assignor;
        private String processId;
        private StreamsGroupMemberMetadataValue.Endpoint userEndpoint;
        private Map<String, String> clientTags;
        private Map<String, Set<Integer>> assignedActiveTasks = Collections.emptyMap();
        private Map<String, Set<Integer>> assignedStandbyTasks = Collections.emptyMap();
        private Map<String, Set<Integer>> assignedWarmupTasks = Collections.emptyMap();
        private Map<String, Set<Integer>> activeTasksPendingRevocation = Collections.emptyMap();

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId);
        }

        public Builder(StreamsGroupMember member) {
            Objects.requireNonNull(member);

            this.memberId = member.memberId;
            this.memberEpoch = member.memberEpoch;
            this.previousMemberEpoch = member.previousMemberEpoch;
            this.instanceId = member.instanceId;
            this.rackId = member.rackId;
            this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.topologyId = member.topologyId;
            this.assignor = member.assignor;
            this.processId = member.processId;
            this.userEndpoint = member.userEndpoint;
            this.clientTags = member.clientTags;
            this.state = member.state;
            this.assignedActiveTasks = member.assignedActiveTasks;
            this.assignedStandbyTasks = member.assignedStandbyTasks;
            this.assignedWarmupTasks = member.assignedWarmupTasks;
            this.activeTasksPendingRevocation = member.activeTasksPendingRevocation;
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
            this.instanceId = instanceId;
            return this;
        }

        public Builder maybeUpdateInstanceId(Optional<String> instanceId) {
            this.instanceId = instanceId.orElse(this.instanceId);
            return this;
        }

        public Builder setRackId(String rackId) {
            this.rackId = rackId;
            return this;
        }

        public Builder maybeUpdateRackId(Optional<String> rackId) {
            this.rackId = rackId.orElse(this.rackId);
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

        public StreamsGroupMember.Builder maybeUpdateAssignor(Optional<String> assignor) {
            this.assignor = assignor.orElse(this.assignor);
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

        public Builder setTopologyId(String topologyId) {
            this.topologyId = topologyId;
            return this;
        }

        public Builder maybeUpdateTopologyId(Optional<String> topologyId) {
            this.topologyId = topologyId.orElse(this.topologyId);
            return this;
        }

        public Builder setAssignor(String assignor) {
            this.assignor = assignor;
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
            this.userEndpoint = userEndpoint;
            return this;
        }

        public Builder maybeUpdateUserEndpoint(Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint) {
            this.userEndpoint = userEndpoint.orElse(this.userEndpoint);
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

        public Builder setActiveTasksPendingRevocation(Map<String, Set<Integer>> activeTasksPendingRevocation) {
            this.activeTasksPendingRevocation = activeTasksPendingRevocation;
            return this;
        }

        public Builder updateWith(StreamsGroupMemberMetadataValue record) {
            setInstanceId(record.instanceId());
            setRackId(record.rackId());
            setClientId(record.clientId());
            setClientHost(record.clientHost());
            setRebalanceTimeoutMs(record.rebalanceTimeoutMs());
            setTopologyId(record.topologyId());
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
            setActiveTasksPendingRevocation(assignmentFromTaskIds(record.activeTasksPendingRevocation()));
            return this;
        }

        private Map<String, Set<Integer>> assignmentFromTaskIds(
            List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> topicPartitionsList
        ) {
            return topicPartitionsList.stream().collect(Collectors.toMap(
                StreamsGroupCurrentMemberAssignmentValue.TaskIds::subtopology,
                taskIds -> Collections.unmodifiableSet(new HashSet<>(taskIds.partitions()))));
        }

        public StreamsGroupMember build() {
            return new StreamsGroupMember(
                memberId,
                memberEpoch,
                previousMemberEpoch,
                instanceId,
                rackId,
                rebalanceTimeoutMs,
                clientId,
                clientHost,
                topologyId,
                assignor,
                processId,
                userEndpoint,
                clientTags,
                state,
                assignedActiveTasks,
                assignedStandbyTasks,
                assignedWarmupTasks,
                activeTasksPendingRevocation
            );
        }
    }

    /**
     * The member id.
     */
    private final String memberId;

    /**
     * The current member epoch.
     */
    private final int memberEpoch;

    /**
     * The previous member epoch.
     */
    private final int previousMemberEpoch;

    /**
     * The member state.
     */
    private final MemberState state;

    /**
     * The instance id provided by the member.
     */
    private final String instanceId;

    /**
     * The rack id provided by the member.
     */
    private final String rackId;

    /**
     * The rebalance timeout provided by the member.
     */
    private final int rebalanceTimeoutMs;

    /**
     * The client id reported by the member.
     */
    private final String clientId;

    /**
     * The host reported by the member.
     */
    private final String clientHost;

    /**
     * The topology ID
     */
    private final String topologyId;

    /**
     * The assignor
     */
    private final String assignor;

    /**
     * The process ID
     */
    private final String processId;

    /**
     * The endpoint
     */
    private final StreamsGroupMemberMetadataValue.Endpoint userEndpoint;

    /**
     * The client tags
     */
    private final Map<String, String> clientTags;

    /**
     * Active tasks assigned to this member.
     */
    private final Map<String, Set<Integer>> assignedActiveTasks;

    /**
     * Standby tasks assigned to this member.
     */
    private final Map<String, Set<Integer>> assignedStandbyTasks;

    /**
     * Warmup tasks assigned to this member.
     */
    private final Map<String, Set<Integer>> assignedWarmupTasks;

    /**
     * Active tasks being revoked by this member.
     */
    private final Map<String, Set<Integer>> activeTasksPendingRevocation;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private StreamsGroupMember(
        String memberId,
        int memberEpoch,
        int previousMemberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        String topologyId,
        String assignor,
        String processId,
        StreamsGroupMemberMetadataValue.Endpoint userEndpoint,
        Map<String, String> clientTags,
        MemberState state,
        Map<String, Set<Integer>> assignedActiveTasks,
        Map<String, Set<Integer>> assignedStandbyTasks,
        Map<String, Set<Integer>> assignedWarmupTasks,
        Map<String, Set<Integer>> activeTasksPendingRevocation
    ) {
        this.memberId = memberId;
        this.memberEpoch = memberEpoch;
        this.previousMemberEpoch = previousMemberEpoch;
        this.state = state;
        this.instanceId = instanceId;
        this.rackId = rackId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.topologyId = topologyId;
        this.assignor = assignor;
        this.processId = processId;
        this.userEndpoint = userEndpoint;
        this.clientTags = clientTags;
        this.assignedActiveTasks = assignedActiveTasks;
        this.assignedStandbyTasks = assignedStandbyTasks;
        this.assignedWarmupTasks = assignedWarmupTasks;
        this.activeTasksPendingRevocation = activeTasksPendingRevocation;
    }

    /**
     * @return The member id.
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
     * @return The instance id.
     */
    public String instanceId() {
        return instanceId;
    }

    /**
     * @return The rack id.
     */
    public String rackId() {
        return rackId;
    }

    /**
     * @return The rebalance timeout in millis.
     */
    public int rebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }

    /**
     * @return The client id.
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
     * @return The topology ID
     */
    public String topologyId() {
        return topologyId;
    }

    /**
     * @return The assignor
     */
    public Optional<String> assignor() {
        return Optional.ofNullable(assignor);
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
    public StreamsGroupMemberMetadataValue.Endpoint userEndpoint() {
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

    private static List<ConsumerGroupDescribeResponseData.TopicPartitions> topicPartitionsFromMap(
        Map<Uuid, Set<Integer>> partitions,
        TopicsImage topicsImage
    ) {
        List<ConsumerGroupDescribeResponseData.TopicPartitions> topicPartitions = new ArrayList<>();
        partitions.forEach((topicId, partitionSet) -> {
            String topicName = lookupTopicNameById(topicId, topicsImage);
            if (topicName != null) {
                topicPartitions.add(new ConsumerGroupDescribeResponseData.TopicPartitions()
                    .setTopicId(topicId)
                    .setTopicName(topicName)
                    .setPartitions(new ArrayList<>(partitionSet)));
            }
        });
        return topicPartitions;
    }

    private static String lookupTopicNameById(
        Uuid topicId,
        TopicsImage topicsImage
    ) {
        TopicImage topicImage = topicsImage.getTopic(topicId);
        if (topicImage != null) {
            return topicImage.name();
        } else {
            return null;
        }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamsGroupMember that = (StreamsGroupMember) o;
        return memberEpoch == that.memberEpoch
            && previousMemberEpoch == that.previousMemberEpoch
            && rebalanceTimeoutMs == that.rebalanceTimeoutMs
            && Objects.equals(memberId, that.memberId)
            && state == that.state
            && Objects.equals(instanceId, that.instanceId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(clientId, that.clientId)
            && Objects.equals(clientHost, that.clientHost)
            && Objects.deepEquals(topologyId, that.topologyId)
            && Objects.equals(assignor, that.assignor)
            && Objects.equals(processId, that.processId)
            && Objects.equals(userEndpoint, that.userEndpoint)
            && Objects.equals(clientTags, that.clientTags)
            && Objects.equals(assignedActiveTasks, that.assignedActiveTasks)
            && Objects.equals(assignedStandbyTasks, that.assignedStandbyTasks)
            && Objects.equals(assignedWarmupTasks, that.assignedWarmupTasks)
            && Objects.equals(activeTasksPendingRevocation, that.activeTasksPendingRevocation);
    }

    @Override
    public int hashCode() {
        int result = memberId != null ? memberId.hashCode() : 0;
        result = 31 * result + memberEpoch;
        result = 31 * result + previousMemberEpoch;
        result = 31 * result + Objects.hashCode(state);
        result = 31 * result + Objects.hashCode(instanceId);
        result = 31 * result + Objects.hashCode(rackId);
        result = 31 * result + rebalanceTimeoutMs;
        result = 31 * result + Objects.hashCode(clientId);
        result = 31 * result + Objects.hashCode(clientHost);
        result = 31 * result + Objects.hashCode(topologyId);
        result = 31 * result + Objects.hashCode(assignor);
        result = 31 * result + Objects.hashCode(processId);
        result = 31 * result + Objects.hashCode(userEndpoint);
        result = 31 * result + Objects.hashCode(clientTags);
        result = 31 * result + Objects.hashCode(assignedActiveTasks);
        result = 31 * result + Objects.hashCode(assignedStandbyTasks);
        result = 31 * result + Objects.hashCode(assignedWarmupTasks);
        result = 31 * result + Objects.hashCode(activeTasksPendingRevocation);
        return result;
    }

    @Override
    public String toString() {
        return "StreamsGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", state='" + state + '\'' +
            ", instanceId='" + instanceId + '\'' +
            ", rackId='" + rackId + '\'' +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", assignedActiveTasks=" + assignedActiveTasks +
            ", assignedStandbyTasks=" + assignedStandbyTasks +
            ", assignedWarmupTasks=" + assignedWarmupTasks +
            ", activeTasksPendingRevocation=" + activeTasksPendingRevocation +
            ')';
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
