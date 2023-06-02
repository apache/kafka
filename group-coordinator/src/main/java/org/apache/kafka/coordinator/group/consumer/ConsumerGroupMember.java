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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ConsumerGroupMember contains all the information related to a member
 * within a consumer group. This class is immutable and is fully backed
 * by records stored in the __consumer_offsets topic.
 */
public class ConsumerGroupMember {
    /**
     * A builder that facilitates the creation of a new member or the update of
     * an existing one.
     *
     * Please refer to the javadoc of {{@link ConsumerGroupMember}} for the
     * definition of the fields.
     */
    public static class Builder {
        private final String memberId;
        private int memberEpoch = 0;
        private int previousMemberEpoch = -1;
        private int targetMemberEpoch = 0;
        private String instanceId = null;
        private String rackId = null;
        private int rebalanceTimeoutMs = -1;
        private String clientId = "";
        private String clientHost = "";
        private List<String> subscribedTopicNames = Collections.emptyList();
        private String subscribedTopicRegex = "";
        private String serverAssignorName = null;
        private List<ClientAssignor> clientAssignors = Collections.emptyList();
        private Map<Uuid, Set<Integer>> assignedPartitions = Collections.emptyMap();
        private Map<Uuid, Set<Integer>> partitionsPendingRevocation = Collections.emptyMap();
        private Map<Uuid, Set<Integer>> partitionsPendingAssignment = Collections.emptyMap();

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId);
        }

        public Builder(ConsumerGroupMember member) {
            Objects.requireNonNull(member);

            this.memberId = member.memberId;
            this.memberEpoch = member.memberEpoch;
            this.previousMemberEpoch = member.previousMemberEpoch;
            this.targetMemberEpoch = member.targetMemberEpoch;
            this.instanceId = member.instanceId;
            this.rackId = member.rackId;
            this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.subscribedTopicNames = member.subscribedTopicNames;
            this.subscribedTopicRegex = member.subscribedTopicRegex;
            this.serverAssignorName = member.serverAssignorName;
            this.clientAssignors = member.clientAssignors;
            this.assignedPartitions = member.assignedPartitions;
            this.partitionsPendingRevocation = member.partitionsPendingRevocation;
            this.partitionsPendingAssignment = member.partitionsPendingAssignment;
        }

        public Builder setMemberEpoch(int memberEpoch) {
            this.memberEpoch = memberEpoch;
            return this;
        }

        public Builder setPreviousMemberEpoch(int previousMemberEpoch) {
            this.previousMemberEpoch = previousMemberEpoch;
            return this;
        }

        public Builder setTargetMemberEpoch(int targetMemberEpoch) {
            this.targetMemberEpoch = targetMemberEpoch;
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

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setClientHost(String clientHost) {
            this.clientHost = clientHost;
            return this;
        }

        public Builder setSubscribedTopicNames(List<String> subscribedTopicNames) {
            this.subscribedTopicNames = subscribedTopicNames;
            this.subscribedTopicNames.sort(Comparator.naturalOrder());
            return this;
        }

        public Builder maybeUpdateSubscribedTopicNames(Optional<List<String>> subscribedTopicNames) {
            this.subscribedTopicNames = subscribedTopicNames.orElse(this.subscribedTopicNames);
            this.subscribedTopicNames.sort(Comparator.naturalOrder());
            return this;
        }

        public Builder setSubscribedTopicRegex(String subscribedTopicRegex) {
            this.subscribedTopicRegex = subscribedTopicRegex;
            return this;
        }

        public Builder maybeUpdateSubscribedTopicRegex(Optional<String> subscribedTopicRegex) {
            this.subscribedTopicRegex = subscribedTopicRegex.orElse(this.subscribedTopicRegex);
            return this;
        }

        public Builder setServerAssignorName(String serverAssignorName) {
            this.serverAssignorName = serverAssignorName;
            return this;
        }

        public Builder maybeUpdateServerAssignorName(Optional<String> serverAssignorName) {
            this.serverAssignorName = serverAssignorName.orElse(this.serverAssignorName);
            return this;
        }

        public Builder setClientAssignors(List<ClientAssignor> clientAssignors) {
            this.clientAssignors = clientAssignors;
            return this;
        }

        public Builder maybeUpdateClientAssignors(Optional<List<ClientAssignor>> clientAssignors) {
            this.clientAssignors = clientAssignors.orElse(this.clientAssignors);
            return this;
        }

        public Builder setAssignedPartitions(Map<Uuid, Set<Integer>> assignedPartitions) {
            this.assignedPartitions = assignedPartitions;
            return this;
        }

        public Builder setPartitionsPendingRevocation(Map<Uuid, Set<Integer>> partitionsPendingRevocation) {
            this.partitionsPendingRevocation = partitionsPendingRevocation;
            return this;
        }

        public Builder setPartitionsPendingAssignment(Map<Uuid, Set<Integer>> partitionsPendingAssignment) {
            this.partitionsPendingAssignment = partitionsPendingAssignment;
            return this;
        }

        public Builder updateWith(ConsumerGroupMemberMetadataValue record) {
            setInstanceId(record.instanceId());
            setRackId(record.rackId());
            setClientId(record.clientId());
            setClientHost(record.clientHost());
            setSubscribedTopicNames(record.subscribedTopicNames());
            setSubscribedTopicRegex(record.subscribedTopicRegex());
            setRebalanceTimeoutMs(record.rebalanceTimeoutMs());
            setServerAssignorName(record.serverAssignor());
            setClientAssignors(record.assignors().stream()
                .map(ClientAssignor::fromRecord)
                .collect(Collectors.toList()));
            return this;
        }

        public Builder updateWith(ConsumerGroupCurrentMemberAssignmentValue record) {
            setMemberEpoch(record.memberEpoch());
            setPreviousMemberEpoch(record.previousMemberEpoch());
            setTargetMemberEpoch(record.targetMemberEpoch());
            setAssignedPartitions(assignmentFromTopicPartitions(record.assignedPartitions()));
            setPartitionsPendingRevocation(assignmentFromTopicPartitions(record.partitionsPendingRevocation()));
            setPartitionsPendingAssignment(assignmentFromTopicPartitions(record.partitionsPendingAssignment()));
            return this;
        }

        private Map<Uuid, Set<Integer>> assignmentFromTopicPartitions(
            List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitionsList
        ) {
            return topicPartitionsList.stream().collect(Collectors.toMap(
                ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions::topicId,
                topicPartitions -> Collections.unmodifiableSet(new HashSet<>(topicPartitions.partitions()))));
        }

        public ConsumerGroupMember build() {
            MemberState state;
            if (!partitionsPendingRevocation.isEmpty()) {
                state = MemberState.REVOKING;
            } else if (!partitionsPendingAssignment.isEmpty()) {
                state = MemberState.ASSIGNING;
            } else {
                state = MemberState.STABLE;
            }

            return new ConsumerGroupMember(
                memberId,
                memberEpoch,
                previousMemberEpoch,
                targetMemberEpoch,
                instanceId,
                rackId,
                rebalanceTimeoutMs,
                clientId,
                clientHost,
                subscribedTopicNames,
                subscribedTopicRegex,
                serverAssignorName,
                clientAssignors,
                state,
                assignedPartitions,
                partitionsPendingRevocation,
                partitionsPendingAssignment
            );
        }
    }

    /**
     * The various states that a member can be in. For their definition,
     * refer to the documentation of {{@link CurrentAssignmentBuilder}}.
     */
    public enum MemberState {
        REVOKING("revoking"),
        ASSIGNING("assigning"),
        STABLE("stable");

        private final String name;

        MemberState(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
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
     * The next member epoch. This corresponds to the target
     * assignment epoch used to compute the current assigned,
     * revoking and assigning partitions.
     */
    private final int targetMemberEpoch;

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
     * The list of subscriptions (topic names) configured by the member.
     */
    private final List<String> subscribedTopicNames;

    /**
     * The subscription pattern configured by the member.
     */
    private final String subscribedTopicRegex;

    /**
     * The server side assignor selected by the member.
     */
    private final String serverAssignorName;

    /**
     * The states of the client side assignors of the member.
     */
    private final List<ClientAssignor> clientAssignors;

    /**
     * The member state.
     */
    private final MemberState state;

    /**
     * The partitions assigned to this member.
     */
    private final Map<Uuid, Set<Integer>> assignedPartitions;

    /**
     * The partitions being revoked by this member.
     */
    private final Map<Uuid, Set<Integer>> partitionsPendingRevocation;

    /**
     * The partitions waiting to be assigned to this
     * member. They will be assigned when they are
     * released by their previous owners.
     */
    private final Map<Uuid, Set<Integer>> partitionsPendingAssignment;

    private ConsumerGroupMember(
        String memberId,
        int memberEpoch,
        int previousMemberEpoch,
        int targetMemberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String serverAssignorName,
        List<ClientAssignor> clientAssignors,
        MemberState state,
        Map<Uuid, Set<Integer>> assignedPartitions,
        Map<Uuid, Set<Integer>> partitionsPendingRevocation,
        Map<Uuid, Set<Integer>> partitionsPendingAssignment
    ) {
        this.memberId = memberId;
        this.memberEpoch = memberEpoch;
        this.previousMemberEpoch = previousMemberEpoch;
        this.targetMemberEpoch = targetMemberEpoch;
        this.instanceId = instanceId;
        this.rackId = rackId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.subscribedTopicNames = subscribedTopicNames;
        this.subscribedTopicRegex = subscribedTopicRegex;
        this.serverAssignorName = serverAssignorName;
        this.clientAssignors = clientAssignors;
        this.state = state;
        this.assignedPartitions = assignedPartitions;
        this.partitionsPendingRevocation = partitionsPendingRevocation;
        this.partitionsPendingAssignment = partitionsPendingAssignment;
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
     * @return The target member epoch.
     */
    public int targetMemberEpoch() {
        return targetMemberEpoch;
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
     * @return The list of subscribed topic names.
     */
    public List<String> subscribedTopicNames() {
        return subscribedTopicNames;
    }

    /**
     * @return The regular expression based subscription.
     */
    public String subscribedTopicRegex() {
        return subscribedTopicRegex;
    }

    /**
     * @return The server side assignor or an empty optional.
     */
    public Optional<String> serverAssignorName() {
        return Optional.ofNullable(serverAssignorName);
    }

    /**
     * @return The list of client side assignors.
     */
    public List<ClientAssignor> clientAssignors() {
        return clientAssignors;
    }

    /**
     * @return The current state.
     */
    public MemberState state() {
        return state;
    }

    /**
     * @return The set of assigned partitions.
     */
    public Map<Uuid, Set<Integer>> assignedPartitions() {
        return assignedPartitions;
    }

    /**
     * @return The set of partitions awaiting revocation from the member.
     */
    public Map<Uuid, Set<Integer>> partitionsPendingRevocation() {
        return partitionsPendingRevocation;
    }

    /**
     * @return The set of partitions awaiting assignment to the member.
     */
    public Map<Uuid, Set<Integer>> partitionsPendingAssignment() {
        return partitionsPendingAssignment;
    }

    /**
     * @return A string representation of the current assignment state.
     */
    public String currentAssignmentSummary() {
        return "CurrentAssignment(memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", targetMemberEpoch=" + targetMemberEpoch +
            ", state=" + state +
            ", assignedPartitions=" + assignedPartitions +
            ", partitionsPendingRevocation=" + partitionsPendingRevocation +
            ", partitionsPendingAssignment=" + partitionsPendingAssignment +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerGroupMember that = (ConsumerGroupMember) o;
        return memberEpoch == that.memberEpoch
            && previousMemberEpoch == that.previousMemberEpoch
            && targetMemberEpoch == that.targetMemberEpoch
            && rebalanceTimeoutMs == that.rebalanceTimeoutMs
            && Objects.equals(memberId, that.memberId)
            && Objects.equals(instanceId, that.instanceId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(clientId, that.clientId)
            && Objects.equals(clientHost, that.clientHost)
            && Objects.equals(subscribedTopicNames, that.subscribedTopicNames)
            && Objects.equals(subscribedTopicRegex, that.subscribedTopicRegex)
            && Objects.equals(serverAssignorName, that.serverAssignorName)
            && Objects.equals(clientAssignors, that.clientAssignors)
            && Objects.equals(assignedPartitions, that.assignedPartitions)
            && Objects.equals(partitionsPendingRevocation, that.partitionsPendingRevocation)
            && Objects.equals(partitionsPendingAssignment, that.partitionsPendingAssignment);
    }

    @Override
    public int hashCode() {
        int result = memberId != null ? memberId.hashCode() : 0;
        result = 31 * result + memberEpoch;
        result = 31 * result + previousMemberEpoch;
        result = 31 * result + targetMemberEpoch;
        result = 31 * result + Objects.hashCode(instanceId);
        result = 31 * result + Objects.hashCode(rackId);
        result = 31 * result + rebalanceTimeoutMs;
        result = 31 * result + Objects.hashCode(clientId);
        result = 31 * result + Objects.hashCode(clientHost);
        result = 31 * result + Objects.hashCode(subscribedTopicNames);
        result = 31 * result + Objects.hashCode(subscribedTopicRegex);
        result = 31 * result + Objects.hashCode(serverAssignorName);
        result = 31 * result + Objects.hashCode(clientAssignors);
        result = 31 * result + Objects.hashCode(assignedPartitions);
        result = 31 * result + Objects.hashCode(partitionsPendingRevocation);
        result = 31 * result + Objects.hashCode(partitionsPendingAssignment);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", targetMemberEpoch=" + targetMemberEpoch +
            ", instanceId='" + instanceId + '\'' +
            ", rackId='" + rackId + '\'' +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", subscribedTopicNames=" + subscribedTopicNames +
            ", subscribedTopicRegex='" + subscribedTopicRegex + '\'' +
            ", serverAssignorName='" + serverAssignorName + '\'' +
            ", clientAssignors=" + clientAssignors +
            ", state=" + state +
            ", assignedPartitions=" + assignedPartitions +
            ", partitionsPendingRevocation=" + partitionsPendingRevocation +
            ", partitionsPendingAssignment=" + partitionsPendingAssignment +
            ')';
    }
}
