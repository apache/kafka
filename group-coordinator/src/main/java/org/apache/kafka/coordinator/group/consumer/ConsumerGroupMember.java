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
 * within a consumer group. This class is immutable.
 */
public class ConsumerGroupMember {
    /**
     * A builder allowing to create a new member or update an
     * existing one.
     */
    public static class Builder {
        private final String memberId;
        private int memberEpoch = 0;
        private int previousMemberEpoch = -1;
        private int nextMemberEpoch = 0;
        private String instanceId = null;
        private String rackId = null;
        private int rebalanceTimeoutMs = -1;
        private String clientId = "";
        private String clientHost = "";
        private List<String> subscribedTopicNames = Collections.emptyList();
        private String subscribedTopicRegex = "";
        private String serverAssignorName = null;
        private List<ClientAssignor> clientAssignors = Collections.emptyList();
        private Map<Uuid, Set<Integer>> assigned = Collections.emptyMap();
        private Map<Uuid, Set<Integer>> revoking = Collections.emptyMap();
        private Map<Uuid, Set<Integer>> assigning = Collections.emptyMap();

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId);
        }

        public Builder(ConsumerGroupMember member) {
            Objects.requireNonNull(member);

            this.memberId = member.memberId;
            this.memberEpoch = member.memberEpoch;
            this.previousMemberEpoch = member.previousMemberEpoch;
            this.nextMemberEpoch = member.nextMemberEpoch;
            this.instanceId = member.instanceId;
            this.rackId = member.rackId;
            this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.subscribedTopicNames = member.subscribedTopicNames;
            this.subscribedTopicRegex = member.subscribedTopicRegex;
            this.serverAssignorName = member.serverAssignorName;
            this.clientAssignors = member.clientAssignors;
            this.assigned = member.assigned;
            this.revoking = member.revoking;
            this.assigning = member.assigning;
        }

        public Builder setMemberEpoch(int memberEpoch) {
            this.memberEpoch = memberEpoch;
            return this;
        }

        public Builder setPreviousMemberEpoch(int previousMemberEpoch) {
            this.previousMemberEpoch = previousMemberEpoch;
            return this;
        }

        public Builder setNextMemberEpoch(int nextMemberEpoch) {
            this.nextMemberEpoch = nextMemberEpoch;
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

        public Builder setAssigned(Map<Uuid, Set<Integer>> assigned) {
            this.assigned = assigned;
            return this;
        }

        public Builder setRevoking(Map<Uuid, Set<Integer>> revoking) {
            this.revoking = revoking;
            return this;
        }

        public Builder setAssigning(Map<Uuid, Set<Integer>> assigning) {
            this.assigning = assigning;
            return this;
        }

        public Builder mergeWith(ConsumerGroupMemberMetadataValue record) {
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

        public Builder mergeWith(ConsumerGroupCurrentMemberAssignmentValue record) {
            setMemberEpoch(record.memberEpoch());
            setPreviousMemberEpoch(record.previousMemberEpoch());
            setNextMemberEpoch(record.targetMemberEpoch());
            setAssigned(assignmentFromTopicPartitions(record.assigned()));
            setRevoking(assignmentFromTopicPartitions(record.revoking()));
            setAssigning(assignmentFromTopicPartitions(record.assigning()));
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
            if (!revoking.isEmpty()) {
                state = MemberState.REVOKING;
            } else if (!assigning.isEmpty()) {
                state = MemberState.ASSIGNING;
            } else {
                state = MemberState.STABLE;
            }

            return new ConsumerGroupMember(
                memberId,
                memberEpoch,
                previousMemberEpoch,
                nextMemberEpoch,
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
                assigned,
                revoking,
                assigning
            );
        }
    }

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
    private final int nextMemberEpoch;

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
     * The subscription pattern configured by the member,
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
    private final Map<Uuid, Set<Integer>> assigned;

    /**
     * The partitions being revoked by this member.
     */
    private final Map<Uuid, Set<Integer>> revoking;

    /**
     * The partitions waiting to be assigned to this
     * member. They will be assigned when they are
     * released by their previous owners.
     */
    private final Map<Uuid, Set<Integer>> assigning;

    private ConsumerGroupMember(
        String memberId,
        int memberEpoch,
        int previousMemberEpoch,
        int nextMemberEpoch,
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
        Map<Uuid, Set<Integer>> assigned,
        Map<Uuid, Set<Integer>> revoking,
        Map<Uuid, Set<Integer>> assigning
    ) {
        this.memberId = memberId;
        this.memberEpoch = memberEpoch;
        this.previousMemberEpoch = previousMemberEpoch;
        this.nextMemberEpoch = nextMemberEpoch;
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
        this.assigned = assigned;
        this.revoking = revoking;
        this.assigning = assigning;
    }

    public String memberId() {
        return memberId;
    }

    public int memberEpoch() {
        return memberEpoch;
    }

    public int previousMemberEpoch() {
        return previousMemberEpoch;
    }

    public int nextMemberEpoch() {
        return nextMemberEpoch;
    }

    public String instanceId() {
        return instanceId;
    }

    public String rackId() {
        return rackId;
    }

    public int rebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }

    public String clientId() {
        return clientId;
    }

    public String clientHost() {
        return clientHost;
    }

    public List<String> subscribedTopicNames() {
        return subscribedTopicNames;
    }

    public String subscribedTopicRegex() {
        return subscribedTopicRegex;
    }

    public Optional<String> serverAssignorName() {
        return Optional.ofNullable(serverAssignorName);
    }

    public List<ClientAssignor> clientAssignors() {
        return clientAssignors;
    }

    public MemberState state() {
        return state;
    }

    public Map<Uuid, Set<Integer>> assigned() {
        return assigned;
    }

    public Map<Uuid, Set<Integer>> revoking() {
        return revoking;
    }

    public Map<Uuid, Set<Integer>> assigning() {
        return assigning;
    }

    public String currentAssignmentSummary() {
        return "CurrentAssignment(" +
            ", memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", nextMemberEpoch=" + nextMemberEpoch +
            ", state=" + state +
            ", assigned=" + assigned +
            ", revoking=" + revoking +
            ", assigning=" + assigning +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerGroupMember that = (ConsumerGroupMember) o;

        if (memberEpoch != that.memberEpoch) return false;
        if (previousMemberEpoch != that.previousMemberEpoch) return false;
        if (nextMemberEpoch != that.nextMemberEpoch) return false;
        if (rebalanceTimeoutMs != that.rebalanceTimeoutMs) return false;
        if (!Objects.equals(memberId, that.memberId)) return false;
        if (!Objects.equals(instanceId, that.instanceId)) return false;
        if (!Objects.equals(rackId, that.rackId)) return false;
        if (!Objects.equals(clientId, that.clientId)) return false;
        if (!Objects.equals(clientHost, that.clientHost)) return false;
        if (!Objects.equals(subscribedTopicNames, that.subscribedTopicNames)) return false;
        if (!Objects.equals(subscribedTopicRegex, that.subscribedTopicRegex)) return false;
        if (!Objects.equals(serverAssignorName, that.serverAssignorName)) return false;
        if (!Objects.equals(clientAssignors, that.clientAssignors)) return false;
        if (!Objects.equals(assigned, that.assigned)) return false;
        if (!Objects.equals(revoking, that.revoking)) return false;
        return Objects.equals(assigning, that.assigning);
    }

    @Override
    public int hashCode() {
        int result = memberId != null ? memberId.hashCode() : 0;
        result = 31 * result + memberEpoch;
        result = 31 * result + previousMemberEpoch;
        result = 31 * result + nextMemberEpoch;
        result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);
        result = 31 * result + (rackId != null ? rackId.hashCode() : 0);
        result = 31 * result + rebalanceTimeoutMs;
        result = 31 * result + (clientId != null ? clientId.hashCode() : 0);
        result = 31 * result + (clientHost != null ? clientHost.hashCode() : 0);
        result = 31 * result + (subscribedTopicNames != null ? subscribedTopicNames.hashCode() : 0);
        result = 31 * result + (subscribedTopicRegex != null ? subscribedTopicRegex.hashCode() : 0);
        result = 31 * result + (serverAssignorName != null ? serverAssignorName.hashCode() : 0);
        result = 31 * result + (clientAssignors != null ? clientAssignors.hashCode() : 0);
        result = 31 * result + (assigned != null ? assigned.hashCode() : 0);
        result = 31 * result + (revoking != null ? revoking.hashCode() : 0);
        result = 31 * result + (assigning != null ? assigning.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", nextMemberEpoch=" + nextMemberEpoch +
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
            ", assigned=" + assigned +
            ", revoking=" + revoking +
            ", assigning=" + assigning +
            ')';
    }
}
