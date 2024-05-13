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
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;

import java.util.ArrayList;
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
        private MemberState state = MemberState.STABLE;
        private String instanceId = null;
        private String rackId = null;
        private int rebalanceTimeoutMs = -1;
        private String clientId = "";
        private String clientHost = "";
        private List<String> subscribedTopicNames = Collections.emptyList();
        private String subscribedTopicRegex = "";
        private String serverAssignorName = null;
        private Map<Uuid, Set<Integer>> assignedPartitions = Collections.emptyMap();
        private Map<Uuid, Set<Integer>> partitionsPendingRevocation = Collections.emptyMap();
        private ConsumerGroupMemberMetadataValue.ClassicMemberMetadata classicMemberMetadata = null;

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId);
        }

        public Builder(ConsumerGroupMember member) {
            Objects.requireNonNull(member);

            this.memberId = member.memberId;
            this.memberEpoch = member.memberEpoch;
            this.previousMemberEpoch = member.previousMemberEpoch;
            this.instanceId = member.instanceId;
            this.rackId = member.rackId;
            this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.subscribedTopicNames = member.subscribedTopicNames;
            this.subscribedTopicRegex = member.subscribedTopicRegex;
            this.serverAssignorName = member.serverAssignorName;
            this.state = member.state;
            this.assignedPartitions = member.assignedPartitions;
            this.partitionsPendingRevocation = member.partitionsPendingRevocation;
            this.classicMemberMetadata = member.classicMemberMetadata;
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

        public Builder setState(MemberState state) {
            this.state = state;
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

        public Builder setClassicMemberMetadata(ConsumerGroupMemberMetadataValue.ClassicMemberMetadata classicMemberMetadata) {
            this.classicMemberMetadata = classicMemberMetadata;
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
            setClassicMemberMetadata(record.classicMemberMetadata());
            return this;
        }

        public Builder updateWith(ConsumerGroupCurrentMemberAssignmentValue record) {
            setMemberEpoch(record.memberEpoch());
            setPreviousMemberEpoch(record.previousMemberEpoch());
            setState(MemberState.fromValue(record.state()));
            setAssignedPartitions(assignmentFromTopicPartitions(record.assignedPartitions()));
            setPartitionsPendingRevocation(assignmentFromTopicPartitions(record.partitionsPendingRevocation()));
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
            return new ConsumerGroupMember(
                memberId,
                memberEpoch,
                previousMemberEpoch,
                instanceId,
                rackId,
                rebalanceTimeoutMs,
                clientId,
                clientHost,
                subscribedTopicNames,
                subscribedTopicRegex,
                serverAssignorName,
                state,
                assignedPartitions,
                partitionsPendingRevocation,
                classicMemberMetadata
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
     * The partitions assigned to this member.
     */
    private final Map<Uuid, Set<Integer>> assignedPartitions;

    /**
     * The partitions being revoked by this member.
     */
    private final Map<Uuid, Set<Integer>> partitionsPendingRevocation;

    /**
     * The classic member metadata if the consumer uses the classic protocol.
     */
    private final ConsumerGroupMemberMetadataValue.ClassicMemberMetadata classicMemberMetadata;

    private ConsumerGroupMember(
        String memberId,
        int memberEpoch,
        int previousMemberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String serverAssignorName,
        MemberState state,
        Map<Uuid, Set<Integer>> assignedPartitions,
        Map<Uuid, Set<Integer>> partitionsPendingRevocation,
        ConsumerGroupMemberMetadataValue.ClassicMemberMetadata classicMemberMetadata
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
        this.subscribedTopicNames = subscribedTopicNames;
        this.subscribedTopicRegex = subscribedTopicRegex;
        this.serverAssignorName = serverAssignorName;
        this.assignedPartitions = assignedPartitions;
        this.partitionsPendingRevocation = partitionsPendingRevocation;
        this.classicMemberMetadata = classicMemberMetadata;
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
     * @return The supported classic protocols converted to JoinGroupRequestProtocolCollection.
     */
    public JoinGroupRequestData.JoinGroupRequestProtocolCollection supportedJoinGroupRequestProtocols() {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
        supportedClassicProtocols().ifPresent(classicProtocols -> classicProtocols.forEach(protocol ->
            protocols.add(
                new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(protocol.name())
                    .setMetadata(protocol.metadata())
            )
        ));
        return protocols;
    }

    /**
     * @return The session timeout if the member uses the classic protocol.
     */
    public Optional<Integer> classicProtocolSessionTimeout() {
        if (useClassicProtocol()) {
            return Optional.ofNullable(classicMemberMetadata.sessionTimeoutMs());
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return The classicMemberMetadata if the consumer uses the classic protocol.
     */
    public Optional<ConsumerGroupMemberMetadataValue.ClassicMemberMetadata> classicMemberMetadata() {
        return Optional.ofNullable(classicMemberMetadata);
    }

    /**
     * @return The list of protocols if the consumer uses the classic protocol.
     */
    public Optional<List<ConsumerGroupMemberMetadataValue.ClassicProtocol>> supportedClassicProtocols() {
        if (useClassicProtocol()) {
            return Optional.ofNullable(classicMemberMetadata.supportedProtocols());
        } else {
            return Optional.empty();
        }
    }

    /**
     * @param targetAssignment The target assignment of this member in the corresponding group.
     *
     * @return The ConsumerGroupMember mapped as ConsumerGroupDescribeResponseData.Member.
     */
    public ConsumerGroupDescribeResponseData.Member asConsumerGroupDescribeMember(
        Assignment targetAssignment,
        TopicsImage topicsImage
    ) {
        return new ConsumerGroupDescribeResponseData.Member()
            .setMemberEpoch(memberEpoch)
            .setMemberId(memberId)
            .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
                .setTopicPartitions(topicPartitionsFromMap(assignedPartitions, topicsImage)))
            .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
                .setTopicPartitions(topicPartitionsFromMap(
                    targetAssignment != null ? targetAssignment.partitions() : Collections.emptyMap(),
                    topicsImage
                )))
            .setClientHost(clientHost)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setSubscribedTopicNames(subscribedTopicNames)
            .setSubscribedTopicRegex(subscribedTopicRegex);
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

    /**
     * Converts the JoinGroupRequestProtocolCollection to a list of ClassicProtocol.
     *
     * @param protocols The JoinGroupRequestProtocolCollection.
     * @return The converted list of ClassicProtocol.
     */
    public static List<ConsumerGroupMemberMetadataValue.ClassicProtocol> classicProtocolListFromJoinRequestProtocolCollection(
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols
    ) {
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> newSupportedProtocols = new ArrayList<>();
        protocols.forEach(protocol ->
            newSupportedProtocols.add(
                new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                    .setName(protocol.name())
                    .setMetadata(protocol.metadata())
            )
        );
        return newSupportedProtocols;
    }

    /**
     * @return A boolean indicating whether the member uses the classic protocol.
     */
    public boolean useClassicProtocol() {
        return classicMemberMetadata != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerGroupMember that = (ConsumerGroupMember) o;
        return memberEpoch == that.memberEpoch
            && previousMemberEpoch == that.previousMemberEpoch
            && state == that.state
            && rebalanceTimeoutMs == that.rebalanceTimeoutMs
            && Objects.equals(memberId, that.memberId)
            && Objects.equals(instanceId, that.instanceId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(clientId, that.clientId)
            && Objects.equals(clientHost, that.clientHost)
            && Objects.equals(subscribedTopicNames, that.subscribedTopicNames)
            && Objects.equals(subscribedTopicRegex, that.subscribedTopicRegex)
            && Objects.equals(serverAssignorName, that.serverAssignorName)
            && Objects.equals(assignedPartitions, that.assignedPartitions)
            && Objects.equals(partitionsPendingRevocation, that.partitionsPendingRevocation)
            && Objects.equals(classicMemberMetadata, that.classicMemberMetadata);
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
        result = 31 * result + Objects.hashCode(subscribedTopicNames);
        result = 31 * result + Objects.hashCode(subscribedTopicRegex);
        result = 31 * result + Objects.hashCode(serverAssignorName);
        result = 31 * result + Objects.hashCode(assignedPartitions);
        result = 31 * result + Objects.hashCode(partitionsPendingRevocation);
        result = 31 * result + Objects.hashCode(classicMemberMetadata);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", state='" + state + '\'' +
            ", instanceId='" + instanceId + '\'' +
            ", rackId='" + rackId + '\'' +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", subscribedTopicNames=" + subscribedTopicNames +
            ", subscribedTopicRegex='" + subscribedTopicRegex + '\'' +
            ", serverAssignorName='" + serverAssignorName + '\'' +
            ", assignedPartitions=" + assignedPartitions +
            ", partitionsPendingRevocation=" + partitionsPendingRevocation +
            ", classicMemberMetadata='" + classicMemberMetadata + '\'' +
            ')';
    }

    /**
     * @return True of the two provided members have different assigned partitions.
     */
    public static boolean hasAssignedPartitionsChanged(
        ConsumerGroupMember member1,
        ConsumerGroupMember member2
    ) {
        return !member1.assignedPartitions().equals(member2.assignedPartitions());
    }
}
