/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.coordinator.group.generic;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.generic.GenericGroupMember.MemberSummary;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.CompletingRebalance;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.Dead;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.Empty;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.PreparingRebalance;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.Stable;

/**
 * Java rewrite of {@link kafka.coordinator.group.GroupMetadata} that is used
 * by the new group coordinator (KIP-848).
 */
public class GenericGroup {

    public static class GroupSummary {
        private final String state;
        private final String protocolName;
        private final String protocolType;
        private final List<MemberSummary> members;

        public GroupSummary(String state,
                            String protocolName,
                            String protocolType,
                            List<MemberSummary> members) {

            this.state = state;
            this.protocolName = protocolName;
            this.protocolType = protocolType;
            this.members = members;
        }

        public String state() {
            return this.state;
        }

        public String protocolName() {
            return this.protocolName;
        }

        public String protocolType() {
            return protocolType;
        }

        public List<MemberSummary> members() {
            return this.members;
        }

    }

    public static final int NO_GENERATION = -1;
    public static final String NO_PROTOCOL_NAME = "";
    public static final String NO_LEADER = "";
    private static final String MEMBER_ID_DELIMITER = "-";
    /**
     * The slf4j log context, used to create new loggers.
     */
    private final LogContext logContext;

    /**
     * The slf4j logger.
     */
    private final Logger log;
    private final String groupId;
    private final GenericGroupState initialState;
    private final Time time;
    private final Lock lock = new ReentrantLock();
    private GenericGroupState state;
    private Optional<Long> currentStateTimestamp;
    private Optional<String> protocolType = Optional.empty();
    private Optional<String> protocolName = Optional.empty();
    private int generationId = 0;
    private Optional<String> leaderId = Optional.empty();
    private final Map<String, GenericGroupMember> members = new HashMap<>();
    private final Map<String, String> staticMembers = new HashMap<>();
    private final Set<String> pendingMembers = new HashSet<>();
    private int numMembersAwaitingJoinResponse = 0;
    private final Map<String, Integer> supportedProtocols = new HashMap<>();
    private final Map<TopicPartition, CommitRecordMetadataAndOffset> offsets = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsetCommits = new HashMap<>();
    private final Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> pendingTransactionalOffsetCommits = new HashMap<>();
    private boolean receivedConsumerOffsetCommits = false;
    private boolean receivedTransactionOffsetCommits = false;
    private final Set<String> pendingSyncMembers = new HashSet<>();
    private Optional<Set<String>> subscribedTopics = Optional.empty();
    private boolean newMemberAdded = false;

    public GenericGroup(
        LogContext logContext,
        String groupId,
        GenericGroupState initialState,
        Time time
    ) {
        this.logContext = logContext;
        this.log = logContext.logger(GenericGroup.class);
        this.groupId = groupId;
        this.initialState = initialState;
        this.state = initialState;
        this.time = time;
        this.currentStateTimestamp = Optional.of(time.milliseconds());
    }

    public boolean is(GenericGroupState groupState) {
        return this.state == groupState;
    }

    public boolean has(String memberId) {
        return members.containsKey(memberId);
    }

    public GenericGroupMember get(String memberId) {
        return members.get(memberId);
    }

    public int size() {
        return members.size();
    }

    public boolean isLeader(String memberId) {
        return leaderId.map(id -> id.equals(memberId)).orElse(false);
    }

    public String leaderOrNull() {
        return leaderId.orElse(null);
    }

    public long currentStateTimestampOrDefault() {
        return currentStateTimestamp.orElse(-1L);
    }

    public boolean isGenericGroup() {
        return protocolType.map(type ->
            type.equals(ConsumerProtocol.PROTOCOL_TYPE)
        ).orElse(false);
    }

    public void add(GenericGroupMember member) {
        add(member, null);
    }

    public void add(GenericGroupMember member, CompletableFuture<JoinGroupResponseData> callback) {
        member.groupInstanceId().ifPresent(instanceId -> {
            if (staticMembers.containsKey(instanceId)) {
                throw new IllegalStateException("Static member with groupInstanceId=" +
                    instanceId + " cannot be added to group " + groupId + " since" +
                    " it is already a member.");
            }
            staticMembers.put(instanceId, member.memberId());
        });

        if (members.isEmpty()) {
            this.protocolType = Optional.of(member.protocolType());
        }

        assert(Objects.equals(this.protocolType.orElse(null), member.protocolType()));
        assert(supportsProtocols(member.protocolType(),
            GenericGroupMember.plainProtocolSet(member.supportedProtocols())));

        if (!leaderId.isPresent()) {
            leaderId = Optional.of(member.memberId());
        }

        members.put(member.memberId(), member);
        incrementSupportedProtocols(member);
        member.setAwaitingJoinCallback(callback);

        if (member.isAwaitingJoin()) {
            numMembersAwaitingJoinResponse++;
        }

        pendingMembers.remove(member.memberId());
    }

    public void remove(String memberId) {
        GenericGroupMember removedMember = members.remove(memberId);
        if (removedMember != null) {
            decrementSupportedProtocols(removedMember);
            if (removedMember.isAwaitingJoin()) {
                numMembersAwaitingJoinResponse--;
            }

            removedMember.groupInstanceId().ifPresent(staticMembers::remove);
        }

        if (isLeader(memberId)) {
            Iterator<String> iter = members.keySet().iterator();
            String newLeader = iter.hasNext() ? iter.next() : null;
            leaderId = Optional.ofNullable(newLeader);
        }

        pendingMembers.remove(memberId);
        pendingSyncMembers.remove(memberId);
    }

    /**
     * Check whether current leader has rejoined. If not, try to find another joined member to be
     * new leader. Return false if
     *   1. the group is currently empty (has no designated leader)
     *   2. no member rejoined
     */
    public boolean maybeElectNewJoinedLeader() {
        if (leaderId.isPresent()) {
            GenericGroupMember currentLeader = get(leaderId.get());
            if (!currentLeader.isAwaitingJoin()) {
                for (GenericGroupMember member : members.values()) {
                    if (member.isAwaitingJoin()) {
                        leaderId = Optional.of(member.memberId());
                        log.info("Group leader [memberId: {}, groupInstanceId: {}] " +
                            "failed to join before the rebalance timeout. Member {} " +
                            "was elected as the new leader.",
                            currentLeader.memberId(),
                            currentLeader.groupInstanceId().orElse("None"),
                            member
                        );
                        return true;
                    }
                    log.info("Group leader [memberId: {}, groupInstanceId: {}] " +
                        "failed to join before the rebalance timeout and the " +
                        "group couldn't proceed to the next generation because " +
                        "no member joined.",
                        currentLeader.memberId(),
                        currentLeader.groupInstanceId().orElse("None")
                    );
                    return false;
                }
            }
            return false;
        }
        return false;
    }

    /**
     * [For static members only]: Replace the old member id with the new one,
     * keep everything else unchanged and return the updated member.
     */
    public GenericGroupMember replaceStaticMember(
        String groupInstanceId,
        String oldMemberId,
        String newMemberId
    ) {
        GenericGroupMember removedMember = members.remove(oldMemberId);
        if (removedMember == null) {
            throw new IllegalArgumentException("Cannot replace non-existing member id " + oldMemberId);
        }

        // Fence potential duplicate member immediately if someone awaits join/sync callback.
        JoinGroupResponseData joinGroupResponse = new JoinGroupResponseData()
            .setMembers(Collections.emptyList())
            .setMemberId(oldMemberId)
            .setGenerationId(NO_GENERATION)
            .setProtocolName(null)
            .setProtocolType(null)
            .setLeader(NO_LEADER)
            .setSkipAssignment(false)
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code());
        maybeInvokeJoinCallback(removedMember, joinGroupResponse);

        SyncGroupResponseData syncGroupResponse = new SyncGroupResponseData()
            .setAssignment(new byte[0])
            .setProtocolName(null)
            .setProtocolType(null)
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code());
        maybeInvokeSyncCallback(removedMember, syncGroupResponse);

        GenericGroupMember newMember = new GenericGroupMember.Builder(newMemberId)
            .setGroupInstanceId(removedMember.groupInstanceId())
            .setClientId(removedMember.clientId())
            .setClientHost(removedMember.clientHost())
            .setRebalanceTimeoutMs(removedMember.rebalanceTimeoutMs())
            .setSessionTimeoutMs(removedMember.sessionTimeoutMs())
            .setProtocolType(removedMember.protocolType())
            .setSupportedProtocols(removedMember.supportedProtocols())
            .setAssignment(removedMember.assignment())
            .build();

        members.put(newMemberId, newMember);

        if (isLeader(oldMemberId)) {
            leaderId = Optional.of(newMemberId);
        }

        staticMembers.put(groupInstanceId, newMemberId);
        return removedMember;
    }

    public boolean isPendingMember(String memberId) {
        return pendingMembers.contains(memberId);
    }

    public boolean addPendingMember(String memberId) {
        if (has(memberId)) {
            throw new IllegalStateException("Attept to add pending member " + memberId +
                " which is already a stable member of the group.");
        }
        return pendingMembers.add(memberId);
    }

    public boolean removePendingSyncMember(String memberId) {
        if (has(memberId)) {
            throw new IllegalStateException("Attept to add pending member " + memberId +
                " which is already a stable member of the group.");
        }
        return pendingSyncMembers.remove(memberId);
    }
    
    public boolean hasReceivedSyncFromAllMembers() {
        return pendingSyncMembers.isEmpty();
    }
    
    public Set<String> allPendingSyncMembers() {
        return pendingSyncMembers;
    }
    
    public void clearPendingSyncMembers() {
        pendingSyncMembers.clear();
    }
    
    public boolean hasStaticMember(String groupInstanceId) {
        return staticMembers.containsKey(groupInstanceId);
    }
    
    public String currentStaticMemberId(String groupInstanceId) {
        return staticMembers.get(groupInstanceId);
    }
    
    public GenericGroupState currentState() {
        return state;
    }
    
    public Map<String, GenericGroupMember> notYetRejoinedMembers() {
        Map<String, GenericGroupMember> notYetRejoinedMembers = new HashMap<>();
        members.values().forEach(member -> {
            if (!member.isAwaitingJoin()) {
                notYetRejoinedMembers.put(member.memberId(), member);
            }
        });
        return notYetRejoinedMembers;
    }
    
    public boolean hasAllMembersJoined() {
        return members.size() == numMembersAwaitingJoinResponse && pendingMembers.isEmpty();
    }

    public Set<String> allMembers() {
        return members.keySet();
    }

    public Set<String> allStaticMembers() {
        return staticMembers.keySet();
    }

    // For testing only.
    Set<String> allDynamicMembers() {
        Set<String> dynamicMemberSet = new HashSet<>(allMembers());
        staticMembers.values().forEach(dynamicMemberSet::remove);
        return dynamicMemberSet;
    }

    public int numPending() {
        return pendingMembers.size();
    }

    public int numAwaiting() {
        return numMembersAwaitingJoinResponse;
    }

    public Collection<GenericGroupMember> allGenericGroupMembers() {
        return members.values();
    }

    public int rebalanceTimeoutMs() {
        int maxRebalanceTimeoutMs = 0;
        for (GenericGroupMember member : members.values()) {
            maxRebalanceTimeoutMs = Math.max(maxRebalanceTimeoutMs, member.rebalanceTimeoutMs());
        }
        return maxRebalanceTimeoutMs;
    }

    public String generateMemberId(String clientId, Optional<String> groupInstanceId) {
        return groupInstanceId.map(s -> s + MEMBER_ID_DELIMITER + UUID.randomUUID())
            .orElseGet(() -> clientId + MEMBER_ID_DELIMITER + UUID.randomUUID());
    }

    /**
     * Verify the member.id is up to date for static members. Return true if both conditions met:
     *   1. given member is a known static member to group
     *   2. group stored member.id doesn't match with given member.id
     */
    public boolean isStaticMemberFenced(
        String groupInstanceId,
        String memberId
    ) {
        String existingMemberId = currentStaticMemberId(groupInstanceId);
        return existingMemberId != null && !existingMemberId.equals(memberId);
    }

    public boolean canRebalance() {
        return PreparingRebalance.validPreviousStates().contains(state);
    }

    public void transitionTo(GenericGroupState groupState) {
        assertValidTransition(groupState);
        state = groupState;
        currentStateTimestamp = Optional.of(time.milliseconds());
    }

    public String selectProtocol() {
        if (members.isEmpty()) {
            throw new IllegalStateException("Cannot select protocol for empty group");
        }

        // select the protocol for this group which is supported by all members
        Set<String> candidates = candidateProtocols();

        // let each member vote for one of the protocols
        Map<String, Integer> votesByProtocol = new HashMap<>();
        allGenericGroupMembers().stream().map(member -> member.vote(candidates))
            .forEach(protocolName -> {
                int count = votesByProtocol.getOrDefault(protocolName, 0);
                votesByProtocol.put(protocolName, count + 1);
            });

        // choose the one with the most votes
        return votesByProtocol.entrySet().stream()
            .max(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey).orElse(null);
    }

    private void incrementSupportedProtocols(GenericGroupMember member) {
        member.supportedProtocols().forEach( protocol -> {
           int count = supportedProtocols.getOrDefault(protocol.name(), 0);
           supportedProtocols.put(protocol.name(), count + 1);
        });
    }

    private void decrementSupportedProtocols(GenericGroupMember member) {
        member.supportedProtocols().forEach( protocol -> {
            int count = supportedProtocols.getOrDefault(protocol.name(), 0);
            supportedProtocols.put(protocol.name(), count - 1);
        });
    }

    private Set<String> candidateProtocols() {
        // get the set of protocols that are commonly supported by all members
        return supportedProtocols.entrySet().stream()
            .filter(protocol -> protocol.getValue() == members.size())
            .map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    public boolean supportsProtocols(String memberProtocolType, Set<String> memberProtocols) {
        if (is(Empty)) {
            return !memberProtocolType.isEmpty() && !memberProtocols.isEmpty();
        } else {
            return protocolType.map(type -> type.equals(memberProtocolType)).orElse(false) &&
                memberProtocols.stream().anyMatch(
                    name -> supportedProtocols.getOrDefault(name, 0) == members.size()
                );
        }
    }

    public Optional<Set<String>> subscribedTopics() {
        return subscribedTopics;
    }

    /**
     * Returns true if the consumer group is actively subscribed to the topic. When the consumer
     * group does not know, because the information is not available yet or because the it has
     * failed to parse the Consumer Protocol, it returns true to be safe.
     */
    public boolean isSubscribedToTopic(String topic) {
        return subscribedTopics.map(topics -> topics.contains(topic))
            .orElse(true);
    }

    /**
     * Collects the set of topics that the members are subscribed to when the Protocol Type is equal
     * to 'consumer'. None is returned if
     * - the protocol type is not equal to 'consumer';
     * - the protocol is not defined yet; or
     * - the protocol metadata does not comply with the schema.
     */
    Optional<Set<String>> computeSubscribedTopics() {
        if (!protocolType.isPresent()) {
            return Optional.empty();
        }
        String type = protocolType.get();
        if (!type.equals(ConsumerProtocol.PROTOCOL_TYPE)) {
            return Optional.empty();
        }
        if (members.isEmpty()) {
            return Optional.of(Collections.emptySet());
        }

        if (protocolName.isPresent()) {
            try {
                Set<String> allSubscribedTopics = new HashSet<>();
                members.values().forEach( member -> {
                    ByteBuffer buffer = ByteBuffer.wrap(member.metadata(protocolName.get()));
                    ConsumerProtocol.deserializeVersion(buffer);
                    allSubscribedTopics.addAll(new HashSet<>(
                        ConsumerProtocol.deserializeSubscription(buffer, (short) 0).topics()
                    ));
                });
                return Optional.of(allSubscribedTopics);
            } catch (SchemaException e) {
                log.warn("Failed to parse Consumer Protocol " + ConsumerProtocol.PROTOCOL_TYPE + ":" +
                    protocolName.get() + " of group " + groupId + ". " +
                    "Consumer group coordinator is not aware of the subscribed topics.", e);
            }
        }

        return Optional.empty();
    }

    public void updateMember(
        GenericGroupMember member,
        List<Protocol> protocols,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        CompletableFuture<JoinGroupResponseData> callback
    ) {
        decrementSupportedProtocols(member);
        member.setSupportedProtocols(protocols);
        incrementSupportedProtocols(member);
        member.setRebalanceTimeoutMs(rebalanceTimeoutMs);
        member.setSessionTimeoutMs(sessionTimeoutMs);

        if (callback != null && !member.isAwaitingJoin()) {
            numMembersAwaitingJoinResponse++;
        } else if (callback == null && member.isAwaitingJoin()) {
            numMembersAwaitingJoinResponse--;
        }
        member.setAwaitingJoinCallback(callback);
    }

    public void maybeInvokeJoinCallback(
        GenericGroupMember member,
        JoinGroupResponseData response
    ) {
        if (member.isAwaitingJoin()) {
            if (!member.awaitingJoinCallback().complete(response)) {
                log.error("Failed to invoke join callback for {}", member);
                response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                member.awaitingJoinCallback().complete(response);
            }
            member.setAwaitingJoinCallback(null);
            numMembersAwaitingJoinResponse--;
        }
    }

    /**
     * @return true if a sync callback actually performs.
     */
    public boolean maybeInvokeSyncCallback(
        GenericGroupMember member,
        SyncGroupResponseData response
    ) {
        if (member.isAwaitingSync()) {
            if (!member.awaitingSyncCallback().complete(response)) {
                log.error("Failed to invoke join callback for {}", member);
                response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                member.awaitingSyncCallback().complete(response);
            }
            member.setAwaitingSyncCallback(null);
            numMembersAwaitingJoinResponse--;
            return true;
        }
        return false;
    }

    public void initNextGeneration() {
        generationId++;
        if (!members.isEmpty()) {
            protocolName = Optional.of(selectProtocol());
            subscribedTopics = computeSubscribedTopics();
            transitionTo(CompletingRebalance);
        } else {
            protocolName = Optional.empty();
            subscribedTopics = computeSubscribedTopics();
            transitionTo(Empty);
        }
        receivedConsumerOffsetCommits = false;
        receivedTransactionOffsetCommits = false;
        clearPendingSyncMembers();
    }

    public List<JoinGroupResponseMember> currentGenericGroupMembers() {
        if (is(Dead) || is(PreparingRebalance)) {
            throw new IllegalStateException("Cannot obtain generic member metadata for group " +
                groupId + " in state " + state);
        }

        return members.values().stream().map(member ->
            new JoinGroupResponseMember()
                .setMemberId(member.memberId())
                .setGroupInstanceId(member.groupInstanceId().orElse(null))
                .setMetadata(member.metadata(protocolName.orElse(null))))
            .collect(Collectors.toList());
    }

    public GroupSummary summary() {
        if (is(Stable)) {
            String protocolName = this.protocolName.orElse(null);
            if (protocolName == null) {
                throw new IllegalStateException("Invalid null group protocol for stable group");
            }
            List<MemberSummary> members = this.members.values().stream().map(member ->
                member.summary(protocolName)).collect(Collectors.toList());

            return new GroupSummary(state.toString(), protocolName, protocolType.orElse(""), members);
        }

        List<MemberSummary> members = this.members.values().stream()
            .map(GenericGroupMember::summaryNoMetadata).collect(Collectors.toList());

        return new GroupSummary(state.toString(), NO_PROTOCOL_NAME, protocolType.orElse(""), members);
    }

    public ListGroupsResponseData.ListedGroup asListedGroup() {
        return new ListGroupsResponseData.ListedGroup()
            .setGroupId(groupId)
            .setProtocolType(protocolType.orElse(""))
            .setGroupState(state.toString());
    }

    public void initializeOffsets(
        Map<TopicPartition, CommitRecordMetadataAndOffset> offsets,
        Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> pendingTxnOffsets
    ) {
        this.offsets.putAll(offsets);
        this.pendingTransactionalOffsetCommits.putAll(pendingTxnOffsets);
    }

    public void onOffsetCommitAppend(TopicIdPartition topicIdPartition,
                                     CommitRecordMetadataAndOffset offsetWithCommitRecordMetadata) {

        TopicPartition topicPartition = topicIdPartition.topicPartition();
        if (pendingOffsetCommits.containsKey(topicPartition)) {
            if (!offsetWithCommitRecordMetadata.appendedBatchOffset().isPresent()) {
                throw new IllegalStateException("Cannot complete offset commit write without providing " +
                    "the metadata of the record in the log.");
            }

            CommitRecordMetadataAndOffset offset = offsets.get(topicPartition);
            if (offset == null || offset.olderThan(offsetWithCommitRecordMetadata)) {
                offsets.put(topicPartition, offsetWithCommitRecordMetadata);
            }
        }

        OffsetAndMetadata stagedOffset = pendingOffsetCommits.get(topicPartition);
        if (stagedOffset != null && offsetWithCommitRecordMetadata.offsetAndMetadata().equals(stagedOffset)) {
            pendingOffsetCommits.remove(topicPartition);
        } else {
            // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
            // its entries would be removed from the cache by the `removeOffsets` method.
        }
    }

    public void failPendingOffsetWrite(TopicIdPartition topicIdPartition, OffsetAndMetadata offset) {
        TopicPartition topicPartition = topicIdPartition.topicPartition();
        OffsetAndMetadata pendingOffset = pendingOffsetCommits.get(topicPartition);
        if (offset.equals(pendingOffset)) {
            pendingOffsetCommits.remove(topicPartition);
        }
    }

    public void prepareOffsetCommit(Map<TopicIdPartition, OffsetAndMetadata> offsets) {
        this.receivedConsumerOffsetCommits = true;
        offsets.forEach(((topicIdPartition, offsetAndMetadata) ->
            pendingOffsetCommits.put(topicIdPartition.topicPartition(), offsetAndMetadata)));
    }

    public void prepareTxnOffsetCommit(long producerId, Map<TopicIdPartition, OffsetAndMetadata> offsets) {
        log.trace("TxnOffsetCommit for producer {} and group {} with offsets {} is pending.",
            producerId, groupId, offsets);
        this.receivedTransactionOffsetCommits = true;

        Map<TopicPartition, CommitRecordMetadataAndOffset> producerOffsets = pendingTransactionalOffsetCommits
            .computeIfAbsent(producerId, (__) -> new HashMap<>());

        offsets.forEach(((topicIdPartition, offsetAndMetadata) ->
            producerOffsets.put(
                topicIdPartition.topicPartition(), new CommitRecordMetadataAndOffset(
                    Optional.empty(),
                    offsetAndMetadata)
            )));
    }

    public boolean hashReceivedConsistentOffsetCommits() {
        return !receivedConsumerOffsetCommits || !receivedTransactionOffsetCommits;
    }

    /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
     * We will return an error and the client will retry the request, potentially to a different coordinator.
     */
    public void failPendingTxnOffsetCommit(long producerId, TopicIdPartition topicIdPartition) {
        TopicPartition topicPartition = topicIdPartition.topicPartition();
        Map<TopicPartition, CommitRecordMetadataAndOffset> pendingOffsets = pendingTransactionalOffsetCommits.get(producerId);
        if (pendingOffsets != null) {
            CommitRecordMetadataAndOffset pendingOffsetCommit = pendingOffsets.remove(topicPartition);
            log.trace("TxnOffsetCommit for producer {} and group {} with offests {} failed to be appended to the log.",
                producerId, groupId, pendingOffsetCommit);

            if (pendingOffsets.isEmpty()) {
                pendingTransactionalOffsetCommits.remove(producerId);
            }
        } else {
            // We may hit this case if the partition in question has emigrated already.
        }
    }

    public void onTxnOffsetCommitAppend(
        long producerId,
        TopicIdPartition topicIdPartition,
        CommitRecordMetadataAndOffset commitRecordMetadataAndOffset
    ) {
        TopicPartition topicPartition = topicIdPartition.topicPartition();
        Map<TopicPartition, CommitRecordMetadataAndOffset> pendingOffset =
            pendingTransactionalOffsetCommits.get(producerId);

        if (pendingOffset != null &&
            pendingOffset.containsKey(topicPartition) &&
            pendingOffset.get(topicPartition).offsetAndMetadata()
                .equals(commitRecordMetadataAndOffset.offsetAndMetadata())) {

            pendingOffset.put(topicPartition, commitRecordMetadataAndOffset);
        } else {
            // We may hit this case if the partition in question has emigrated.
        }
    }

    public void completePendingTxnOffsetCommit(long producerId, boolean isCommit) {
        Map<TopicPartition, CommitRecordMetadataAndOffset> pendingOffsets =
            pendingTransactionalOffsetCommits.remove(producerId);

        if (isCommit) {
            if (pendingOffsets != null) {
                pendingOffsets.forEach(((topicPartition, commitRecordMetadataAndOffset) -> {
                    if (!commitRecordMetadataAndOffset.appendedBatchOffset().isPresent()) {
                        throw new IllegalStateException("Trying to complete a transactional offset commit " +
                            "for producerId " + producerId + " and groupId " + groupId + " even though the " +
                            "offset commit record itself hasn't been appended to the log.");
                    }
                    CommitRecordMetadataAndOffset currentOffset = offsets.get(topicPartition);
                    if (currentOffset == null || currentOffset.olderThan(commitRecordMetadataAndOffset)) {
                        log.trace("TxnOffsetCommit for producer {} and group {} with offset {} " +
                            "committed and loaded into the cache.",
                            producerId, groupId, commitRecordMetadataAndOffset);

                        offsets.put(topicPartition, commitRecordMetadataAndOffset);
                    } else {
                        log.trace("TxnOffsetCommit for producer {} and group {} with offset {} was " +
                            "committed, but not loaded since its offset is older than current " +
                            "offset {}", producerId, groupId, commitRecordMetadataAndOffset, currentOffset);
                    }
                }));
            }
        } else {
            log.trace("TxnOffsetCommit for producer {} and group {} with offsets {} aborted.",
                producerId, groupId, pendingOffsets);
        }
    }

    public Set<Long> activeProducers() {
        return pendingTransactionalOffsetCommits.keySet();
    }

    public boolean hasPendingOffsetCommitsFromProducer(long producerId) {
        return pendingTransactionalOffsetCommits.containsKey(producerId);
    }

    public boolean hasPendingOffsetCommitsForTopicPartition(TopicPartition topicPartition) {
        if (pendingOffsetCommits.containsKey(topicPartition)) {
            return true;
        }
        for (Map<TopicPartition, CommitRecordMetadataAndOffset> pendingOffsets :
            pendingTransactionalOffsetCommits.values()) {

            if (pendingOffsets.containsKey(topicPartition)) {
                return true;
            }
        }
        return false;
    }

    public Map<TopicPartition, OffsetAndMetadata> removeAllOffsets() {
        return removeOffsets(offsets.keySet());
    }

    public Map<TopicPartition, OffsetAndMetadata> removeOffsets(
        Collection<TopicPartition> topicPartitions
    ) {
        Map<TopicPartition, OffsetAndMetadata> removedOffsets = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            pendingOffsetCommits.remove(topicPartition);
            pendingTransactionalOffsetCommits.forEach((__, pendingOffsets) ->
                pendingOffsets.remove(topicPartition));

            CommitRecordMetadataAndOffset removedOffset = offsets.remove(topicPartition);
            removedOffsets.put(topicPartition, removedOffset.offsetAndMetadata());
        }

        return removedOffsets;
    }

    private Map<TopicPartition, OffsetAndMetadata> expiredOffsets(
        long currentTimestamp,
        long offsetRetentionMs,
        Function<CommitRecordMetadataAndOffset, Long> baseTimestamp,
        Set<String> subscribedTopics
    ) {
        Map<TopicPartition, OffsetAndMetadata> expiredOffsets = new HashMap<>();

        offsets.entrySet().stream().filter((entry -> {
            TopicPartition topicPartition = entry.getKey();
            CommitRecordMetadataAndOffset commitRecordMetadataAndOffset = entry.getValue();

            boolean hasExpired;
            Optional<Long> expireTimestamp = commitRecordMetadataAndOffset.offsetAndMetadata().expireTimestamp();
            if (expireTimestamp.isPresent()) {
                // older versions with explicit expire_timestamp field => old expiration semantics is used
                hasExpired = currentTimestamp >= expireTimestamp.get();
            } else {
                // current version with no per partition retention
                hasExpired =
                    currentTimestamp - baseTimestamp.apply(commitRecordMetadataAndOffset) >= offsetRetentionMs;
            }

            return !subscribedTopics.contains(topicPartition.topic()) &&
                !pendingOffsetCommits.containsKey(topicPartition) &&
                hasExpired;

        })).forEach(entry ->
            expiredOffsets.put(entry.getKey(), entry.getValue().offsetAndMetadata()));

        return expiredOffsets;
    }

    public Map<TopicPartition, OffsetAndMetadata> removeExpiredOffsets(
        long currentTimestamp,
        long offsetRetentionMs
    ) {

        Map<TopicPartition, OffsetAndMetadata> expiredOffsets;
        if (protocolType.isPresent()) {
            if (is(Empty)) {
                // no consumer exists in the group =>
                // - if current state timestamp exists and retention period has passed since group became Empty,
                //   expire all offsets with no pending offset commit;
                // - if there is no current state timestamp (old group metadata schema) and retention period has passed
                //   since the last commit timestamp, expire the offset
                expiredOffsets = expiredOffsets(
                    currentTimestamp,
                    offsetRetentionMs,
                    commitRecordMetadataAndOffset -> currentStateTimestamp
                        .orElse(commitRecordMetadataAndOffset.offsetAndMetadata().commitTimestamp()),
                    Collections.emptySet());
            } else if (ConsumerProtocol.PROTOCOL_TYPE.equals(protocolType.get()) &&
                subscribedTopics.isPresent() && is(Stable)) {
                // consumers exist in the group and group is stable =>
                // - if the group is aware of the subscribed topics and retention period had passed since the
                //   the last commit timestamp, expire the offset. offset with pending offset commit are not
                //   expired
                expiredOffsets = expiredOffsets(
                    currentTimestamp, offsetRetentionMs,
                    offset -> offset.offsetAndMetadata().commitTimestamp(),
                    subscribedTopics.get());
            } else {
                expiredOffsets = Collections.emptyMap();
            }
        } else {
            // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
            // expire offsets with no pending offset commit that retention period has passed since their last commit
            expiredOffsets = expiredOffsets(
                currentTimestamp,
                offsetRetentionMs,
                offset -> offset.offsetAndMetadata().commitTimestamp(),
                Collections.emptySet());
        }

        if (!expiredOffsets.isEmpty()) {
            log.debug("Expired offsets from group '{}': {}", groupId, expiredOffsets.keySet());
        }
        expiredOffsets.keySet().forEach(offsets::remove);
        return expiredOffsets;
    }

    public Map<TopicPartition, OffsetAndMetadata> allOffsets() {
        return offsets.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offsetAndMetadata()));
    }

    // Visible for testing
    Optional<CommitRecordMetadataAndOffset> offsetWithRecordMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(offsets.get(topicPartition));
    }

    // Used for testing
    Optional<OffsetAndMetadata> pendingOffsetCommit(TopicIdPartition topicIdPartition) {
        return Optional.ofNullable(pendingOffsetCommits.get(topicIdPartition.topicPartition()));
    }

    // Used for testing
    Optional<CommitRecordMetadataAndOffset> pendingTxnOffsetCommit(
        long producerId,
        TopicIdPartition topicIdPartition
    ) {
        return Optional.ofNullable(pendingTransactionalOffsetCommits.get(producerId)).flatMap(
            pendingOffsets -> Optional.ofNullable(pendingOffsets.get(topicIdPartition.topicPartition()))
        );
    }

    public int numOffsets() {
        return offsets.size();
    }

    public boolean hasOffsets() {
        return !offsets.isEmpty() ||
            !pendingOffsetCommits.isEmpty() ||
            !pendingTransactionalOffsetCommits.isEmpty();
    }

    private void assertValidTransition(GenericGroupState targetState) {
        if (!targetState.validPreviousStates().contains(state)) {
            throw new IllegalStateException("Group " + groupId + " should be in one of " +
                targetState.validPreviousStates() + " states before moving to " + targetState +
                " state. Instead it is in " + state + " state.");
        }
    }

    @Override
    public String toString() {
        return "GenericGroupMetadata(" +
            "groupId=" + groupId + ", " +
            "generation=" + generationId + ", " +
            "protocolType=" + protocolType + ", " +
            "currentState=" + currentState() + ", " +
            "members=" + members + ")";
    }
}
