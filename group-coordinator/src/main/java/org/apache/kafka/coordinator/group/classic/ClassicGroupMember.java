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

package org.apache.kafka.coordinator.group.classic;

import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.utils.Bytes;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class encapsulates a classic group member's metadata.
 *
 * Member metadata contains the following:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance future: when the group is in the prepare-rebalance state,
 *                                 its rebalance future will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync future: when the group is in the awaiting-sync state, its sync future
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
public class ClassicGroupMember {

    /**
     * An empty assignment.
     */
    public static final byte[] EMPTY_ASSIGNMENT = Bytes.EMPTY;

    /**
     * The member id.
     */
    private final String memberId;

    /**
     * The group instance id.
     */
    private final Optional<String> groupInstanceId;

    /**
     * The client id.
     */
    private final String clientId;

    /**
     * The client host.
     */
    private final String clientHost;

    /**
     * The rebalance timeout in milliseconds.
     */
    private int rebalanceTimeoutMs;

    /**
     * The session timeout in milliseconds.
     */
    private int sessionTimeoutMs;

    /**
     * The protocol type.
     */
    private final String protocolType;

    /**
     * The list of supported protocols.
     */
    private JoinGroupRequestProtocolCollection supportedProtocols;

    /**
     * The assignment stored by the client assignor.
     */
    private byte[] assignment;

    /**
     * The future that is invoked once this member joins the group.
     */
    private CompletableFuture<JoinGroupResponseData> awaitingJoinFuture = null;

    /**
     * The future that is invoked once this member completes the sync group phase.
     */
    private CompletableFuture<SyncGroupResponseData> awaitingSyncFuture = null;

    /**
     * Indicates whether the member is a new member of the group.
     */
    private boolean isNew = false;

    public ClassicGroupMember(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String clientHost,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        String protocolType,
        JoinGroupRequestProtocolCollection supportedProtocols
    ) {
        this(
            memberId,
            groupInstanceId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            supportedProtocols,
            EMPTY_ASSIGNMENT
        );
    }

    public ClassicGroupMember(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String clientHost,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        String protocolType,
        JoinGroupRequestProtocolCollection supportedProtocols,
        byte[] assignment
    ) {
        this.memberId = memberId;
        this.groupInstanceId = groupInstanceId;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.protocolType = protocolType;
        this.supportedProtocols = supportedProtocols;
        this.assignment = assignment;
    }

    /**
     * @return true if the member is utilizing static membership, false otherwise.
     */
    public boolean isStaticMember() {
        return groupInstanceId.isPresent();
    }

    /**
     * @return whether the member is awaiting join.
     */
    public boolean isAwaitingJoin() {
        return awaitingJoinFuture != null;
    }

    /**
     * @return whether the member is awaiting sync.
     */
    public boolean isAwaitingSync() {
        return awaitingSyncFuture != null;
    }

    /**
     * Get the metadata corresponding to the provided protocol.
     */
    public byte[] metadata(String protocolName) {
        for (JoinGroupRequestProtocol protocol : supportedProtocols) {
            if (protocol.name().equals(protocolName)) {
                return protocol.metadata();
            }
        }

        throw new IllegalArgumentException("Member does not support protocol " +
            protocolName);
    }

    /**
     * The heartbeat is always considered satisfied when an existing member has made a
     * successful join/sync group request during a rebalance.
     *
     * @return true if heartbeat was satisfied; false otherwise.
     */
    public boolean hasSatisfiedHeartbeat() {
        if (isNew) {
            // New members can be expired even while awaiting join, so we check this first
            return false;
        } else {
            // Members that are awaiting a rebalance automatically satisfy expected heartbeats
            return isAwaitingJoin() || isAwaitingSync();
        }
    }

    /**
     * Compare the given list of protocols with the member's supported protocols.
     *
     * @param protocols list of protocols to match.
     * @return true if the given list matches the member's list of supported protocols,
     *         false otherwise.
     */
    public boolean matches(JoinGroupRequestProtocolCollection protocols) {
        return protocols.equals(this.supportedProtocols);
    }

    /**
     * Vote for one of the potential group protocols. This takes into account the protocol preference as
     * indicated by the order of supported protocols and returns the first one also contained in the set
     *
     * @param candidates The protocol names that this member can vote for
     * @return the first supported protocol that matches one of the candidates
     */
    public String vote(Set<String> candidates) {
        for (JoinGroupRequestProtocol protocol : supportedProtocols) {
            if (candidates.contains(protocol.name())) {
                return protocol.name();
            }
        }

        throw new IllegalArgumentException("Member does not support any of the candidate protocols");
    }

    /**
     * Transform protocols into their respective names.
     *
     * @param supportedProtocols list of supported protocols.
     * @return a set of protocol names from the given list of supported protocols.
     */
    public static Set<String> plainProtocolSet(
        JoinGroupRequestProtocolCollection supportedProtocols
    ) {
        Set<String> protocolNames = new HashSet<>();
        for (JoinGroupRequestProtocol protocol : supportedProtocols) {
            protocolNames.add(protocol.name());
        }
        return protocolNames;
    }

    /**
     * @return whether the member has an assignment set.
     */
    public boolean hasAssignment() {
        return assignment != null && assignment.length > 0;
    }

    /**
     * @return the member id.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * @return the group instance id.
     */
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * @return the client id.
     */
    public String clientId() {
        return clientId;
    }

    /**
     * @return the client host.
     */
    public String clientHost() {
        return clientHost;
    }

    /**
     * @return the rebalance timeout in milliseconds.
     */
    public int rebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }

    /**
     * @return the session timeout in milliseconds.
     */
    public int sessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    /**
     * @return the protocol type.
     */
    public String protocolType() {
        return protocolType;
    }

    /**
     * @return the list of supported protocols.
     */
    public JoinGroupRequestProtocolCollection supportedProtocols() {
        return supportedProtocols;
    }

    /**
     * @return the member's assignment.
     */
    public byte[] assignment() {
        return assignment;
    }

    /**
     * @return the awaiting join future.
     */
    public CompletableFuture<JoinGroupResponseData> awaitingJoinFuture() {
        return awaitingJoinFuture;
    }

    /**
     * @return the awaiting sync future.
     */
    public CompletableFuture<SyncGroupResponseData> awaitingSyncFuture() {
        return awaitingSyncFuture;
    }

    /**
     * @return true if the member is new, false otherwise.
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * @return The described group member without metadata.
     */
    public DescribeGroupsResponseData.DescribedGroupMember describeNoMetadata() {
        return new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId(memberId())
            .setGroupInstanceId(groupInstanceId().orElse(null))
            .setClientId(clientId())
            .setClientHost(clientHost())
            .setMemberAssignment(assignment());
    }

    /**
     * @return The described group member with metadata corresponding to the provided protocol.
     */
    public DescribeGroupsResponseData.DescribedGroupMember describe(String protocolName) {
        return describeNoMetadata().setMemberMetadata(metadata(protocolName));
    }

    /**
     * @param value the new rebalance timeout in milliseconds.
     */
    public void setRebalanceTimeoutMs(int value) {
        this.rebalanceTimeoutMs = value;
    }

    /**
     * @param value the new session timeout in milliseconds.
     */
    public void setSessionTimeoutMs(int value) {
        this.sessionTimeoutMs = value;
    }

    /**
     * @param value the new list of supported protocols.
     */
    public void setSupportedProtocols(JoinGroupRequestProtocolCollection value) {
        this.supportedProtocols = value;
    }

    /**
     * @param value the new assignment.
     */
    public void setAssignment(byte[] value) {
        this.assignment = value;
    }

    /**
     * @param value the updated join future.
     */
    public void setAwaitingJoinFuture(CompletableFuture<JoinGroupResponseData> value) {
        this.awaitingJoinFuture = value;
    }

    /**
     * @param value the updated sync future.
     */
    public void setAwaitingSyncFuture(CompletableFuture<SyncGroupResponseData> value) {
        this.awaitingSyncFuture = value;
    }

    /**
     * @param value true if the member is new, false otherwise.
     */
    public void setIsNew(boolean value) {
        this.isNew = value;
    }

    @Override
    public String toString() {
        return "ClassicGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", groupInstanceId='" + groupInstanceId.orElse("") + '\'' +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", sessionTimeoutMs=" + sessionTimeoutMs +
            ", protocolType='" + protocolType + '\'' +
            ", supportedProtocols=" + supportedProtocols.stream()
                .map(JoinGroupRequestProtocol::name)
                .collect(Collectors.toList()) +
            ')';
    }
}
