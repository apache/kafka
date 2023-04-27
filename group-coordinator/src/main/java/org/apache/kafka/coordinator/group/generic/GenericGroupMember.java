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

import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Java rewrite of {@link kafka.coordinator.group.MemberMetadata} that is used
 * by the new group coordinator (KIP-848).
 */
public class GenericGroupMember {

    /**
     * A builder allowing to create a new generic member or update an
     * existing one.
     *
     * Please refer to the javadoc of {{@link GenericGroupMember}} for the
     * definition of the fields.
     */
    public static class Builder {
        private final String memberId;
        private Optional<String> groupInstanceId = Optional.empty();
        private String clientId = "";
        private String clientHost = "";
        private int rebalanceTimeoutMs = -1;
        private int sessionTimeoutMs = -1;
        private String protocolType = "";
        private List<Protocol> supportedProtocols = Collections.emptyList();
        private byte[] assignment = new byte[0];

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId);
        }

        public Builder(GenericGroupMember member) {
            Objects.requireNonNull(member);

            this.memberId = member.memberId;
            this.groupInstanceId = member.groupInstanceId;
            this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
            this.sessionTimeoutMs = member.sessionTimeoutMs;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.protocolType = member.protocolType;
            this.supportedProtocols = member.supportedProtocols;
            this.assignment = member.assignment;
        }

        public Builder setGroupInstanceId(Optional<String> groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
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

        public Builder setRebalanceTimeoutMs(int rebalanceTimeoutMs) {
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            return this;
        }

        public Builder setSessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        public Builder setProtocolType(String protocolType) {
            this.protocolType = protocolType;
            return this;
        }

        public Builder setSupportedProtocols(List<Protocol> protocols) {
            this.supportedProtocols = protocols;
            return this;
        }

        public Builder setAssignment(byte[] assignment) {
            this.assignment = assignment;
            return this;
        }

        public GenericGroupMember build() {
            return new GenericGroupMember(
                memberId,
                groupInstanceId,
                clientId,
                clientHost,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                protocolType,
                supportedProtocols,
                assignment
            );
        }
    }

    private static class MemberSummary {
        private final String memberId;
        private final Optional<String> groupInstanceId;
        private final String clientId;
        private final String clientHost;
        private final byte[] metadata;
        private final byte[] assignment;
        
        public MemberSummary(String memberId,
                             Optional<String> groupInstanceId,
                             String clientId,
                             String clientHost,
                             byte[] metadata,
                             byte[] assignment) {
            
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
            this.clientId = clientId;
            this.clientHost = clientHost;
            this.metadata = metadata;
            this.assignment = assignment;
        }

        public String memberId() {
            return memberId;
        }

        public Optional<String> getGroupInstanceId() {
            return groupInstanceId;
        }

        public String clientId() {
            return clientId;
        }

        public String clientHost() {
            return clientHost;
        }

        public byte[] metadata() {
            return metadata;
        }

        public byte[] assignment() {
            return assignment;
        }
        
    }

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
    private final int rebalanceTimeoutMs;

    /**
     * The session timeout in milliseconds.
     */
    private final int sessionTimeoutMs;

    /**
     * The protocol type.
     */
    private final String protocolType;

    /**
     * The list of supported protocols.
     */
    private final List<Protocol> supportedProtocols;

    /**
     * The assignment stored by the client assignor.
     */
    private final byte[] assignment;

    /**
     * The callback that is invoked once this member joins the group.
     */
    private CompletableFuture<JoinGroupResponseData> awaitingJoinCallback = null;

    /**
     * The callback that is invoked once this member completes the sync group phase.
     */
    private CompletableFuture<SyncGroupResponseData> awaitingSyncCallback = null;

    /**
     * Indicates whether the member is a new member of the group.
     */
    private boolean isNew = false;

    /**
     * This variable is used to track heartbeat completion through the delayed
     * heartbeat purgatory. When scheduling a new heartbeat expiration, we set
     * this value to `false`. Upon receiving the heartbeat (or any other event
     * indicating the liveness of the client), we set it to `true` so that the
     * delayed heartbeat can be completed.
     */
    private boolean heartbeatSatisfied = false;


    public GenericGroupMember(String memberId,
                              Optional<String> groupInstanceId,
                              String clientId,
                              String clientHost,
                              int rebalanceTimeoutMs,
                              int sessionTimeoutMs,
                              String protocolType,
                              List<Protocol> supportedProtocols) {

        this(memberId,
             groupInstanceId,
             clientId,
             clientHost,
             rebalanceTimeoutMs,
             sessionTimeoutMs,
             protocolType,
             supportedProtocols,
             new byte[0]);
    }

    public GenericGroupMember(String memberId,
                              Optional<String> groupInstanceId,
                              String clientId,
                              String clientHost,
                              int rebalanceTimeoutMs,
                              int sessionTimeoutMs,
                              String protocolType,
                              List<Protocol> supportedProtocols,
                              byte[] assignment) {

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
        return awaitingJoinCallback != null;
    }

    /**
     * @return whether the member is awaiting sync.
     */
    public boolean isAwaitingSync() {
        return awaitingSyncCallback != null;
    }

    /**
     * Get the metadata corresponding to the provided protocol.
     */
    public byte[] metadata(String protocolName) {
        Optional<Protocol> match = supportedProtocols.stream().filter(protocol ->
            protocol.name().equals(protocolName))
                .findFirst();

        if (match.isPresent()) {
            return match.get().metadata();
        } else {
            throw new IllegalArgumentException("Member does not support protocol " +
                protocolName);
        }
    }

    /**
     * The heartbeat is always considered satisfied when an existing member has made a
     * successful join/sync group request during a rebalance.
     *
     * @return true if heartbeat was satisfied; false otherwise.
     */
    public boolean hasSatisfiedHeartbeat() {
        if (isNew) {
            // New members can be expired while awaiting join, so we have to check this first
            return heartbeatSatisfied;
        } else if (isAwaitingJoin() || isAwaitingSync()) {
            // Members that are awaiting a rebalance automatically satisfy expected heartbeats
            return true;
        } else {
            // Otherwise, we require the next heartbeat
            return heartbeatSatisfied;
        }
    }

    /**
     * @param protocols list of protocols to match.
     * @return true if the given list matches the member's list of supported protocols,
     *         false otherwise.
     */
    public boolean matches(List<Protocol> protocols) {
        return protocols.equals(this.supportedProtocols);
    }

    /**
     * @param protocolName the protocol name.
     * @return MemberSummary object with metadata corresponding to the protocol name.
     */
    public MemberSummary summary(String protocolName) {
        return new MemberSummary(
            memberId,
            groupInstanceId,
            clientId,
            clientHost,
            metadata(protocolName),
            assignment
        );
    }

    /**
     * @return MemberSummary object with no metadata.
     */
    public MemberSummary summaryNoMetadata() {
        return new MemberSummary(
            memberId,
            groupInstanceId,
            clientId,
            clientHost,
            new byte[0],
            new byte[0]
        );
    }

    /**
     * Vote for one of the potential group protocols. This takes into account the protocol preference as
     * indicated by the order of supported protocols and returns the first one also contained in the set
     */
    public String vote(Set<String> candidates) {
        Optional<Protocol> match = supportedProtocols.stream().filter(protocol ->
            candidates.contains(protocol.name())).findFirst();
        
        if (match.isPresent()) {
            return match.get().name();
        } else {
            throw new IllegalArgumentException("Member does not support any of the candidate protocols");
        }
    }

    /**
     * @param supportedProtocols list of supported protocols.
     * @return a set of protocol names from the given list of supported protocols.
     */
    public static Set<String> plainProtocolSet(List<Protocol> supportedProtocols) {
        return supportedProtocols.stream().map(Protocol::name).collect(Collectors.toSet());
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
    public List<Protocol> supportedProtocols() {
        return supportedProtocols;
    }

    public byte[] assignment() {
        return assignment;
    }

    /**
     * @return the awaiting join callback.
     */
    public CompletableFuture<JoinGroupResponseData> awaitingJoinCallback() {
        return awaitingJoinCallback;
    }

    /**
     * @param value the updated join callback.
     */
    public void setAwaitingJoinCallback(CompletableFuture<JoinGroupResponseData> value) {
        this.awaitingJoinCallback = value;
    }

    /**
     * @return the awaiting sync callback.
     */
    public CompletableFuture<SyncGroupResponseData> awaitingSyncCallback() {
        return awaitingSyncCallback;
    }

    /**
     * @param value the updated sync callback.
     */
    public void setAwaitingSyncCallback(CompletableFuture<SyncGroupResponseData> value) {
        this.awaitingSyncCallback = value;
    }

    /**
     * @return true if the member is new, false otherwise.
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * @param value true if the member is new, false otherwise.
     */
    public void setIsNew(boolean value) {
        this.isNew = value;
    }

    public boolean heartBeatSatisfied() {
        return heartbeatSatisfied;
    }

    public void setHeartBeatSatisfied(boolean value) {
        this.heartbeatSatisfied = value;
    }

    @Override
    public String toString() {
        return "GenericGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", groupInstanceId='" + groupInstanceId + '\'' +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", sessionTimeoutMs=" + sessionTimeoutMs +
            ", protocolType='" + protocolType + '\'' +
            ", supportedProtocols=" + supportedProtocols +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericGroupMember that = (GenericGroupMember) o;

        return memberId.equals(that.memberId) &&
            groupInstanceId.equals(that.groupInstanceId) &&
            clientId.equals(that.clientId) &&
            clientHost.equals(that.clientHost) &&
            rebalanceTimeoutMs == that.rebalanceTimeoutMs &&
            sessionTimeoutMs == that.sessionTimeoutMs &&
            protocolType.equals(that.protocolType) &&
            supportedProtocols.equals(that.supportedProtocols) &&
            Arrays.equals(assignment, that.assignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            memberId,
            groupInstanceId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            supportedProtocols,
            Arrays.hashCode(assignment)
        );
    }
}
