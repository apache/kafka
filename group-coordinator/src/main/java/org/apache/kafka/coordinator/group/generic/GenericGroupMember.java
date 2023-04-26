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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Java rewrite of {@link kafka.coordinator.group.MemberMetadata} that is used
 * by {@link org.apache.kafka.coordinator.group.GroupCoordinator}
 */
public class GenericGroupMember {

    private static class Protocol {
        private final String name;
        private final byte[] metadata;

        private Protocol(String name, byte[] metadata) {
            this.name = name;
            this.metadata = metadata;
        }

        @Override
        public boolean equals(Object o) {
            if (!this.getClass().equals(o.getClass())) {
                return false;
            }

            Protocol that = (Protocol) o;
            return name.equals(that.name) &&
                Arrays.equals(this.metadata, that.metadata);
        }

        @Override
        public String toString() {
            return name;
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
    private CompletableFuture<JoinGroupResult> awaitingJoinCallback = null;

    /**
     * The callback that is invoked once this member completes the sync group phase.
     */
    private CompletableFuture<SyncGroupResult> awaitingSyncCallback = null;

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
            protocol.name.equals(protocolName))
                .findFirst();

        if (match.isPresent()) {
            return match.get().metadata;
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
            candidates.contains(protocol.name)).findFirst();
        
        if (match.isPresent()) {
            return match.get().name;
        } else {
            throw new IllegalArgumentException("Member does not support any of the candidate protocols");
        }
    }

    /**
     * @param supportedProtocols list of supported protocols.
     * @return a set of protocol names from the given list of supported protocols.
     */
    public static Set<String> plainProtocolSet(List<Protocol> supportedProtocols) {
        return supportedProtocols.stream().map(protocol -> protocol.name).collect(Collectors.toSet());
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

    /**
     * @return the awaiting join callback.
     */
    public CompletableFuture<JoinGroupResult> awaitingJoinCallback() {
        return awaitingJoinCallback;
    }

    /**
     * @param value the updated join callback.
     */
    public void setAwaitingJoinCallback(CompletableFuture<JoinGroupResult> value) {
        this.awaitingJoinCallback = value;
    }

    /**
     * @return the awaiting sync callback.
     */
    public CompletableFuture<SyncGroupResult> awaitingSyncCallback() {
        return awaitingSyncCallback;
    }

    /**
     * @param value the updated sync callback.
     */
    public void setAwaitingSyncCallback(CompletableFuture<SyncGroupResult> value) {
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
