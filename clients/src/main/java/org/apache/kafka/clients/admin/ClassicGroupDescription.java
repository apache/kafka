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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A detailed description of a single classic group in the cluster.
 * <p>
 * This is the result type of {@link Admin#describeClassicGroups(java.util.Collection)}.
 * Classic groups are the legacy group coordination protocol used before KIP-848 introduced the new
 * consumer group protocol. They are used by classic consumers and Kafka Connect.
 * <p>
 * The {@link #protocol() protocol type} indicates the kind of group: {@code "consumer"} for
 * classic consumer groups, {@code "connect"} for Connect groups, or an empty string for simple
 * consumer groups that use manual partition assignment without coordinator-based rebalancing.
 * <p>
 * The {@link #protocolData() protocol data} depends on the protocol type: for a classic consumer
 * group it is the partition assignor name; for a Connect group it indicates which Connect
 * protocols are enabled.
 *
 * @see Admin#describeClassicGroups(java.util.Collection)
 */
public class ClassicGroupDescription {
    private final String groupId;
    private final String protocol;
    private final String protocolData;
    private final Collection<MemberDescription> members;
    private final ClassicGroupState state;
    private final Node coordinator;
    private final Set<AclOperation> authorizedOperations;

    /**
     * @param groupId         The group id.
     * @param protocol        The group protocol type.
     * @param protocolData    The group protocol data.
     * @param members         The group members.
     * @param state           The classic group state.
     * @param coordinator     The group coordinator.
     */
    public ClassicGroupDescription(String groupId,
                                   String protocol,
                                   String protocolData,
                                   Collection<MemberDescription> members,
                                   ClassicGroupState state,
                                   Node coordinator) {
        this(groupId, protocol, protocolData, members, state, coordinator, Set.of());
    }

    /**
     * @param groupId               The group id.
     * @param protocol              The group protocol type.
     * @param protocolData          The group protocol data.
     * @param members               The group members.
     * @param state                 The classic group state.
     * @param coordinator           The group coordinator.
     * @param authorizedOperations  The authorized operations.
     */
    public ClassicGroupDescription(String groupId,
                                   String protocol,
                                   String protocolData,
                                   Collection<MemberDescription> members,
                                   ClassicGroupState state,
                                   Node coordinator,
                                   Set<AclOperation> authorizedOperations) {
        this.groupId = groupId == null ? "" : groupId;
        this.protocol = protocol;
        this.protocolData = protocolData == null ? "" : protocolData;
        this.members = members == null ? List.of() : List.copyOf(members);
        this.state = state;
        this.coordinator = coordinator;
        this.authorizedOperations = authorizedOperations;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ClassicGroupDescription that = (ClassicGroupDescription) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(protocol, that.protocol) &&
            Objects.equals(protocolData, that.protocolData) &&
            Objects.equals(members, that.members) &&
            state == that.state &&
            Objects.equals(coordinator, that.coordinator) &&
            Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, protocol, protocolData, members, state, coordinator, authorizedOperations);
    }

    /**
     * The id of the classic group.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * The group protocol type, or the empty string.
     */
    public String protocol() {
        return protocol;
    }

    /**
     * The group protocol data. The meaning depends on the group protocol type.
     * For a classic consumer group, this is the partition assignor name.
     * For a classic connect group, this indicates which Connect protocols are enabled.
     */
    public String protocolData() {
        return protocolData;
    }

    /**
     * Whether this is a simple consumer group (i.e., a group with an empty protocol type).
     */
    public boolean isSimpleConsumerGroup() {
        return protocol.isEmpty();
    }

    /**
     * A list of the members of the classic group.
     */
    public Collection<MemberDescription> members() {
        return members;
    }

    /**
     * The classic group state, or UNKNOWN if the state is too new for us to parse.
     */
    public ClassicGroupState state() {
        return state;
    }

    /**
     * The classic group coordinator, or null if the coordinator is not known.
     */
    public Node coordinator() {
        return coordinator;
    }

    /**
     * authorizedOperations for this group, or null if that information is not known.
     */
    public Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", protocol='" + protocol + '\'' +
            ", protocolData=" + protocolData +
            ", members=" + members.stream().map(MemberDescription::toString).collect(Collectors.joining(",")) +
            ", state=" + state +
            ", coordinator=" + coordinator +
            ", authorizedOperations=" + authorizedOperations +
            ")";
    }
}
