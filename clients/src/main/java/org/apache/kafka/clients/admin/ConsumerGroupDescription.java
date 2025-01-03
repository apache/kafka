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

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A detailed description of a single consumer group in the cluster.
 */
public class ConsumerGroupDescription {
    private final String groupId;
    private final boolean isSimpleConsumerGroup;
    private final Collection<MemberDescription> members;
    private final String partitionAssignor;
    private final GroupType type;
    private final GroupState groupState;
    private final Node coordinator;
    private final Set<AclOperation> authorizedOperations;
    private final Optional<Integer> groupEpoch;
    private final Optional<Integer> targetAssignmentEpoch;

    /**
     * @deprecated Since 4.0. Use {@link #ConsumerGroupDescription(String, boolean, Collection, String, GroupType, GroupState, Node, Set, Optional, Optional)}.
     */
    @Deprecated
    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    ConsumerGroupState state,
                                    Node coordinator) {
        this(groupId, isSimpleConsumerGroup, members, partitionAssignor, state, coordinator, Collections.emptySet());
    }

    /**
     * @deprecated Since 4.0. Use {@link #ConsumerGroupDescription(String, boolean, Collection, String, GroupType, GroupState, Node, Set, Optional, Optional)}.
     */
    @Deprecated
    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    ConsumerGroupState state,
                                    Node coordinator,
                                    Set<AclOperation> authorizedOperations) {
        this(groupId, isSimpleConsumerGroup, members, partitionAssignor, GroupType.CLASSIC, state, coordinator, authorizedOperations);
    }

    /**
     * @deprecated Since 4.0. Use {@link #ConsumerGroupDescription(String, boolean, Collection, String, GroupType, GroupState, Node, Set, Optional, Optional)}.
     */
    @Deprecated
    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    GroupType type,
                                    ConsumerGroupState state,
                                    Node coordinator,
                                    Set<AclOperation> authorizedOperations) {
        this.groupId = groupId == null ? "" : groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        this.members = members == null ? Collections.emptyList() : List.copyOf(members);
        this.partitionAssignor = partitionAssignor == null ? "" : partitionAssignor;
        this.type = type;
        this.groupState = GroupState.parse(state.toString());
        this.coordinator = coordinator;
        this.authorizedOperations = authorizedOperations;
        this.groupEpoch = Optional.empty();
        this.targetAssignmentEpoch = Optional.empty();
    }

    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    GroupType type,
                                    GroupState groupState,
                                    Node coordinator,
                                    Set<AclOperation> authorizedOperations,
                                    Optional<Integer> groupEpoch,
                                    Optional<Integer> targetAssignmentEpoch) {
        this.groupId = groupId == null ? "" : groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        this.members = members == null ? Collections.emptyList() : List.copyOf(members);
        this.partitionAssignor = partitionAssignor == null ? "" : partitionAssignor;
        this.type = type;
        this.groupState = groupState;
        this.coordinator = coordinator;
        this.authorizedOperations = authorizedOperations;
        this.groupEpoch = groupEpoch;
        this.targetAssignmentEpoch = targetAssignmentEpoch;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConsumerGroupDescription that = (ConsumerGroupDescription) o;
        return isSimpleConsumerGroup == that.isSimpleConsumerGroup &&
            Objects.equals(groupId, that.groupId) &&
            Objects.equals(members, that.members) &&
            Objects.equals(partitionAssignor, that.partitionAssignor) &&
            type == that.type &&
            groupState == that.groupState &&
            Objects.equals(coordinator, that.coordinator) &&
            Objects.equals(authorizedOperations, that.authorizedOperations) &&
            Objects.equals(groupEpoch, that.groupEpoch) &&
            Objects.equals(targetAssignmentEpoch, that.targetAssignmentEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, isSimpleConsumerGroup, members, partitionAssignor, type, groupState, coordinator,
            authorizedOperations, groupEpoch, targetAssignmentEpoch);
    }

    /**
     * The id of the consumer group.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * If consumer group is simple or not.
     */
    public boolean isSimpleConsumerGroup() {
        return isSimpleConsumerGroup;
    }

    /**
     * A list of the members of the consumer group.
     */
    public Collection<MemberDescription> members() {
        return members;
    }

    /**
     * The consumer group partition assignor.
     */
    public String partitionAssignor() {
        return partitionAssignor;
    }

    /**
     * The group type (or the protocol) of this consumer group. It defaults
     * to Classic if not provided by the server.
     */
    public GroupType type() {
        return type;
    }

    /**
     * The consumer group state, or UNKNOWN if the state is too new for us to parse.
     * @deprecated Since 4.0. Use {@link #groupState()} instead.
     */
    @Deprecated
    public ConsumerGroupState state() {
        return ConsumerGroupState.parse(groupState.toString());
    }

    /**
     * The group state, or UNKNOWN if the state is too new for us to parse.
     */
    public GroupState groupState() {
        return groupState;
    }

    /**
     * The consumer group coordinator, or null if the coordinator is not known.
     */
    public Node coordinator() {
        return coordinator;
    }

    /**
     * authorizedOperations for this group, or null if that information is not known.
     */
    public  Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    /**
     * The epoch of the consumer group.
     * The optional is set to an integer if it is a {@link GroupType#CONSUMER} group, and to empty if it
     * is a {@link GroupType#CLASSIC} group.
     */
    public Optional<Integer> groupEpoch() {
        return groupEpoch;
    }

    /**
     * The epoch of the target assignment.
     * The optional is set to an integer if it is a {@link GroupType#CONSUMER} group, and to empty if it
     * is a {@link GroupType#CLASSIC} group.
     */
    public Optional<Integer> targetAssignmentEpoch() {
        return targetAssignmentEpoch;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", isSimpleConsumerGroup=" + isSimpleConsumerGroup +
            ", members=" + members.stream().map(MemberDescription::toString).collect(Collectors.joining(",")) +
            ", partitionAssignor=" + partitionAssignor +
            ", type=" + type +
            ", groupState=" + groupState +
            ", coordinator=" + coordinator +
            ", authorizedOperations=" + authorizedOperations +
            ", groupEpoch=" + groupEpoch.orElse(null) +
            ", targetAssignmentEpoch=" + targetAssignmentEpoch.orElse(null) +
            ")";
    }
}
