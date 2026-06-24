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

import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A detailed description of a single streams group in the cluster.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public class StreamsGroupDescription {

    private final String groupId;
    private final int groupEpoch;
    private final int targetAssignmentEpoch;
    private final int topologyEpoch;
    private final Collection<StreamsGroupSubtopologyDescription> subtopologies;
    private final Collection<StreamsGroupMemberDescription> members;
    private final GroupState groupState;
    private final Node coordinator;
    private final Set<AclOperation> authorizedOperations;
    private final Optional<StreamsGroupTopologyDescription> topologyDescription;
    private final StreamsGroupTopologyDescriptionStatus topologyDescriptionStatus;

    public StreamsGroupDescription(
            final String groupId,
            final int groupEpoch,
            final int targetAssignmentEpoch,
            final int topologyEpoch,
            final Collection<StreamsGroupSubtopologyDescription> subtopologies,
            final Collection<StreamsGroupMemberDescription> members,
            final GroupState groupState,
            final Node coordinator,
            final Set<AclOperation> authorizedOperations
    ) {
        this(
            groupId,
            groupEpoch,
            targetAssignmentEpoch,
            topologyEpoch,
            subtopologies,
            members,
            groupState,
            coordinator,
            authorizedOperations,
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED
        );
    }

    public StreamsGroupDescription(
            final String groupId,
            final int groupEpoch,
            final int targetAssignmentEpoch,
            final int topologyEpoch,
            final Collection<StreamsGroupSubtopologyDescription> subtopologies,
            final Collection<StreamsGroupMemberDescription> members,
            final GroupState groupState,
            final Node coordinator,
            final Set<AclOperation> authorizedOperations,
            final Optional<StreamsGroupTopologyDescription> topologyDescription,
            final StreamsGroupTopologyDescriptionStatus topologyDescriptionStatus
    ) {
        this.groupId = Objects.requireNonNull(groupId, "groupId must be non-null");
        this.groupEpoch = groupEpoch;
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        this.topologyEpoch = topologyEpoch;
        this.subtopologies = Objects.requireNonNull(subtopologies, "subtopologies must be non-null");
        this.members = Objects.requireNonNull(members, "members must be non-null");
        this.groupState = Objects.requireNonNull(groupState, "groupState must be non-null");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator must be non-null");
        this.authorizedOperations = authorizedOperations;
        this.topologyDescription = Objects.requireNonNull(topologyDescription, "topologyDescription must be non-null");
        this.topologyDescriptionStatus = Objects.requireNonNull(topologyDescriptionStatus, "topologyDescriptionStatus must be non-null");
    }

    /**
     * The id of the streams group.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * The epoch of the consumer group.
     */
    public int groupEpoch() {
        return groupEpoch;
    }

    /**
     * The epoch of the target assignment.
     */
    public int targetAssignmentEpoch() {
        return targetAssignmentEpoch;
    }

    /**
     * The epoch of the currently used topology.
     */
    public int topologyEpoch() {
        return topologyEpoch;
    }

    /**
     * A list of the members of the streams group.
     */
    public Collection<StreamsGroupMemberDescription> members() {
        return members;
    }

    /**
     * A list of the subtopologies in the streams group.
     */
    public Collection<StreamsGroupSubtopologyDescription> subtopologies() {
        return subtopologies;
    }

    /**
     * The state of the streams group, or UNKNOWN if the state is too new for us to parse.
     */
    public GroupState groupState() {
        return groupState;
    }

    /**
     * The group coordinator, or null if the coordinator is not known.
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

    /**
     * The full topology description for this group, as recorded by the broker's topology description plugin.
     * Present if and only if {@link #topologyDescriptionStatus()} is
     * {@link StreamsGroupTopologyDescriptionStatus#AVAILABLE AVAILABLE}.
     */
    public Optional<StreamsGroupTopologyDescription> topologyDescription() {
        return topologyDescription;
    }

    /**
     * The status of the topology description for this group, paired with {@link #topologyDescription()}.
     */
    public StreamsGroupTopologyDescriptionStatus topologyDescriptionStatus() {
        return topologyDescriptionStatus;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsGroupDescription that = (StreamsGroupDescription) o;
        return groupEpoch == that.groupEpoch
            && targetAssignmentEpoch == that.targetAssignmentEpoch
            && topologyEpoch == that.topologyEpoch
            && Objects.equals(groupId, that.groupId)
            && Objects.equals(subtopologies, that.subtopologies)
            && Objects.equals(members, that.members)
            && groupState == that.groupState
            && Objects.equals(coordinator, that.coordinator)
            && Objects.equals(authorizedOperations, that.authorizedOperations)
            && Objects.equals(topologyDescription, that.topologyDescription)
            && topologyDescriptionStatus == that.topologyDescriptionStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            groupId,
            groupEpoch,
            targetAssignmentEpoch,
            topologyEpoch,
            subtopologies,
            members,
            groupState,
            coordinator,
            authorizedOperations,
            topologyDescription,
            topologyDescriptionStatus
        );
    }

    @Override
    public String toString() {
        return "(" +
            "groupId=" + groupId +
            ", groupEpoch=" + groupEpoch +
            ", targetAssignmentEpoch=" + targetAssignmentEpoch +
            ", topologyEpoch=" + topologyEpoch +
            ", subtopologies=" + subtopologies.stream().map(StreamsGroupSubtopologyDescription::toString).collect(Collectors.joining(",")) +
            ", members=" + members.stream().map(StreamsGroupMemberDescription::toString).collect(Collectors.joining(",")) +
            ", groupState=" + groupState +
            ", coordinator=" + coordinator +
            ", authorizedOperations=" + authorizedOperations.stream().map(AclOperation::toString).collect(Collectors.joining(",")) +
            ", topologyDescription=" + topologyDescription.map(Object::toString).orElse("") +
            ", topologyDescriptionStatus=" + topologyDescriptionStatus +
            ')';
    }
}
