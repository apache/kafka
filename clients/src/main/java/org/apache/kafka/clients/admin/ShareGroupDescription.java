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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A detailed description of a single share group in the cluster.
 */
@InterfaceStability.Evolving
public class ShareGroupDescription {
    private final String groupId;
    private final Collection<ShareMemberDescription> members;
    private final GroupState groupState;
    private final Node coordinator;
    private final Set<AclOperation> authorizedOperations;

    public ShareGroupDescription(String groupId,
                                 Collection<ShareMemberDescription> members,
                                 GroupState groupState,
                                 Node coordinator) {
        this(groupId, members, groupState, coordinator, Collections.emptySet());
    }

    public ShareGroupDescription(String groupId,
                                 Collection<ShareMemberDescription> members,
                                 GroupState groupState,
                                 Node coordinator,
                                 Set<AclOperation> authorizedOperations) {
        this.groupId = groupId == null ? "" : groupId;
        this.members = members == null ? Collections.emptyList() : List.copyOf(members);
        this.groupState = groupState;
        this.coordinator = coordinator;
        this.authorizedOperations = authorizedOperations;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ShareGroupDescription that = (ShareGroupDescription) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(members, that.members) &&
            groupState == that.groupState &&
            Objects.equals(coordinator, that.coordinator) &&
            Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, members, groupState, coordinator, authorizedOperations);
    }

    /**
     * The id of the share group.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * A list of the members of the share group.
     */
    public Collection<ShareMemberDescription> members() {
        return members;
    }

    /**
     * The group state, or UNKNOWN if the state is too new for us to parse.
     */
    public GroupState groupState() {
        return groupState;
    }

    /**
     * The share group coordinator, or null if the coordinator is not known.
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
            ", members=" + members.stream().map(ShareMemberDescription::toString).collect(Collectors.joining(",")) +
            ", groupState=" + groupState +
            ", coordinator=" + coordinator +
            ", authorizedOperations=" + authorizedOperations +
            ")";
    }
}
