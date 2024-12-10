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

import org.apache.kafka.common.GroupType;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

/**
 * A detailed description of a single group member in the cluster.
 */
public class MemberDescription {
    private final String memberId;
    private final Optional<String> groupInstanceId;
    private final String clientId;
    private final String host;
    private final MemberAssignment assignment;
    private final Optional<MemberAssignment> targetAssignment;
    private final Optional<Integer> memberEpoch;
    private final Optional<Boolean> upgraded;

    public MemberDescription(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host,
        MemberAssignment assignment,
        Optional<MemberAssignment> targetAssignment,
        Optional<Integer> memberEpoch,
        Optional<Boolean> upgraded
    ) {
        this.memberId = memberId == null ? "" : memberId;
        this.groupInstanceId = groupInstanceId;
        this.clientId = clientId == null ? "" : clientId;
        this.host = host == null ? "" : host;
        this.assignment = assignment == null ?
            new MemberAssignment(Collections.emptySet()) : assignment;
        this.targetAssignment = targetAssignment;
        this.memberEpoch = memberEpoch;
        this.upgraded = upgraded;
    }

    /**
     * @deprecated Since 4.0. Use {@link #MemberDescription(String, Optional, String, String, MemberAssignment, Optional, Optional, Optional)}.
     */
    @Deprecated
    public MemberDescription(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host,
        MemberAssignment assignment,
        Optional<MemberAssignment> targetAssignment
    ) {
        this(
            memberId,
            groupInstanceId,
            clientId,
            host,
            assignment,
            targetAssignment,
            Optional.empty(),
            Optional.empty()
        );
    }

    /**
     * @deprecated Since 4.0. Use {@link #MemberDescription(String, Optional, String, String, MemberAssignment, Optional, Optional, Optional)}.
     */
    @Deprecated
    public MemberDescription(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host,
        MemberAssignment assignment
    ) {
        this(
            memberId,
            groupInstanceId,
            clientId,
            host,
            assignment,
            Optional.empty()
        );
    }

    /**
     * @deprecated Since 4.0. Use {@link #MemberDescription(String, Optional, String, String, MemberAssignment, Optional, Optional, Optional)}.
     */
    @Deprecated
    public MemberDescription(String memberId,
                             String clientId,
                             String host,
                             MemberAssignment assignment) {
        this(memberId, Optional.empty(), clientId, host, assignment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberDescription that = (MemberDescription) o;
        return memberId.equals(that.memberId) &&
            groupInstanceId.equals(that.groupInstanceId) &&
            clientId.equals(that.clientId) &&
            host.equals(that.host) &&
            assignment.equals(that.assignment) &&
            targetAssignment.equals(that.targetAssignment) &&
            memberEpoch.equals(that.memberEpoch) &&
            upgraded.equals(that.upgraded);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, groupInstanceId, clientId, host, assignment, targetAssignment, memberEpoch, upgraded);
    }

    /**
     * The consumer id of the group member.
     */
    public String consumerId() {
        return memberId;
    }

    /**
     * The instance id of the group member.
     */
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * The client id of the group member.
     */
    public String clientId() {
        return clientId;
    }

    /**
     * The host where the group member is running.
     */
    public String host() {
        return host;
    }

    /**
     * The assignment of the group member. Provided for both classic group and consumer group.
     */
    public MemberAssignment assignment() {
        return assignment;
    }

    /**
     * The target assignment of the member. Provided only for consumer group.
     */
    public Optional<MemberAssignment> targetAssignment() {
        return targetAssignment;
    }

    /**
     * The epoch of the group member.
     * The optional is set to an integer if the member is in a {@link GroupType#CONSUMER} group, and to empty if it
     * is in a {@link GroupType#CLASSIC} group.
     */
    public Optional<Integer> memberEpoch() {
        return memberEpoch;
    }

    /**
     * The flag indicating whether a member within a {@link GroupType#CONSUMER} group uses the
     * {@link GroupType#CONSUMER} protocol.
     * The optional is set to true if it does, to false if it does not, and to empty if it is unknown or if the group
     * is a {@link GroupType#CLASSIC} group.
     */
    public Optional<Boolean> upgraded() {
        return upgraded;
    }

    @Override
    public String toString() {
        return "(memberId=" + memberId +
            ", groupInstanceId=" + groupInstanceId.orElse("null") +
            ", clientId=" + clientId +
            ", host=" + host +
            ", assignment=" + assignment +
            ", targetAssignment=" + targetAssignment +
            ", memberEpoch=" + memberEpoch.orElse(null) +
            ", upgraded=" + upgraded.orElse(null) +
            ")";
    }
}
