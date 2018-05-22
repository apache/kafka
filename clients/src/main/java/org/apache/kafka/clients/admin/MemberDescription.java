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

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Objects;

/**
 * A detailed description of a single group instance in the cluster.
 */
public class MemberDescription {
    private final String memberId;
    private final String clientId;
    private final String host;
    private final MemberAssignment assignment;

    public MemberDescription(String memberId, String clientId, String host, MemberAssignment assignment) {
        this.memberId = memberId == null ? "" : memberId;
        this.clientId = clientId == null ? "" : clientId;
        this.host = host == null ? "" : host;
        this.assignment = assignment == null ?
            new MemberAssignment(Collections.<TopicPartition>emptySet()) : assignment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberDescription that = (MemberDescription) o;
        return memberId.equals(that.memberId) &&
            clientId.equals(that.clientId) &&
            host.equals(that.host) &&
            assignment.equals(that.assignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, clientId, host, assignment);
    }

    /**
     * The consumer id of the group member.
     */
    public String consumerId() {
        return memberId;
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
     * The assignment of the group member.
     */
    public MemberAssignment assignment() {
        return assignment;
    }

    @Override
    public String toString() {
        return "(memberId=" + memberId +
            ", clientId=" + clientId +
            ", host=" + host +
            ", assignment=" + assignment + ")";
    }
}
