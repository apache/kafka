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

/**
 * A detailed description of a single group instance in the cluster.
 */
public class MemberDescription {

    private final String memberId;
    private final String clientId;
    private final String host;
    private final MemberAssignment assignment;

    /**
     * Creates an instance with the specified parameters.
     *
     * @param memberId The consumer id
     * @param clientId   The client id
     * @param host       The host
     * @param assignment The assignment
     */
    public MemberDescription(String memberId, String clientId, String host, MemberAssignment assignment) {
        this.memberId = memberId;
        this.clientId = clientId;
        this.host = host;
        this.assignment = assignment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberDescription that = (MemberDescription) o;

        if (memberId != null ? !memberId.equals(that.memberId) : that.memberId != null) return false;
        if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
        return assignment != null ? assignment.equals(that.assignment) : that.assignment == null;
    }

    @Override
    public int hashCode() {
        int result = memberId != null ? memberId.hashCode() : 0;
        result = 31 * result + (clientId != null ? clientId.hashCode() : 0);
        result = 31 * result + (assignment != null ? assignment.hashCode() : 0);
        return result;
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
        return "(memberId=" + memberId + ", clientId=" + clientId + ", host=" + host + ", assignment=" +
            assignment + ")";
    }
}
