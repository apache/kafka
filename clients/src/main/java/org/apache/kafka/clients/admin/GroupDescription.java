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

import org.apache.kafka.common.utils.Utils;

import java.util.List;

/**
 * A detailed description of a single group in the cluster.
 */
public class GroupDescription {

    private final String groupId;
    private final String protocolType;
    private final List<MemberDescription> members;

    /**
     * Creates an instance with the specified parameters.
     *
     * @param groupId      The group id
     * @param protocolType The protocol type
     * @param members      The group members
     */
    public GroupDescription(String groupId, String protocolType, List<MemberDescription> members) {
        this.groupId = groupId;
        this.protocolType = protocolType;
        this.members = members;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupDescription that = (GroupDescription) o;

        if (groupId != null ? !groupId.equals(that.groupId) : that.groupId != null) return false;
        if (protocolType != null ? !protocolType.equals(that.protocolType) : that.protocolType != null) return false;
        return members != null ? members.equals(that.members) : that.members == null;
    }

    @Override
    public int hashCode() {
        int result = groupId != null ? groupId.hashCode() : 0;
        result = 31 * result + (protocolType != null ? protocolType.hashCode() : 0);
        result = 31 * result + (members != null ? members.hashCode() : 0);
        return result;
    }

    /**
     * The id of the group.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * The protocol type of the group.
     */
    public String protocolType() {
        return protocolType;
    }

    /**
     * A list of the members of the group.
     */
    public List<MemberDescription> members() {
        return members;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId + ", protocolType=" + protocolType + ", members=" +
            Utils.join(members, ",") + ")";
    }
}
