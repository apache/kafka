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
 * A detailed description of a single consumer group in the cluster.
 */
public class ConsumerGroupDescription {

    private final String groupId;
    private final boolean isSimpleConsumerGroup;
    private final List<MemberDescription> members;
    private final String partitionAssignor;

    /**
     * Creates an instance with the specified parameters.
     *
     * @param groupId               The consumer group id
     * @param isSimpleConsumerGroup If Consumer Group is simple
     * @param members               The consumer group members
     * @param partitionAssignor     The consumer group partition assignor
     */
    public ConsumerGroupDescription(String groupId, boolean isSimpleConsumerGroup, List<MemberDescription> members, String partitionAssignor) {
        this.groupId = groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        this.members = members;
        this.partitionAssignor = partitionAssignor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerGroupDescription that = (ConsumerGroupDescription) o;

        if (isSimpleConsumerGroup != that.isSimpleConsumerGroup) return false;
        if (groupId != null ? !groupId.equals(that.groupId) : that.groupId != null) return false;
        if (members != null ? !members.equals(that.members) : that.members != null) return false;
        return partitionAssignor != null ? partitionAssignor.equals(that.partitionAssignor) : that.partitionAssignor == null;
    }

    @Override
    public int hashCode() {
        int result = groupId != null ? groupId.hashCode() : 0;
        result = 31 * result + (isSimpleConsumerGroup ? 1 : 0);
        result = 31 * result + (members != null ? members.hashCode() : 0);
        result = 31 * result + (partitionAssignor != null ? partitionAssignor.hashCode() : 0);
        return result;
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
    public List<MemberDescription> members() {
        return members;
    }

    /**
     * The consumer group partition assignor.
     */
    public String partitionAssignor() {
        return partitionAssignor;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId + ", isSimpleConsumerGroup=" + isSimpleConsumerGroup + ", members=" +
            Utils.join(members, ",") + ", partitionAssignor=" + partitionAssignor + ")";
    }
}
