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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * A detailed description of a single consumer group in the cluster.
 */
public class ConsumerGroupDescription {
    private final String groupId;
    private final boolean isSimpleConsumerGroup;
    private final Collection<MemberDescription> members;
    private final String partitionAssignor;
    private final ConsumerGroupState state;
    private final Node coordinator;

    public ConsumerGroupDescription(String groupId,
                                    boolean isSimpleConsumerGroup,
                                    Collection<MemberDescription> members,
                                    String partitionAssignor,
                                    ConsumerGroupState state,
                                    Node coordinator) {
        this.groupId = groupId == null ? "" : groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        this.members = members == null ? Collections.<MemberDescription>emptyList() :
            Collections.unmodifiableList(new ArrayList<>(members));
        this.partitionAssignor = partitionAssignor == null ? "" : partitionAssignor;
        this.state = state;
        this.coordinator = coordinator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerGroupDescription that = (ConsumerGroupDescription) o;
        return isSimpleConsumerGroup == that.isSimpleConsumerGroup &&
            groupId.equals(that.groupId) &&
            members.equals(that.members) &&
            partitionAssignor.equals(that.partitionAssignor) &&
            state.equals(that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSimpleConsumerGroup, groupId, members, partitionAssignor, state);
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
     * The consumer group state, or UNKNOWN if the state is too new for us to parse.
     */
    public ConsumerGroupState state() {
        return state;
    }

    /**
     * The consumer group coordinator, or null if the coordinator is not known.
     */
    public Node coordinator() {
        return coordinator;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", isSimpleConsumerGroup=" + isSimpleConsumerGroup +
            ", members=" + Utils.join(members, ",") +
            ", partitionAssignor=" + partitionAssignor +
            ", state=" + state +
            ", coordinator=" + coordinator +
            ")";
    }
}
