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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * A detailed description of a single consumer group in the cluster.
 */
public class ConsumerGroupDescription {
    private final static String PREPARING_REBALANCE = "PreparingRebalance";
    private final static String COMPLETING_REBALANCE = "CompletingRebalance";
    private final static String DEAD = "Dead";
    private final static String STABLE = "Stable";

    private final String groupId;
    private final boolean isSimpleConsumerGroup;
    private final Collection<MemberDescription> members;
    private final String partitionAssignor;
    private final String state;

    public static class Builder {
        private final String groupId;
        private boolean isSimpleConsumerGroup = true;
        private Collection<MemberDescription> members = null;
        private String partitionAssignor = "";
        private String state = "";

        public Builder(String groupId) {
            this.groupId = groupId;
        }

        public Builder isSimpleConsumerGroup(boolean isSimpleConsumerGroup) {
            this.isSimpleConsumerGroup = isSimpleConsumerGroup;
            return this;
        }

        public Builder members(Collection<MemberDescription> members) {
            this.members = members;
            return this;
        }

        public Builder partitionAssignor(String partitionAssignor) {
            this.partitionAssignor = partitionAssignor;
            return this;
        }

        public Builder state(String state) {
            this.state = state;
            return this;
        }

        public ConsumerGroupDescription build() {
            return new ConsumerGroupDescription(groupId, isSimpleConsumerGroup,
                members, partitionAssignor, state);
        }
    }

    private ConsumerGroupDescription(String groupId, boolean isSimpleConsumerGroup,
            Collection<MemberDescription> members, String partitionAssignor, String state) {
        this.groupId = groupId == null ? "" : groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        this.members = members == null ? Collections.<MemberDescription>emptyList() :
            Collections.unmodifiableList(new ArrayList<>(members));
        this.partitionAssignor = partitionAssignor == null ? "" : partitionAssignor;
        this.state = state == null ? "" : state;
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
     * The consumer group partition assignor.
     */
    public String state() {
        return state;
    }

    /**
     * Returns true if the consumer group is currently rebalancing.
     * If the consumer group is rebalancing, the member list will be empty.
     */
    public boolean isRebalancing() {
        return state.equals(PREPARING_REBALANCE) || state.equals(COMPLETING_REBALANCE);
    }

    /**
     * Returns true if the consumer group is currently dead.
     * If the consumer group is dead, the member list will be empty.
     */
    public boolean isDead() {
        return state.equals(DEAD);
    }

    /**
     * Returns true if the consumer group is currently stable.
     */
    public boolean isStable() {
        return state.equals(STABLE);
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", isSimpleConsumerGroup=" + isSimpleConsumerGroup +
            ", members=" + Utils.join(members, ",") +
            ", partitionAssignor=" + partitionAssignor +
            ", state=" + state + ")";
    }
}
