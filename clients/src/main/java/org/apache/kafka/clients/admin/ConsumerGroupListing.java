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

import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupType;

/**
 * A listing of a consumer group in the cluster.
 */
public class ConsumerGroupListing {
    private final String groupId;
    private final boolean isSimpleConsumerGroup;
    private final Optional<ConsumerGroupState> state;
    private final Optional<GroupType> type;

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId Group Id
     * @param isSimpleConsumerGroup If consumer group is simple or not.
     */
    public ConsumerGroupListing(String groupId, boolean isSimpleConsumerGroup) {
        this(groupId, isSimpleConsumerGroup, Optional.empty(), Optional.empty());
    }

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId Group Id
     * @param isSimpleConsumerGroup If consumer group is simple or not.
     * @param state The state of the consumer group
     */
    public ConsumerGroupListing(String groupId, boolean isSimpleConsumerGroup, Optional<ConsumerGroupState> state) {
        this(groupId, isSimpleConsumerGroup, state, Optional.empty());
    }

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId                   Group Id.
     * @param isSimpleConsumerGroup     If consumer group is simple or not.
     * @param state                     The state of the consumer group.
     * @param type                      The type of the consumer group.
     */
    public ConsumerGroupListing(
        String groupId,
        boolean isSimpleConsumerGroup,
        Optional<ConsumerGroupState> state,
        Optional<GroupType> type
    ) {
        this.groupId = groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
        this.state = Objects.requireNonNull(state);
        this.type = Objects.requireNonNull(type);
    }

    /**
     * Consumer Group Id
     */
    public String groupId() {
        return groupId;
    }

    /**
     * If Consumer Group is simple or not.
     */
    public boolean isSimpleConsumerGroup() {
        return isSimpleConsumerGroup;
    }

    /**
     * Consumer Group state
     */
    public Optional<ConsumerGroupState> state() {
        return state;
    }

    /**
     * The type of the consumer group.
     *
     * @return An Optional containing the type, if available.
     */
    public Optional<GroupType> type() {
        return type;
    }

    @Override
    public String toString() {
        return "(" +
            "groupId='" + groupId + '\'' +
            ", isSimpleConsumerGroup=" + isSimpleConsumerGroup +
            ", state=" + state +
            ", type=" + type +
            ')';
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, isSimpleConsumerGroup(), state, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerGroupListing)) return false;
        ConsumerGroupListing that = (ConsumerGroupListing) o;
        return isSimpleConsumerGroup() == that.isSimpleConsumerGroup() &&
            Objects.equals(groupId, that.groupId) &&
            Objects.equals(state, that.state) &&
            Objects.equals(type, that.type);
    }
}
