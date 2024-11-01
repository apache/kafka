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

import java.util.Objects;
import java.util.Optional;

/**
 * A listing of a group in the cluster.
 */
public class GroupListing {
    private final String groupId;
    private final Optional<GroupType> type;
    private final String protocol;

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId Group Id
     * @param type Group type
     * @param protocol Protocol
     */
    public GroupListing(String groupId, Optional<GroupType> type, String protocol) {
        this.groupId = groupId;
        this.type = Objects.requireNonNull(type);
        this.protocol = protocol;
    }

    /**
     * The group Id.
     *
     * @return Group Id
     */
    public String groupId() {
        return groupId;
    }

    /**
     * The type of the group.
     * <p>
     * If the broker returns a group type which is not recognised, as might
     * happen when talking to a broker with a later version, the type will be
     * <code>Optional.of(GroupType.UNKNOWN)</code>. If the broker is earlier than version 2.6.0,
     * the group type will not be available, and the type will be <code>Optional.empty()</code>.
     *
     * @return An Optional containing the type, if available
     */
    public Optional<GroupType> type() {
        return type;
    }

    /**
     * The protocol of the group.
     *
     * @return The protocol
     */
    public String protocol() {
        return protocol;
    }

    /**
     * If the group is a simple consumer group or not.
     */
    public boolean isSimpleConsumerGroup() {
        return type.filter(gt -> gt == GroupType.CLASSIC).isPresent() && protocol.isEmpty();
    }

    @Override
    public String toString() {
        return "(" +
            "groupId='" + groupId + '\'' +
            ", type=" + type.map(GroupType::toString).orElse("none") +
            ", protocol='" + protocol + '\'' +
            ')';
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, type, protocol);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupListing)) return false;
        GroupListing that = (GroupListing) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(type, that.type) &&
            Objects.equals(protocol, that.protocol);
    }
}
