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

import org.apache.kafka.common.GroupType;

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
     * @param protocol Protocol
     */
    public GroupListing(String groupId, String protocol) {
        this.groupId = groupId;
        this.type = Optional.empty();
        this.protocol = protocol;
    }

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId Group Id
     * @param type Group type
     * @param protocol Protocol
     */
    public GroupListing(String groupId, GroupType type, String protocol) {
        this.groupId = groupId;
        this.type = Optional.of(Objects.requireNonNull(type));
        this.protocol = protocol;
    }

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

    @Override
    public String toString() {
        return "(" +
                "groupId='" + groupId + '\'' +
                ", type=" + type +
                ", protocol=" + protocol +
                ')';
    }

    protected String toStringBase() {
        return "groupId='" + groupId + '\'' +
                ", type=" + type +
                ", protocol=" + protocol;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, type);
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
