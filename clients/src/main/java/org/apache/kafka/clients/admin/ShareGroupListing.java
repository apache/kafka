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

import org.apache.kafka.common.ShareGroupState;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;
import java.util.Optional;

/**
 * A listing of a share group in the cluster.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ShareGroupListing {
    private final String groupId;
    private final Optional<ShareGroupState> state;

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId Group Id
     */
    public ShareGroupListing(String groupId) {
        this(groupId, Optional.empty());
    }

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId Group Id
     * @param state The state of the share group
     */
    public ShareGroupListing(String groupId, Optional<ShareGroupState> state) {
        this.groupId = groupId;
        this.state = Objects.requireNonNull(state);
    }

    /**
     * The id of the share group.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * The share group state.
     */
    public Optional<ShareGroupState> state() {
        return state;
    }

    @Override
    public String toString() {
        return "(" +
            "groupId='" + groupId + '\'' +
            ", state=" + state +
            ')';
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShareGroupListing)) return false;
        ShareGroupListing that = (ShareGroupListing) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(state, that.state);
    }
}