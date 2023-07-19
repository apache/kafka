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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The assignment specification for a consumer group member.
 */
public class AssignmentMemberSpec {
    /**
     * The instance ID if provided.
     */
    private final Optional<String> instanceId;

    /**
     * The rack ID if provided.
     */
    private final Optional<String> rackId;

    /**
     * Topics Ids that the member is subscribed to.
     */
    private final Collection<Uuid> subscribedTopicIds;

    /**
     * Partitions assigned keyed by topicId.
     */
    private final Map<Uuid, Set<Integer>> assignedPartitions;

    /**
     * @return The instance ID as an Optional.
     */
    public Optional<String> instanceId() {
        return instanceId;
    }

    /**
     * @return The rack ID as an Optional.
     */
    public Optional<String> rackId() {
        return rackId;
    }

    /**
     * @return Collection of subscribed topic Ids.
     */
    public Collection<Uuid> subscribedTopicIds() {
        return subscribedTopicIds;
    }

    /**
     * @return Assigned partitions keyed by topic Ids.
     */
    public Map<Uuid, Set<Integer>> assignedPartitions() {
        return assignedPartitions;
    }

    public AssignmentMemberSpec(
        Optional<String> instanceId,
        Optional<String> rackId,
        Collection<Uuid> subscribedTopicIds,
        Map<Uuid, Set<Integer>> assignedPartitions
    ) {
        Objects.requireNonNull(instanceId);
        Objects.requireNonNull(rackId);
        Objects.requireNonNull(subscribedTopicIds);
        Objects.requireNonNull(assignedPartitions);
        this.instanceId = instanceId;
        this.rackId = rackId;
        this.subscribedTopicIds = subscribedTopicIds;
        this.assignedPartitions = assignedPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssignmentMemberSpec that = (AssignmentMemberSpec) o;
        if (!instanceId.equals(that.instanceId)) return false;
        if (!rackId.equals(that.rackId)) return false;
        if (!subscribedTopicIds.equals(that.subscribedTopicIds)) return false;
        return assignedPartitions.equals(that.assignedPartitions);
    }

    @Override
    public int hashCode() {
        int result = instanceId.hashCode();
        result = 31 * result + rackId.hashCode();
        result = 31 * result + subscribedTopicIds.hashCode();
        result = 31 * result + assignedPartitions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AssignmentMemberSpec(instanceId=" + instanceId +
            ", rackId=" + rackId +
            ", subscribedTopicIds=" + subscribedTopicIds +
            ", assignedPartitions=" + assignedPartitions +
            ')';
    }
}
