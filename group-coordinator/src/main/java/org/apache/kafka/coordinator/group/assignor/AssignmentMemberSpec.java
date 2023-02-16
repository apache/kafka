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

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * The assignment specification for a consumer group member.
 */
public class AssignmentMemberSpec {
    /**
     * The instance ID if provided.
     */
    final Optional<String> instanceId;

    /**
     * The rack ID if provided.
     */
    final Optional<String> rackId;

    /**
     * The topics that the member is subscribed to.
     */
    final Collection<String> subscribedTopics;

    /**
     * The current target partitions of the member.
     */
    final Collection<TopicPartition> targetPartitions;

    public AssignmentMemberSpec(
        Optional<String> instanceId,
        Optional<String> rackId,
        Collection<String> subscribedTopics,
        Collection<TopicPartition> targetPartitions
    ) {
        Objects.requireNonNull(instanceId);
        Objects.requireNonNull(rackId);
        Objects.requireNonNull(subscribedTopics);
        Objects.requireNonNull(targetPartitions);
        this.instanceId = instanceId;
        this.rackId = rackId;
        this.subscribedTopics = subscribedTopics;
        this.targetPartitions = targetPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AssignmentMemberSpec that = (AssignmentMemberSpec) o;

        if (!instanceId.equals(that.instanceId)) return false;
        if (!rackId.equals(that.rackId)) return false;
        if (!subscribedTopics.equals(that.subscribedTopics)) return false;
        return targetPartitions.equals(that.targetPartitions);
    }

    @Override
    public int hashCode() {
        int result = instanceId.hashCode();
        result = 31 * result + rackId.hashCode();
        result = 31 * result + subscribedTopics.hashCode();
        result = 31 * result + targetPartitions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AssignmentMemberSpec(instanceId=" + instanceId +
            ", rackId=" + rackId +
            ", subscribedTopics=" + subscribedTopics +
            ", targetPartitions=" + targetPartitions +
            ')';
    }
}
