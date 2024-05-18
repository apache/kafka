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
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The assignment builder is used to construct the target assignment based on the members' subscriptions.
 */
public abstract class AbstractUniformAssignmentBuilder {
    protected abstract GroupAssignment buildAssignment();

    /**
     * Adds the topic's partition to the member's target assignment.
     */
    protected static void addPartitionToAssignment(
        Map<String, MemberAssignment> memberAssignments,
        String memberId,
        Uuid topicId,
        int partition
    ) {
        memberAssignments.get(memberId)
            .targetPartitions()
            .computeIfAbsent(topicId, __ -> new HashSet<>())
            .add(partition);
    }

    /**
     * Constructs a set of {@code TopicIdPartition} including all the given topic Ids based on their partition counts.
     *
     * @param topicIds                      Collection of topic Ids.
     * @param subscribedTopicDescriber      Describer to fetch partition counts for topics.
     *
     * @return Set of {@code TopicIdPartition} including all the provided topic Ids.
     */
    protected static Set<TopicIdPartition> topicIdPartitions(
        Collection<Uuid> topicIds,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        return topicIds.stream()
            .flatMap(topic -> IntStream
                .range(0, subscribedTopicDescriber.numPartitions(topic))
                .mapToObj(i -> new TopicIdPartition(topic, i))
            ).collect(Collectors.toSet());
    }
}
