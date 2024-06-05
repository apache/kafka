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
package org.apache.kafka.jmh.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.image.MetadataDelta;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AssignorBenchmarkUtils {
    /**
     * Generate a reverse look up map of partition to member target assignments from the given member spec.
     *
     * @param groupAssignment       The group assignment.
     * @return Map of topic partition to member assignments.
     */
    public static Map<Uuid, Map<Integer, String>> computeInvertedTargetAssignment(
        GroupAssignment groupAssignment
    ) {
        Map<Uuid, Map<Integer, String>> invertedTargetAssignment = new HashMap<>();
        for (Map.Entry<String, MemberAssignment> memberEntry : groupAssignment.members().entrySet()) {
            String memberId = memberEntry.getKey();
            Map<Uuid, Set<Integer>> topicsAndPartitions = memberEntry.getValue().partitions();

            for (Map.Entry<Uuid, Set<Integer>> topicEntry : topicsAndPartitions.entrySet()) {
                Uuid topicId = topicEntry.getKey();
                Set<Integer> partitions = topicEntry.getValue();

                Map<Integer, String> partitionMap = invertedTargetAssignment.computeIfAbsent(topicId, k -> new HashMap<>());

                for (Integer partitionId : partitions) {
                    partitionMap.put(partitionId, memberId);
                }
            }
        }
        return invertedTargetAssignment;
    }

    public static void addTopic(
        MetadataDelta delta,
        Uuid topicId,
        String topicName,
        int numPartitions
    ) {
        // For testing purposes, the following criteria are used:
        // - Number of replicas for each partition: 2
        // - Number of brokers available in the cluster: 4
        delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
        for (int i = 0; i < numPartitions; i++) {
            delta.replay(new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(i)
                .setReplicas(Arrays.asList(i % 4, (i + 1) % 4)));
        }
    }
}
