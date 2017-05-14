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
package org.apache.kafka.clients.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

public class PartitionMovements {
    private static final Logger log = LoggerFactory.getLogger(PartitionMovements.class);

    private Map<String, Map<ConsumerPair, Set<TopicPartition>>> partitionMovementsByTopic = new HashMap<>();
    private Map<TopicPartition, ConsumerPair> partitionMovements = new HashMap<>();

    private ConsumerPair removeMovementRecordOfPartition(TopicPartition partition) {
        ConsumerPair pair = partitionMovements.remove(partition);

        String topic = partition.topic();
        Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
        partitionMovementsForThisTopic.get(pair).remove(partition);
        if (partitionMovementsForThisTopic.get(pair).isEmpty())
            partitionMovementsForThisTopic.remove(pair);
        if (partitionMovementsByTopic.get(topic).isEmpty())
            partitionMovementsByTopic.remove(topic);

        return pair;
    }

    private void addPartitionMovementRecord(TopicPartition partition, ConsumerPair pair) {
        partitionMovements.put(partition, pair);

        String topic = partition.topic();
        if (!partitionMovementsByTopic.containsKey(topic))
            partitionMovementsByTopic.put(topic, new HashMap<ConsumerPair, Set<TopicPartition>>());

        Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
        if (!partitionMovementsForThisTopic.containsKey(pair))
            partitionMovementsForThisTopic.put(pair, new HashSet<TopicPartition>());

        partitionMovementsForThisTopic.get(pair).add(partition);
    }

    public void movePartition(TopicPartition partition, String oldConsumer, String newConsumer) {
        ConsumerPair pair = new ConsumerPair(oldConsumer, newConsumer);

        if (partitionMovements.containsKey(partition)) {
            // this partition has previously moved
            ConsumerPair existingPair = removeMovementRecordOfPartition(partition);
            assert existingPair.dst.equals(oldConsumer);
            if (!existingPair.src.equals(newConsumer)) {
                // the partition is not moving back to its previous consumer
                // return new ConsumerPair2(existingPair.src, newConsumer);
                addPartitionMovementRecord(partition, new ConsumerPair(existingPair.src, newConsumer));
            }
        } else
            addPartitionMovementRecord(partition, pair);
    }

    public TopicPartition getTheActualPartitionToBeMoved(TopicPartition partition, String oldConsumer, String newConsumer) {
        String topic = partition.topic();

        if (!partitionMovementsByTopic.containsKey(topic))
            return partition;

        if (partitionMovements.containsKey(partition)) {
            // this partition has previously moved
            assert oldConsumer.equals(partitionMovements.get(partition).dst);
            oldConsumer = partitionMovements.get(partition).src;
        }

        Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
        ConsumerPair reversePair = new ConsumerPair(newConsumer, oldConsumer);
        if (!partitionMovementsForThisTopic.containsKey(reversePair))
            return partition;

        return partitionMovementsForThisTopic.get(reversePair).iterator().next();
    }

    public static boolean verifyStickiness(Map<String, List<TopicPartition>> prevAssignments,
                                           Map<String, List<TopicPartition>> postAssignments) {
        if (prevAssignments.isEmpty())
            return true;

        int size = prevAssignments.size();

        Map<ConsumerPair, Set<String>> movements = new HashMap<>();
        Map<String, Set<ConsumerPair>> perTopicMovements = new HashMap<>();

        List<String> consumers = Utils.sorted(prevAssignments.keySet());

        for (int i = 0; i < size; ++i) {
            String consumer = consumers.get(i);
            List<TopicPartition> prev = prevAssignments.get(consumer);
            for (TopicPartition partition: prev) {

                if (postAssignments.containsKey(consumer) && postAssignments.get(consumer).contains(partition))
                    // the partition hasn't moved
                    continue;

                for (int j = 0; j < size; ++j) {
                    if (j == i)
                        continue;

                    String otherConsumer = consumers.get(j);
                    if (otherConsumer == null || !postAssignments.containsKey(otherConsumer))
                        continue;

                    ConsumerPair pair = new ConsumerPair(consumer, otherConsumer);
                    List<TopicPartition> post = postAssignments.get(otherConsumer);
                    if (post.contains(partition)) {
                        String topic = partition.topic();
                        // movement of partition from consumer to otherConsumer
                        if (movements.get(pair) == null) {
                            Set<String> moved = new HashSet<>(Collections.singleton(topic));
                            movements.put(pair, moved);
                        } else
                            movements.get(pair).add(topic);

                        // movement per topic
                        if (perTopicMovements.containsKey(topic)) {
                            if (!pair.in(perTopicMovements.get(topic)))
                                perTopicMovements.get(topic).add(pair);
                        } else
                            perTopicMovements.put(topic, new HashSet<>(Collections.singleton(pair)));
                    } else
                        movements.put(pair, null);
                }
            }
        }

        for (Map.Entry<String, Set<ConsumerPair>> topicMovements: perTopicMovements.entrySet()) {
            if (ConsumerPair.hasReversePairs(topicMovements.getValue())) {
                log.error("Stickiness is violated for topic " + topicMovements.getKey()
                        + "\nPartition movements for this topic occurred between the following consumer pairs:"
                        + "\n" + topicMovements.getValue().toString());
                return false;
            }
        }

        return true;
    }
}
