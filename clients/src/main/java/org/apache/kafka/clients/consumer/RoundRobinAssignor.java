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

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The round robin assignor lays out all the available partitions and all the available consumers. It
 * then proceeds to do a round robin assignment from partition to consumer. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumers.)
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0: [t0p0, t0p2, t1p1]
 * C1: [t0p1, t1p0, t1p2]
 *
 * When subscriptions differ across consumer instances, the assignment process still considers each
 * consumer instance in round robin fashion but skips over an instance if it is not subscribed to
 * the topic. Unlike the case when subscriptions are identical, this can result in imbalanced
 * assignments. For example, we have three consumers C0, C1, C2, and three topics t0, t1, t2,
 * with 1, 2, and 3 partitions, respectively. Therefore, the partitions are t0p0, t1p0, t1p1, t2p0,
 * t2p1, t2p2. C0 is subscribed to t0; C1 is subscribed to t0, t1; and C2 is subscribed to t0, t1, t2.
 *
 * That assignment will be:
 * C0: [t0p0]
 * C1: [t1p0]
 * C2: [t1p1, t2p0, t2p1, t2p2]
 */
public class RoundRobinAssignor extends AbstractPartitionAssignor {

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());

        CircularIterator<String> assigner = new CircularIterator<>(Utils.sorted(subscriptions.keySet()));
        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            final String topic = partition.topic();
            while (!subscriptions.get(assigner.peek()).topics().contains(topic))
                assigner.next();
            assignment.get(assigner.next()).add(partition);
        }
        return assignment;
    }


    public List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        SortedSet<String> topics = new TreeSet<>();
        for (Subscription subscription : subscriptions.values())
            topics.addAll(subscription.topics());

        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : topics) {
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null)
                allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
        return allPartitions;
    }

    @Override
    public String name() {
        return "roundrobin";
    }

}
