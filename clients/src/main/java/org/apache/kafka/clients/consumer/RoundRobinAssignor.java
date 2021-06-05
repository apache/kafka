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
 * <p>The round robin assignor lays out all the available partitions and all the available consumers. It
 * then proceeds to do a round robin assignment from partition to consumer. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumers.)
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and <code>t1</code>,
 * and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>, <code>t0p2</code>,
 * <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p2, t1p1]</code>
 * <li><code>C1: [t0p1, t1p0, t1p2]</code>
 * </ul>
 *
 * <p>When subscriptions differ across consumer instances, the assignment process still considers each
 * consumer instance in round robin fashion but skips over an instance if it is not subscribed to
 * the topic. Unlike the case when subscriptions are identical, this can result in imbalanced
 * assignments. For example, we have three consumers <code>C0</code>, <code>C1</code>, <code>C2</code>,
 * and three topics <code>t0</code>, <code>t1</code>, <code>t2</code>, with 1, 2, and 3 partitions, respectively.
 * Therefore, the partitions are <code>t0p0</code>, <code>t1p0</code>, <code>t1p1</code>, <code>t2p0</code>, <code>t2p1</code>, <code>t2p2</code>.
 * <code>C0</code> is subscribed to <code>t0</code>;
 * <code>C1</code> is subscribed to <code>t0</code>, <code>t1</code>;
 * and <code>C2</code> is subscribed to <code>t0</code>, <code>t1</code>, <code>t2</code>.
 *
 * <p>That assignment will be:
 * <ul>
 * <li><code>C0: [t0p0]</code>
 * <li><code>C1: [t1p0]</code>
 * <li><code>C2: [t1p1, t2p0, t2p1, t2p2]</code>
 * </ul>
 *
 * Since the introduction of static membership, we could leverage <code>group.instance.id</code> to make the assignment behavior more sticky.
 * For example, we have three consumers with assigned <code>member.id</code> <code>C0</code>, <code>C1</code>, <code>C2</code>,
 * two topics <code>t0</code> and <code>t1</code>, and each topic has 3 partitions, resulting in partitions <code>t0p0</code>,
 * <code>t0p1</code>, <code>t0p2</code>, <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>. We choose to honor
 * the sorted order based on ephemeral <code>member.id</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t1p0]</code>
 * <li><code>C1: [t0p1, t1p1]</code>
 * <li><code>C2: [t0p2, t1p2]</code>
 * </ul>
 *
 * After one rolling bounce, group coordinator will attempt to assign new <code>member.id</code> towards consumers,
 * for example <code>C0</code> -&gt; <code>C5</code> <code>C1</code> -&gt; <code>C3</code>, <code>C2</code> -&gt; <code>C4</code>.
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>C3 (was C1): [t0p0, t1p0] (before was [t0p1, t1p1])</code>
 * <li><code>C4 (was C2): [t0p1, t1p1] (before was [t0p2, t1p2])</code>
 * <li><code>C5 (was C0): [t0p2, t1p2] (before was [t0p0, t1p0])</code>
 * </ul>
 *
 * This issue could be mitigated by the introduction of static membership. Consumers will have individual instance ids
 * <code>I1</code>, <code>I2</code>, <code>I3</code>. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>I0: [t0p0, t1p0]</code>
 * <li><code>I1: [t0p1, t1p1]</code>
 * <li><code>I2: [t0p2, t1p2]</code>
 * </ul>
 */
public class RoundRobinAssignor extends AbstractPartitionAssignor {

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        List<MemberInfo> memberInfoList = new ArrayList<>();
        for (Map.Entry<String, Subscription> memberSubscription : subscriptions.entrySet()) {
            assignment.put(memberSubscription.getKey(), new ArrayList<>());
            memberInfoList.add(new MemberInfo(memberSubscription.getKey(),
                                              memberSubscription.getValue().groupInstanceId()));
        }

        CircularIterator<MemberInfo> assigner = new CircularIterator<>(Utils.sorted(memberInfoList));

        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            final String topic = partition.topic();
            while (!subscriptions.get(assigner.peek().memberId).topics().contains(topic))
                assigner.next();
            assignment.get(assigner.next().memberId).add(partition);
        }
        return assignment;
    }

    private List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
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
