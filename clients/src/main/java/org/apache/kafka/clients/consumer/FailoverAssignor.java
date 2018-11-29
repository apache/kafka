package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.util.*;

/**
 * All partitions will be assigned to one consumer, while others consumer are failover.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0: [t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2]
 * C1: []
 *
 * When C0 crashes , the result of the assignment will be
 * C1: [t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2]
 *
 * 冷备模式: 单个 consumer 消费所有 partition 数据, 宕机后重新分配到另外一个 consumer 消费所有数据
 * User: FengHong
 * Date: 2018/11/2811:02
 */
public class FailoverAssignor extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        List<String> subscriptionsKeySet = Utils.sorted(subscriptions.keySet());
        List<TopicPartition> allPartitionsSorted = allPartitionsSorted(partitionsPerTopic, subscriptions);

        assignment.get(subscriptionsKeySet.get(0)).addAll(allPartitionsSorted);
        return assignment;
    }


    @Override
    public String name() {
        return "failover";

    }

    public List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        SortedSet<String> topics = new TreeSet<>();
        for (Subscription subscription : subscriptions.values())
            topics.addAll(subscription.topics());

        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : topics) {
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null) allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
        return allPartitions;
    }


}
