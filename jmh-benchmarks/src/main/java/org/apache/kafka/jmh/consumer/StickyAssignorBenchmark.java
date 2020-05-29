package org.apache.kafka.jmh.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class StickyAssignorBenchmark {

    static final int NUM_CONSUMERS = 2100;
    static final int NUM_PARTITIONS_PER_TOPIC = 2100;
    static final int NUM_TOPICS = 100;

    private static final String CONSUMER_ID = "consumer";
    private static final String TOPIC_NAME = "topic";

    private CooperativeStickyAssignor assignor;
    private List<String> allTopics;
    private Map<String, Subscription> subscriptions;
    private Map<String, Integer> partitionsPerTopic;

    @Setup(Level.Trial)
    public void setup() {
        assignor = new CooperativeStickyAssignor();

        allTopics = new ArrayList<>();
        partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < NUM_TOPICS; ++i) {
            final String topicName = TOPIC_NAME + "-" + i;
            partitionsPerTopic.put(topicName, NUM_PARTITIONS_PER_TOPIC);
            allTopics.add(topicName);
        }

        subscriptions = new HashMap<>();
        for (int i = 0; i < NUM_CONSUMERS; ++i) {
            final Subscription subscription = new Subscription(allTopics, null);
            subscriptions.put(CONSUMER_ID + "-" + i, subscription);
        }

    }

    @Benchmark
    public void testCooperativeStickyAssignor() {
        //WIP
        long startTime = System.currentTimeMillis();
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        long endTime = System.currentTimeMillis();

        System.out.println("Total time to run was " + (endTime - startTime));

    }

    /*
    TODO: need to setup differently for StickyAssignor
    @Benchmark
    public void testStickyAssignor() {
        assignor = new StickyAssignor();
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
    }
    */

}
