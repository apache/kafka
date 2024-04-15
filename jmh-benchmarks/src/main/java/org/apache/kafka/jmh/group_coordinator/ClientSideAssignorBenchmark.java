package org.apache.kafka.jmh.group_coordinator;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.max;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ClientSideAssignorBenchmark {

    @Param({"10", "50", "100"})
    private int partitionsPerTopicCount;

    @Param({"100"})
    private int topicCount;

    @Param({"500", "1000", "10000"})
    private int memberCount;

    @Param({"true", "false"})
    private boolean isRackAware;

    @Param({"true", "false"})
    private boolean isSubscriptionUniform;

    @Param({"true", "false"})
    private boolean isRangeAssignor;

    @Param({"true", "false"})
    private boolean isReassignment;

    private Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();

    private final int numBrokerRacks = 3;

    private final int replicationFactor = 2;

    protected AbstractPartitionAssignor assignor;

    private Map<String, List<PartitionInfo>> partitionsPerTopic;

    private final List<String> allTopicNames = new ArrayList<>(topicCount);

    @Setup(Level.Trial)
    public void setup() {
        this.partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = "topic" + i;
            allTopicNames.add(topicName);
            this.partitionsPerTopic.put(topicName, partitionInfos(topicName, partitionsPerTopicCount));
        }

        addTopicSubscriptions();
        if (isRangeAssignor) {
            this.assignor = new RangeAssignor();
        } else {
            this.assignor = new CooperativeStickyAssignor();
        }

        if (isReassignment) {
            Map<String, List<TopicPartition>> initialAssignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
            Map<String, ConsumerPartitionAssignor.Subscription> newSubscriptions = new HashMap<>();
            subscriptions.forEach((member, subscription) ->
                newSubscriptions.put(
                    member,
                    subscriptionWithOwnedPartitions(initialAssignment.get(member), subscription)
                )
            );
            // Add new member to trigger a reassignment.
            newSubscriptions.put("newMember", subscription(
                new ArrayList<>(partitionsPerTopic.keySet()),
                memberCount
            ));

            this.subscriptions = newSubscriptions;
        }
    }

    private void addTopicSubscriptions() {
        subscriptions.clear();
        int topicCounter = 0;

        for (int i = 0; i < memberCount; i++) {
            String memberName = "member" + i;

            List<String> subscribedTopics;
            // When subscriptions are uniform, all members are assigned all topics.
            if (isSubscriptionUniform) {
                subscribedTopics = allTopicNames;
            } else {
                subscribedTopics = Arrays.asList(
                    allTopicNames.get(i % topicCount),
                    allTopicNames.get((i+1) % topicCount)
                );
                topicCounter = max (topicCounter, ((i+1) % topicCount));

                if (i == memberCount - 1 && topicCounter < topicCount - 1) {
                    subscribedTopics.addAll(allTopicNames.subList(topicCounter + 1, topicCount - 1));
                }
            }

            subscriptions.put(memberName, subscription(subscribedTopics, i));
        }
    }

    private List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions) {
        // Ensure there are enough racks and brokers for the replication factor.
        if (numBrokerRacks < replicationFactor) {
            throw new IllegalArgumentException("Number of broker racks must be at least equal to the replication factor.");
        }

        // Create nodes (brokers), one for each rack.
        List<Node> nodes = new ArrayList<>(numBrokerRacks);
        for (int i = 0; i < numBrokerRacks; i++) {
            nodes.add(new Node(i, "", i, "rack" + i));
        }

        // Create PartitionInfo for each partition.
        List<PartitionInfo> partitionInfos = new ArrayList<>(numberOfPartitions);
        for (int i = 0; i < numberOfPartitions; i++) {
            Node[] replicas = new Node[replicationFactor];
            for (int j = 0; j < replicationFactor; j++) {
                // Assign nodes based on partition number to mimic mkMapOfPartitionRacks logic.
                int nodeIndex = (i + j) % numBrokerRacks;
                replicas[j] = nodes.get(nodeIndex);
            }
            partitionInfos.add(new PartitionInfo(topic, i, replicas[0], replicas, replicas));
        }
        return partitionInfos;
    }

    protected ConsumerPartitionAssignor.Subscription subscription(List<String> topics, int consumerIndex) {
        Optional<String> rackId = isRackAware ?
            Optional.of("rack" + consumerIndex % numBrokerRacks) :
            Optional.empty();

        return new ConsumerPartitionAssignor.Subscription(
            topics,
            null,
            Collections.emptyList(),
            DEFAULT_GENERATION,
            rackId
        );
    }

    protected ConsumerPartitionAssignor.Subscription subscriptionWithOwnedPartitions(
        List<TopicPartition> ownedPartitions,
        ConsumerPartitionAssignor.Subscription prevSubscription
    ) {
        return new ConsumerPartitionAssignor.Subscription(
            prevSubscription.topics(),
            null,
            ownedPartitions,
            DEFAULT_GENERATION,
            prevSubscription.rackId()
        );
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        assignor.assignPartitions(partitionsPerTopic, subscriptions);
    }
}
