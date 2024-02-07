package org.apache.kafka.jmh.group_coordinator;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ClientSideAssignorBenchmark {

    @Param({"10"})
    private int topicCount;

    @Param({"8"})
    private int partitionCount;

    @Param({"10"})
    private int memberCount;

    @Param({"false", "true"})
    private boolean isRackAware;

    @Param({"false", "true"})
    private boolean isSubscriptionUniform;

    @Param({"false", "true"})
    private boolean isRangeAssignor;

    private Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();
    protected int numBrokerRacks = 4;
    protected int replicationFactor = 2;
    protected AbstractPartitionAssignor assignor;
    private Map<String, List<PartitionInfo>> partitionsPerTopic;

    @Setup(Level.Trial)
    public void setup() {
        List<String> topics = new ArrayList<>();
        this.partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = "topic" + i;
            topics.add(topicName);
            this.partitionsPerTopic.put(topicName, partitionInfos(topicName, partitionCount));
        }

        addSubscriptions(topics);
        if (isRangeAssignor) {
            this.assignor = new RangeAssignor();
        } else {
            this.assignor = new CooperativeStickyAssignor();
        }
    }

    private void addSubscriptions(List<String> topics) {
        for (int i = 0; i < memberCount; i++) {
            String memberName = "member" + i;
            if (i == memberCount - 1 && !isSubscriptionUniform) {
                this.subscriptions.put(memberName, subscription(topics.subList(0, 1), i));
            } else {
                this.subscriptions.put(memberName, subscription(topics, i));
            }
        }
    }

    private List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions) {
        // Ensure there are enough racks and brokers for the replication factor
        if ( numBrokerRacks < replicationFactor) {
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
        String rackId = "rack" + consumerIndex % 4;
        if (isRackAware) {
            return new ConsumerPartitionAssignor.Subscription(topics, null, Collections.emptyList(), DEFAULT_GENERATION, Optional.of(rackId));
        }
        return new ConsumerPartitionAssignor.Subscription(topics, null, Collections.emptyList(), DEFAULT_GENERATION, Optional.empty());
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        assignor.assignPartitions(partitionsPerTopic, subscriptions);
    }
}
