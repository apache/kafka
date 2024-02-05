package org.apache.kafka.jmh.group_coordinator;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
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

    @Param({"100"})
    private int topicCount;

    @Param({"20"})
    private int partitionCount;

    @Param({"20"})
    private int memberCount;

    @Param({"false", "true"})
    private boolean isRackAware;

    @Param({"false", "true"})
    private boolean isSubscriptionUniform;

    @Param({"false", "true"})
    private boolean isRangeAssignor;

    private Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();
    private int nextPartitionIndex;
    protected int numBrokerRacks;
    protected int replicationFactor = 2;
    protected AbstractStickyAssignor assignor;
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
        this.assignor = new CooperativeStickyAssignor();
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

    protected List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions) {
        int nextIndex = nextPartitionIndex;
        nextPartitionIndex += 1;
        return AbstractPartitionAssignorTest.partitionInfos(topic, numberOfPartitions,
            replicationFactor, numBrokerRacks, nextIndex);
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
