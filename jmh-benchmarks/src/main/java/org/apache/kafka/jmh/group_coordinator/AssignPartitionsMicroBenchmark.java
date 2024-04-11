package org.apache.kafka.jmh.group_coordinator;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.server.common.TopicIdPartition;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AssignPartitionsMicroBenchmark {
    @Param({"1000"})
    private int partitionsPerTopicCount;

    @Param({"50"})
    private int topicCount;

    @Param({"10"})
    private int memberCount;

    @Param({"true", "false"})
    private boolean isClientAssignor;

    protected AbstractPartitionAssignor assignor;

    private PartitionAssignor partitionAssignor;
    private Map<String, List<TopicPartition>> clientAssignment;

    private Map<String, MemberAssignment> serverAssignment;
    private Map<String, Integer> currentAssignmentCounts;

    private List<TopicPartition> topicPartitions;

    private List<TopicIdPartition> topicIdPartitions;
    private List<String> members;


    @Setup(Level.Trial)
    public void setup() {

        this.topicPartitions = new ArrayList<>();
        this.topicIdPartitions = new ArrayList<>();

        members = new ArrayList<>();
        for (int i = 0; i < memberCount; i++) {
            String memberName = "member" + i;
            members.add(memberName);
        }

        int totalPartitionsCount = partitionsPerTopicCount * topicCount;
        int maxQuota = (int) Math.ceil(((double) totalPartitionsCount) / memberCount);

        if (isClientAssignor) {
            for (int t = 0; t < topicCount; t++) {
                String topicName = "topic" + t;
                for (int i = 0; i < partitionsPerTopicCount; i++) {
                    topicPartitions.add(new TopicPartition(topicName, i));
                }
            }
            this.clientAssignment = new HashMap<>(members.stream()
                .collect(Collectors.toMap(c -> c, c -> new ArrayList<>(maxQuota))));
        } else {
            for (int t = 0; t < topicCount; t++) {
                Uuid topicId = Uuid.randomUuid();
                for (int i = 0; i < partitionsPerTopicCount; i++) {
                    topicIdPartitions.add(new TopicIdPartition(topicId, i));
                }
            }
            this.serverAssignment = new HashMap<>();
            this.currentAssignmentCounts = new HashMap<>();
            members.forEach(memberId -> {
                this.serverAssignment.put(memberId, new MemberAssignment(new HashMap<>()));
                this.currentAssignmentCounts.put(memberId, 0);
            });
        }

        this.assignor = new CooperativeStickyAssignor();
        this.partitionAssignor = new UniformAssignor();
    }

    private int assignNewPartitionClient(TopicPartition unassignedPartition, String consumer) {
        List<TopicPartition> consumerAssignment = clientAssignment.get(consumer);
        consumerAssignment.add(unassignedPartition);
        return consumerAssignment.size();
    }

    private void assignAllPartitionsClient() {
        for (TopicPartition topicPartition : topicPartitions) {
            assignNewPartitionClient(topicPartition, members.get(0));
        }
        clientAssignment.get(members.get(0)).clear();
    }

    private int assignNewPartitionServer(TopicIdPartition topicIdPartition, String memberId) {
        serverAssignment.get(memberId)
            .targetPartitions()
            .computeIfAbsent(topicIdPartition.topicId(), __ -> new HashSet<>())
            .add(topicIdPartition.partitionId());
        return currentAssignmentCounts.merge(memberId, 1, Integer::sum);
    }

    private void assignAllPartitionsServer() {
        for (int t = 0 ; t < topicCount; t ++){
            TopicIdPartition topicIdPartition = topicIdPartitions.get(t);
            for (int i = 0; i < partitionsPerTopicCount; i++) {
                assignNewPartitionServer(topicIdPartition, members.get(0));
            }
        }
    }

    private void assignAllPartitions() {
        if (isClientAssignor) {
            assignAllPartitionsClient();
        } else {
            assignAllPartitionsServer();
        }
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        assignAllPartitions();
    }
}
