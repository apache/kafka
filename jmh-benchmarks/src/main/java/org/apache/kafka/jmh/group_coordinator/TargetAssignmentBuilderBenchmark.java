package org.apache.kafka.jmh.group_coordinator;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicMetadata;
import org.apache.kafka.coordinator.group.consumer.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.consumer.VersionedMetadata;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.max;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TargetAssignmentBuilderBenchmark {

    @Param({"1000", "10000"})
    private int memberCount;

    @Param({"10", "50"})
    private int partitionsPerTopicCount;

    @Param({"1000"})
    private int topicCount;

    @Param({"true", "false"})
    private boolean isSubscriptionUniform;

    @Param({"true", "false"})
    private boolean isRangeAssignor;

    @Param({"true", "false"})
    private boolean isRackAware;

    /**
     * The group Id.
     */
    String groupId = "benchmark-group";

    /**
     * The group epoch.
     */
    private final int groupEpoch = 0;

    /**
     * The partition partitionAssignor used to compute the assignment.
     */
    private PartitionAssignor partitionAssignor;

    /**
     * The members in the group.
     */
    private Map<String, ConsumerGroupMember> members = Collections.emptyMap();

    /**
     * The subscription metadata.
     */
    private Map<String, TopicMetadata> subscriptionMetadata = Collections.emptyMap();

    /**
     * The existing target assignment.
     */
    private Map<String, Assignment> existingTargetAssignment = Collections.emptyMap();

    private TargetAssignmentBuilder targetAssignmentBuilder;

    private AssignmentSpec assignmentSpec;

    int numberOfRacks = 3;

    List<String> allTopicNames = new ArrayList<>(topicCount);
    List<Uuid> allTopicIds = new ArrayList<>(topicCount);

    @Setup(Level.Trial)
    public void setup() {

        if (isRangeAssignor) {
            this.partitionAssignor = new RangeAssignor();
        } else {
            this.partitionAssignor = new UniformAssignor();
        }

        this.subscriptionMetadata = generateMockSubscriptionMetadata();

        this.members = generateMockMembers();

        this.existingTargetAssignment = generateMockInitialTargetAssignment();

        // Add a new member to trigger a rebalance.
        Set<String> subscribedTopics = new HashSet<>(subscriptionMetadata.keySet());
        String rackId = isRackAware ? "rack" + (memberCount + 1) % numberOfRacks : "";
        ConsumerGroupMember new_member = new ConsumerGroupMember.Builder("new-member")
            .setSubscribedTopicNames(new ArrayList<>(subscribedTopics))
            .setRackId(rackId)
            .build();

        this.targetAssignmentBuilder = new TargetAssignmentBuilder(groupId, groupEpoch, partitionAssignor)
            .withMembers(members)
            .withSubscriptionMetadata(subscriptionMetadata)
            .withTargetAssignment(existingTargetAssignment)
            .addOrUpdateMember(new_member.memberId(), new_member);
    }

    private Map<String, ConsumerGroupMember> generateMockMembers() {
        Map<String, ConsumerGroupMember> members = new HashMap<>();

        int topicCounter = 0;

        for (int i = 0; i < memberCount; i++) {
            Set<String> subscribedTopics;
            if (isSubscriptionUniform) {
                subscribedTopics = new HashSet<>(allTopicNames);
            } else {
                subscribedTopics = new HashSet<>(Arrays.asList(allTopicNames.get(i % topicCount), allTopicNames.get((i+1) % topicCount)));
                topicCounter = max (topicCounter, (i+1) % topicCount);
                if (i == memberCount - 1 && topicCounter < topicCount - 1) {
                    subscribedTopics.addAll(allTopicNames.subList(topicCounter + 1, topicCount));
                }
            }

            String rackId = isRackAware ? "rack" + i % numberOfRacks : "" ;
            ConsumerGroupMember member = new ConsumerGroupMember.Builder("member" + i)
                .setSubscribedTopicNames(new ArrayList<>(subscribedTopics))
                .setRackId(rackId)
                .build();
            members.put("member" + i, member);
        }
        return members;
    }

    private Map<String, TopicMetadata> generateMockSubscriptionMetadata() {
        Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = "topic-" + i;
            Uuid topicId = Uuid.randomUuid();
            allTopicNames.add(topicName);
            allTopicIds.add(topicId);
            Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(partitionsPerTopicCount);
            TopicMetadata metadata = new TopicMetadata(topicId, topicName, partitionsPerTopicCount, partitionRacks);
            subscriptionMetadata.put(topicName, metadata);
        }

        return subscriptionMetadata;
    }

    private Map<String, Assignment> generateMockInitialTargetAssignment() {
        Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>(topicCount);
        subscriptionMetadata.forEach((topicName, topicMetadata) ->
            topicMetadataMap.put(
                topicMetadata.id(),
                topicMetadata
            )
        );

        addTopicSubscriptions();

        GroupAssignment groupAssignment = partitionAssignor.assign(
            assignmentSpec,
            new SubscribedTopicMetadata(topicMetadataMap)
        );

        Map<String, Assignment> initialTargetAssignment = new HashMap<>(memberCount);

        for (Map.Entry<String, MemberAssignment> entry : groupAssignment.members().entrySet()) {
            String memberId = entry.getKey();
            Map<Uuid, Set<Integer>> topicPartitions = entry.getValue().targetPartitions();

            Assignment assignment = new Assignment((byte) 0, topicPartitions, VersionedMetadata.EMPTY);

            initialTargetAssignment.put(memberId, assignment);
        }

        return initialTargetAssignment;
    }

    private Map<Integer, Set<String>> mkMapOfPartitionRacks(int numPartitions) {
        Map<Integer, Set<String>> partitionRacks = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitionRacks.put(i, new HashSet<>(Arrays.asList("rack" + i % numberOfRacks, "rack" + (i + 1) % numberOfRacks)));
        }
        return partitionRacks;
    }

    private void addTopicSubscriptions() {
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        int topicCounter = 0;

        for (int i = 0; i < memberCount; i++) {
            String memberName = "member" + i;
            Optional<String> rackId = isRackAware ? Optional.of("rack" + i % numberOfRacks) : Optional.empty();
            List<Uuid> subscribedTopicIds;

            // When subscriptions are uniform, all members are assigned all topics.
            if (isSubscriptionUniform) {
                subscribedTopicIds = allTopicIds;
            } else {
                subscribedTopicIds = Arrays.asList(
                    allTopicIds.get(i % topicCount),
                    allTopicIds.get((i+1) % topicCount)
                );
                topicCounter = max (topicCounter, ((i+1) % topicCount));

                if (i == memberCount - 1 && topicCounter < topicCount - 1) {
                    subscribedTopicIds.addAll(allTopicIds.subList(topicCounter + 1, topicCount - 1));
                }
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                rackId,
                subscribedTopicIds,
                Collections.emptyMap()
            ));
        }
        this.assignmentSpec = new AssignmentSpec(members);
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void build() {
        this.targetAssignmentBuilder.build();
    }
}
