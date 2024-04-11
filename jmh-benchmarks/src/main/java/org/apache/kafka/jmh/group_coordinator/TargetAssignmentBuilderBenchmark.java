package org.apache.kafka.jmh.group_coordinator;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicMetadata;
import org.apache.kafka.coordinator.group.consumer.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.consumer.VersionedMetadata;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TargetAssignmentBuilderBenchmark {

    @Param({"1000", "10000"})
    private int memberCount;

    @Param({"10", "50"})
    private int partitionsPerTopicCount;

    @Param({"1000"})
    private int topicCount;


    @Param({"true"})
    private boolean isSubscriptionUniform;

    @Param({"false"})
    private boolean isRangeAssignor;

    @Param({"true"})
    private boolean isRackAware;

    @Param({"true"})
    private boolean isReassignment;

    /**
     * The group id.
     */
    private final String groupId = "benchmark-group";

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

    @Setup(Level.Trial)
    public void setup() {

        if (isRangeAssignor) {
            this.partitionAssignor = new RangeAssignor();
        } else {
            this.partitionAssignor = new UniformAssignor();
        }

        this.subscriptionMetadata = generateMockSubscriptionMetadata();
        //System.out.println("subscription metadata is " + subscriptionMetadata);

        this.members = generateMockMembers();
        //System.out.println("members are" + members);

        this.existingTargetAssignment = generateMockInitialTargetAssignment();
        //System.out.println("existing assignment is" + existingTargetAssignment);

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
        Set<String> allTopicNames = subscriptionMetadata.keySet();

        for (int i = 0; i < memberCount; i++) {
            // Add different subscribed topics for testing non-uniform subscriptions
            Set<String> subscribedTopics = new HashSet<>(allTopicNames);
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
            Map<Integer, Set<String>> partitionRacks = mkMapOfPartitionRacks(partitionsPerTopicCount);
            TopicMetadata metadata = new TopicMetadata(topicId, topicName, partitionsPerTopicCount, partitionRacks);
            subscriptionMetadata.put(topicName, metadata);
        }
        //System.out.println("subscription metadata" + subscriptionMetadata);
        return subscriptionMetadata;
    }

    private Map<String, Assignment> generateMockInitialTargetAssignment() {
        // Prepare the topic metadata.
        Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>(topicCount);
        subscriptionMetadata.forEach((topicName, topicMetadata) ->
            topicMetadataMap.put(
                topicMetadata.id(),
                topicMetadata
            )
        );

        addTopicSubscriptions(topicMetadataMap);

        GroupAssignment groupAssignment = partitionAssignor.assign(
            assignmentSpec,
            new SubscribedTopicMetadata(topicMetadataMap)
        );

        Map<String, Assignment> initialTargetAssignment = new HashMap<>(memberCount);

        // Convert the structure from newClientTypeAssignment to the expected structure in Assignment
        for (Map.Entry<String, List<TopicIdPartition>> entry : groupAssignment.getNewClientTypeAssignment().entrySet()) {
            String memberId = entry.getKey();
            List<TopicIdPartition> topicIdPartitions = entry.getValue();

            // Create an Assignment object for the member
            Assignment assignment = new Assignment((byte) 0, topicIdPartitions, VersionedMetadata.EMPTY);

            // Put the created Assignment object into the initialTargetAssignment map
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

    private void addTopicSubscriptions(Map<Uuid, TopicMetadata> topicMetadata) {
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());

        for (int i = 0; i < memberCount; i++) {
            String memberName = "member" + i;
            Optional<String> rackId = isRackAware ? Optional.of("rack" + i % numberOfRacks) : Optional.empty();

            // When subscriptions are uniform, all members are assigned all topics.
            // Add logic for subscriptions when it is not uniform.
            List<Uuid> assignedTopics = topicUuids;

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                rackId,
                assignedTopics,
                Collections.emptyMap()));
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
