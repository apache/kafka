package org.apache.kafka.jmh.group_coordinator;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicMetadata;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ServerSideAssignorBenchmark {

    @Param({"10", "50", "100"})
    private int partitionsPerTopicCount;

    @Param({"100"})
    private int topicCount;

    @Param({"500", "1000"})
    private int memberCount;

    @Param({"true"})
    private boolean isRackAware;

    @Param({"true"})
    private boolean isSubscriptionUniform;

    @Param({"false"})
    private boolean isRangeAssignor;

    @Param({"true"})
    private boolean isReassignment;

    private PartitionAssignor partitionAssignor;

    private final int numberOfRacks = 3;

    private AssignmentSpec assignmentSpec;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    @Setup(Level.Trial)
    public void setup() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();

        if (!isRackAware) {
            for (int i = 1; i <= topicCount; i++) {
                Uuid topicUuid = Uuid.randomUuid();
                String topicName = "topic" + i;
                topicMetadata.put(topicUuid, new TopicMetadata(
                    topicUuid, topicName, partitionsPerTopicCount, Collections.emptyMap()));
            }
        } else {
            for (int i = 1; i <= topicCount; i++) {
                Uuid topicUuid = Uuid.randomUuid();
                String topicName = "topic" + i;
                topicMetadata.put(topicUuid, new TopicMetadata(
                    topicUuid, topicName, partitionsPerTopicCount, mkMapOfPartitionRacks(partitionsPerTopicCount)));
            }
        }

        addTopicSubscriptions(topicMetadata);
        this.subscribedTopicDescriber = new SubscribedTopicMetadata(topicMetadata);

        if (isRangeAssignor) {
            this.partitionAssignor = new RangeAssignor();
        } else {
            this.partitionAssignor = new UniformAssignor();
        }

        if (isReassignment) {
            GroupAssignment initialAssignment = partitionAssignor.assign(assignmentSpec, subscribedTopicDescriber);
            Map<String, MemberAssignment> members;
            if (!isRangeAssignor && isSubscriptionUniform) {
                members = initialAssignment.convertNewClientTypeAssignment(initialAssignment.getNewClientTypeAssignment());
            } else {
                members = initialAssignment.members();
            }
            // Update the AssignmentSpec with the results from the initial assignment.
            Map<String, AssignmentMemberSpec> updatedMembers = new HashMap<>();

            members.forEach((memberId, memberAssignment) -> {
                AssignmentMemberSpec memberSpec = assignmentSpec.members().get(memberId);
                updatedMembers.put(memberId, new AssignmentMemberSpec(
                    memberSpec.instanceId(),
                    memberSpec.rackId(),
                    memberSpec.subscribedTopicIds(),
                    memberAssignment.targetPartitions(),
                    initialAssignment.getNewClientTypeAssignment().get(memberId)
                ));
            });

            // Add new member to trigger a reassignment.
            if (isRackAware) {
                String rackId = "rack" + (memberCount + 1) % numberOfRacks;
                updatedMembers.put("newMember", new AssignmentMemberSpec(
                    Optional.empty(),
                    Optional.of(rackId),
                    topicMetadata.keySet(),
                    Collections.emptyMap(),
                    Collections.emptyList()
                ));
            } else {
                updatedMembers.put("newMember", new AssignmentMemberSpec(
                    Optional.empty(),
                    Optional.empty(),
                    topicMetadata.keySet(),
                    Collections.emptyMap(),
                    Collections.emptyList()
                ));
            }
            this.assignmentSpec = new AssignmentSpec(updatedMembers);
        }
    }

    private Map<Integer, Set<String>> mkMapOfPartitionRacks(int numPartitions) {
        Map<Integer, Set<String>> partitionRacks = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitionRacks.put(i, new HashSet<>(Arrays.asList("rack" + i % numberOfRacks, "rack" + (i + 1) % numberOfRacks)));
        }
        return partitionRacks;
    }

    private void addTopicSubscriptions(Map<Uuid, TopicMetadata> topicMetadata) {
        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());

        if (!isRackAware) {
            for (int i = 1; i <= memberCount; i++) {
                String memberName = "member" + i;

                // Distribute topics among members.
                List<Uuid> assignedTopics = new ArrayList<>();
                if (i == memberCount - 1 && !isSubscriptionUniform) {
                    assignedTopics.add(topicUuids.get(0));
                    assignedTopics.add(topicUuids.get(1));
                } else {
                    assignedTopics = topicUuids;
                }

                members.put(memberName, new AssignmentMemberSpec(
                    Optional.empty(),
                    Optional.empty(),
                    assignedTopics,
                    Collections.emptyMap()));
            }
        } else {
            for (int i = 1; i <= memberCount; i++) {
                String memberName = "member" + i;
                String rackId = "rack" + i % numberOfRacks;

                // Distribute topics among members.
                List<Uuid> assignedTopics = new ArrayList<>();
                if (i == memberCount - 1 && !isSubscriptionUniform) {
                    assignedTopics.add(topicUuids.get(0));
                    assignedTopics.add(topicUuids.get(1));
                } else {
                    assignedTopics = topicUuids;
                }

                members.put(memberName, new AssignmentMemberSpec(
                    Optional.empty(),
                    Optional.of(rackId),
                    assignedTopics,
                    Collections.emptyMap()));
            }
        }

        this.assignmentSpec = new AssignmentSpec(members);
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void doAssignment() {
        partitionAssignor.assign(assignmentSpec, subscribedTopicDescriber);
    }
}
