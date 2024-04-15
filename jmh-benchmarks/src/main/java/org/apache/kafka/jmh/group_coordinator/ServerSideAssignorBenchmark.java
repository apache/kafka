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

import static java.lang.Integer.max;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ServerSideAssignorBenchmark {

    @Param({"10", "50", "100"})
    private int partitionsPerTopicCount;

    @Param({"100"})
    private int topicCount;

    @Param({"500", "1000"})
    private int memberCount;

    @Param({"true", "false"})
    private boolean isRackAware;

    @Param({"true", "false"})
    private boolean isSubscriptionUniform;

    @Param({"true", "false"})
    private boolean isRangeAssignor;

    @Param({"true", "false"})
    private boolean isReassignment;

    private PartitionAssignor partitionAssignor;

    private final int numberOfRacks = 3;

    private AssignmentSpec assignmentSpec;

    private SubscribedTopicDescriber subscribedTopicDescriber;

    @Setup(Level.Trial)
    public void setup() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        Map<Integer, Set<String>> partitionRacks = isRackAware ?
            mkMapOfPartitionRacks(partitionsPerTopicCount) :
            Collections.emptyMap();

        for (int i = 1; i <= topicCount; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopicCount, partitionRacks));
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

            members = initialAssignment.members();

            // Update the AssignmentSpec with the results from the initial assignment.
            Map<String, AssignmentMemberSpec> updatedMembers = new HashMap<>();

            members.forEach((memberId, memberAssignment) -> {
                AssignmentMemberSpec memberSpec = assignmentSpec.members().get(memberId);
                updatedMembers.put(memberId, new AssignmentMemberSpec(
                    memberSpec.instanceId(),
                    memberSpec.rackId(),
                    memberSpec.subscribedTopicIds(),
                    memberAssignment.targetPartitions()
                ));
            });

            // Add new member to trigger a reassignment.
            Optional<String> rackId = isRackAware ? Optional.of("rack" + (memberCount + 1) % numberOfRacks) : Optional.empty();

            updatedMembers.put("newMember", new AssignmentMemberSpec(
                Optional.empty(),
                rackId,
                topicMetadata.keySet(),
                Collections.emptyMap()
            ));

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
        List<Uuid> allTopicIds = new ArrayList<>(topicMetadata.keySet());
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
    public void doAssignment() {
        partitionAssignor.assign(assignmentSpec, subscribedTopicDescriber);
    }
}
