/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.jmh.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicIds;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the server side partition assignors of consumer groups.
 *
 * <p>The parameters describe the group and what happened to it before the assignment:
 * <ul>
 *     <li>{@code memberCount}, {@code topicCount} and {@code partitionCount}: the members, the
 *     subscribed topics and the partitions over all topics.</li>
 *     <li>{@code distribution}: how the partitions are split over the topics, see
 *     {@link Distribution}.</li>
 *     <li>{@code subscription}: how the members subscribe, see {@link Subscription}.</li>
 *     <li>{@code rack}: whether the members have a rack, see {@link Rack}.</li>
 *     <li>{@code assignor}: the assignor.</li>
 *     <li>{@code event}: what happened to the group, see {@link Event}. The group has
 *     {@code memberCount} members when the assignment is computed, and the joining or leaving
 *     members are those with the highest indices.</li>
 * </ul>
 *
 * <p>The parameters form 5040 combinations. These three runs of {@code jmh.sh} cover the ones
 * of interest: the scaling with the group size, then the events on a large group with many
 * topics and on a small group with very many topics. The largest groups need about 4 GB of
 * heap, so pass {@code -jvmArgs -Xmx8g} to {@code jmh.sh} when the default heap is smaller.
 * <pre>
 * ./jmh-benchmarks/jmh.sh -prof gc -w 1s -r 1s -p event=FULL,STABLE,JOIN_ONE \
 *     -p distribution=EQUAL -p subscription=HOMOGENEOUS ConsumerAssignorBenchmark
 * ./jmh-benchmarks/jmh.sh -prof gc -w 1s -r 1s -p memberCount=10000 -p topicCount=1000 \
 *     -p subscription=HOMOGENEOUS,HETEROGENEOUS_NESTED ConsumerAssignorBenchmark
 * ./jmh-benchmarks/jmh.sh -prof gc -w 1s -r 1s -p memberCount=20 -p topicCount=10000 \
 *     -p subscription=HOMOGENEOUS ConsumerAssignorBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ConsumerAssignorBenchmark {

    /**
     * The assignor computing the assignment.
     */
    public enum AssignorType {
        /**
         * The range assignor.
         */
        RANGE,

        /**
         * The uniform assignor.
         */
        UNIFORM
    }

    /**
     * How the partitions are split over the topics.
     */
    public enum Distribution {
        /**
         * Every topic has the same number of partitions, up to the rounding remainder.
         */
        EQUAL,

        /**
         * The topics form geometric tiers: the first tier holds two thirds of the topics, and
         * every following tier holds a third of the topics of the previous one, with twice as
         * many partitions per topic. This gives a few large topics, a band of small ones and a
         * majority of topics with the smallest size, which is how topics are commonly sized.
         * Each tier doubles the partitions of the previous one, so the geometric split needs at
         * least about twice as many partitions as topics. With fewer, the partitions are split
         * equally, as with {@link #EQUAL}.
         */
        SKEWED
    }

    /**
     * How the members subscribe to the topics.
     *
     * <p>The heterogeneous subscriptions put the members in buckets, member {@code i} in bucket
     * {@code i mod bucketCount}, and cut the topics, largest first, into as many consecutive
     * ranges of about the same size. With 20 members, 10 topics and 5 buckets, bucket 0 holds
     * members 0, 5, 10 and 15, and range 0 holds topics 0 and 1. The bucket count is the
     * smallest of five, the member count and the topic count, see {@link #BUCKET_COUNT}.
     */
    public enum Subscription {
        /**
         * Every member subscribes to every topic.
         */
        HOMOGENEOUS,

        /**
         * The members of bucket {@code b} subscribe to range {@code b} only, so every topic has
         * a single bucket of subscribers, as when independent applications share a group.
         */
        HETEROGENEOUS_DISJOINT,

        /**
         * The members of bucket {@code b} subscribe to ranges 0 to {@code b}, so the first range
         * is subscribed by every member and the last one by the last bucket only. Topics are
         * shared by members whose subscriptions differ.
         */
        HETEROGENEOUS_NESTED
    }

    /**
     * Whether the members have a rack.
     */
    public enum Rack {
        /**
         * The members have no rack.
         */
        NONE,

        /**
         * The members are spread over the racks of the brokers.
         */
        PROVIDED
    }

    /**
     * What happened to the group before the assignment is computed.
     */
    public enum Event {
        /**
         * No member holds partitions.
         */
        FULL,

        /**
         * The members hold the output of the assignor for the same group, so nothing has to
         * change.
         */
        STABLE,

        /**
         * One member joined and holds nothing.
         */
        JOIN_ONE,

        /**
         * A tenth of the members, rounded up, joined and hold nothing.
         */
        JOIN_MANY,

        /**
         * One member left, leaving its partitions unassigned.
         */
        LEAVE_ONE,

        /**
         * A tenth of the members, rounded up, left, leaving their partitions unassigned.
         */
        LEAVE_MANY,

        /**
         * A tenth of the topics, rounded up, gained a partition.
         */
        PARTITIONS_ADDED
    }

    /**
     * The input of an assignment: the spec of the group, and the views of the cluster metadata
     * the assignor takes. The resolver is cleared before every assignment, as the coordinator
     * uses a new one for every assignment.
     */
    private record Group(
        GroupSpec spec,
        TopicIds.TopicResolver topicResolver,
        SubscribedTopicDescriber describer
    ) { }

    /**
     * Builds the input of an assignment. Every build creates the metadata image of the cluster
     * and new views of it, so that nothing is shared between the groups built.
     *
     * <p>Topic {@code i} is called {@code topic-<i>}, and the partitions are split over the
     * topics as the distribution says, see {@link #partitionCounts}, the largest topics first. The
     * topics with an added partition, taken at regular intervals over the topics, have one more
     * partition than the split gives them.
     *
     * <p>The cluster has one broker per rack, and every partition has two replicas on adjacent
     * brokers, so that it is in two racks. Topic ids come from a generator with a fixed seed, so
     * that the clusters built before and after partitions were added give the same ids to the
     * same topics, and the ids are spread like real ones.
     *
     * <p>Member {@code i} is called {@code member<i>}, is in rack {@code i mod rackCount} when
     * the members have a rack, and is in bucket {@code i mod bucketCount} for the heterogeneous
     * subscriptions, see {@link Subscription}, so that members added at the end are spread over
     * the buckets. The bucket count is fixed by the caller rather than derived from the member
     * count, so that the topics of a bucket are the same in groups built with different member
     * counts. Every member holds its own copy of the topics of its bucket, as members do in the
     * coordinator, so the largest groups take gigabytes of heap.
     */
    private static final class GroupBuilder {
        /**
         * Topic ids are drawn from a generator with this seed.
         */
        private static final long TOPIC_ID_SEED = 42L;

        private int topicCount = 0;
        private int partitionCount = 0;
        private Distribution distribution = Distribution.EQUAL;
        private int topicsWithAddedPartition = 0;
        private int rackCount = 1;
        private Subscription subscription = Subscription.HOMOGENEOUS;
        private Rack rack = Rack.NONE;
        private int bucketCount = 1;
        private int memberCount = 0;
        private GroupAssignment currentAssignment = new GroupAssignment(Map.of());

        /**
         * @param topicCount    The number of topics.
         */
        GroupBuilder withTopicCount(int topicCount) {
            this.topicCount = topicCount;
            return this;
        }

        /**
         * @param partitionCount    The total number of partitions over all topics.
         */
        GroupBuilder withPartitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        /**
         * @param distribution  How the partitions are split over the topics.
         */
        GroupBuilder withDistribution(Distribution distribution) {
            this.distribution = distribution;
            return this;
        }

        /**
         * @param topicsWithAddedPartition  The number of topics which gained a partition, taken
         *                                  at regular intervals over the topics.
         */
        GroupBuilder withTopicsWithAddedPartition(int topicsWithAddedPartition) {
            this.topicsWithAddedPartition = topicsWithAddedPartition;
            return this;
        }

        /**
         * @param rackCount The number of racks of the brokers, and of the members having a rack.
         */
        GroupBuilder withRackCount(int rackCount) {
            this.rackCount = rackCount;
            return this;
        }

        /**
         * @param subscription  How the members subscribe to the topics.
         */
        GroupBuilder withSubscription(Subscription subscription) {
            this.subscription = subscription;
            return this;
        }

        /**
         * @param rack  Whether the members have a rack.
         */
        GroupBuilder withRack(Rack rack) {
            this.rack = rack;
            return this;
        }

        /**
         * @param bucketCount   The number of member buckets for the heterogeneous subscriptions.
         */
        GroupBuilder withBucketCount(int bucketCount) {
            this.bucketCount = bucketCount;
            return this;
        }

        /**
         * @param memberCount   The number of members.
         */
        GroupBuilder withMemberCount(int memberCount) {
            this.memberCount = memberCount;
            return this;
        }

        /**
         * @param currentAssignment The partitions the members hold. Members without an entry
         *                          hold nothing, and the entries of members not in the group
         *                          are ignored.
         */
        GroupBuilder withCurrentAssignment(GroupAssignment currentAssignment) {
            this.currentAssignment = currentAssignment;
            return this;
        }

        /**
         * @return The input of an assignment for the group, with its own metadata image and
         *         views of it.
         */
        Group build() {
            var topicNames = new ArrayList<String>(topicCount);
            for (int topic = 0; topic < topicCount; topic++) {
                topicNames.add("topic-" + topic);
            }
            var partitionCounts = partitionCounts(distribution, topicCount, partitionCount);
            for (int i = 0; i < topicsWithAddedPartition; i++) {
                partitionCounts[(int) ((long) i * topicCount / topicsWithAddedPartition)]++;
            }
            var image = createImage(topicNames, partitionCounts);
            var topicResolver = new TopicIds.CachedTopicResolver(image);
            var describer = new SubscribedTopicDescriberImpl(image);

            var bucketTopics = new ArrayList<List<String>>(bucketCount);
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                bucketTopics.add(topicsOfBucket(bucket, topicNames));
            }

            var members = new HashMap<String, MemberSubscriptionAndAssignmentImpl>();
            var invertedTargetAssignment = new HashMap<Uuid, Map<Integer, String>>();
            for (int i = 0; i < memberCount; i++) {
                var memberId = "member" + i;
                var memberAssignment = currentAssignment.members().get(memberId);
                Map<Uuid, Set<Integer>> partitions = Map.of();
                if (memberAssignment != null) {
                    partitions = memberAssignment.partitions();
                    partitions.forEach((topicId, topicPartitions) -> {
                        var owners = invertedTargetAssignment.computeIfAbsent(topicId, id -> new HashMap<>());
                        topicPartitions.forEach(partition -> owners.put(partition, memberId));
                    });
                }
                members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                    rack == Rack.NONE ? Optional.empty() : Optional.of(rackId(i)),
                    Optional.empty(),
                    new TopicIds(new HashSet<>(bucketTopics.get(i % bucketCount)), topicResolver),
                    new Assignment(partitions)
                ));
            }

            var spec = new GroupSpecImpl(
                members,
                subscription == Subscription.HOMOGENEOUS ? SubscriptionType.HOMOGENEOUS : SubscriptionType.HETEROGENEOUS,
                invertedTargetAssignment
            );

            return new Group(spec, topicResolver, describer);
        }

        /**
         * @return The metadata image of the cluster holding the topics.
         */
        private CoordinatorMetadataImage createImage(List<String> topicNames, int[] partitionCounts) {
            var delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
            for (int brokerId = 0; brokerId < rackCount; brokerId++) {
                delta.replay(new RegisterBrokerRecord().setBrokerId(brokerId).setRack(rackId(brokerId)));
            }
            var random = new Random(TOPIC_ID_SEED);
            for (int topic = 0; topic < topicNames.size(); topic++) {
                var topicId = topicId(random);
                delta.replay(new TopicRecord().setTopicId(topicId).setName(topicNames.get(topic)));
                for (int partition = 0; partition < partitionCounts[topic]; partition++) {
                    delta.replay(new PartitionRecord()
                        .setTopicId(topicId)
                        .setPartitionId(partition)
                        .setReplicas(List.of(partition % rackCount, (partition + 1) % rackCount)));
                }
            }
            return new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY));
        }

        /**
         * Draws a topic id from the generator, with the same constraints as
         * {@link Uuid#randomUuid()}.
         */
        private static Uuid topicId(Random random) {
            var uuid = new Uuid(random.nextLong(), random.nextLong());
            while (Uuid.RESERVED.contains(uuid) || uuid.toString().contains("-")) {
                uuid = new Uuid(random.nextLong(), random.nextLong());
            }
            return uuid;
        }

        /**
         * @return The topics the members of the bucket subscribe to.
         */
        private List<String> topicsOfBucket(int bucket, List<String> topicNames) {
            return switch (subscription) {
                case HOMOGENEOUS -> topicNames;
                case HETEROGENEOUS_DISJOINT -> topicNames.subList(
                    topicCount * bucket / bucketCount,
                    topicCount * (bucket + 1) / bucketCount
                );
                case HETEROGENEOUS_NESTED -> topicNames.subList(0, topicCount * (bucket + 1) / bucketCount);
            };
        }

        /**
         * @return The rack of the member or broker with the given index.
         */
        private String rackId(int index) {
            return "rack" + (index % rackCount);
        }

        /**
         * @param distribution      How the partitions are split over the topics.
         * @param topicCount        The number of topics.
         * @param partitionCount    The total number of partitions.
         * @return The number of partitions of each topic, largest first and at least one, so
         *         that the total may exceed {@code partitionCount} when there are more topics
         *         than partitions.
         */
        private static int[] partitionCounts(Distribution distribution, int topicCount, int partitionCount) {
            int[] counts = new int[topicCount];
            if (distribution == Distribution.SKEWED) {
                List<Integer> tierSizes = new ArrayList<>();
                double fraction = 2.0 / 3.0;
                for (int remaining = topicCount; remaining > 0; fraction /= 3.0) {
                    int size = Math.min(remaining, Math.max(1, (int) Math.round(topicCount * fraction)));
                    tierSizes.add(size);
                    remaining -= size;
                }
                long weight = 0;
                for (int tier = 0; tier < tierSizes.size(); tier++) {
                    weight += (long) tierSizes.get(tier) << tier;
                }
                if (weight <= partitionCount) {
                    long base = partitionCount / weight;
                    int topic = 0;
                    for (int tier = tierSizes.size() - 1; tier >= 0; tier--) {
                        Arrays.fill(counts, topic, topic + tierSizes.get(tier), (int) (base << tier));
                        topic += tierSizes.get(tier);
                    }
                    spreadRemainder(counts, partitionCount);
                    return counts;
                }
            }
            Arrays.fill(counts, Math.max(1, partitionCount / topicCount));
            spreadRemainder(counts, partitionCount);
            return counts;
        }

        /**
         * Hands the partitions not given yet, if any, to the topics one at a time from the first.
         */
        private static void spreadRemainder(int[] counts, int partitionCount) {
            long remainder = partitionCount - Arrays.stream(counts).asLongStream().sum();
            for (long i = 0; i < remainder; i++) {
                counts[(int) (i % counts.length)]++;
            }
        }
    }

    /**
     * The brokers are spread over this many racks, and so are the members having a rack.
     */
    private static final int RACK_COUNT = 3;

    /**
     * The number of member buckets for the heterogeneous subscriptions, at most. Groups with
     * fewer members or topics have as many buckets as the smaller of the two counts, see
     * {@link Subscription}.
     */
    private static final int BUCKET_COUNT = 5;

    /**
     * The events on many members change one member in this many.
     */
    private static final int MANY_MEMBERS_DIVISOR = 10;

    /**
     * The partitions added event adds a partition to one topic in this many.
     */
    private static final int ADDED_PARTITIONS_TOPIC_DIVISOR = 10;

    @Param({"2", "20", "1000", "5000", "10000"})
    private int memberCount;

    @Param({"10", "1000", "10000"})
    private int topicCount;

    @Param({"10000", "100000"})
    private int partitionCount;

    @Param({"EQUAL", "SKEWED"})
    private Distribution distribution;

    @Param({"HOMOGENEOUS", "HETEROGENEOUS_DISJOINT", "HETEROGENEOUS_NESTED"})
    private Subscription subscription;

    @Param({"NONE", "PROVIDED"})
    private Rack rack;

    @Param({"RANGE", "UNIFORM"})
    private AssignorType assignor;

    @Param({"FULL", "STABLE", "JOIN_ONE", "JOIN_MANY", "LEAVE_ONE", "LEAVE_MANY", "PARTITIONS_ADDED"})
    private Event event;

    private PartitionAssignor partitionAssignor;

    private Group group;

    @Setup(Level.Trial)
    public void setup() {
        partitionAssignor = createAssignor();

        var builder = new GroupBuilder()
            .withTopicCount(topicCount)
            .withPartitionCount(partitionCount)
            .withDistribution(distribution)
            .withRackCount(RACK_COUNT)
            .withSubscription(subscription)
            .withRack(rack)
            .withBucketCount(Math.min(BUCKET_COUNT, Math.min(memberCount, topicCount)));

        // The previous assignment is the output of the assignor for the group as it was before
        // the event: without the joining members, with the leaving members, and before the
        // partitions were added.
        var previousAssignment = new GroupAssignment(Map.of());
        if (event != Event.FULL) {
            var previousGroup = builder
                .withMemberCount(previousMemberCount())
                .build();
            previousAssignment = partitionAssignor.assign(previousGroup.spec(), previousGroup.describer());
        }

        group = builder
            .withMemberCount(memberCount)
            .withTopicsWithAddedPartition(topicsWithAddedPartition())
            .withCurrentAssignment(previousAssignment)
            .build();
    }

    private PartitionAssignor createAssignor() {
        return switch (assignor) {
            case RANGE -> new RangeAssignor();
            case UNIFORM -> new UniformAssignor();
        };
    }

    /**
     * @return The number of members of the group before the event.
     */
    private int previousMemberCount() {
        int manyMembers = divideRoundingUp(memberCount, MANY_MEMBERS_DIVISOR);
        return switch (event) {
            case JOIN_ONE -> memberCount - 1;
            case JOIN_MANY -> memberCount - manyMembers;
            case LEAVE_ONE -> memberCount + 1;
            case LEAVE_MANY -> memberCount + manyMembers;
            default -> memberCount;
        };
    }

    /**
     * @return The number of topics which gained a partition before the event.
     */
    private int topicsWithAddedPartition() {
        return event == Event.PARTITIONS_ADDED ? divideRoundingUp(topicCount, ADDED_PARTITIONS_TOPIC_DIVISOR) : 0;
    }

    /**
     * @return The count divided by the divisor, rounded up.
     */
    private static int divideRoundingUp(int count, int divisor) {
        return (count + divisor - 1) / divisor;
    }

    @Benchmark
    @Threads(1)
    public GroupAssignment doAssignment() {
        group.topicResolver().clear();
        return partitionAssignor.assign(group.spec(), group.describer());
    }
}
