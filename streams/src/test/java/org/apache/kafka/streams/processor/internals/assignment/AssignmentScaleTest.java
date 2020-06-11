package org.apache.kafka.streams.processor.internals.assignment;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class AssignmentScaleTest {
    private static final int NUM_TOPICS_XL = 10;
    private static final int NUM_PARTITIONS_PER_TOPIC_XL = 1_000;
    private static final int NUM_CONSUMERS_XL = 100;
    private static final List<String> TOPICS_LIST_XL = new ArrayList<>();
    private static final Map<TopicPartition, Long> CHANGELOG_END_OFFSETS_XL = new HashMap<>();
    private static final List<PartitionInfo> PARTITION_INFOS_XL = getPartitionInfos(NUM_TOPICS_XL, NUM_PARTITIONS_PER_TOPIC_XL);
    private static final Cluster CLUSTER_METADATA_XL = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        PARTITION_INFOS_XL,
        emptySet(),
        emptySet()
    );

    private final Class<? extends TaskAssignor> taskAssignor;
    private final StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();
    private InternalTopologyBuilder builder = new InternalTopologyBuilder();

    @BeforeClass
    public static void setupScaleTest() {
        for (int i = 1; i <= NUM_TOPICS_XL; ++i) {
            TOPICS_LIST_XL.add("topic" + i);
        }

        for (int p = 0; p < NUM_PARTITIONS_PER_TOPIC_XL; ++p) {
            CHANGELOG_END_OFFSETS_XL.put(new TopicPartition("stream-partition-assignor-test-store-changelog", p), 100_000L);
        }
    }

    @Parameterized.Parameters(name = "task assignor = {0}")
    public static Collection<Object[]> parameters() {
        return asList(
            new Object[]{HighAvailabilityTaskAssignor.class},
            new Object[]{StickyTaskAssignor.class},
            new Object[]{FallbackPriorTaskAssignor.class}
        );
    }

    public AssignmentScaleTest(final Class<? extends TaskAssignor> taskAssignor) {
        this.taskAssignor = taskAssignor;
    }

    @Test(timeout = 30 * 1000)
    public void shouldCompleteLargeAssignmentInAReasonableAmountOfTime() {
        builder.addSource(null, "source", null, null, null, TOPICS_LIST_XL.toArray(new String[0]));
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(new MockKeyValueStoreBuilder("store", false), "processor");

        for (int i = 0; i < NUM_CONSUMERS_XL; ++i) {
            subscriptions.put("consumer-" + i,
                              new Subscription(
                                  TOPICS_LIST_XL,
                                  getInfo(uuidForInt(i), EMPTY_TASKS, EMPTY_TASKS).encode())
            );
        }
        createMockTaskManager(EMPTY_TASKS, EMPTY_TASKS);
        createMockAdminClient(CHANGELOG_END_OFFSETS_XL);
        configurePartitionAssignorWith(singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 3));

        final Map<String, Assignment> assignments =
            partitionAssignor.assign(CLUSTER_METADATA_XL, new GroupSubscription(subscriptions)).groupAssignment();

        // Use the assignment to generate the subscriptions' prev task data for the next rebalance
        for (int i = 0; i < NUM_CONSUMERS_XL; ++i) {
            final String consumer = "consumer-" + i;
            final Assignment assignment = assignments.get(consumer);
            final AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

            subscriptions.put("consumer-" + i,
                              new Subscription(
                                  TOPICS_LIST_XL,
                                  getInfo(uuidForInt(i), new HashSet<>(info.activeTasks()), info.standbyTasks().keySet()).encode(),
                                  assignment.partitions())
            );
        }

        final Map<String, Assignment> secondAssignments =
            partitionAssignor.assign(CLUSTER_METADATA_XL, new GroupSubscription(subscriptions)).groupAssignment();
    }

    private static List<PartitionInfo> getPartitionInfos(final int numTopics, final int numPartitionsPerTopic) {
        final List<PartitionInfo> partitionInfos = new ArrayList<>();
        for (int t = 1; t <= numTopics; ++t) { // topic numbering starts from 1
            for (int p = 0; p < numPartitionsPerTopic; ++p) {
                partitionInfos.add(new PartitionInfo("topic" + t, p, Node.noNode(), new Node[0], new Node[0]));
            }
        }
        return  partitionInfos;
    }
}
