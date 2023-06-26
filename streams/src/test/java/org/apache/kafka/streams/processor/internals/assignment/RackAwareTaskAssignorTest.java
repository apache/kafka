package org.apache.kafka.streams.processor.internals.assignment;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.RackAwareTaskAssignor;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.junit.Test;

public class RackAwareTaskAssignorTest {
    private final static String TOPIC0 = "topic0";
    private final static String TOPIC1 = "topic1";
    private static final String USER_END_POINT = "localhost:8080";
    private static final String APPLICATION_ID = "stream-partition-assignor-test";

    private final Node node0 = new Node(0, "node0", 1, "rack1");
    private final Node node1 = new Node(1, "node1", 1, "rack2");
    private final Node node2 = new Node(2, "node2", 1, "rack3");
    private final Node[] replicas = new Node[] {node0, node1, node2};

    private final PartitionInfo partitionInfo00 = new PartitionInfo(TOPIC0, 0, node0, replicas, replicas);
    private final PartitionInfo partitionInfo01 = new PartitionInfo(TOPIC0, 1, node0, replicas, replicas);

    private final TopicPartition tp00 = new TopicPartition(TOPIC0, 0);
    private final TopicPartition tp01 = new TopicPartition(TOPIC0, 1);
    private final TopicPartition tp10 = new TopicPartition(TOPIC1, 0);

    private final UUID process0UUID  = UUID.randomUUID();
    private final UUID process1UUID = UUID.randomUUID();

    private final Subtopology subtopology1 = new Subtopology(1, "topology1");

    private final MockTime time = new MockTime();
    private final StreamsConfig streamsConfig = new StreamsConfig(configProps());
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();

    private final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            streamsConfig,
            mockClientSupplier.restoreConsumer,
            false
    );

    private Map<String, Object> configProps() {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);

        final ReferenceContainer referenceContainer = new ReferenceContainer();
        /*
        referenceContainer.mainConsumer = consumer;
        referenceContainer.adminClient = adminClient;
        referenceContainer.taskManager = taskManager;
        referenceContainer.streamsMetadataState = streamsMetadataState;
        referenceContainer.time = time;
        */
        configurationMap.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        return configurationMap;
    }

    @Test
    public void disableActiveSinceMissingClusterInfo() {
        final Cluster metadata = new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2)),
            new HashSet<>(Arrays.asList(partitionInfo00, partitionInfo01)),
            Collections.emptySet(),
            Collections.emptySet()
        );

        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        processRacks.put(process0UUID , Collections.singletonMap("consumer1", Optional.of("rack1")));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            metadata,
            Collections.singletonMap(new TaskId(1, 1), new HashSet<>(Arrays.asList(tp00, tp10))),
            Collections.singletonMap(subtopology1, Collections.singleton(new TaskId(1, 1))),
            processRacks,
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // False since tp10 is missing in cluster metadata
        assertFalse(assignor.canEnableForActive());
    }

    @Test
    public void disableActiveSinceRackMissingInClient() {
        final Cluster metadata = new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2)),
            new HashSet<>(Arrays.asList(partitionInfo00, partitionInfo01)),
            Collections.emptySet(),
            Collections.emptySet()
        );

        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        // Missing rackId config in client
        processRacks.put(process0UUID , Collections.singletonMap("consumer1", Optional.empty()));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            metadata,
            Collections.singletonMap(new TaskId(1, 1), Collections.singleton(tp00)),
            Collections.singletonMap(subtopology1, Collections.singleton(new TaskId(1, 1))),
            processRacks,
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // False since process1 doesn't have rackId
        assertFalse(assignor.canEnableForActive());
    }

    @Test
    public void disableActiveSinceRackDiffersInSameProcess() {
        final Cluster metadata = new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2)),
            new HashSet<>(Arrays.asList(partitionInfo00, partitionInfo01)),
            Collections.emptySet(),
            Collections.emptySet()
        );

        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        // Different rackId for same process
        processRacks.computeIfAbsent(process0UUID , k -> new HashMap<>()).put("consumer1", Optional.of("rack1"));
        processRacks.computeIfAbsent(process0UUID , k -> new HashMap<>()).put("consumer2", Optional.of("rack2"));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            metadata,
            Collections.singletonMap(new TaskId(1, 1), Collections.singleton(tp00)),
            Collections.singletonMap(subtopology1, Collections.singleton(new TaskId(1, 1))),
            processRacks,
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertFalse(assignor.canEnableForActive());
    }

    @Test
    public void enableRackAwareAssignorForActiveWithoutDescribingTopics() {
        final Cluster metadata = new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2)),
            new HashSet<>(Arrays.asList(partitionInfo00, partitionInfo01)),
            Collections.emptySet(),
            Collections.emptySet()
        );

        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        processRacks.computeIfAbsent(process0UUID , k -> new HashMap<>()).put("consumer1", Optional.of("rack1"));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            metadata,
            Collections.singletonMap(new TaskId(1, 1), Collections.singleton(tp00)),
            Collections.singletonMap(subtopology1, Collections.singleton(new TaskId(1, 1))),
            processRacks,
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // tp00 has rackInfo in cluster metadata
        assertTrue(assignor.canEnableForActive());
    }

    @Test
    public void enableRackAwareAssignorForActiveWithDescribingTopics() {
        final PartitionInfo noNodeInfo = new PartitionInfo(TOPIC0, 0, null, new Node[0], new Node[0]);

        final Cluster metadata = new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2, Node.noNode())), // mockClientSupplier.setCluster requires noNode
            Collections.singleton(noNodeInfo),
            Collections.emptySet(),
            Collections.emptySet()
        );

        MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(
            Collections.singletonMap(
                TOPIC0,
                Collections.singletonList(
                    new TopicPartitionInfo(0, node0, Arrays.asList(replicas), Collections.emptyList())
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(TOPIC0));

        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        processRacks.computeIfAbsent(process0UUID , k -> new HashMap<>()).put("consumer1", Optional.of("rack1"));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            metadata,
            Collections.singletonMap(new TaskId(1, 1), Collections.singleton(tp00)),
            Collections.singletonMap(subtopology1, Collections.singleton(new TaskId(1, 1))),
            processRacks,
            spyTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertTrue(assignor.canEnableForActive());
    }

    @Test
    public void enableRackAwareAssignorForActiveWithDescribingTopicsFailure() {
        final PartitionInfo noNodeInfo = new PartitionInfo(TOPIC0, 0, null, new Node[0], new Node[0]);

        final Cluster metadata = new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2, Node.noNode())), // mockClientSupplier.setCluster requires noNode
            Collections.singleton(noNodeInfo),
            Collections.emptySet(),
            Collections.emptySet()
        );

        MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doThrow(new TimeoutException("Timeout describing topic")).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(TOPIC0));

        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        processRacks.computeIfAbsent(process0UUID , k -> new HashMap<>()).put("consumer1", Optional.of("rack1"));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            metadata,
            Collections.singletonMap(new TaskId(1, 1), Collections.singleton(tp00)),
            Collections.singletonMap(subtopology1, Collections.singleton(new TaskId(1, 1))),
            processRacks,
            spyTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertFalse(assignor.canEnableForActive());
    }
}
