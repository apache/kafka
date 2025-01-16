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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.LegacyStickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.LegacyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockKeyValueStoreBuilder;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.assignTasksToThreads;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CHANGELOG_END_OFFSETS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CLIENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.REPLICA_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.REPLICA_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.REPLICA_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.REPLICA_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.REPLICA_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.createMockAdminClientForAssignor;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getInfo;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@Timeout(30_000)
public class StreamsPartitionAssignorTest {

    private static final String CONSUMER_1 = "consumer1";
    private static final String CONSUMER_2 = "consumer2";
    private static final String CONSUMER_3 = "consumer3";
    private static final String CONSUMER_4 = "consumer4";

    private final Set<String> allTopics = Set.of("topic1", "topic2");

    private final TopicPartition t1p0 = new TopicPartition("topic1", 0);
    private final TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private final TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private final TopicPartition t1p3 = new TopicPartition("topic1", 3);
    private final TopicPartition t2p0 = new TopicPartition("topic2", 0);
    private final TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private final TopicPartition t2p2 = new TopicPartition("topic2", 2);
    private final TopicPartition t2p3 = new TopicPartition("topic2", 3);
    private final TopicPartition t3p0 = new TopicPartition("topic3", 0);
    private final TopicPartition t3p1 = new TopicPartition("topic3", 1);
    private final TopicPartition t3p2 = new TopicPartition("topic3", 2);
    private final TopicPartition t3p3 = new TopicPartition("topic3", 3);

    private final List<PartitionInfo> infos = asList(
        new PartitionInfo("topic1", 0, NODE_0, REPLICA_0, REPLICA_0),
        new PartitionInfo("topic1", 1, NODE_1, REPLICA_1, REPLICA_1),
        new PartitionInfo("topic1", 2, NODE_2, REPLICA_2, REPLICA_2),
        new PartitionInfo("topic2", 0, NODE_3, REPLICA_3, REPLICA_3),
        new PartitionInfo("topic2", 1, NODE_4, REPLICA_4, REPLICA_4),
        new PartitionInfo("topic2", 2, NODE_0, REPLICA_0, REPLICA_0),
        new PartitionInfo("topic3", 0, NODE_1, REPLICA_1, REPLICA_1),
        new PartitionInfo("topic3", 1, NODE_2, REPLICA_2, REPLICA_2),
        new PartitionInfo("topic3", 2, NODE_3, REPLICA_3, REPLICA_3),
        new PartitionInfo("topic3", 3, NODE_0, REPLICA_0, REPLICA_0)
    );

    private final SubscriptionInfo defaultSubscriptionInfo = getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS);

    private final Cluster metadata = new Cluster(
        "cluster",
        Arrays.asList(NODE_0, NODE_1, NODE_2, NODE_3, NODE_4),
        infos,
        emptySet(),
        emptySet()
    );

    private final StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private static final String USER_END_POINT = "localhost:8080";
    private static final String OTHER_END_POINT = "other:9090";
    private static final String APPLICATION_ID = "stream-partition-assignor-test";

    private TaskManager taskManager;
    private Admin adminClient;
    private InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private TopologyMetadata topologyMetadata;
    @Mock
    private StreamsMetadataState streamsMetadataState;
    @Captor
    private ArgumentCaptor<Map<TopicPartition, PartitionInfo>> topicPartitionInfoCaptor;
    private final Map<String, Subscription> subscriptions = new HashMap<>();
    private Map<String, String> clientTags;

    private final ReferenceContainer referenceContainer = new ReferenceContainer();
    private final MockTime time = new MockTime();
    private final byte uniqueField = 1;

    @SuppressWarnings("unchecked")
    private Map<String, Object> configProps(final Map<String, Object> parameterizedConfig) {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);
        referenceContainer.mainConsumer = mock(Consumer.class);
        referenceContainer.adminClient = adminClient != null ? adminClient : mock(Admin.class);
        referenceContainer.taskManager = taskManager;
        referenceContainer.streamsMetadataState = streamsMetadataState;
        referenceContainer.time = time;
        referenceContainer.clientTags = clientTags != null ? clientTags : EMPTY_CLIENT_TAGS;
        configurationMap.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        configurationMap.putAll(parameterizedConfig);
        return configurationMap;
    }

    private MockInternalTopicManager configureDefault(final Map<String, Object> parameterizedConfig) {
        createDefaultMockTaskManager();
        return configureDefaultPartitionAssignor(parameterizedConfig);
    }

    // Make sure to complete setting up any mocks (such as TaskManager or AdminClient) before configuring the assignor
    private MockInternalTopicManager configureDefaultPartitionAssignor(final Map<String, Object> parameterizedConfig) {
        return configurePartitionAssignorWith(emptyMap(), parameterizedConfig);
    }

    // Make sure to complete setting up any mocks (such as TaskManager or AdminClient) before configuring the assignor
    private MockInternalTopicManager configurePartitionAssignorWith(final Map<String, Object> props,
                                                                    final Map<String, Object> parameterizedConfig) {
        return configurePartitionAssignorWith(props, null, parameterizedConfig);
    }

    private MockInternalTopicManager configurePartitionAssignorWith(final Map<String, Object> props,
                                                                    final List<Map<String, List<TopicPartitionInfo>>> topicPartitionInfo,
                                                                    final Map<String, Object> parameterizedConfig) {
        final Map<String, Object> configMap = configProps(parameterizedConfig);
        configMap.putAll(props);

        partitionAssignor.configure(configMap);

        topologyMetadata = new TopologyMetadata(builder, new StreamsConfig(configProps(parameterizedConfig)));
        return overwriteInternalTopicManagerWithMock(false, topicPartitionInfo, parameterizedConfig);
    }

    private void createDefaultMockTaskManager() {
        createMockTaskManager(EMPTY_TASKS, EMPTY_TASKS);
    }

    private void createMockTaskManager(final Set<TaskId> activeTasks,
                                       final Set<TaskId> standbyTasks) {
        taskManager = mock(TaskManager.class);
        lenient().when(taskManager.topologyMetadata()).thenReturn(topologyMetadata);
        lenient().when(taskManager.taskOffsetSums()).thenReturn(getTaskOffsetSums(activeTasks, standbyTasks));
        lenient().when(taskManager.processId()).thenReturn(PID_1);
        builder.setApplicationId(APPLICATION_ID);
        topologyMetadata.buildAndRewriteTopology();
    }

    // If mockCreateInternalTopics is true the internal topic manager will report that it had to create all internal
    // topics and we will skip the listOffsets request for these changelogs
    private MockInternalTopicManager overwriteInternalTopicManagerWithMock(final boolean mockCreateInternalTopics,
                                                                           final Map<String, Object> parameterizedConfig) {
        return overwriteInternalTopicManagerWithMock(mockCreateInternalTopics, null, parameterizedConfig);
    }

    private MockInternalTopicManager overwriteInternalTopicManagerWithMock(final boolean mockCreateInternalTopics,
                                                                           final List<Map<String, List<TopicPartitionInfo>>> topicPartitionInfo,
                                                                           final Map<String, Object> parameterizedConfig) {
        final MockInternalTopicManager mockInternalTopicManager = spy(new MockInternalTopicManager(
            time,
            new StreamsConfig(configProps(parameterizedConfig)),
            mockClientSupplier.restoreConsumer,
            mockCreateInternalTopics
        ));

        if (topicPartitionInfo != null) {
            lenient().when(mockInternalTopicManager.getTopicPartitionInfo(anySet())).thenAnswer(
                invocation -> {
                    final Set<String> topics = invocation.getArgument(0);
                    for (final Map<String, List<TopicPartitionInfo>> tp : topicPartitionInfo) {
                        if (topics.equals(tp.keySet())) {
                            return tp;
                        }
                    }
                    return emptyMap();
                }
            );
        }

        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);
        return mockInternalTopicManager;
    }

    static Stream<Arguments> parameter() {
        return Stream.of(
            Arguments.of(buildParameterizedConfig(HighAvailabilityTaskAssignor.class, null, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)),
            Arguments.of(buildParameterizedConfig(HighAvailabilityTaskAssignor.class, null, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE)),
            Arguments.of(buildParameterizedConfig(LegacyStickyTaskAssignor.class, null, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)),
            Arguments.of(buildParameterizedConfig(LegacyStickyTaskAssignor.class, null, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE)),
            Arguments.of(buildParameterizedConfig(FallbackPriorTaskAssignor.class, null, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)),
            Arguments.of(buildParameterizedConfig(FallbackPriorTaskAssignor.class, null, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE)),
            Arguments.of(buildParameterizedConfig(null, StickyTaskAssignor.class, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE)),
            Arguments.of(buildParameterizedConfig(null, StickyTaskAssignor.class, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)),
            Arguments.of(buildParameterizedConfig(HighAvailabilityTaskAssignor.class, StickyTaskAssignor.class, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE))
        );
    }

    private static Map<String, Object> buildParameterizedConfig(final Class<? extends LegacyTaskAssignor> internalTaskAssignor,
                                                                final Class<? extends TaskAssignor> customTaskAssignor,
                                                                final String rackAwareAssignorStrategy) {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, rackAwareAssignorStrategy);
        if (internalTaskAssignor != null) {
            configurationMap.put(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, internalTaskAssignor.getName());
        }
        if (customTaskAssignor != null) {
            configurationMap.put(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG, customTaskAssignor.getName());
        }

        return configurationMap;
    }

    private void setUp(final Map<String, Object> parameterizedConfig, final boolean mockListOffsets) {
        adminClient = createMockAdminClientForAssignor(EMPTY_CHANGELOG_END_OFFSETS, mockListOffsets);
        topologyMetadata = new TopologyMetadata(builder, new StreamsConfig(configProps(parameterizedConfig)));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldUseEagerRebalancingProtocol(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_23), parameterizedConfig);

        assertEquals(1, partitionAssignor.supportedProtocols().size());
        assertTrue(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.EAGER));
        assertFalse(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.COOPERATIVE));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldUseCooperativeRebalancingProtocol(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        configureDefault(parameterizedConfig);

        assertEquals(2, partitionAssignor.supportedProtocols().size());
        assertTrue(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.COOPERATIVE));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldProduceStickyAndBalancedAssignmentWhenNothingChanges(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final List<TaskId> allTasks =
                asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3);

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, asList(TASK_0_0, TASK_1_1, TASK_1_3)),
            mkEntry(CONSUMER_2, asList(TASK_0_3, TASK_1_0)),
            mkEntry(CONSUMER_3, asList(TASK_0_1, TASK_0_2, TASK_1_2))
        );

        final ClientState state = new ClientState();
        final SortedSet<String> consumers = mkSortedSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        state.addPreviousTasksAndOffsetSums(CONSUMER_1, getTaskOffsetSums(asList(TASK_0_0, TASK_1_1, TASK_1_3), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_2, getTaskOffsetSums(asList(TASK_0_3, TASK_1_0), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_3, getTaskOffsetSums(asList(TASK_0_1, TASK_0_2, TASK_1_2), EMPTY_TASKS));
        state.initializePrevTasks(emptyMap(), false);
        state.computeTaskLags(PID_1, getTaskEndOffsetSums(allTasks));

        assertEquivalentAssignment(
            previousAssignment,
            assignTasksToThreads(
                allTasks,
                true,
                consumers,
                state,
                new HashMap<>()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldProduceStickyAndBalancedAssignmentWhenNewTasksAreAdded(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final List<TaskId> allTasks =
            new ArrayList<>(asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3));

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, new ArrayList<>(asList(TASK_0_0, TASK_1_1, TASK_1_3))),
            mkEntry(CONSUMER_2, new ArrayList<>(asList(TASK_0_3, TASK_1_0))),
            mkEntry(CONSUMER_3, new ArrayList<>(asList(TASK_0_1, TASK_0_2, TASK_1_2)))
        );

        final ClientState state = new ClientState();
        final SortedSet<String> consumers = mkSortedSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        state.addPreviousTasksAndOffsetSums(CONSUMER_1, getTaskOffsetSums(asList(TASK_0_0, TASK_1_1, TASK_1_3), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_2, getTaskOffsetSums(asList(TASK_0_3, TASK_1_0), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_3, getTaskOffsetSums(asList(TASK_0_1, TASK_0_2, TASK_1_2), EMPTY_TASKS));
        state.initializePrevTasks(emptyMap(), false);
        state.computeTaskLags(PID_1, getTaskEndOffsetSums(allTasks));

        // We should be able to add a new task without sacrificing stickiness
        final TaskId newTask = TASK_2_0;
        allTasks.add(newTask);
        state.assignActiveTasks(allTasks);

        final Map<String, List<TaskId>> newAssignment =
            assignTasksToThreads(
                allTasks,
                true,
                consumers,
                state,
                new HashMap<>()
            );

        previousAssignment.get(CONSUMER_2).add(newTask);
        assertEquivalentAssignment(previousAssignment, newAssignment);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldProduceMaximallyStickyAssignmentWhenMemberLeaves(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final List<TaskId> allTasks =
            asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3);

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, asList(TASK_0_0, TASK_1_1, TASK_1_3)),
            mkEntry(CONSUMER_2, asList(TASK_0_3, TASK_1_0)),
            mkEntry(CONSUMER_3, asList(TASK_0_1, TASK_0_2, TASK_1_2))
        );

        final ClientState state = new ClientState();
        final SortedSet<String> consumers = mkSortedSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        state.addPreviousTasksAndOffsetSums(CONSUMER_1, getTaskOffsetSums(asList(TASK_0_0, TASK_1_1, TASK_1_3), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_2, getTaskOffsetSums(asList(TASK_0_3, TASK_1_0), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_3, getTaskOffsetSums(asList(TASK_0_1, TASK_0_2, TASK_1_2), EMPTY_TASKS));
        state.initializePrevTasks(emptyMap(), false);
        state.computeTaskLags(PID_1, getTaskEndOffsetSums(allTasks));

        // Consumer 3 leaves the group
        consumers.remove(CONSUMER_3);

        final Map<String, List<TaskId>> assignment = assignTasksToThreads(
            allTasks,
            true,
            consumers,
            state,
            new HashMap<>()
        );

        // Each member should have all of its previous tasks reassigned plus some of consumer 3's tasks
        // We should give one of its tasks to consumer 1, and two of its tasks to consumer 2
        assertTrue(assignment.get(CONSUMER_1).containsAll(previousAssignment.get(CONSUMER_1)));
        assertTrue(assignment.get(CONSUMER_2).containsAll(previousAssignment.get(CONSUMER_2)));

        assertThat(assignment.get(CONSUMER_1).size(), equalTo(4));
        assertThat(assignment.get(CONSUMER_2).size(), equalTo(4));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldProduceStickyEnoughAssignmentWhenNewMemberJoins(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final List<TaskId> allTasks =
            asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3);

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, asList(TASK_0_0, TASK_1_1, TASK_1_3)),
            mkEntry(CONSUMER_2, asList(TASK_0_3, TASK_1_0)),
            mkEntry(CONSUMER_3, asList(TASK_0_1, TASK_0_2, TASK_1_2))
        );

        final ClientState state = new ClientState();
        final SortedSet<String> consumers = mkSortedSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        state.addPreviousTasksAndOffsetSums(CONSUMER_1, getTaskOffsetSums(asList(TASK_0_0, TASK_1_1, TASK_1_3), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_2, getTaskOffsetSums(asList(TASK_0_3, TASK_1_0), EMPTY_TASKS));
        state.addPreviousTasksAndOffsetSums(CONSUMER_3, getTaskOffsetSums(asList(TASK_0_1, TASK_0_2, TASK_1_2), EMPTY_TASKS));

        // Consumer 4 joins the group
        consumers.add(CONSUMER_4);
        state.addPreviousTasksAndOffsetSums(CONSUMER_4, getTaskOffsetSums(EMPTY_TASKS, EMPTY_TASKS));

        state.initializePrevTasks(emptyMap(), false);
        state.computeTaskLags(PID_1, getTaskEndOffsetSums(allTasks));

        final Map<String, List<TaskId>> assignment = assignTasksToThreads(
            allTasks,
            true,
            consumers,
            state,
            new HashMap<>()
        );

        // we should move one task each from consumer 1 and consumer 3 to the new member, and none from consumer 2
        assertTrue(previousAssignment.get(CONSUMER_1).containsAll(assignment.get(CONSUMER_1)));
        assertTrue(previousAssignment.get(CONSUMER_3).containsAll(assignment.get(CONSUMER_3)));

        assertTrue(assignment.get(CONSUMER_2).containsAll(previousAssignment.get(CONSUMER_2)));


        assertThat(assignment.get(CONSUMER_1).size(), equalTo(2));
        assertThat(assignment.get(CONSUMER_2).size(), equalTo(2));
        assertThat(assignment.get(CONSUMER_3).size(), equalTo(2));
        assertThat(assignment.get(CONSUMER_4).size(), equalTo(2));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldInterleaveTasksByGroupIdDuringNewAssignment(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final List<TaskId> allTasks =
            asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1);

        final Map<String, List<TaskId>> assignment = mkMap(
            mkEntry(CONSUMER_1, new ArrayList<>(asList(TASK_0_0, TASK_0_3, TASK_1_2))),
            mkEntry(CONSUMER_2, new ArrayList<>(asList(TASK_0_1, TASK_1_0, TASK_2_0))),
            mkEntry(CONSUMER_3, new ArrayList<>(asList(TASK_0_2, TASK_1_1, TASK_2_1)))
        );

        final ClientState state = new ClientState();
        final SortedSet<String> consumers = mkSortedSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        state.addPreviousTasksAndOffsetSums(CONSUMER_1, emptyMap());
        state.addPreviousTasksAndOffsetSums(CONSUMER_2, emptyMap());
        state.addPreviousTasksAndOffsetSums(CONSUMER_3, emptyMap());

        Collections.shuffle(allTasks);

        final Map<String, List<TaskId>> interleavedTaskIds =
            assignTasksToThreads(
                allTasks,
                true,
                consumers,
                state,
                new HashMap<>()
            );

        assertThat(interleavedTaskIds, equalTo(assignment));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testEagerSubscription(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");

        final Set<TaskId> prevTasks = Set.of(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1)
        );
        final Set<TaskId> standbyTasks = Set.of(
            new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2)
        );

        createMockTaskManager(prevTasks, standbyTasks);
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_23), parameterizedConfig);
        assertThat(partitionAssignor.rebalanceProtocol(), equalTo(RebalanceProtocol.EAGER));

        final Set<String> topics = Set.of("topic1", "topic2");
        final Subscription subscription = new Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        Collections.sort(subscription.topics());
        assertEquals(asList("topic1", "topic2"), subscription.topics());

        final SubscriptionInfo info = getInfo(PID_1, prevTasks, standbyTasks, uniqueField);
        assertEquals(info, SubscriptionInfo.decode(subscription.userData()));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testCooperativeSubscription(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);

        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");

        final Set<TaskId> prevTasks = Set.of(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1));
        final Set<TaskId> standbyTasks = Set.of(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
            new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2));

        createMockTaskManager(prevTasks, standbyTasks);
        configureDefaultPartitionAssignor(parameterizedConfig);

        final Set<String> topics = Set.of("topic1", "topic2");
        final Subscription subscription = new Subscription(
            new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        Collections.sort(subscription.topics());
        assertEquals(asList("topic1", "topic2"), subscription.topics());

        final SubscriptionInfo info = getInfo(PID_1, prevTasks, standbyTasks, uniqueField);
        assertEquals(info, SubscriptionInfo.decode(subscription.userData()));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignBasic(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store", false), "processor");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);

        final Set<TaskId> prevTasks10 = Set.of(TASK_0_0);
        final Set<TaskId> prevTasks11 = Set.of(TASK_0_1);
        final Set<TaskId> prevTasks20 = Set.of(TASK_0_2);
        final Set<TaskId> standbyTasks10 = EMPTY_TASKS;
        final Set<TaskId> standbyTasks11 = Set.of(TASK_0_2);
        final Set<TaskId> standbyTasks20 = Set.of(TASK_0_0);

        createMockTaskManager(prevTasks10, standbyTasks10);
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
            singletonList(APPLICATION_ID + "-store-changelog"),
            singletonList(3)),
            true
        );

        final List<Map<String, List<TopicPartitionInfo>>> partitionInfo = singletonList(mkMap(mkEntry(
                "stream-partition-assignor-test-store-changelog",
                singletonList(
                    new TopicPartitionInfo(
                        0,
                        new Node(1, "h1", 80),
                        singletonList(new Node(1, "h1", 80)),
                        emptyList()
                    )
                )
            )
        ));
        configurePartitionAssignorWith(emptyMap(), partitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(PID_1, prevTasks10, standbyTasks10).encode(),
                              Collections.emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_3)
                          ));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              getInfo(PID_1, prevTasks11, standbyTasks11).encode(),
                              Collections.emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_3)
                          ));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(PID_2, prevTasks20, standbyTasks20).encode(),
                              Collections.emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_0)
                          ));

        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();


        // check the assignment
        assertEquals(Set.of(Set.of(t1p0, t2p0), Set.of(t1p1, t2p1)),
            Set.of(new HashSet<>(assignments.get("consumer10").partitions()),
                new HashSet<>(assignments.get("consumer11").partitions())));
        assertEquals(Set.of(t1p2, t2p2), new HashSet<>(assignments.get("consumer20").partitions()));

        // check assignment info

        // the first consumer
        final AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        final Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());

        // the second consumer
        final AssignmentInfo info11 = checkAssignment(allTopics, assignments.get("consumer11"));
        allActiveTasks.addAll(info11.activeTasks());

        assertEquals(Set.of(TASK_0_0, TASK_0_1), allActiveTasks);

        // the third consumer
        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignEvenlyAcrossConsumersOneClientMultipleThreads(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");
        builder.addProcessor("processorII", new MockApiProcessorSupplier<>(), "source2");

        final List<PartitionInfo> localInfos = asList(
            new PartitionInfo("topic1", 0, NODE_0, REPLICA_0, REPLICA_0),
            new PartitionInfo("topic1", 1, NODE_1, REPLICA_1, REPLICA_1),
            new PartitionInfo("topic1", 2, NODE_2, REPLICA_2, REPLICA_2),
            new PartitionInfo("topic1", 3, NODE_3, REPLICA_3, REPLICA_3),
            new PartitionInfo("topic2", 0, NODE_4, REPLICA_4, REPLICA_4),
            new PartitionInfo("topic2", 1, NODE_0, REPLICA_0, REPLICA_0),
            new PartitionInfo("topic2", 2, NODE_1, REPLICA_1, REPLICA_1),
            new PartitionInfo("topic2", 3, NODE_2, REPLICA_2, REPLICA_2)
        );

        final Cluster localMetadata = new Cluster(
            "cluster",
            asList(NODE_0, NODE_1, NODE_2, NODE_3, NODE_4),
            localInfos,
            emptySet(),
            emptySet()
        );

        final List<String> topics = asList("topic1", "topic2");

        configureDefault(parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          ));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          ));

        final Map<String, Assignment> assignments = partitionAssignor.assign(localMetadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partitions
        assertEquals(Set.of(Set.of(t2p2, t1p0, t1p2, t2p0), Set.of(t1p1, t2p1, t1p3, t2p3)),
                     Set.of(new HashSet<>(assignments.get("consumer10").partitions()), new HashSet<>(assignments.get("consumer11").partitions())));

        // the first consumer
        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());

        final List<TaskId> expectedInfo10TaskIds = asList(TASK_0_0, TASK_0_2, TASK_1_0, TASK_1_2);
        assertEquals(expectedInfo10TaskIds, info10.activeTasks());

        // the second consumer
        final AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        final List<TaskId> expectedInfo11TaskIds = asList(TASK_0_1, TASK_0_3, TASK_1_1, TASK_1_3);

        assertEquals(expectedInfo11TaskIds, info11.activeTasks());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotAssignTemporaryStandbyTask(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1");

        final List<PartitionInfo> localInfos = asList(
            new PartitionInfo("topic1", 0, NODE_0, REPLICA_0, REPLICA_0),
            new PartitionInfo("topic1", 1, NODE_1, REPLICA_1, REPLICA_1),
            new PartitionInfo("topic1", 2, NODE_2, REPLICA_2, REPLICA_2),
            new PartitionInfo("topic1", 3, NODE_0, REPLICA_1, REPLICA_2)
        );

        final Cluster localMetadata = new Cluster(
            "cluster",
            asList(NODE_0, NODE_1, NODE_2),
            localInfos,
            emptySet(),
            emptySet()
        );

        final List<String> topics = singletonList("topic1");

        createMockTaskManager(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), emptySet());
        configureDefaultPartitionAssignor(parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(PID_1, Set.of(TASK_0_0, TASK_0_2), emptySet()).encode(),
                              asList(t1p0, t1p2),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          ));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              getInfo(PID_1, Set.of(TASK_0_1, TASK_0_3), emptySet()).encode(),
                              asList(t1p1, t1p3),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          ));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(PID_2, emptySet(), emptySet()).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          ));

        final Map<String, Assignment> assignments = partitionAssignor.assign(localMetadata, new GroupSubscription(subscriptions)).groupAssignment();

        // neither active nor standby tasks should be assigned to consumer 3, which will have to wait until
        // the followup cooperative rebalance to get the active task(s) it was assigned (and does not need
        // a standby copy before that since it previously had no tasks at all)
        final AssignmentInfo info20 = AssignmentInfo.decode(assignments.get("consumer20").userData());
        assertTrue(info20.activeTasks().isEmpty());
        assertTrue(info20.standbyTasks().isEmpty());

    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignEmptyMetadata(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);

        final Set<TaskId> prevTasks10 = Set.of(TASK_0_0);
        final Set<TaskId> standbyTasks10 = Set.of(TASK_0_1);
        final Cluster emptyMetadata = new Cluster("cluster", Collections.singletonList(Node.noNode()),
                                                  emptySet(),
                                                  emptySet(),
                                                  emptySet());

        createMockTaskManager(prevTasks10, standbyTasks10);
        configureDefaultPartitionAssignor(parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(PID_1, prevTasks10, standbyTasks10).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_3)
                          ));

        // initially metadata is empty
        Map<String, Assignment> assignments =
            partitionAssignor.assign(emptyMetadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partitions
        assertEquals(emptySet(),
                     new HashSet<>(assignments.get("consumer10").partitions()));

        // check assignment info
        AssignmentInfo info10 = checkAssignment(emptySet(), assignments.get("consumer10"));
        final Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());

        assertEquals(0, allActiveTasks.size());

        // then metadata gets populated
        assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        // check assigned partitions
        assertEquals(Set.of(new HashSet<>(List.of(t1p0, t2p0, t1p0, t2p0, t1p1, t2p1, t1p2, t2p2))),
                     Set.of(new HashSet<>(assignments.get("consumer10").partitions())));

        // the first consumer
        info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        allActiveTasks.addAll(info10.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithNewTasks(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addSource(null, "source3", null, null, null, "topic3");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2", "source3");
        final List<String> topics = asList("topic1", "topic2", "topic3");
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);

        // assuming that previous tasks do not have topic3
        final Set<TaskId> prevTasks10 = Set.of(TASK_0_0);
        final Set<TaskId> prevTasks11 = Set.of(TASK_0_1);
        final Set<TaskId> prevTasks20 = Set.of(TASK_0_2);
        
        createMockTaskManager(prevTasks10, EMPTY_TASKS);
        configureDefaultPartitionAssignor(parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(PID_1, prevTasks10, EMPTY_TASKS).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_4)));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              getInfo(PID_1, prevTasks11, EMPTY_TASKS).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_4)));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(PID_2, prevTasks20, EMPTY_TASKS).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_1)));

        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partitions: since there is no previous task for topic 3 it will be assigned randomly so we cannot check exact match
        // also note that previously assigned partitions / tasks may not stay on the previous host since we may assign the new task first and
        // then later ones will be re-assigned to other hosts due to load balancing
        AssignmentInfo info = AssignmentInfo.decode(assignments.get("consumer10").userData());
        final Set<TaskId> allActiveTasks = new HashSet<>(info.activeTasks());
        final Set<TopicPartition> allPartitions = new HashSet<>(assignments.get("consumer10").partitions());

        info = AssignmentInfo.decode(assignments.get("consumer11").userData());
        allActiveTasks.addAll(info.activeTasks());
        allPartitions.addAll(assignments.get("consumer11").partitions());

        info = AssignmentInfo.decode(assignments.get("consumer20").userData());
        allActiveTasks.addAll(info.activeTasks());
        allPartitions.addAll(assignments.get("consumer20").partitions());

        assertEquals(allTasks, allActiveTasks);
        assertEquals(Set.of(t1p0, t1p1, t1p2, t2p0, t2p1, t2p2, t3p0, t3p1, t3p2, t3p3), allPartitions);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithStates(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");

        builder.addProcessor("processor-1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor-1");

        builder.addProcessor("processor-2", new MockApiProcessorSupplier<>(), "source2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store2", false), "processor-2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store3", false), "processor-2");

        final List<String> topics = asList("topic1", "topic2");

        final List<TaskId> tasks = asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                asList(APPLICATION_ID + "-store1-changelog",
                    APPLICATION_ID + "-store2-changelog",
                    APPLICATION_ID + "-store3-changelog"),
                asList(3, 3, 3)),
            true
        );

        createDefaultMockTaskManager();
        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            3,
            singletonList(Set.of(
                APPLICATION_ID + "-store1-changelog",
                APPLICATION_ID + "-store2-changelog",
                APPLICATION_ID + "-store3-changelog"
            )));
        configurePartitionAssignorWith(emptyMap(), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_1)));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_1)));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(PID_2, EMPTY_TASKS, EMPTY_TASKS).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)));

        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partition size: since there is no previous task and there are two sub-topologies the assignment is random so we cannot check exact match
        assertEquals(2, assignments.get("consumer10").partitions().size());
        assertEquals(2, assignments.get("consumer11").partitions().size());
        assertEquals(2, assignments.get("consumer20").partitions().size());

        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());
        final AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        final AssignmentInfo info20 = AssignmentInfo.decode(assignments.get("consumer20").userData());

        assertEquals(2, info10.activeTasks().size());
        assertEquals(2, info11.activeTasks().size());
        assertEquals(2, info20.activeTasks().size());

        final Set<TaskId> allTasks = new HashSet<>();
        allTasks.addAll(info10.activeTasks());
        allTasks.addAll(info11.activeTasks());
        allTasks.addAll(info20.activeTasks());
        assertEquals(new HashSet<>(tasks), allTasks);

        // check tasks for state topics
        final Map<Subtopology, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.subtopologyToTopicsInfo();

        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2), tasksForState("store1", tasks, topicGroups));
        assertEquals(Set.of(TASK_1_0, TASK_1_1, TASK_1_2), tasksForState("store2", tasks, topicGroups));
        assertEquals(Set.of(TASK_1_0, TASK_1_1, TASK_1_2), tasksForState("store3", tasks, topicGroups));
    }

    private static Set<TaskId> tasksForState(final String storeName,
                                             final List<TaskId> tasks,
                                             final Map<Subtopology, InternalTopologyBuilder.TopicsInfo> topicGroups) {
        final String changelogTopic = ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, storeName, null);

        final Set<TaskId> ids = new HashSet<>();
        for (final Map.Entry<Subtopology, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            final Set<String> stateChangelogTopics = entry.getValue().stateChangelogTopics.keySet();

            if (stateChangelogTopics.contains(changelogTopic)) {
                for (final TaskId id : tasks) {
                    if (id.subtopology() == entry.getKey().nodeGroupId) {
                        ids.add(id);
                    }
                }
            }
        }
        return ids;
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithStandbyReplicasAndStatelessTasks(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1", "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");

        final List<String> topics = asList("topic1", "topic2");

        createMockTaskManager(Set.of(TASK_0_0), emptySet());
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1), parameterizedConfig);

        subscriptions.put("consumer10",
            new Subscription(
                topics,
                getInfo(PID_1, Set.of(TASK_0_0), emptySet()).encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_0)));
        subscriptions.put("consumer20",
            new Subscription(
                topics,
                getInfo(PID_2, Set.of(TASK_0_2), emptySet()).encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_2)));

        final Map<String, Assignment> assignments =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        final AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        assertTrue(info10.standbyTasks().isEmpty());

        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        assertTrue(info20.standbyTasks().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithStandbyReplicasAndLoggingDisabled(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1", "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false).withLoggingDisabled(), "processor");

        final List<String> topics = asList("topic1", "topic2");

        createMockTaskManager(Set.of(TASK_0_0), emptySet());
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1), parameterizedConfig);

        subscriptions.put("consumer10",
            new Subscription(
                topics,
                getInfo(PID_1, Set.of(TASK_0_0), emptySet()).encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_1)));
        subscriptions.put("consumer20",
            new Subscription(
                topics,
                getInfo(PID_2, Set.of(TASK_0_2), emptySet()).encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_3)));

        final Map<String, Assignment> assignments =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        final AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        assertTrue(info10.standbyTasks().isEmpty());

        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        assertTrue(info20.standbyTasks().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithStandbyReplicas(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor");

        final List<String> topics = asList("topic1", "topic2");
        final Set<TopicPartition> allTopicPartitions = topics.stream()
            .map(topic -> asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2)))
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);

        final Set<TaskId> prevTasks00 = Set.of(TASK_0_0);
        final Set<TaskId> prevTasks01 = Set.of(TASK_0_1);
        final Set<TaskId> prevTasks02 = Set.of(TASK_0_2);
        final Set<TaskId> standbyTasks00 = Set.of(TASK_0_0);
        final Set<TaskId> standbyTasks01 = Set.of(TASK_0_1);
        final Set<TaskId> standbyTasks02 = Set.of(TASK_0_2);

        createMockTaskManager(prevTasks00, standbyTasks01);
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                singletonList(APPLICATION_ID + "-store1-changelog"),
                singletonList(3)),
            true
        );

        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            3,
            singletonList(Set.of(APPLICATION_ID + "-store1-changelog")));
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(PID_1, prevTasks00, EMPTY_TASKS, USER_END_POINT).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_0)));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              getInfo(PID_1, prevTasks01, standbyTasks02, USER_END_POINT).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_0)));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(PID_2, prevTasks02, standbyTasks00, OTHER_END_POINT).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_4)));

        final Map<String, Assignment> assignments =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // the first consumer
        final AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        final Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());
        final Set<TaskId> allStandbyTasks = new HashSet<>(info10.standbyTasks().keySet());

        // the second consumer
        final AssignmentInfo info11 = checkAssignment(allTopics, assignments.get("consumer11"));
        allActiveTasks.addAll(info11.activeTasks());
        allStandbyTasks.addAll(info11.standbyTasks().keySet());

        assertNotEquals(info11.standbyTasks().keySet(), info10.standbyTasks().keySet(), "same processId has same set of standby tasks");

        // check active tasks assigned to the first client
        assertEquals(Set.of(TASK_0_0, TASK_0_1), new HashSet<>(allActiveTasks));
        assertEquals(Set.of(TASK_0_2), new HashSet<>(allStandbyTasks));

        // the third consumer
        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks());
        allStandbyTasks.addAll(info20.standbyTasks().keySet());

        // all task ids are in the active tasks and also in the standby tasks
        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);

        assertEquals(3, allStandbyTasks.size());
        assertEquals(allTasks, allStandbyTasks);

        // Check host partition assignments
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost = info10.partitionsByHost();
        assertEquals(2, partitionsByHost.size());
        assertEquals(allTopicPartitions, partitionsByHost.values().stream()
            .flatMap(Collection::stream).collect(Collectors.toSet()));

        final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost = info10.standbyPartitionByHost();
        assertEquals(2, standbyPartitionsByHost.size());
        assertEquals(allTopicPartitions, standbyPartitionsByHost.values().stream()
            .flatMap(Collection::stream).collect(Collectors.toSet()));

        for (final HostInfo hostInfo : partitionsByHost.keySet()) {
            assertTrue(Collections.disjoint(partitionsByHost.get(hostInfo), standbyPartitionsByHost.get(hostInfo)));
        }

        // All consumers got the same host info
        assertEquals(partitionsByHost, info11.partitionsByHost());
        assertEquals(partitionsByHost, info20.partitionsByHost());
        assertEquals(standbyPartitionsByHost, info11.standbyPartitionByHost());
        assertEquals(standbyPartitionsByHost, info20.standbyPartitionByHost());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithStandbyReplicasBalanceSparse(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor");

        final List<String> topics = singletonList("topic1");

        createMockTaskManager(EMPTY_TASKS, EMPTY_TASKS);
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                singletonList(APPLICATION_ID + "-store1-changelog"),
                singletonList(3)),
            true
        );
        final Map<String, List<TopicPartitionInfo>> changelogTopicPartitionInfo = mkMap(
            mkEntry(APPLICATION_ID + "-store1-changelog",
                asList(
                    new TopicPartitionInfo(0, NODE_0, asList(REPLICA_0), asList(REPLICA_0)),
                    new TopicPartitionInfo(1, NODE_1, asList(REPLICA_1), asList(REPLICA_1)),
                    new TopicPartitionInfo(2, NODE_3, asList(REPLICA_3), asList(REPLICA_3))
                )
            )
        );
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1), singletonList(changelogTopicPartitionInfo), parameterizedConfig);

        final List<String> client1Consumers = asList("consumer10", "consumer11", "consumer12", "consumer13");
        final List<String> client2Consumers = asList("consumer20", "consumer21", "consumer22");

        for (final String consumerId : client1Consumers) {
            subscriptions.put(consumerId,
                    new Subscription(
                            topics,
                            getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                            emptyList(),
                            DEFAULT_GENERATION,
                            Optional.of(RACK_2)));
        }
        for (final String consumerId : client2Consumers) {
            subscriptions.put(consumerId,
                    new Subscription(
                            topics,
                            getInfo(PID_2, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                            emptyList(),
                            DEFAULT_GENERATION,
                            Optional.of(RACK_4)));
        }

        final Map<String, Assignment> assignments =
                partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // Consumers
        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());
        final AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        final AssignmentInfo info12 = AssignmentInfo.decode(assignments.get("consumer12").userData());
        final AssignmentInfo info13 = AssignmentInfo.decode(assignments.get("consumer13").userData());
        final AssignmentInfo info20 = AssignmentInfo.decode(assignments.get("consumer20").userData());
        final AssignmentInfo info21 = AssignmentInfo.decode(assignments.get("consumer21").userData());
        final AssignmentInfo info22 = AssignmentInfo.decode(assignments.get("consumer22").userData());

        // Check each consumer has no more than 1 task
        assertTrue(info10.activeTasks().size() + info10.standbyTasks().size() <= 1);
        assertTrue(info11.activeTasks().size() + info11.standbyTasks().size() <= 1);
        assertTrue(info12.activeTasks().size() + info12.standbyTasks().size() <= 1);
        assertTrue(info13.activeTasks().size() + info13.standbyTasks().size() <= 1);
        assertTrue(info20.activeTasks().size() + info20.standbyTasks().size() <= 1);
        assertTrue(info21.activeTasks().size() + info21.standbyTasks().size() <= 1);
        assertTrue(info22.activeTasks().size() + info22.standbyTasks().size() <= 1);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithStandbyReplicasBalanceDense(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor");

        final List<String> topics = singletonList("topic1");

        createMockTaskManager(EMPTY_TASKS, EMPTY_TASKS);
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                singletonList(APPLICATION_ID + "-store1-changelog"),
                singletonList(3)),
            true
        );
        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            3,
            singletonList(Set.of(APPLICATION_ID + "-store1-changelog")));
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
                new Subscription(
                        topics,
                        getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                        emptyList(),
                        DEFAULT_GENERATION,
                        Optional.of(RACK_4)));
        subscriptions.put("consumer20",
                new Subscription(
                        topics,
                        getInfo(PID_2, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                        emptyList(),
                        DEFAULT_GENERATION,
                        Optional.of(RACK_4)));

        final Map<String, Assignment> assignments =
                partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // Consumers
        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());
        final AssignmentInfo info20 = AssignmentInfo.decode(assignments.get("consumer20").userData());

        // Check each consumer has 3 tasks
        assertEquals(3, info10.activeTasks().size() + info10.standbyTasks().size());
        assertEquals(3, info20.activeTasks().size() + info20.standbyTasks().size());
        // Check that not all the actives are on one node
        assertTrue(info10.activeTasks().size() < 3);
        assertTrue(info20.activeTasks().size() < 3);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithStandbyReplicasBalanceWithStatelessTasks(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor_with_state", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor_with_state");

        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source2");

        final List<String> topics = asList("topic1", "topic2");

        createMockTaskManager(EMPTY_TASKS, EMPTY_TASKS);
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                singletonList(APPLICATION_ID + "-store1-changelog"),
                singletonList(3)),
            true
        );
        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            3,
            singletonList(Set.of(APPLICATION_ID + "-store1-changelog")));
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
                new Subscription(
                        topics,
                        getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                        emptyList(),
                        DEFAULT_GENERATION,
                        Optional.of(RACK_0)));
        subscriptions.put("consumer11",
                new Subscription(
                        topics,
                        getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                        emptyList(),
                        DEFAULT_GENERATION,
                        Optional.of(RACK_0)));
        subscriptions.put("consumer20",
                new Subscription(
                        topics,
                        getInfo(PID_2, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                        emptyList(),
                        DEFAULT_GENERATION,
                        Optional.of(RACK_2)));
        subscriptions.put("consumer21",
                new Subscription(
                        topics,
                        getInfo(PID_2, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                        emptyList(),
                        DEFAULT_GENERATION,
                        Optional.of(RACK_2)));

        final Map<String, Assignment> assignments =
                partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // Consumers
        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());
        final AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        final AssignmentInfo info20 = AssignmentInfo.decode(assignments.get("consumer20").userData());
        final AssignmentInfo info21 = AssignmentInfo.decode(assignments.get("consumer21").userData());

        // 9 tasks spread over 4 consumers, so we should have no more than 3 tasks per consumer
        assertTrue(info10.activeTasks().size() + info10.standbyTasks().size() <= 3);
        assertTrue(info11.activeTasks().size() + info11.standbyTasks().size() <= 3);
        assertTrue(info20.activeTasks().size() + info20.standbyTasks().size() <= 3);
        assertTrue(info21.activeTasks().size() + info21.standbyTasks().size() <= 3);
        // No more than 1 standby per node.
        assertTrue(info10.standbyTasks().size() <= 1);
        assertTrue(info11.standbyTasks().size() <= 1);
        assertTrue(info20.standbyTasks().size() <= 1);
        assertTrue(info21.standbyTasks().size() <= 1);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testOnAssignment(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        taskManager = mock(TaskManager.class);

        final Map<HostInfo, Set<TopicPartition>> hostState = Collections.singletonMap(
            new HostInfo("localhost", 9090),
            Set.of(t3p0, t3p3));

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(TASK_0_0, Set.of(t3p0));
        activeTasks.put(TASK_0_3, Set.of(t3p3));
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        standbyTasks.put(TASK_0_1, Set.of(t3p1));
        standbyTasks.put(TASK_0_2, Set.of(t3p2));

        streamsMetadataState = mock(StreamsMetadataState.class);

        configureDefaultPartitionAssignor(parameterizedConfig);

        final List<TaskId> activeTaskList = asList(TASK_0_0, TASK_0_3);
        final AssignmentInfo info = new AssignmentInfo(LATEST_SUPPORTED_VERSION, activeTaskList, standbyTasks, hostState, emptyMap(), 0);
        final Assignment assignment = new Assignment(asList(t3p0, t3p3), info.encode());

        partitionAssignor.onAssignment(assignment, null);

        verify(streamsMetadataState).onChange(eq(hostState), any(), topicPartitionInfoCaptor.capture());
        verify(taskManager).handleAssignment(activeTasks, standbyTasks);

        assertTrue(topicPartitionInfoCaptor.getValue().containsKey(t3p0));
        assertTrue(topicPartitionInfoCaptor.getValue().containsKey(t3p3));
        assertEquals(2, topicPartitionInfoCaptor.getValue().size());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithInternalTopics(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addInternalTopic("topicX", InternalTopicProperties.empty());
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addSink("sink1", "topicX", null, null, null, "processor1");
        builder.addSource(null, "source2", null, null, null, "topicX");
        builder.addProcessor("processor2", new MockApiProcessorSupplier<>(), "source2");
        final List<String> topics = asList("topic1", APPLICATION_ID + "-topicX");
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);

        createDefaultMockTaskManager();
        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            4,
            singletonList(Set.of(APPLICATION_ID + "-topicX")));
        final MockInternalTopicManager internalTopicManager = configurePartitionAssignorWith(emptyMap(), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2))
        );
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        // check prepared internal topics
        assertEquals(1, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get(APPLICATION_ID + "-topicX"));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testAssignWithInternalTopicThatsSourceIsAnotherInternalTopic(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addInternalTopic("topicX", InternalTopicProperties.empty());
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addSink("sink1", "topicX", null, null, null, "processor1");
        builder.addSource(null, "source2", null, null, null, "topicX");
        builder.addInternalTopic("topicZ", InternalTopicProperties.empty());
        builder.addProcessor("processor2", new MockApiProcessorSupplier<>(), "source2");
        builder.addSink("sink2", "topicZ", null, null, null, "processor2");
        builder.addSource(null, "source3", null, null, null, "topicZ");
        final List<String> topics = asList("topic1", APPLICATION_ID + "-topicX", APPLICATION_ID + "-topicZ");
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);

        createDefaultMockTaskManager();
        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            4,
            singletonList(Set.of(APPLICATION_ID + "-topicX", APPLICATION_ID + "-topicZ")));
        final MockInternalTopicManager internalTopicManager = configurePartitionAssignorWith(emptyMap(), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_3))
        );
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        // check prepared internal topics
        assertEquals(2, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get(APPLICATION_ID + "-topicZ"));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldGenerateTasksForAllCreatedPartitions(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // KStream with 3 partitions
        final KStream<Object, Object> stream1 = streamsBuilder
            .stream("topic1")
            // force creation of internal repartition topic
            .map((KeyValueMapper<Object, Object, KeyValue<Object, Object>>) KeyValue::new);

        // KTable with 4 partitions
        final KTable<Object, Long> table1 = streamsBuilder
            .table("topic3")
            // force creation of internal repartition topic
            .groupBy(KeyValue::new)
            .count();

        // joining the stream and the table
        // this triggers the enforceCopartitioning() routine in the StreamsPartitionAssignor,
        // forcing the stream.map to get repartitioned to a topic with four partitions.
        stream1.join(
            table1,
            (ValueJoiner<Object, Object, Void>) (value1, value2) -> null);

        final String client = "client1";
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());
        topologyMetadata = new TopologyMetadata(builder, new StreamsConfig(configProps(parameterizedConfig)));

        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                asList(APPLICATION_ID + "-topic3-STATE-STORE-0000000002-changelog",
                    APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-changelog"),
                asList(4, 4)),
            true
        );

        createDefaultMockTaskManager();
        final List<Map<String, List<TopicPartitionInfo>>> topicPartitionInfo = getTopicPartitionInfo(
            4,
            asList(
                Set.of(
                    APPLICATION_ID + "-topic3-STATE-STORE-0000000002-changelog",
                    APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-changelog"
                ),
                Set.of(
                    APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition",
                    APPLICATION_ID + "-KSTREAM-MAP-0000000001-repartition"
                )
            )
        );

        final MockInternalTopicManager mockInternalTopicManager = configurePartitionAssignorWith(emptyMap(), topicPartitionInfo, parameterizedConfig);

        subscriptions.put(client,
                          new Subscription(
                              asList("topic1", "topic3"),
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_4))
        );

        final Map<String, Assignment> assignment =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        final Map<String, Integer> expectedCreatedInternalTopics = new HashMap<>();
        expectedCreatedInternalTopics.put(APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 4);
        expectedCreatedInternalTopics.put(APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-changelog", 4);
        expectedCreatedInternalTopics.put(APPLICATION_ID + "-topic3-STATE-STORE-0000000002-changelog", 4);
        expectedCreatedInternalTopics.put(APPLICATION_ID + "-KSTREAM-MAP-0000000001-repartition", 4);

        // check if all internal topics were created as expected
        assertThat(mockInternalTopicManager.readyTopics, equalTo(expectedCreatedInternalTopics));

        final List<TopicPartition> expectedAssignment = asList(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic1", 2),
            new TopicPartition("topic3", 0),
            new TopicPartition("topic3", 1),
            new TopicPartition("topic3", 2),
            new TopicPartition("topic3", 3),
            new TopicPartition(APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 0),
            new TopicPartition(APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 1),
            new TopicPartition(APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 2),
            new TopicPartition(APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 3),
            new TopicPartition(APPLICATION_ID + "-KSTREAM-MAP-0000000001-repartition", 0),
            new TopicPartition(APPLICATION_ID + "-KSTREAM-MAP-0000000001-repartition", 1),
            new TopicPartition(APPLICATION_ID + "-KSTREAM-MAP-0000000001-repartition", 2),
            new TopicPartition(APPLICATION_ID + "-KSTREAM-MAP-0000000001-repartition", 3)
        );

        // check if we created a task for all expected topicPartitions.
        assertThat(new HashSet<>(assignment.get(client).partitions()), equalTo(new HashSet<>(expectedAssignment)));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowTimeoutExceptionWhenCreatingRepartitionTopicsTimesOut(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("topic1").repartition();

        final String client = "client1";
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());

        createDefaultMockTaskManager();
        partitionAssignor.configure(configProps(parameterizedConfig));
        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            new StreamsConfig(configProps(parameterizedConfig)),
            mockClientSupplier.restoreConsumer,
            false
        ) {
            @Override
            public Set<String> makeReady(final Map<String, InternalTopicConfig> topics) {
                throw new TimeoutException("KABOOM!");
            }
        };
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        subscriptions.put(client,
                          new Subscription(
                              singletonList("topic1"),
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          )
        );
        assertThrows(TimeoutException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowTimeoutExceptionWhenCreatingChangelogTopicsTimesOut(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final StreamsConfig config = new StreamsConfig(configProps(parameterizedConfig));
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table("topic1", Materialized.as("store"));

        final String client = "client1";
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());
        topologyMetadata = new TopologyMetadata(builder, config);

        createDefaultMockTaskManager();
        partitionAssignor.configure(configProps(parameterizedConfig));
        final MockInternalTopicManager mockInternalTopicManager =  new MockInternalTopicManager(
            time,
            config,
            mockClientSupplier.restoreConsumer,
            false
        ) {
            @Override
            public Set<String> makeReady(final Map<String, InternalTopicConfig> topics) {
                if (topics.isEmpty()) {
                    return emptySet();
                }
                throw new TimeoutException("KABOOM!");
            }
        };
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        subscriptions.put(client,
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_2)
            )
        );

        assertThrows(TimeoutException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAddUserDefinedEndPointToSubscription(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source", null, null, null, "input");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT), parameterizedConfig);

        final Set<String> topics = Set.of("input");
        final ByteBuffer userData = partitionAssignor.subscriptionUserData(topics);
        final Subscription subscription =
            new Subscription(new ArrayList<>(topics), userData);
        final SubscriptionInfo subscriptionInfo = SubscriptionInfo.decode(subscription.userData());
        assertEquals("localhost:8080", subscriptionInfo.userEndPoint());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldMapUserEndPointToTopicPartitions(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source", null, null, null, "topic1");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        final List<String> topics = Collections.singletonList("topic1");

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT), parameterizedConfig);

        subscriptions.put("consumer1",
                          new Subscription(
                              topics,
                              getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          )
        );
        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        final Assignment consumerAssignment = assignments.get("consumer1");
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumerAssignment.userData());
        final Set<TopicPartition> topicPartitions = assignmentInfo.partitionsByHost().get(new HostInfo("localhost", 8080));
        assertEquals(
            Set.of(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic1", 2)),
            topicPartitions);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowExceptionIfApplicationServerConfigIsNotHostPortPair(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        createDefaultMockTaskManager();
        try {
            configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost"), parameterizedConfig);
            fail("expected to an exception due to invalid config");
        } catch (final ConfigException e) {
            // pass
        }
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowExceptionIfApplicationServerConfigPortIsNotAnInteger(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        createDefaultMockTaskManager();
        assertThrows(ConfigException.class, () -> configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:j87yhk"), parameterizedConfig));
    }

    @SuppressWarnings("deprecation")
    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotLoopInfinitelyOnMissingMetadataAndShouldNotCreateRelatedTasks(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<Object, Object> stream1 = streamsBuilder

            // Task 1 (should get created):
            .stream("topic1")
            // force repartitioning for aggregation
            .selectKey((key, value) -> null)
            .groupByKey()

            // Task 2 (should get created):
            // create repartitioning and changelog topic as task 1 exists
            .count(Materialized.as("count"))

            // force repartitioning for join, but second join input topic unknown
            // -> internal repartitioning topic should not get created
            .toStream()
            .map((KeyValueMapper<Object, Long, KeyValue<Object, Object>>) (key, value) -> null);

        streamsBuilder
            // Task 3 (should not get created because input topic unknown)
            .stream("unknownTopic")

            // force repartitioning for join, but input topic unknown
            // -> thus should not create internal repartitioning topic
            .selectKey((key, value) -> null)

            // Task 4 (should not get created because input topics unknown)
            // should not create any of both input repartition topics or any of both changelog topics
            .join(
                stream1,
                (ValueJoiner<Object, Object, Void>) (value1, value2) -> null,
                JoinWindows.of(ofMillis(0))
            );

        final String client = "client1";

        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());

        final MockInternalTopicManager mockInternalTopicManager = configureDefault(parameterizedConfig);

        subscriptions.put(client,
                          new Subscription(
                              Collections.singletonList("unknownTopic"),
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2)
                          )
        );
        final Map<String, Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(mockInternalTopicManager.readyTopics.isEmpty(), equalTo(true));

        assertThat(assignment.get(client).partitions().isEmpty(), equalTo(true));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldUpdateClusterMetadataAndHostInfoOnAssignment(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final Map<HostInfo, Set<TopicPartition>> initialHostState = mkMap(
            mkEntry(new HostInfo("localhost", 9090), Set.of(t1p0, t1p1)),
            mkEntry(new HostInfo("otherhost", 9090), Set.of(t2p0, t2p1))
        );

        final Map<HostInfo, Set<TopicPartition>> newHostState = mkMap(
            mkEntry(new HostInfo("localhost", 9090), Set.of(t1p0, t1p1)),
            mkEntry(new HostInfo("newotherhost", 9090), Set.of(t2p0, t2p1))
        );

        streamsMetadataState = mock(StreamsMetadataState.class);

        createDefaultMockTaskManager();
        configureDefaultPartitionAssignor(parameterizedConfig);

        partitionAssignor.onAssignment(createAssignment(initialHostState), null);
        partitionAssignor.onAssignment(createAssignment(newHostState), null);

        verify(streamsMetadataState).onChange(eq(initialHostState), any(), any());
        verify(streamsMetadataState).onChange(eq(newHostState), any(), any());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldTriggerImmediateRebalanceOnHostInfoChange(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final Map<HostInfo, Set<TopicPartition>> oldHostState = mkMap(
            mkEntry(new HostInfo("localhost", 9090), Set.of(t1p0, t1p1)),
            mkEntry(new HostInfo("otherhost", 9090), Set.of(t2p0, t2p1))
        );

        final Map<HostInfo, Set<TopicPartition>> newHostState = mkMap(
            mkEntry(new HostInfo("newhost", 9090), Set.of(t1p0, t1p1)),
            mkEntry(new HostInfo("otherhost", 9090), Set.of(t2p0, t2p1))
        );

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "newhost:9090"), parameterizedConfig);

        partitionAssignor.onAssignment(createAssignment(oldHostState), null);

        assertThat(referenceContainer.nextScheduledRebalanceMs.get(), is(0L));

        partitionAssignor.onAssignment(createAssignment(newHostState), null);

        assertThat(referenceContainer.nextScheduledRebalanceMs.get(), is(Long.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldTriggerImmediateRebalanceOnTasksRevoked(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);
        final List<TopicPartition> allPartitions = asList(t1p0, t1p1, t1p2);

        subscriptions.put(CONSUMER_1,
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(PID_1, allTasks, EMPTY_TASKS).encode(),
                              allPartitions,
                              DEFAULT_GENERATION,
                              Optional.of(RACK_0))
        );
        subscriptions.put(CONSUMER_2,
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(PID_1, EMPTY_TASKS, allTasks).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_0))
        );

        createMockTaskManager(allTasks, allTasks);
        configurePartitionAssignorWith(singletonMap(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 0L), parameterizedConfig);

        final Map<String, Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // Verify at least one partition was revoked
        assertThat(assignment.get(CONSUMER_1).partitions(), not(allPartitions));
        assertThat(assignment.get(CONSUMER_2).partitions(), equalTo(emptyList()));

        // Verify that stateless revoked tasks would not be assigned as standbys
        assertThat(AssignmentInfo.decode(assignment.get(CONSUMER_2).userData()).activeTasks(), equalTo(emptyList()));
        assertThat(AssignmentInfo.decode(assignment.get(CONSUMER_2).userData()).standbyTasks(), equalTo(emptyMap()));

        partitionAssignor.onAssignment(assignment.get(CONSUMER_2), null);

        assertThat(referenceContainer.nextScheduledRebalanceMs.get(), is(0L));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotAddStandbyTaskPartitionsToPartitionsForHost(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final Map<String, Object> props = configProps(parameterizedConfig);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("topic1").groupByKey().count();
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());
        topologyMetadata = new TopologyMetadata(builder, new StreamsConfig(props));

        createDefaultMockTaskManager();
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                singletonList(APPLICATION_ID + "-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog"),
                singletonList(3)),
            true
        );

        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            3, singletonList(Set.of(APPLICATION_ID + "-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog")));
        configurePartitionAssignorWith(props, changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer1",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_3))
        );
        subscriptions.put("consumer2",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(PID_2, EMPTY_TASKS, EMPTY_TASKS, OTHER_END_POINT).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_1))
        );
        final Set<TopicPartition> allPartitions = Set.of(t1p0, t1p1, t1p2);
        final Map<String, Assignment> assign = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        final Assignment consumer1Assignment = assign.get("consumer1");
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumer1Assignment.userData());

        final Set<TopicPartition> consumer1ActivePartitions = assignmentInfo.partitionsByHost().get(new HostInfo("localhost", 8080));
        final Set<TopicPartition> consumer2ActivePartitions = assignmentInfo.partitionsByHost().get(new HostInfo("other", 9090));
        final Set<TopicPartition> consumer1StandbyPartitions = assignmentInfo.standbyPartitionByHost().get(new HostInfo("localhost", 8080));
        final Set<TopicPartition> consumer2StandbyPartitions = assignmentInfo.standbyPartitionByHost().get(new HostInfo("other", 9090));
        final HashSet<TopicPartition> allAssignedPartitions = new HashSet<>(consumer1ActivePartitions);
        allAssignedPartitions.addAll(consumer2ActivePartitions);
        assertThat(consumer1ActivePartitions, not(allPartitions));
        assertThat(consumer2ActivePartitions, not(allPartitions));
        assertThat(consumer1ActivePartitions, equalTo(consumer2StandbyPartitions));
        assertThat(consumer2ActivePartitions, equalTo(consumer1StandbyPartitions));
        assertThat(allAssignedPartitions, equalTo(allPartitions));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowKafkaExceptionIfReferenceContainerNotConfigured(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final Map<String, Object> config = configProps(parameterizedConfig);
        config.remove(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR);

        final KafkaException expected = assertThrows(
            KafkaException.class,
            () -> partitionAssignor.configure(config)
        );
        assertThat(expected.getMessage(), equalTo("ReferenceContainer is not specified"));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowKafkaExceptionIfReferenceContainerConfigIsNotTaskManagerInstance(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final Map<String, Object> config = configProps(parameterizedConfig);
        config.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, "i am not a reference container");

        final KafkaException expected = assertThrows(
            KafkaException.class,
            () -> partitionAssignor.configure(config)
        );
        assertThat(
            expected.getMessage(),
            equalTo("java.lang.String is not an instance of org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer")
        );
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V2(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 2, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V3(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 3, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV2V3(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(2, 3, parameterizedConfig);
    }

    private void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(final int smallestVersion,
                                                                                     final int otherVersion,
                                                                                     final Map<String, Object> paramterizedObject) {
        subscriptions.put("consumer1",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfoForOlderVersion(smallestVersion,
                                  PID_1, EMPTY_TASKS, EMPTY_TASKS).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_2))
        );
        subscriptions.put("consumer2",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfoForOlderVersion(otherVersion, PID_2, EMPTY_TASKS, EMPTY_TASKS).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_1)
                          )
        );

        configureDefault(paramterizedObject);

        final Map<String, Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));
        assertThat(AssignmentInfo.decode(assignment.get("consumer1").userData()).version(), equalTo(smallestVersion));
        assertThat(AssignmentInfo.decode(assignment.get("consumer2").userData()).version(), equalTo(smallestVersion));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDownGradeSubscriptionToVersion1(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_0100), parameterizedConfig);

        final Set<String> topics = Set.of("topic1");
        final Subscription subscription = new Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        assertThat(SubscriptionInfo.decode(subscription.userData()).version(), equalTo(1));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDownGradeSubscriptionToVersion2For0101(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0101, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDownGradeSubscriptionToVersion2For0102(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0102, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDownGradeSubscriptionToVersion2For0110(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0110, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDownGradeSubscriptionToVersion2For10(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_10, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDownGradeSubscriptionToVersion2For11(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_11, parameterizedConfig);
    }

    private void shouldDownGradeSubscriptionToVersion2(final Object upgradeFromValue, final Map<String, Object> parameterizedConfig) {
        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFromValue), parameterizedConfig);

        final Set<String> topics = Set.of("topic1");
        final Subscription subscription = new Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        assertThat(SubscriptionInfo.decode(subscription.userData()).version(), equalTo(2));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnInterleavedAssignmentWithUnrevokedPartitionsRemovedWhenNewConsumerJoins(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);

        subscriptions.put(
            CONSUMER_1,
            new Subscription(
                Collections.singletonList("topic1"),
                getInfo(PID_1, allTasks, EMPTY_TASKS).encode(),
                asList(t1p0, t1p1, t1p2),
                DEFAULT_GENERATION,
                Optional.of(RACK_1)
            )
        );
        subscriptions.put(
            CONSUMER_2,
            new Subscription(
                Collections.singletonList("topic1"),
                getInfo(PID_2, EMPTY_TASKS, EMPTY_TASKS).encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_2)
            )
        );

        createMockTaskManager(allTasks, allTasks);
        configureDefaultPartitionAssignor(parameterizedConfig);

        final Map<String, Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));

        // The new consumer's assignment should be empty until c1 has the chance to revoke its partitions/tasks
        assertThat(assignment.get(CONSUMER_2).partitions(), equalTo(emptyList()));

        final AssignmentInfo actualAssignment = AssignmentInfo.decode(assignment.get(CONSUMER_2).userData());
        assertThat(actualAssignment.version(), is(LATEST_SUPPORTED_VERSION));
        assertThat(actualAssignment.activeTasks(), empty());
        // Note we're not asserting anything about standbys. If the assignor gave an active task to CONSUMER_2, it would
        // be converted to a standby, but we don't know whether the assignor will do that.
        assertThat(actualAssignment.partitionsByHost(), anEmptyMap());
        assertThat(actualAssignment.standbyPartitionByHost(), anEmptyMap());
        assertThat(actualAssignment.errCode(), is(0));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnInterleavedAssignmentForOnlyFutureInstancesDuringVersionProbing(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);

        subscriptions.put(CONSUMER_1,
            new Subscription(
                Collections.singletonList("topic1"),
                encodeFutureSubscription(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_1)
            )
        );
        subscriptions.put(CONSUMER_2,
            new Subscription(
                Collections.singletonList("topic1"),
                encodeFutureSubscription(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_1)
            )
        );

        createMockTaskManager(allTasks, allTasks);
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1), parameterizedConfig);

        final Map<String, Assignment> assignment =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));

        assertThat(assignment.get(CONSUMER_1).partitions(), equalTo(asList(t1p0, t1p2)));
        assertThat(
            AssignmentInfo.decode(assignment.get(CONSUMER_1).userData()),
            equalTo(new AssignmentInfo(LATEST_SUPPORTED_VERSION, asList(TASK_0_0, TASK_0_2), emptyMap(), emptyMap(), emptyMap(), 0)));


        assertThat(assignment.get(CONSUMER_2).partitions(), equalTo(Collections.singletonList(t1p1)));
        assertThat(
            AssignmentInfo.decode(assignment.get(CONSUMER_2).userData()),
            equalTo(new AssignmentInfo(LATEST_SUPPORTED_VERSION, Collections.singletonList(TASK_0_1), emptyMap(), emptyMap(), emptyMap(), 0)));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldEncodeAssignmentErrorIfV1SubscriptionAndFutureSubscriptionIsMixed(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        shouldEncodeAssignmentErrorIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(1, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldEncodeAssignmentErrorIfV2SubscriptionAndFutureSubscriptionIsMixed(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        shouldEncodeAssignmentErrorIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(2, parameterizedConfig);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotFailOnBranchedMultiLevelRepartitionConnectedTopology(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        // Test out a topology with 3 level of sub-topology as:
        //            0
        //          /   \
        //         1    3
        //          \  /
        //           2
        //  where each pair of the sub topology is connected by repartition topic.
        //  The purpose of this test is to verify the robustness of the stream partition assignor algorithm,
        //  especially whether it could build the repartition topic counts (step zero) with a complex topology.
        //  The traversal path 0 -> 1 -> 2 -> 3 hits the case where sub-topology 2 will be initialized while its
        //  parent 3 hasn't been initialized yet.
        builder.addSource(null, "KSTREAM-SOURCE-0000000000",  null, null, null, "input-stream");
        builder.addProcessor("KSTREAM-FLATMAPVALUES-0000000001", new MockApiProcessorSupplier<>(), "KSTREAM-SOURCE-0000000000");
        builder.addProcessor("KSTREAM-BRANCH-0000000002", new MockApiProcessorSupplier<>(), "KSTREAM-FLATMAPVALUES-0000000001");
        builder.addProcessor("KSTREAM-BRANCHCHILD-0000000003", new MockApiProcessorSupplier<>(), "KSTREAM-BRANCH-0000000002");
        builder.addProcessor("KSTREAM-BRANCHCHILD-0000000004", new MockApiProcessorSupplier<>(), "KSTREAM-BRANCH-0000000002");
        builder.addProcessor("KSTREAM-MAP-0000000005", new MockApiProcessorSupplier<>(), "KSTREAM-BRANCHCHILD-0000000003");

        builder.addInternalTopic("odd_store-repartition", InternalTopicProperties.empty());
        builder.addProcessor("odd_store-repartition-filter", new MockApiProcessorSupplier<>(), "KSTREAM-MAP-0000000005");
        builder.addSink("odd_store-repartition-sink", "odd_store-repartition", null, null, null, "odd_store-repartition-filter");
        builder.addSource(null, "odd_store-repartition-source", null, null, null, "odd_store-repartition");
        builder.addProcessor("KSTREAM-REDUCE-0000000006", new MockApiProcessorSupplier<>(), "odd_store-repartition-source");
        builder.addProcessor("KTABLE-TOSTREAM-0000000010", new MockApiProcessorSupplier<>(), "KSTREAM-REDUCE-0000000006");
        builder.addProcessor("KSTREAM-PEEK-0000000011", new MockApiProcessorSupplier<>(), "KTABLE-TOSTREAM-0000000010");
        builder.addProcessor("KSTREAM-MAP-0000000012", new MockApiProcessorSupplier<>(), "KSTREAM-PEEK-0000000011");

        builder.addInternalTopic("odd_store_2-repartition", InternalTopicProperties.empty());
        builder.addProcessor("odd_store_2-repartition-filter", new MockApiProcessorSupplier<>(), "KSTREAM-MAP-0000000012");
        builder.addSink("odd_store_2-repartition-sink", "odd_store_2-repartition", null, null, null, "odd_store_2-repartition-filter");
        builder.addSource(null, "odd_store_2-repartition-source", null, null, null, "odd_store_2-repartition");
        builder.addProcessor("KSTREAM-REDUCE-0000000013", new MockApiProcessorSupplier<>(), "odd_store_2-repartition-source");
        builder.addProcessor("KSTREAM-MAP-0000000017", new MockApiProcessorSupplier<>(), "KSTREAM-BRANCHCHILD-0000000004");

        builder.addInternalTopic("even_store-repartition", InternalTopicProperties.empty());
        builder.addProcessor("even_store-repartition-filter", new MockApiProcessorSupplier<>(), "KSTREAM-MAP-0000000017");
        builder.addSink("even_store-repartition-sink", "even_store-repartition", null, null, null, "even_store-repartition-filter");
        builder.addSource(null, "even_store-repartition-source", null, null, null, "even_store-repartition");
        builder.addProcessor("KSTREAM-REDUCE-0000000018", new MockApiProcessorSupplier<>(), "even_store-repartition-source");
        builder.addProcessor("KTABLE-TOSTREAM-0000000022", new MockApiProcessorSupplier<>(), "KSTREAM-REDUCE-0000000018");
        builder.addProcessor("KSTREAM-PEEK-0000000023", new MockApiProcessorSupplier<>(), "KTABLE-TOSTREAM-0000000022");
        builder.addProcessor("KSTREAM-MAP-0000000024", new MockApiProcessorSupplier<>(), "KSTREAM-PEEK-0000000023");

        builder.addInternalTopic("even_store_2-repartition", InternalTopicProperties.empty());
        builder.addProcessor("even_store_2-repartition-filter", new MockApiProcessorSupplier<>(), "KSTREAM-MAP-0000000024");
        builder.addSink("even_store_2-repartition-sink", "even_store_2-repartition", null, null, null, "even_store_2-repartition-filter");
        builder.addSource(null, "even_store_2-repartition-source", null, null, null, "even_store_2-repartition");
        builder.addProcessor("KSTREAM-REDUCE-0000000025", new MockApiProcessorSupplier<>(), "even_store_2-repartition-source");
        builder.addProcessor("KTABLE-JOINTHIS-0000000030", new MockApiProcessorSupplier<>(), "KSTREAM-REDUCE-0000000013");
        builder.addProcessor("KTABLE-JOINOTHER-0000000031", new MockApiProcessorSupplier<>(), "KSTREAM-REDUCE-0000000025");
        builder.addProcessor("KTABLE-MERGE-0000000029", new MockApiProcessorSupplier<>(), "KTABLE-JOINTHIS-0000000030", "KTABLE-JOINOTHER-0000000031");
        builder.addProcessor("KTABLE-TOSTREAM-0000000032", new MockApiProcessorSupplier<>(), "KTABLE-MERGE-0000000029");

        final List<String> topics = asList("input-stream", "test-even_store-repartition", "test-even_store_2-repartition", "test-odd_store-repartition", "test-odd_store_2-repartition");

        createDefaultMockTaskManager();
        final List<Map<String, List<TopicPartitionInfo>>> repartitionTopics = getTopicPartitionInfo(
            4,
            asList(
                Set.of(APPLICATION_ID + "-odd_store-repartition"),
                Set.of(
                    APPLICATION_ID + "-odd_store-repartition",
                    APPLICATION_ID + "-odd_store_2-repartition",
                    APPLICATION_ID + "-even_store-repartition",
                    APPLICATION_ID + "-even_store_2-repartition"
                )
            )
        );
        configurePartitionAssignorWith(emptyMap(), repartitionTopics, parameterizedConfig);

        subscriptions.put("consumer10",
            new Subscription(
                topics,
                defaultSubscriptionInfo.encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_0))
        );

        final Cluster metadata = new Cluster(
            "cluster",
            asList(NODE_0, NODE_1, NODE_3),
            Collections.singletonList(new PartitionInfo("input-stream", 0, NODE_0, REPLICA_0, REPLICA_0)),
            emptySet(),
            emptySet());

        // This shall fail if we have bugs in the repartition topic creation due to the inconsistent order of sub-topologies.
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldGetAssignmentConfigs(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        createDefaultMockTaskManager();

        final Map<String, Object> props = configProps(parameterizedConfig);
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 11);
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 33);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 44);
        props.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 55 * 60 * 1000L);

        partitionAssignor.configure(props);

        assertThat(partitionAssignor.acceptableRecoveryLag(), equalTo(11L));
        assertThat(partitionAssignor.maxWarmupReplicas(), equalTo(33));
        assertThat(partitionAssignor.numStandbyReplicas(), equalTo(44));
        assertThat(partitionAssignor.probingRebalanceIntervalMs(), equalTo(55 * 60 * 1000L));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldGetTime(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        time.setCurrentTimeMs(Long.MAX_VALUE);

        createDefaultMockTaskManager();
        final Map<String, Object> props = configProps(parameterizedConfig);
        final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(props);

        assertThat(assignorConfiguration.referenceContainer().time.milliseconds(), equalTo(Long.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowIllegalStateExceptionIfAnyPartitionsMissingFromChangelogEndOffsets(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final int changelogNumPartitions = 3;
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");

        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                singletonList(APPLICATION_ID + "-store1-changelog"),
                singletonList(changelogNumPartitions - 1)),
            true
        );

        createDefaultMockTaskManager();
        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            changelogNumPartitions - 1,
            singletonList(Set.of(APPLICATION_ID + "-store1-changelog")));
        configurePartitionAssignorWith(emptyMap(), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_1)
            ));
        assertThrows(IllegalStateException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowIllegalStateExceptionIfAnyTopicsMissingFromChangelogEndOffsets(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store2", false), "processor1");

        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
                singletonList(APPLICATION_ID + "-store1-changelog"),
                singletonList(3)),
            true
        );

        createDefaultMockTaskManager();
        final List<Map<String, List<TopicPartitionInfo>>> changelogTopicPartitionInfo = getTopicPartitionInfo(
            3,
            singletonList(Set.of(APPLICATION_ID + "-store1-changelog")));
        configurePartitionAssignorWith(emptyMap(), changelogTopicPartitionInfo, parameterizedConfig);

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_3)
            ));
        assertThrows(IllegalStateException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldSkipListOffsetsRequestForNewlyCreatedChangelogTopics(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        adminClient = mock(AdminClient.class);
        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        final KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = new KafkaFutureImpl<>();
        allFuture.complete(emptyMap());

        when(adminClient.listOffsets(emptyMap())).thenReturn(result);

        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");

        subscriptions.put("consumer10",
                          new Subscription(
                              singletonList("topic1"),
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_4)
                          ));

        configureDefault(parameterizedConfig);
        final List<Map<String, List<TopicPartitionInfo>>> partitionInfo = singletonList(mkMap(mkEntry(
                "stream-partition-assignor-test-store-changelog",
                singletonList(
                    new TopicPartitionInfo(
                        0,
                        new Node(1, "h1", 80),
                        singletonList(new Node(1, "h1", 80)),
                        emptyList()
                    )
                )
            )
        ));
        overwriteInternalTopicManagerWithMock(true, partitionInfo, parameterizedConfig);

        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldRequestEndOffsetsForPreexistingChangelogs(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final Set<TopicPartition> changelogs = Set.of(
            new TopicPartition(APPLICATION_ID + "-store-changelog", 0),
            new TopicPartition(APPLICATION_ID + "-store-changelog", 1),
            new TopicPartition(APPLICATION_ID + "-store-changelog", 2)
        );
        adminClient = mock(AdminClient.class);
        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        for (final TopicPartition entry : changelogs) {
            final KafkaFutureImpl<ListOffsetsResultInfo> partitionFuture = new KafkaFutureImpl<>();
            final ListOffsetsResultInfo info = mock(ListOffsetsResultInfo.class);
            when(info.offset()).thenReturn(Long.MAX_VALUE);
            partitionFuture.complete(info);
            when(result.partitionResult(entry)).thenReturn(partitionFuture);
        }

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<TopicPartition, OffsetSpec>> capturedChangelogs = ArgumentCaptor.forClass(Map.class);

        when(adminClient.listOffsets(capturedChangelogs.capture())).thenReturn(result);

        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store", false), "processor1");

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_3)
            ));

        configureDefault(parameterizedConfig);
        final List<Map<String, List<TopicPartitionInfo>>> partitionInfo = singletonList(mkMap(mkEntry(
                "stream-partition-assignor-test-store-changelog",
                singletonList(
                    new TopicPartitionInfo(
                        0,
                        new Node(1, "h1", 80),
                        singletonList(new Node(1, "h1", 80)),
                        emptyList()
                    )
                )
            )
        ));
        overwriteInternalTopicManagerWithMock(false, partitionInfo, parameterizedConfig);

        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        assertThat(
            capturedChangelogs.getValue().keySet(),
            equalTo(changelogs)
        );
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldRequestCommittedOffsetsForPreexistingSourceChangelogs(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, true);
        final Set<TopicPartition> changelogs = Set.of(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic1", 2)
        );

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table("topic1", Materialized.as("store"));

        final Properties props = new Properties();
        props.putAll(configProps(parameterizedConfig));
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build(props));
        topologyMetadata = new TopologyMetadata(builder, new StreamsConfig(props));

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode(),
                emptyList(),
                DEFAULT_GENERATION,
                Optional.of(RACK_3)
            ));

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(singletonMap(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE), parameterizedConfig);
        overwriteInternalTopicManagerWithMock(false, parameterizedConfig);

        final Consumer<byte[], byte[]> consumerClient = referenceContainer.mainConsumer;
        when(consumerClient.committed(changelogs))
            .thenReturn(changelogs.stream().collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndMetadata(Long.MAX_VALUE))));

        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldEncodeMissingSourceTopicError(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        final Cluster emptyClusterMetadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            emptyList(),
            emptySet(),
            emptySet()
        );

        builder.addSource(null, "source1", null, null, null, "topic1");
        configureDefault(parameterizedConfig);

        subscriptions.put("consumer",
                          new Subscription(
                              singletonList("topic"),
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_0)
                          ));
        final Map<String, Assignment> assignments = partitionAssignor.assign(emptyClusterMetadata, new GroupSubscription(subscriptions)).groupAssignment();
        assertThat(AssignmentInfo.decode(assignments.get("consumer").userData()).errCode(),
                   equalTo(AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code()));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testUniqueField(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        createDefaultMockTaskManager();
        configureDefaultPartitionAssignor(parameterizedConfig);
        final Set<String> topics = Set.of("input");

        assertEquals(0, partitionAssignor.uniqueField());
        partitionAssignor.subscriptionUserData(topics);
        assertEquals(1, partitionAssignor.uniqueField());
        partitionAssignor.subscriptionUserData(topics);
        assertEquals(2, partitionAssignor.uniqueField());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testUniqueFieldOverflow(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        createDefaultMockTaskManager();
        configureDefaultPartitionAssignor(parameterizedConfig);
        final Set<String> topics = Set.of("input");

        for (int i = 0; i < 127; i++) {
            partitionAssignor.subscriptionUserData(topics);
        }
        assertEquals(127, partitionAssignor.uniqueField());
        partitionAssignor.subscriptionUserData(topics);
        assertEquals(-128, partitionAssignor.uniqueField());
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldThrowTaskAssignmentExceptionWhenUnableToResolvePartitionCount(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        builder = new CorruptedInternalTopologyBuilder();
        topologyMetadata = new TopologyMetadata(builder, new StreamsConfig(configProps(parameterizedConfig)));

        final InternalStreamsBuilder streamsBuilder = new InternalStreamsBuilder(builder);

        final KStream<String, String> inputTopic = streamsBuilder.stream(singleton("topic1"), new ConsumedInternal<>(Consumed.with(null, null)));
        final KTable<String, String> inputTable = streamsBuilder.table("topic2", new ConsumedInternal<>(Consumed.with(null, null)), new MaterializedInternal<>(Materialized.as("store")));
        inputTopic
            .groupBy(
                (k, v) -> k,
                Grouped.with("GroupName", Serdes.String(), Serdes.String())
            )
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10)))
            .aggregate(
                () -> "",
                (k, v, a) -> a + k)
            .leftJoin(
                inputTable,
                v -> v,
                (x, y) -> x + y
            );
        streamsBuilder.buildAndOptimizeTopology();

        configureDefault(parameterizedConfig);

        subscriptions.put("consumer",
                          new Subscription(
                              singletonList("topic"),
                              defaultSubscriptionInfo.encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_1)
                          ));
        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        assertThat(AssignmentInfo.decode(assignments.get("consumer").userData()).errCode(),
                   equalTo(AssignorError.ASSIGNMENT_ERROR.code()));
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void testClientTags(final Map<String, Object> parameterizedConfig) {
        setUp(parameterizedConfig, false);
        clientTags = mkMap(mkEntry("cluster", "cluster1"), mkEntry("zone", "az1"));
        createDefaultMockTaskManager();
        configureDefaultPartitionAssignor(parameterizedConfig);
        final Set<String> topics = Set.of("input");
        final Subscription subscription = new Subscription(new ArrayList<>(topics),
                                                           partitionAssignor.subscriptionUserData(topics));
        final SubscriptionInfo info = getInfo(PID_1, EMPTY_TASKS, EMPTY_TASKS, uniqueField, clientTags);

        assertEquals(singletonList("input"), subscription.topics());
        assertEquals(info, SubscriptionInfo.decode(subscription.userData()));
        assertEquals(clientTags, partitionAssignor.clientTags());
    }

    private static class CorruptedInternalTopologyBuilder extends InternalTopologyBuilder {
        private Map<Subtopology, TopicsInfo> corruptedTopicGroups;

        @Override
        public synchronized Map<Subtopology, TopicsInfo> subtopologyToTopicsInfo() {
            if (corruptedTopicGroups == null) {
                corruptedTopicGroups = new HashMap<>();
                for (final Map.Entry<Subtopology, TopicsInfo> topicGroupEntry : super.subtopologyToTopicsInfo().entrySet()) {
                    final TopicsInfo originalInfo = topicGroupEntry.getValue();
                    corruptedTopicGroups.put(
                        topicGroupEntry.getKey(),
                        new TopicsInfo(
                            emptySet(),
                            originalInfo.sourceTopics,
                            originalInfo.repartitionSourceTopics,
                            originalInfo.stateChangelogTopics
                        ));
                }
            }

            return corruptedTopicGroups;
        }
    }

    private static ByteBuffer encodeFutureSubscription() {
        final ByteBuffer buf = ByteBuffer.allocate(4 /* used version */ + 4 /* supported version */);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        return buf;
    }

    private void shouldEncodeAssignmentErrorIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(final int oldVersion, final Map<String, Object> parameterizedConfig) {
        subscriptions.put("consumer1",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfoForOlderVersion(oldVersion, PID_1, EMPTY_TASKS, EMPTY_TASKS).encode(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_0))
        );
        subscriptions.put("future-consumer",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription(),
                              emptyList(),
                              DEFAULT_GENERATION,
                              Optional.of(RACK_1))
        );
        configureDefault(parameterizedConfig);

        final Map<String, Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(AssignmentInfo.decode(assignment.get("consumer1").userData()).errCode(), equalTo(AssignorError.ASSIGNMENT_ERROR.code()));
        assertThat(AssignmentInfo.decode(assignment.get("future-consumer").userData()).errCode(), equalTo(AssignorError.ASSIGNMENT_ERROR.code()));
    }

    private static Assignment createAssignment(final Map<HostInfo, Set<TopicPartition>> firstHostState) {
        final AssignmentInfo info = new AssignmentInfo(LATEST_SUPPORTED_VERSION, emptyList(), emptyMap(), firstHostState, emptyMap(), 0);
        return new Assignment(emptyList(), info.encode());
    }

    private static AssignmentInfo checkAssignment(final Set<String> expectedTopics,
                                                  final Assignment assignment) {

        // This assumed 1) DefaultPartitionGrouper is used, and 2) there is an only one topic group.

        final AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

        // check if the number of assigned partitions == the size of active task id list
        assertEquals(assignment.partitions().size(), info.activeTasks().size());

        // check if active tasks are consistent
        final List<TaskId> activeTasks = new ArrayList<>();
        final Set<String> activeTopics = new HashSet<>();
        for (final TopicPartition partition : assignment.partitions()) {
            // since default grouper, taskid.partition == partition.partition()
            activeTasks.add(new TaskId(0, partition.partition()));
            activeTopics.add(partition.topic());
        }
        assertEquals(activeTasks, info.activeTasks());

        // check if active partitions cover all topics
        assertEquals(expectedTopics, activeTopics);

        // check if standby tasks are consistent
        final Set<String> standbyTopics = new HashSet<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : info.standbyTasks().entrySet()) {
            final TaskId id = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();
            for (final TopicPartition partition : partitions) {
                // since default grouper, taskid.partition == partition.partition()
                assertEquals(id.partition(), partition.partition());

                standbyTopics.add(partition.topic());
            }
        }

        if (!info.standbyTasks().isEmpty()) {
            // check if standby partitions cover all topics
            assertEquals(expectedTopics, standbyTopics);
        }

        return info;
    }

    private static void assertEquivalentAssignment(final Map<String, List<TaskId>> thisAssignment,
                                                   final Map<String, List<TaskId>> otherAssignment) {
        assertEquals(thisAssignment.size(), otherAssignment.size());
        for (final Map.Entry<String, List<TaskId>> entry : thisAssignment.entrySet()) {
            final String consumer = entry.getKey();
            assertTrue(otherAssignment.containsKey(consumer));

            final List<TaskId> thisTaskList = entry.getValue();
            Collections.sort(thisTaskList);
            final List<TaskId> otherTaskList = otherAssignment.get(consumer);
            Collections.sort(otherTaskList);

            assertThat(thisTaskList, equalTo(otherTaskList));
        }
    }

    /**
     * Helper for building the input to createMockAdminClient in cases where we don't care about the actual offsets
     * @param changelogTopics The names of all changelog topics in the topology
     * @param topicsNumPartitions The number of partitions for the corresponding changelog topic, such that the number
     *            of partitions of the ith topic in changelogTopics is given by the ith element of topicsNumPartitions
     */
    private static Map<TopicPartition, Long> getTopicPartitionOffsetsMap(final List<String> changelogTopics,
                                                                         final List<Integer> topicsNumPartitions) {
        if (changelogTopics.size() != topicsNumPartitions.size()) {
            throw new IllegalStateException("Passed in " + changelogTopics.size() + " changelog topic names, but " +
                                               topicsNumPartitions.size() + " different numPartitions for the topics");
        }
        final Map<TopicPartition, Long> changelogEndOffsets = new HashMap<>();
        for (int i = 0; i < changelogTopics.size(); ++i) {
            final String topic = changelogTopics.get(i);
            final int numPartitions = topicsNumPartitions.get(i);
            for (int partition = 0; partition < numPartitions; ++partition) {
                changelogEndOffsets.put(new TopicPartition(topic, partition), Long.MAX_VALUE);
            }
        }
        return changelogEndOffsets;
    }

    private static Map<String, TopicDescription> getTopicDescriptionMap(final List<String> changelogTopics,
                                                                        final List<List<TopicPartitionInfo>> topicPartitionInfos) {
        if (changelogTopics.size() != topicPartitionInfos.size()) {
            throw new IllegalStateException("Passed in " + changelogTopics.size() + " changelog topic names, but " +
                topicPartitionInfos.size() + " different topicPartitionInfo for the topics");
        }
        final Map<String, TopicDescription> changeLogTopicDescriptions = new HashMap<>();
        for (int i = 0; i < changelogTopics.size(); i++) {
            final String topic = changelogTopics.get(i);
            final List<TopicPartitionInfo> topicPartitionInfo = topicPartitionInfos.get(i);
            changeLogTopicDescriptions.put(topic, new TopicDescription(topic, false, topicPartitionInfo));
        }

        return changeLogTopicDescriptions;
    }


    private static SubscriptionInfo getInfoForOlderVersion(final int version,
                                                           final ProcessId processId,
                                                           final Set<TaskId> prevTasks,
                                                           final Set<TaskId> standbyTasks) {
        return new SubscriptionInfo(
            version, LATEST_SUPPORTED_VERSION, processId, null, getTaskOffsetSums(prevTasks, standbyTasks), (byte) 0, 0, EMPTY_CLIENT_TAGS);
    }

    // Stub offset sums for when we only care about the prev/standby task sets, not the actual offsets
    private static Map<TaskId, Long> getTaskOffsetSums(final Collection<TaskId> activeTasks, final Collection<TaskId> standbyTasks) {
        final Map<TaskId, Long> taskOffsetSums = activeTasks.stream().collect(Collectors.toMap(t -> t, t -> Task.LATEST_OFFSET));
        taskOffsetSums.putAll(standbyTasks.stream().collect(Collectors.toMap(t -> t, t -> 0L)));
        return taskOffsetSums;
    }

    // Stub end offsets sums for situations where we don't really care about computing exact lags
    private static Map<TaskId, Long> getTaskEndOffsetSums(final Collection<TaskId> allStatefulTasks) {
        return allStatefulTasks.stream().collect(Collectors.toMap(t -> t, t -> Long.MAX_VALUE));
    }

    private static List<Map<String, List<TopicPartitionInfo>>> getTopicPartitionInfo(final int replicaCount, final List<Set<String>> topicsList) {
        final List<KeyValue<Node, List<Node>>> nodes = asList(
            KeyValue.pair(NODE_0, asList(REPLICA_0)),
            KeyValue.pair(NODE_1, asList(REPLICA_1)),
            KeyValue.pair(NODE_2, asList(REPLICA_2)),
            KeyValue.pair(NODE_3, asList(REPLICA_3))
        );

        final List<Map<String, List<TopicPartitionInfo>>> topicPartitionInfo = new ArrayList<>();
        for (final Set<String> topics : topicsList) {
            final Map<String, List<TopicPartitionInfo>> topicInfoMap = new HashMap<>();
            topicPartitionInfo.add(topicInfoMap);
            for (final String topic : topics) {
                final List<TopicPartitionInfo> topicPartitionInfoList = new ArrayList<>();
                topicInfoMap.put(topic, topicPartitionInfoList);
                for (int i = 0; i < replicaCount; i++) {
                    topicPartitionInfoList.add(new TopicPartitionInfo(i, nodes.get(i).key, nodes.get(i).value, nodes.get(i).value));
                }
            }
        }

        return topicPartitionInfo;
    }

}
