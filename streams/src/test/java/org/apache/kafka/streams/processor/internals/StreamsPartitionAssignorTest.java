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

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
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
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.assignTasksToThreads;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CHANGELOG_END_OFFSETS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
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
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.createMockAdminClientForAssignor;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getInfo;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class StreamsPartitionAssignorTest {
    private static final String CONSUMER_1 = "consumer1";
    private static final String CONSUMER_2 = "consumer2";
    private static final String CONSUMER_3 = "consumer3";
    private static final String CONSUMER_4 = "consumer4";

    private final Set<String> allTopics = mkSet("topic1", "topic2");

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
        new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 3, Node.noNode(), new Node[0], new Node[0])
    );

    private final SubscriptionInfo defaultSubscriptionInfo = getInfo(UUID_1, EMPTY_TASKS, EMPTY_TASKS);

    private final Cluster metadata = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
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
    private StreamsMetadataState streamsMetadataState = EasyMock.createNiceMock(StreamsMetadataState.class);
    private final Map<String, Subscription> subscriptions = new HashMap<>();
    private final Class<? extends TaskAssignor> taskAssignor;

    private final ReferenceContainer referenceContainer = new ReferenceContainer();
    private final MockTime time = new MockTime();
    private final byte uniqueField = 1;

    private Map<String, Object> configProps() {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);
        referenceContainer.mainConsumer = mock(Consumer.class);
        referenceContainer.adminClient = adminClient != null ? adminClient : mock(Admin.class);
        referenceContainer.taskManager = taskManager;
        referenceContainer.streamsMetadataState = streamsMetadataState;
        referenceContainer.time = time;
        configurationMap.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        configurationMap.put(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, taskAssignor.getName());
        return configurationMap;
    }

    private MockInternalTopicManager configureDefault() {
        createDefaultMockTaskManager();
        return configureDefaultPartitionAssignor();
    }

    // Make sure to complete setting up any mocks (such as TaskManager or AdminClient) before configuring the assignor
    private MockInternalTopicManager configureDefaultPartitionAssignor() {
        return configurePartitionAssignorWith(emptyMap());
    }

    // Make sure to complete setting up any mocks (such as TaskManager or AdminClient) before configuring the assignor
    private MockInternalTopicManager configurePartitionAssignorWith(final Map<String, Object> props) {
        final Map<String, Object> configMap = configProps();
        configMap.putAll(props);

        partitionAssignor.configure(configMap);
        EasyMock.replay(taskManager, adminClient);

        return overwriteInternalTopicManagerWithMock(false);
    }

    private void createDefaultMockTaskManager() {
        createMockTaskManager(EMPTY_TASKS, EMPTY_TASKS);
    }

    private void createMockTaskManager(final Set<TaskId> activeTasks,
                                       final Set<TaskId> standbyTasks) {
        taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.builder()).andStubReturn(builder);
        expect(taskManager.getTaskOffsetSums()).andStubReturn(getTaskOffsetSums(activeTasks, standbyTasks));
        expect(taskManager.processId()).andStubReturn(UUID_1);
        builder.setApplicationId(APPLICATION_ID);
        builder.buildTopology();
    }

    // If mockCreateInternalTopics is true the internal topic manager will report that it had to create all internal
    // topics and we will skip the listOffsets request for these changelogs
    private MockInternalTopicManager overwriteInternalTopicManagerWithMock(final boolean mockCreateInternalTopics) {
        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            new StreamsConfig(configProps()),
            mockClientSupplier.restoreConsumer,
            mockCreateInternalTopics
        );
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);
        return mockInternalTopicManager;
    }

    @Parameterized.Parameters(name = "task assignor = {0}")
    public static Collection<Object[]> parameters() {
        return asList(
            new Object[]{HighAvailabilityTaskAssignor.class},
            new Object[]{StickyTaskAssignor.class},
            new Object[]{FallbackPriorTaskAssignor.class}
            );
    }

    public StreamsPartitionAssignorTest(final Class<? extends TaskAssignor> taskAssignor) {
        this.taskAssignor = taskAssignor;
        adminClient = createMockAdminClientForAssignor(EMPTY_CHANGELOG_END_OFFSETS);
    }

    @Test
    public void shouldUseEagerRebalancingProtocol() {
        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_23));

        assertEquals(1, partitionAssignor.supportedProtocols().size());
        assertTrue(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.EAGER));
        assertFalse(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.COOPERATIVE));
    }

    @Test
    public void shouldUseCooperativeRebalancingProtocol() {
        configureDefault();

        assertEquals(2, partitionAssignor.supportedProtocols().size());
        assertTrue(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.COOPERATIVE));
    }

    @Test
    public void shouldProduceStickyAndBalancedAssignmentWhenNothingChanges() {
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
        state.initializePrevTasks(emptyMap());
        state.computeTaskLags(UUID_1, getTaskEndOffsetSums(allTasks));

        assertEquivalentAssignment(
            previousAssignment,
            assignTasksToThreads(
                allTasks,
                emptySet(),
                consumers,
                state
            )
        );
    }

    @Test
    public void shouldProduceStickyAndBalancedAssignmentWhenNewTasksAreAdded() {
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
        state.initializePrevTasks(emptyMap());
        state.computeTaskLags(UUID_1, getTaskEndOffsetSums(allTasks));

        // We should be able to add a new task without sacrificing stickiness
        final TaskId newTask = TASK_2_0;
        allTasks.add(newTask);
        state.assignActiveTasks(allTasks);

        final Map<String, List<TaskId>> newAssignment =
            assignTasksToThreads(
                allTasks,
                emptySet(),
                consumers,
                state
            );

        previousAssignment.get(CONSUMER_2).add(newTask);
        assertEquivalentAssignment(previousAssignment, newAssignment);
    }

    @Test
    public void shouldProduceMaximallyStickyAssignmentWhenMemberLeaves() {
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
        state.initializePrevTasks(emptyMap());
        state.computeTaskLags(UUID_1, getTaskEndOffsetSums(allTasks));

        // Consumer 3 leaves the group
        consumers.remove(CONSUMER_3);

        final Map<String, List<TaskId>> assignment = assignTasksToThreads(
            allTasks,
            emptySet(),
            consumers,
            state
        );

        // Each member should have all of its previous tasks reassigned plus some of consumer 3's tasks
        // We should give one of its tasks to consumer 1, and two of its tasks to consumer 2
        assertTrue(assignment.get(CONSUMER_1).containsAll(previousAssignment.get(CONSUMER_1)));
        assertTrue(assignment.get(CONSUMER_2).containsAll(previousAssignment.get(CONSUMER_2)));

        assertThat(assignment.get(CONSUMER_1).size(), equalTo(4));
        assertThat(assignment.get(CONSUMER_2).size(), equalTo(4));
    }

    @Test
    public void shouldProduceStickyEnoughAssignmentWhenNewMemberJoins() {
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

        state.initializePrevTasks(emptyMap());
        state.computeTaskLags(UUID_1, getTaskEndOffsetSums(allTasks));

        final Map<String, List<TaskId>> assignment = assignTasksToThreads(
            allTasks,
            emptySet(),
            consumers,
            state
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

    @Test
    public void shouldInterleaveTasksByGroupIdDuringNewAssignment() {
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
                emptySet(),
                consumers,
                state
            );

        assertThat(interleavedTaskIds, equalTo(assignment));
    }

    @Test
    public void testEagerSubscription() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");

        final Set<TaskId> prevTasks = mkSet(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1)
        );
        final Set<TaskId> standbyTasks = mkSet(
            new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2)
        );

        createMockTaskManager(prevTasks, standbyTasks);
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_23));
        assertThat(partitionAssignor.rebalanceProtocol(), equalTo(RebalanceProtocol.EAGER));

        final Set<String> topics = mkSet("topic1", "topic2");
        final Subscription subscription = new Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        Collections.sort(subscription.topics());
        assertEquals(asList("topic1", "topic2"), subscription.topics());

        final SubscriptionInfo info = getInfo(UUID_1, prevTasks, standbyTasks, uniqueField);
        assertEquals(info, SubscriptionInfo.decode(subscription.userData()));
    }

    @Test
    public void testCooperativeSubscription() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");

        final Set<TaskId> prevTasks = mkSet(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1));
        final Set<TaskId> standbyTasks = mkSet(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
            new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2));

        createMockTaskManager(prevTasks, standbyTasks);
        configureDefaultPartitionAssignor();

        final Set<String> topics = mkSet("topic1", "topic2");
        final Subscription subscription = new Subscription(
            new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        Collections.sort(subscription.topics());
        assertEquals(asList("topic1", "topic2"), subscription.topics());

        final SubscriptionInfo info = getInfo(UUID_1, prevTasks, standbyTasks, uniqueField);
        assertEquals(info, SubscriptionInfo.decode(subscription.userData()));
    }

    @Test
    public void testAssignBasic() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store", false), "processor");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final Set<TaskId> prevTasks10 = mkSet(TASK_0_0);
        final Set<TaskId> prevTasks11 = mkSet(TASK_0_1);
        final Set<TaskId> prevTasks20 = mkSet(TASK_0_2);
        final Set<TaskId> standbyTasks10 = EMPTY_TASKS;
        final Set<TaskId> standbyTasks11 = mkSet(TASK_0_2);
        final Set<TaskId> standbyTasks20 = mkSet(TASK_0_0);

        createMockTaskManager(prevTasks10, standbyTasks10);
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
            singletonList(APPLICATION_ID + "-store-changelog"),
            singletonList(3))
        );
        configureDefaultPartitionAssignor();

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, prevTasks10, standbyTasks10).encode()
                          ));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, prevTasks11, standbyTasks11).encode()
                          ));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(UUID_2, prevTasks20, standbyTasks20).encode()
                          ));

        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();


        // check the assignment
        assertEquals(mkSet(mkSet(t1p0, t2p0), mkSet(t1p1, t2p1)),
            mkSet(new HashSet<>(assignments.get("consumer10").partitions()),
                new HashSet<>(assignments.get("consumer11").partitions())));
        assertEquals(mkSet(t1p2, t2p2), new HashSet<>(assignments.get("consumer20").partitions()));

        // check assignment info

        // the first consumer
        final AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        final Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());

        // the second consumer
        final AssignmentInfo info11 = checkAssignment(allTopics, assignments.get("consumer11"));
        allActiveTasks.addAll(info11.activeTasks());

        assertEquals(mkSet(TASK_0_0, TASK_0_1), allActiveTasks);

        // the third consumer
        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);
    }

    @Test
    public void shouldAssignEvenlyAcrossConsumersOneClientMultipleThreads() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");
        builder.addProcessor("processorII", new MockApiProcessorSupplier<>(), "source2");

        final List<PartitionInfo> localInfos = asList(
            new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 3, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 3, Node.noNode(), new Node[0], new Node[0])
        );

        final Cluster localMetadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            localInfos,
            emptySet(),
            emptySet());

        final List<String> topics = asList("topic1", "topic2");

        configureDefault();

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode()
                          ));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode()
                          ));

        final Map<String, Assignment> assignments = partitionAssignor.assign(localMetadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partitions
        assertEquals(mkSet(mkSet(t2p2, t1p0, t1p2, t2p0), mkSet(t1p1, t2p1, t1p3, t2p3)),
                     mkSet(new HashSet<>(assignments.get("consumer10").partitions()), new HashSet<>(assignments.get("consumer11").partitions())));

        // the first consumer
        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());

        final List<TaskId> expectedInfo10TaskIds = asList(TASK_0_0, TASK_0_2, TASK_1_0, TASK_1_2);
        assertEquals(expectedInfo10TaskIds, info10.activeTasks());

        // the second consumer
        final AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        final List<TaskId> expectedInfo11TaskIds = asList(TASK_0_1, TASK_0_3, TASK_1_1, TASK_1_3);

        assertEquals(expectedInfo11TaskIds, info11.activeTasks());
    }

    @Test
    public void testAssignEmptyMetadata() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final Set<TaskId> prevTasks10 = mkSet(TASK_0_0);
        final Set<TaskId> standbyTasks10 = mkSet(TASK_0_1);
        final Cluster emptyMetadata = new Cluster("cluster", Collections.singletonList(Node.noNode()),
                                                  emptySet(),
                                                  emptySet(),
                                                  emptySet());

        createMockTaskManager(prevTasks10, standbyTasks10);
        configureDefaultPartitionAssignor();

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, prevTasks10, standbyTasks10).encode()
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
        assertEquals(mkSet(mkSet(t1p0, t2p0, t1p0, t2p0, t1p1, t2p1, t1p2, t2p2)),
                     mkSet(new HashSet<>(assignments.get("consumer10").partitions())));

        // the first consumer
        info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        allActiveTasks.addAll(info10.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);
    }

    @Test
    public void testAssignWithNewTasks() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addSource(null, "source3", null, null, null, "topic3");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2", "source3");
        final List<String> topics = asList("topic1", "topic2", "topic3");
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);

        // assuming that previous tasks do not have topic3
        final Set<TaskId> prevTasks10 = mkSet(TASK_0_0);
        final Set<TaskId> prevTasks11 = mkSet(TASK_0_1);
        final Set<TaskId> prevTasks20 = mkSet(TASK_0_2);
        
        createMockTaskManager(prevTasks10, EMPTY_TASKS);
        configureDefaultPartitionAssignor();

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, prevTasks10, EMPTY_TASKS).encode()));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, prevTasks11, EMPTY_TASKS).encode()));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(UUID_2, prevTasks20, EMPTY_TASKS).encode()));

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
        assertEquals(mkSet(t1p0, t1p1, t1p2, t2p0, t2p1, t2p2, t3p0, t3p1, t3p2, t3p3), allPartitions);
    }

    @Test
    public void testAssignWithStates() {
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
            asList(3, 3, 3))
        );
        configureDefault();

        subscriptions.put("consumer10",
                          new Subscription(topics, defaultSubscriptionInfo.encode()));
        subscriptions.put("consumer11",
                          new Subscription(topics, defaultSubscriptionInfo.encode()));
        subscriptions.put("consumer20",
                          new Subscription(topics, getInfo(UUID_2, EMPTY_TASKS, EMPTY_TASKS).encode()));

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
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        assertEquals(mkSet(TASK_0_0, TASK_0_1, TASK_0_2), tasksForState("store1", tasks, topicGroups));
        assertEquals(mkSet(TASK_1_0, TASK_1_1, TASK_1_2), tasksForState("store2", tasks, topicGroups));
        assertEquals(mkSet(TASK_1_0, TASK_1_1, TASK_1_2), tasksForState("store3", tasks, topicGroups));
    }

    private static Set<TaskId> tasksForState(final String storeName,
                                             final List<TaskId> tasks,
                                             final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups) {
        final String changelogTopic = ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, storeName);

        final Set<TaskId> ids = new HashSet<>();
        for (final Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            final Set<String> stateChangelogTopics = entry.getValue().stateChangelogTopics.keySet();

            if (stateChangelogTopics.contains(changelogTopic)) {
                for (final TaskId id : tasks) {
                    if (id.topicGroupId == entry.getKey()) {
                        ids.add(id);
                    }
                }
            }
        }
        return ids;
    }

    @Test
    public void testAssignWithStandbyReplicasAndStatelessTasks() {
        builder.addSource(null, "source1", null, null, null, "topic1", "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");

        final List<String> topics = asList("topic1", "topic2");

        createMockTaskManager(mkSet(TASK_0_0), emptySet());
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

        subscriptions.put("consumer10",
            new Subscription(
                topics,
                getInfo(UUID_1, mkSet(TASK_0_0), emptySet()).encode()));
        subscriptions.put("consumer20",
            new Subscription(
                topics,
                getInfo(UUID_2, mkSet(TASK_0_2), emptySet()).encode()));

        final Map<String, Assignment> assignments =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        final AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        assertTrue(info10.standbyTasks().isEmpty());

        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        assertTrue(info20.standbyTasks().isEmpty());
    }

    @Test
    public void testAssignWithStandbyReplicasAndLoggingDisabled() {
        builder.addSource(null, "source1", null, null, null, "topic1", "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false).withLoggingDisabled(), "processor");

        final List<String> topics = asList("topic1", "topic2");

        createMockTaskManager(mkSet(TASK_0_0), emptySet());
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

        subscriptions.put("consumer10",
            new Subscription(
                topics,
                getInfo(UUID_1, mkSet(TASK_0_0), emptySet()).encode()));
        subscriptions.put("consumer20",
            new Subscription(
                topics,
                getInfo(UUID_2, mkSet(TASK_0_2), emptySet()).encode()));

        final Map<String, Assignment> assignments =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        final AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        assertTrue(info10.standbyTasks().isEmpty());

        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        assertTrue(info20.standbyTasks().isEmpty());
    }

    @Test
    public void testAssignWithStandbyReplicas() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source1", "source2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor");

        final List<String> topics = asList("topic1", "topic2");
        final Set<TopicPartition> allTopicPartitions = topics.stream()
            .map(topic -> asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2)))
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final Set<TaskId> prevTasks00 = mkSet(TASK_0_0);
        final Set<TaskId> prevTasks01 = mkSet(TASK_0_1);
        final Set<TaskId> prevTasks02 = mkSet(TASK_0_2);
        final Set<TaskId> standbyTasks00 = mkSet(TASK_0_0);
        final Set<TaskId> standbyTasks01 = mkSet(TASK_0_1);
        final Set<TaskId> standbyTasks02 = mkSet(TASK_0_2);

        createMockTaskManager(prevTasks00, standbyTasks01);
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
            singletonList(APPLICATION_ID + "-store1-changelog"),
            singletonList(3))
        );
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, prevTasks00, EMPTY_TASKS, USER_END_POINT).encode()));
        subscriptions.put("consumer11",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, prevTasks01, standbyTasks02, USER_END_POINT).encode()));
        subscriptions.put("consumer20",
                          new Subscription(
                              topics,
                              getInfo(UUID_2, prevTasks02, standbyTasks00, OTHER_END_POINT).encode()));

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

        assertNotEquals("same processId has same set of standby tasks", info11.standbyTasks().keySet(), info10.standbyTasks().keySet());

        // check active tasks assigned to the first client
        assertEquals(mkSet(TASK_0_0, TASK_0_1), new HashSet<>(allActiveTasks));
        assertEquals(mkSet(TASK_0_2), new HashSet<>(allStandbyTasks));

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

    @Test
    public void testOnAssignment() {
        taskManager = EasyMock.createStrictMock(TaskManager.class);

        final Map<HostInfo, Set<TopicPartition>> hostState = Collections.singletonMap(
            new HostInfo("localhost", 9090),
            mkSet(t3p0, t3p3));

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(TASK_0_0, mkSet(t3p0));
        activeTasks.put(TASK_0_3, mkSet(t3p3));
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        standbyTasks.put(TASK_0_1, mkSet(t3p1));
        standbyTasks.put(TASK_0_2, mkSet(t3p2));

        taskManager.handleAssignment(activeTasks, standbyTasks);
        EasyMock.expectLastCall();
        streamsMetadataState = EasyMock.createStrictMock(StreamsMetadataState.class);
        final Capture<Cluster> capturedCluster = EasyMock.newCapture();
        streamsMetadataState.onChange(EasyMock.eq(hostState), EasyMock.anyObject(), EasyMock.capture(capturedCluster));
        EasyMock.expectLastCall();
        EasyMock.replay(streamsMetadataState);

        configureDefaultPartitionAssignor();

        final List<TaskId> activeTaskList = asList(TASK_0_0, TASK_0_3);
        final AssignmentInfo info = new AssignmentInfo(LATEST_SUPPORTED_VERSION, activeTaskList, standbyTasks, hostState, emptyMap(), 0);
        final Assignment assignment = new Assignment(asList(t3p0, t3p3), info.encode());

        partitionAssignor.onAssignment(assignment, null);

        EasyMock.verify(streamsMetadataState);
        EasyMock.verify(taskManager);

        assertEquals(singleton(t3p0.topic()), capturedCluster.getValue().topics());
        assertEquals(2, capturedCluster.getValue().partitionsForTopic(t3p0.topic()).size());
    }

    @Test
    public void testAssignWithInternalTopics() {
        builder.addInternalTopic("topicX", InternalTopicProperties.empty());
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addSink("sink1", "topicX", null, null, null, "processor1");
        builder.addSource(null, "source2", null, null, null, "topicX");
        builder.addProcessor("processor2", new MockApiProcessorSupplier<>(), "source2");
        final List<String> topics = asList("topic1", APPLICATION_ID + "-topicX");
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final MockInternalTopicManager internalTopicManager = configureDefault();

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode())
        );
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        // check prepared internal topics
        assertEquals(1, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get(APPLICATION_ID + "-topicX"));
    }

    @Test
    public void testAssignWithInternalTopicThatsSourceIsAnotherInternalTopic() {
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
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final MockInternalTopicManager internalTopicManager = configureDefault();

        subscriptions.put("consumer10",
                          new Subscription(
                              topics,
                              defaultSubscriptionInfo.encode())
        );
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        // check prepared internal topics
        assertEquals(2, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get(APPLICATION_ID + "-topicZ"));
    }

    @Test
    public void shouldGenerateTasksForAllCreatedPartitions() {
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

        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
            asList(APPLICATION_ID + "-topic3-STATE-STORE-0000000002-changelog",
                   APPLICATION_ID + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-changelog"),
            asList(4, 4))
        );

        final MockInternalTopicManager mockInternalTopicManager = configureDefault();

        subscriptions.put(client,
                          new Subscription(
                              asList("topic1", "topic3"),
                              defaultSubscriptionInfo.encode())
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

    @Test
    public void shouldThrowTimeoutExceptionWhenCreatingRepartitionTopicsTimesOut() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("topic1").repartition();

        final String client = "client1";
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());

        createDefaultMockTaskManager();
        EasyMock.replay(taskManager);
        partitionAssignor.configure(configProps());
        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            new StreamsConfig(configProps()),
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
                              defaultSubscriptionInfo.encode()
                          )
        );
        assertThrows(TimeoutException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenCreatingChangelogTopicsTimesOut() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table("topic1", Materialized.as("store"));

        final String client = "client1";
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());

        createDefaultMockTaskManager();
        EasyMock.replay(taskManager);
        partitionAssignor.configure(configProps());
        final MockInternalTopicManager mockInternalTopicManager =  new MockInternalTopicManager(
            time,
            new StreamsConfig(configProps()),
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
                defaultSubscriptionInfo.encode()
            )
        );

        assertThrows(TimeoutException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @Test
    public void shouldAddUserDefinedEndPointToSubscription() {
        builder.addSource(null, "source", null, null, null, "input");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT));

        final Set<String> topics = mkSet("input");
        final ByteBuffer userData = partitionAssignor.subscriptionUserData(topics);
        final Subscription subscription =
            new Subscription(new ArrayList<>(topics), userData);
        final SubscriptionInfo subscriptionInfo = SubscriptionInfo.decode(subscription.userData());
        assertEquals("localhost:8080", subscriptionInfo.userEndPoint());
    }

    @Test
    public void shouldMapUserEndPointToTopicPartitions() {
        builder.addSource(null, "source", null, null, null, "topic1");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        final List<String> topics = Collections.singletonList("topic1");

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT));

        subscriptions.put("consumer1",
                          new Subscription(
                              topics,
                              getInfo(UUID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode())
        );
        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        final Assignment consumerAssignment = assignments.get("consumer1");
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumerAssignment.userData());
        final Set<TopicPartition> topicPartitions = assignmentInfo.partitionsByHost().get(new HostInfo("localhost", 8080));
        assertEquals(
            mkSet(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic1", 2)),
            topicPartitions);
    }

    @Test
    public void shouldThrowExceptionIfApplicationServerConfigIsNotHostPortPair() {
        createDefaultMockTaskManager();
        try {
            configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost"));
            fail("expected to an exception due to invalid config");
        } catch (final ConfigException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowExceptionIfApplicationServerConfigPortIsNotAnInteger() {
        createDefaultMockTaskManager();
        assertThrows(ConfigException.class, () -> configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:j87yhk")));
    }

    @Test
    public void shouldNotLoopInfinitelyOnMissingMetadataAndShouldNotCreateRelatedTasks() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<Object, Object> stream1 = streamsBuilder

            // Task 1 (should get created):
            .stream("topic1")
            // force repartitioning for aggregation
            .selectKey((key, value) -> null)
            .groupByKey()

            // Task 2 (should get created):
            // create repartioning and changelog topic as task 1 exists
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

        final MockInternalTopicManager mockInternalTopicManager = configureDefault();

        subscriptions.put(client,
                          new Subscription(
                              Collections.singletonList("unknownTopic"),
                              defaultSubscriptionInfo.encode())
        );
        final Map<String, Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(mockInternalTopicManager.readyTopics.isEmpty(), equalTo(true));

        assertThat(assignment.get(client).partitions().isEmpty(), equalTo(true));
    }

    @Test
    public void shouldUpdateClusterMetadataAndHostInfoOnAssignment() {
        final Map<HostInfo, Set<TopicPartition>> initialHostState = mkMap(
            mkEntry(new HostInfo("localhost", 9090), mkSet(t1p0, t1p1)),
            mkEntry(new HostInfo("otherhost", 9090), mkSet(t2p0, t2p1))
        );

        final Map<HostInfo, Set<TopicPartition>> newHostState = mkMap(
            mkEntry(new HostInfo("localhost", 9090), mkSet(t1p0, t1p1)),
            mkEntry(new HostInfo("newotherhost", 9090), mkSet(t2p0, t2p1))
        );

        streamsMetadataState = EasyMock.createStrictMock(StreamsMetadataState.class);

        streamsMetadataState.onChange(EasyMock.eq(initialHostState), EasyMock.anyObject(), EasyMock.anyObject());
        streamsMetadataState.onChange(EasyMock.eq(newHostState), EasyMock.anyObject(), EasyMock.anyObject());
        EasyMock.replay(streamsMetadataState);

        createDefaultMockTaskManager();
        configureDefaultPartitionAssignor();

        partitionAssignor.onAssignment(createAssignment(initialHostState), null);
        partitionAssignor.onAssignment(createAssignment(newHostState), null);

        EasyMock.verify(taskManager, streamsMetadataState);
    }

    @Test
    public void shouldTriggerImmediateRebalanceOnHostInfoChange() {
        final Map<HostInfo, Set<TopicPartition>> oldHostState = mkMap(
            mkEntry(new HostInfo("localhost", 9090), mkSet(t1p0, t1p1)),
            mkEntry(new HostInfo("otherhost", 9090), mkSet(t2p0, t2p1))
        );

        final Map<HostInfo, Set<TopicPartition>> newHostState = mkMap(
            mkEntry(new HostInfo("newhost", 9090), mkSet(t1p0, t1p1)),
            mkEntry(new HostInfo("otherhost", 9090), mkSet(t2p0, t2p1))
        );

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "newhost:9090"));

        partitionAssignor.onAssignment(createAssignment(oldHostState), null);

        assertThat(referenceContainer.nextScheduledRebalanceMs.get(), is(0L));

        partitionAssignor.onAssignment(createAssignment(newHostState), null);

        assertThat(referenceContainer.nextScheduledRebalanceMs.get(), is(Long.MAX_VALUE));
    }

    @Test
    public void shouldTriggerImmediateRebalanceOnTasksRevoked() {
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final List<TopicPartition> allPartitions = asList(t1p0, t1p1, t1p2);

        subscriptions.put(CONSUMER_1,
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(UUID_1, allTasks, EMPTY_TASKS).encode(),
                              allPartitions)
        );
        subscriptions.put(CONSUMER_2,
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(UUID_1, EMPTY_TASKS, allTasks).encode(),
                              emptyList())
        );

        createMockTaskManager(allTasks, allTasks);
        configurePartitionAssignorWith(singletonMap(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 0L));

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

    @Test
    public void shouldNotAddStandbyTaskPartitionsToPartitionsForHost() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("topic1").groupByKey().count();
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build());

        createDefaultMockTaskManager();
        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
            singletonList(APPLICATION_ID + "-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog"),
            singletonList(3))
        );

        final Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT);

        configurePartitionAssignorWith(props);

        subscriptions.put("consumer1",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(UUID_1, EMPTY_TASKS, EMPTY_TASKS, USER_END_POINT).encode())
        );
        subscriptions.put("consumer2",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(UUID_2, EMPTY_TASKS, EMPTY_TASKS, OTHER_END_POINT).encode())
        );
        final Set<TopicPartition> allPartitions = mkSet(t1p0, t1p1, t1p2);
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

    @Test
    public void shouldThrowKafkaExceptionIfReferenceContainerNotConfigured() {
        final Map<String, Object> config = configProps();
        config.remove(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR);

        final KafkaException expected = assertThrows(
            KafkaException.class,
            () -> partitionAssignor.configure(config)
        );
        assertThat(expected.getMessage(), equalTo("ReferenceContainer is not specified"));
    }

    @Test
    public void shouldThrowKafkaExceptionIfReferenceContainerConfigIsNotTaskManagerInstance() {
        final Map<String, Object> config = configProps();
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

    @Test
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V2() {
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 2);
    }

    @Test
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V3() {
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 3);
    }

    @Test
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV2V3() {
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(2, 3);
    }

    private void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(final int smallestVersion,
                                                                                     final int otherVersion) {
        subscriptions.put("consumer1",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfoForOlderVersion(smallestVersion, UUID_1, EMPTY_TASKS, EMPTY_TASKS).encode())
        );
        subscriptions.put("consumer2",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfoForOlderVersion(otherVersion, UUID_2, EMPTY_TASKS, EMPTY_TASKS).encode()
                          )
        );

        configureDefault();

        final Map<String, Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));
        assertThat(AssignmentInfo.decode(assignment.get("consumer1").userData()).version(), equalTo(smallestVersion));
        assertThat(AssignmentInfo.decode(assignment.get("consumer2").userData()).version(), equalTo(smallestVersion));
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion1() {
        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_0100));

        final Set<String> topics = mkSet("topic1");
        final Subscription subscription = new Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        assertThat(SubscriptionInfo.decode(subscription.userData()).version(), equalTo(1));
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For0101() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0101);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For0102() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0102);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For0110() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0110);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For10() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_10);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For11() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_11);
    }

    private void shouldDownGradeSubscriptionToVersion2(final Object upgradeFromValue) {
        createDefaultMockTaskManager();
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFromValue));

        final Set<String> topics = mkSet("topic1");
        final Subscription subscription = new Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        assertThat(SubscriptionInfo.decode(subscription.userData()).version(), equalTo(2));
    }

    @Test
    public void shouldReturnInterleavedAssignmentWithUnrevokedPartitionsRemovedWhenNewConsumerJoins() {
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        subscriptions.put(CONSUMER_1,
                          new Subscription(
                Collections.singletonList("topic1"),
                getInfo(UUID_1, allTasks, EMPTY_TASKS).encode(),
                asList(t1p0, t1p1, t1p2))
        );
        subscriptions.put(CONSUMER_2,
                          new Subscription(
                Collections.singletonList("topic1"),
                getInfo(UUID_2, EMPTY_TASKS, EMPTY_TASKS).encode(),
                emptyList())
        );

        createMockTaskManager(allTasks, allTasks);
        configureDefaultPartitionAssignor();

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

    @Test
    public void shouldReturnInterleavedAssignmentForOnlyFutureInstancesDuringVersionProbing() {
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        subscriptions.put(CONSUMER_1,
                          new Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription(),
                              emptyList())
        );
        subscriptions.put(CONSUMER_2,
                          new Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription(),
                              emptyList())
        );

        createMockTaskManager(allTasks, allTasks);
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

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

    @Test
    public void shouldEncodeAssignmentErrorIfV1SubscriptionAndFutureSubscriptionIsMixed() {
        shouldEncodeAssignmentErrorIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(1);
    }

    @Test
    public void shouldEncodeAssignmentErrorIfV2SubscriptionAndFutureSubscriptionIsMixed() {
        shouldEncodeAssignmentErrorIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(2);
    }

    @Test
    public void shouldNotFailOnBranchedMultiLevelRepartitionConnectedTopology() {
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

        configureDefault();

        subscriptions.put("consumer10",
            new Subscription(
                topics,
                defaultSubscriptionInfo.encode())
        );

        final Cluster metadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            Collections.singletonList(new PartitionInfo("input-stream", 0, Node.noNode(), new Node[0], new Node[0])),
            emptySet(),
            emptySet());

        // This shall fail if we have bugs in the repartition topic creation due to the inconsistent order of sub-topologies.
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));
    }

    @Test
    public void shouldGetAssignmentConfigs() {
        createDefaultMockTaskManager();

        final Map<String, Object> props = configProps();
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

    @Test
    public void shouldGetTime() {
        time.setCurrentTimeMs(Long.MAX_VALUE);

        createDefaultMockTaskManager();
        final Map<String, Object> props = configProps();
        final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(props);

        assertThat(assignorConfiguration.referenceContainer().time.milliseconds(), equalTo(Long.MAX_VALUE));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfAnyPartitionsMissingFromChangelogEndOffsets() {
        final int changelogNumPartitions = 3;
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");

        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
            singletonList(APPLICATION_ID + "-store1-changelog"),
            singletonList(changelogNumPartitions - 1))
        );

        configureDefault();

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode()
            ));
        assertThrows(IllegalStateException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfAnyTopicsMissingFromChangelogEndOffsets() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store2", false), "processor1");

        adminClient = createMockAdminClientForAssignor(getTopicPartitionOffsetsMap(
            singletonList(APPLICATION_ID + "-store1-changelog"),
            singletonList(3))
        );

        configureDefault();

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode()
            ));
        assertThrows(IllegalStateException.class, () -> partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)));
    }

    @Test
    public void shouldSkipListOffsetsRequestForNewlyCreatedChangelogTopics() {
        adminClient = EasyMock.createMock(AdminClient.class);
        final ListOffsetsResult result = EasyMock.createNiceMock(ListOffsetsResult.class);
        final KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = new KafkaFutureImpl<>();
        allFuture.complete(emptyMap());

        expect(adminClient.listOffsets(emptyMap())).andStubReturn(result);
        expect(result.all()).andReturn(allFuture);

        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");

        subscriptions.put("consumer10",
                          new Subscription(
                              singletonList("topic1"),
                              defaultSubscriptionInfo.encode()
                          ));

        EasyMock.replay(result);
        configureDefault();
        overwriteInternalTopicManagerWithMock(true);

        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        EasyMock.verify(adminClient);
    }

    @Test
    public void shouldRequestEndOffsetsForPreexistingChangelogs() {
        final Set<TopicPartition> changelogs = mkSet(
            new TopicPartition(APPLICATION_ID + "-store-changelog", 0),
            new TopicPartition(APPLICATION_ID + "-store-changelog", 1),
            new TopicPartition(APPLICATION_ID + "-store-changelog", 2)
        );
        adminClient = EasyMock.createMock(AdminClient.class);
        final ListOffsetsResult result = EasyMock.createNiceMock(ListOffsetsResult.class);
        final KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = new KafkaFutureImpl<>();
        allFuture.complete(changelogs.stream().collect(Collectors.toMap(
            tp -> tp,
            tp -> {
                final ListOffsetsResultInfo info = EasyMock.createNiceMock(ListOffsetsResultInfo.class);
                expect(info.offset()).andStubReturn(Long.MAX_VALUE);
                EasyMock.replay(info);
                return info;
            }))
        );
        final Capture<Map<TopicPartition, OffsetSpec>> capturedChangelogs = EasyMock.newCapture();

        expect(adminClient.listOffsets(EasyMock.capture(capturedChangelogs))).andReturn(result).once();
        expect(result.all()).andReturn(allFuture);

        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockApiProcessorSupplier<>(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store", false), "processor1");

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode()
            ));

        EasyMock.replay(result);
        configureDefault();
        overwriteInternalTopicManagerWithMock(false);

        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        EasyMock.verify(adminClient);
        assertThat(
            capturedChangelogs.getValue().keySet(),
            equalTo(changelogs)
        );
    }

    @Test
    public void shouldRequestCommittedOffsetsForPreexistingSourceChangelogs() {
        final Set<TopicPartition> changelogs = mkSet(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic1", 2)
        );

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table("topic1", Materialized.as("store"));

        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        builder = TopologyWrapper.getInternalTopologyBuilder(streamsBuilder.build(props));

        subscriptions.put("consumer10",
            new Subscription(
                singletonList("topic1"),
                defaultSubscriptionInfo.encode()
            ));

        createDefaultMockTaskManager();
        configurePartitionAssignorWith(singletonMap(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE));
        overwriteInternalTopicManagerWithMock(false);

        final Consumer<byte[], byte[]> consumerClient = referenceContainer.mainConsumer;
        EasyMock.expect(consumerClient.committed(EasyMock.eq(changelogs)))
            .andReturn(changelogs.stream().collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndMetadata(Long.MAX_VALUE)))).once();

        EasyMock.replay(consumerClient);
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions));

        EasyMock.verify(consumerClient);
    }

    @Test
    public void shouldEncodeMissingSourceTopicError() {
        final Cluster emptyClusterMetadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            emptyList(),
            emptySet(),
            emptySet()
        );

        builder.addSource(null, "source1", null, null, null, "topic1");
        configureDefault();

        subscriptions.put("consumer",
                          new Subscription(
                              singletonList("topic"),
                              defaultSubscriptionInfo.encode()
                          ));
        final Map<String, Assignment> assignments = partitionAssignor.assign(emptyClusterMetadata, new GroupSubscription(subscriptions)).groupAssignment();
        assertThat(AssignmentInfo.decode(assignments.get("consumer").userData()).errCode(),
                   equalTo(AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code()));
    }

    @Test
    public void testUniqueField() {
        createDefaultMockTaskManager();
        configureDefaultPartitionAssignor();
        final Set<String> topics = mkSet("input");

        assertEquals(0, partitionAssignor.uniqueField());
        partitionAssignor.subscriptionUserData(topics);
        assertEquals(1, partitionAssignor.uniqueField());
        partitionAssignor.subscriptionUserData(topics);
        assertEquals(2, partitionAssignor.uniqueField());
    }

    @Test
    public void testUniqueFieldOverflow() {
        createDefaultMockTaskManager();
        configureDefaultPartitionAssignor();
        final Set<String> topics = mkSet("input");

        for (int i = 0; i < 127; i++) {
            partitionAssignor.subscriptionUserData(topics);
        }
        assertEquals(127, partitionAssignor.uniqueField());
        partitionAssignor.subscriptionUserData(topics);
        assertEquals(-128, partitionAssignor.uniqueField());
    }

    @Test
    public void shouldThrowTaskAssignmentExceptionWhenUnableToResolvePartitionCount() {
        builder = new CorruptedInternalTopologyBuilder();
        final InternalStreamsBuilder streamsBuilder = new InternalStreamsBuilder(builder);

        final KStream<String, String> inputTopic = streamsBuilder.stream(singleton("topic1"), new ConsumedInternal<>());
        final KTable<String, String> inputTable = streamsBuilder.table("topic2", new ConsumedInternal<>(), new MaterializedInternal<>(Materialized.as("store")));
        inputTopic
            .groupBy(
                (k, v) -> k,
                Grouped.with("GroupName", Serdes.String(), Serdes.String())
            )
            .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
            .aggregate(
                () -> "",
                (k, v, a) -> a + k)
            .leftJoin(
                inputTable,
                v -> v,
                (x, y) -> x + y
            );
        streamsBuilder.buildAndOptimizeTopology();

        configureDefault();

        subscriptions.put("consumer",
                          new Subscription(
                              singletonList("topic"),
                              defaultSubscriptionInfo.encode()
                          ));
        final Map<String, Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        assertThat(AssignmentInfo.decode(assignments.get("consumer").userData()).errCode(),
                   equalTo(AssignorError.ASSIGNMENT_ERROR.code()));
    }

    private static class CorruptedInternalTopologyBuilder extends InternalTopologyBuilder {
        private Map<Integer, TopicsInfo> corruptedTopicGroups;

        @Override
        public synchronized Map<Integer, TopicsInfo> topicGroups() {
            if (corruptedTopicGroups == null) {
                corruptedTopicGroups = new HashMap<>();
                for (final Map.Entry<Integer, TopicsInfo> topicGroupEntry : super.topicGroups().entrySet()) {
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

    private void shouldEncodeAssignmentErrorIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(final int oldVersion) {
        subscriptions.put("consumer1",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              getInfoForOlderVersion(oldVersion, UUID_1, EMPTY_TASKS, EMPTY_TASKS).encode())
        );
        subscriptions.put("future-consumer",
                          new Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription())
        );
        configureDefault();

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
                assertEquals(id.partition, partition.partition());

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

    private static SubscriptionInfo getInfoForOlderVersion(final int version,
                                                           final UUID processId,
                                                           final Set<TaskId> prevTasks,
                                                           final Set<TaskId> standbyTasks) {
        return new SubscriptionInfo(
            version, LATEST_SUPPORTED_VERSION, processId, null, getTaskOffsetSums(prevTasks, standbyTasks), (byte) 0, 0);
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

}
