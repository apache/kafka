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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.MockProcessorSupplier;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
public class StreamsPartitionAssignorTest {
    private static final String CONSUMER_1 = "consumer1";
    private static final String CONSUMER_2 = "consumer2";
    private static final String CONSUMER_3 = "consumer3";
    private static final String CONSUMER_4 = "consumer4";

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
    private final TopicPartition t4p0 = new TopicPartition("topic4", 0);
    private final TopicPartition t4p1 = new TopicPartition("topic4", 1);
    private final TopicPartition t4p2 = new TopicPartition("topic4", 2);
    private final TopicPartition t4p3 = new TopicPartition("topic4", 3);

    private final TaskId task0_0 = new TaskId(0, 0);
    private final TaskId task0_1 = new TaskId(0, 1);
    private final TaskId task0_2 = new TaskId(0, 2);
    private final TaskId task0_3 = new TaskId(0, 3);
    private final TaskId task1_0 = new TaskId(1, 0);
    private final TaskId task1_1 = new TaskId(1, 1);
    private final TaskId task1_2 = new TaskId(1, 2);
    private final TaskId task1_3 = new TaskId(1, 3);
    private final TaskId task2_0 = new TaskId(2, 0);
    private final TaskId task2_1 = new TaskId(2, 1);
    private final TaskId task2_2 = new TaskId(2, 2);
    private final TaskId task2_3 = new TaskId(2, 3);

    private final Map<TaskId, Set<TopicPartition>> partitionsForTask = mkMap(
        mkEntry(task0_0, mkSet(t1p0, t2p0)),
        mkEntry(task0_1, mkSet(t1p1, t2p1)),
        mkEntry(task0_2, mkSet(t1p2, t2p2)),
        mkEntry(task0_3, mkSet(t1p3, t2p3)),

        mkEntry(task1_0, mkSet(t3p0)),
        mkEntry(task1_1, mkSet(t3p1)),
        mkEntry(task1_2, mkSet(t3p2)),
        mkEntry(task1_3, mkSet(t3p3)),

        mkEntry(task2_0, mkSet(t4p0)),
        mkEntry(task2_1, mkSet(t4p1)),
        mkEntry(task2_2, mkSet(t4p2)),
        mkEntry(task2_3, mkSet(t4p3))
    );

    private final Set<String> allTopics = mkSet("topic1", "topic2");

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

    private final Set<TaskId> emptyTasks = Collections.emptySet();

    private final Cluster metadata = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        infos,
        Collections.emptySet(),
        Collections.emptySet());

    private final StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private final InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private final StreamsConfig streamsConfig = new StreamsConfig(configProps());
    private static final String USER_END_POINT = "localhost:8080";
    private static final String APPLICATION_ID = "stream-partition-assignor-test";

    private TaskManager taskManager;

    private Map<String, Object> configProps() {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);
        configurationMap.put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
        configurationMap.put(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE, new AtomicInteger());
        return configurationMap;
    }

    private void configurePartitionAssignor(final Map<String, Object> props) {
        final Map<String, Object> configurationMap = configProps();
        configurationMap.putAll(props);
        partitionAssignor.configure(configurationMap);
    }

    private void configureDefault() {
        createMockTaskManager();
        partitionAssignor.configure(configProps());
    }

    private void createMockTaskManager() {
        final StreamsBuilder builder = new StreamsBuilder();
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(APPLICATION_ID);

        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), internalTopologyBuilder);
    }

    private void createMockTaskManager(final Set<TaskId> prevTasks,
                                       final Set<TaskId> cachedTasks,
                                       final UUID processId,
                                       final InternalTopologyBuilder builder) {
        taskManager = EasyMock.createNiceMock(TaskManager.class);
        EasyMock.expect(taskManager.adminClient()).andReturn(null).anyTimes();
        EasyMock.expect(taskManager.builder()).andReturn(builder).anyTimes();
        EasyMock.expect(taskManager.previousRunningTaskIds()).andReturn(prevTasks).anyTimes();
        EasyMock.expect(taskManager.activeTaskIds()).andReturn(prevTasks).anyTimes();
        EasyMock.expect(taskManager.cachedTasksIds()).andReturn(cachedTasks).anyTimes();
        EasyMock.expect(taskManager.processId()).andReturn(processId).anyTimes();
    }

    private Map<String, ConsumerPartitionAssignor.Subscription> subscriptions;

    @Before
    public void setUp() {
        if (subscriptions != null) {
            subscriptions.clear();
        } else {
            subscriptions = new HashMap<>();
        }
    }

    private static SubscriptionInfo getInfo(final int version,
                                            final UUID processId,
                                            final Set<TaskId> prevTasks,
                                            final Set<TaskId> standbyTasks,
                                            final String userEndPoint) {
        return new SubscriptionInfo(version, LATEST_SUPPORTED_VERSION, processId, prevTasks, standbyTasks, userEndPoint);
    }

    private static SubscriptionInfo getInfo(final UUID processId,
                                            final Set<TaskId> prevTasks,
                                            final Set<TaskId> standbyTasks,
                                            final String userEndPoint) {
        return new SubscriptionInfo(LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, prevTasks, standbyTasks, userEndPoint);
    }

    private static SubscriptionInfo getInfo(final UUID processId, final Set<TaskId> prevTasks, final Set<TaskId> standbyTasks) {
        return new SubscriptionInfo(LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, prevTasks, standbyTasks, USER_END_POINT);
    }

    @Test
    public void shouldUseEagerRebalancingProtocol() {
        createMockTaskManager();
        final Map<String, Object> props = configProps();
        props.put(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_23);
        partitionAssignor.configure(props);

        assertEquals(1, partitionAssignor.supportedProtocols().size());
        assertTrue(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.EAGER));
        assertFalse(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.COOPERATIVE));
    }

    @Test
    public void shouldUseCooperativeRebalancingProtocol() {
        createMockTaskManager();
        final Map<String, Object> props = configProps();
        partitionAssignor.configure(props);

        assertEquals(2, partitionAssignor.supportedProtocols().size());
        assertTrue(partitionAssignor.supportedProtocols().contains(RebalanceProtocol.COOPERATIVE));
    }

    @Test
    public void shouldProduceStickyAndBalancedAssignmentWhenNothingChanges() {
        configureDefault();
        final ClientState state = new ClientState();
        final List<TaskId> allTasks = asList(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task1_2, task1_3);

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, asList(task0_0, task1_1, task1_3)),
            mkEntry(CONSUMER_2, asList(task0_3, task1_0)),
            mkEntry(CONSUMER_3, asList(task0_1, task0_2, task1_2))
        );

        for (final Map.Entry<String, List<TaskId>> entry : previousAssignment.entrySet()) {
            for (final TaskId task : entry.getValue()) {
                state.addOwnedPartitions(partitionsForTask.get(task), entry.getKey());
            }
        }

        final Set<String> consumers = mkSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        state.assignActiveTasks(allTasks);

        assertEquivalentAssignment(
            previousAssignment,
            partitionAssignor.tryStickyAndBalancedTaskAssignmentWithinClient(
                state,
                consumers,
                partitionsForTask,
                Collections.emptySet()
            )
        );
    }

    @Test
    public void shouldProduceStickyAndBalancedAssignmentWhenNewTasksAreAdded() {
        configureDefault();
        final ClientState state = new ClientState();

        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task1_2, task1_3);

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, new ArrayList<>(asList(task0_0, task1_1, task1_3))),
            mkEntry(CONSUMER_2, new ArrayList<>(asList(task0_3, task1_0))),
            mkEntry(CONSUMER_3, new ArrayList<>(asList(task0_1, task0_2, task1_2)))
        );

        for (final Map.Entry<String, List<TaskId>> entry : previousAssignment.entrySet()) {
            for (final TaskId task : entry.getValue()) {
                state.addOwnedPartitions(partitionsForTask.get(task), entry.getKey());
            }
        }

        final Set<String> consumers = mkSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);

        // We should be able to add a new task without sacrificing stickyness
        final TaskId newTask = task2_0;
        allTasks.add(newTask);
        state.assignActiveTasks(allTasks);

        final Map<String, List<TaskId>> newAssignment = partitionAssignor.tryStickyAndBalancedTaskAssignmentWithinClient(state, consumers, partitionsForTask, Collections.emptySet());

        previousAssignment.get(CONSUMER_2).add(newTask);
        assertEquivalentAssignment(previousAssignment, newAssignment);
    }

    @Test
    public void shouldReturnEmptyMapWhenStickyAndBalancedAssignmentIsNotPossibleBecauseNewConsumerJoined() {
        configureDefault();
        final ClientState state = new ClientState();

        final List<TaskId> allTasks = asList(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task1_2, task1_3);

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, asList(task0_0, task1_1, task1_3)),
            mkEntry(CONSUMER_2, asList(task0_3, task1_0)),
            mkEntry(CONSUMER_3, asList(task0_1, task0_2, task1_2))
        );

        for (final Map.Entry<String, List<TaskId>> entry : previousAssignment.entrySet()) {
            for (final TaskId task : entry.getValue()) {
                state.addOwnedPartitions(partitionsForTask.get(task), entry.getKey());
            }
        }

        // If we add a new consumer here, we cannot produce an assignment that is both sticky and balanced
        final Set<String> consumers = mkSet(CONSUMER_1, CONSUMER_2, CONSUMER_3, CONSUMER_4);
        state.assignActiveTasks(allTasks);

        assertThat(partitionAssignor.tryStickyAndBalancedTaskAssignmentWithinClient(state, consumers, partitionsForTask, Collections.emptySet()),
                   equalTo(emptyMap()));
    }

    @Test
    public void shouldReturnEmptyMapWhenStickyAndBalancedAssignmentIsNotPossibleBecauseOtherClientOwnedPartition() {
        configureDefault();
        final ClientState state = new ClientState();

        final List<TaskId> allTasks = asList(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task1_2, task1_3);

        final Map<String, List<TaskId>> previousAssignment = mkMap(
            mkEntry(CONSUMER_1, new ArrayList<>(asList(task1_1, task1_3))),
            mkEntry(CONSUMER_2, new ArrayList<>(asList(task0_3, task1_0))),
            mkEntry(CONSUMER_3, new ArrayList<>(asList(task0_1, task0_2, task1_2)))
        );

        for (final Map.Entry<String, List<TaskId>> entry : previousAssignment.entrySet()) {
            for (final TaskId task : entry.getValue()) {
                state.addOwnedPartitions(partitionsForTask.get(task), entry.getKey());
            }
        }

        // Add the partitions of task0_0 to allOwnedPartitions but not c1's ownedPartitions/previousAssignment
        final Set<TopicPartition> allOwnedPartitions = new HashSet<>(partitionsForTask.get(task0_0));

        final Set<String> consumers = mkSet(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        state.assignActiveTasks(allTasks);

        assertThat(partitionAssignor.tryStickyAndBalancedTaskAssignmentWithinClient(state, consumers, partitionsForTask, allOwnedPartitions),
                   equalTo(emptyMap()));
    }

    @Test
    public void shouldInterleaveTasksByGroupId() {
        final TaskId taskIdA0 = new TaskId(0, 0);
        final TaskId taskIdA1 = new TaskId(0, 1);
        final TaskId taskIdA2 = new TaskId(0, 2);
        final TaskId taskIdA3 = new TaskId(0, 3);

        final TaskId taskIdB0 = new TaskId(1, 0);
        final TaskId taskIdB1 = new TaskId(1, 1);
        final TaskId taskIdB2 = new TaskId(1, 2);

        final TaskId taskIdC0 = new TaskId(2, 0);
        final TaskId taskIdC1 = new TaskId(2, 1);

        final String c1 = "c1";
        final String c2 = "c2";
        final String c3 = "c3";

        final Set<String> consumers = mkSet(c1, c2, c3);

        final List<TaskId> expectedSubList1 = asList(taskIdA0, taskIdA3, taskIdB2);
        final List<TaskId> expectedSubList2 = asList(taskIdA1, taskIdB0, taskIdC0);
        final List<TaskId> expectedSubList3 = asList(taskIdA2, taskIdB1, taskIdC1);

        final Map<String, List<TaskId>> assignment = new HashMap<>();
        assignment.put(c1, expectedSubList1);
        assignment.put(c2, expectedSubList2);
        assignment.put(c3, expectedSubList3);

        final List<TaskId> tasks = asList(taskIdC0, taskIdC1, taskIdB0, taskIdB1, taskIdB2, taskIdA0, taskIdA1, taskIdA2, taskIdA3);
        Collections.shuffle(tasks);

        final Map<String, List<TaskId>> interleavedTaskIds = StreamsPartitionAssignor.interleaveConsumerTasksByGroupId(tasks, consumers);

        assertThat(interleavedTaskIds, equalTo(assignment));
    }

    @Test
    public void testEagerSubscription() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

        final Set<TaskId> prevTasks = mkSet(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1)
        );
        final Set<TaskId> cachedTasks = mkSet(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
            new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2)
        );

        final UUID processId = UUID.randomUUID();
        createMockTaskManager(prevTasks, cachedTasks, processId, builder);
        EasyMock.replay(taskManager);

        configurePartitionAssignor(emptyMap());
        partitionAssignor.setRebalanceProtocol(RebalanceProtocol.EAGER);

        final Set<String> topics = mkSet("topic1", "topic2");
        final ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        Collections.sort(subscription.topics());
        assertEquals(asList("topic1", "topic2"), subscription.topics());

        final Set<TaskId> standbyTasks = new HashSet<>(cachedTasks);
        standbyTasks.removeAll(prevTasks);

        // When following the eager protocol, we must encode the previous tasks ourselves since we must revoke
        // everything and thus the "ownedPartitions" field in the subscription will be empty
        final SubscriptionInfo info = getInfo(processId, prevTasks, standbyTasks, null);
        assertEquals(info, SubscriptionInfo.decode(subscription.userData()));
    }

    @Test
    public void testCooperativeSubscription() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

        final Set<TaskId> prevTasks = mkSet(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1));
        final Set<TaskId> cachedTasks = mkSet(
            new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
            new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2));

        final UUID processId = UUID.randomUUID();
        createMockTaskManager(prevTasks, cachedTasks, processId, builder);
        EasyMock.replay(taskManager);

        configurePartitionAssignor(emptyMap());

        final Set<String> topics = mkSet("topic1", "topic2");
        final ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(
            new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        Collections.sort(subscription.topics());
        assertEquals(asList("topic1", "topic2"), subscription.topics());

        final Set<TaskId> standbyTasks = new HashSet<>(cachedTasks);
        standbyTasks.removeAll(prevTasks);

        // We don't encode the active tasks when following the cooperative protocol, as these are inferred from the
        // ownedPartitions encoded in the subscription
        final SubscriptionInfo info = getInfo(processId, Collections.emptySet(), standbyTasks, null);
        assertEquals(info, SubscriptionInfo.decode(subscription.userData()));
    }

    @Test
    public void testAssignBasic() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);

        final Set<TaskId> prevTasks10 = mkSet(task0_0);
        final Set<TaskId> prevTasks11 = mkSet(task0_1);
        final Set<TaskId> prevTasks20 = mkSet(task0_2);
        final Set<TaskId> standbyTasks10 = mkSet(task0_1);
        final Set<TaskId> standbyTasks11 = mkSet(task0_2);
        final Set<TaskId> standbyTasks20 = mkSet(task0_0);

        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();

        createMockTaskManager(prevTasks10, standbyTasks10, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, prevTasks10, standbyTasks10, USER_END_POINT).encode()
                          ));
        subscriptions.put("consumer11",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, prevTasks11, standbyTasks11, USER_END_POINT).encode()
                          ));
        subscriptions.put("consumer20",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid2, prevTasks20, standbyTasks20, USER_END_POINT).encode()
                          ));

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partitions
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

        assertEquals(mkSet(task0_0, task0_1), allActiveTasks);

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
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1");
        builder.addProcessor("processorII", new MockProcessorSupplier(), "source2");

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
            Collections.emptySet(),
            Collections.emptySet());

        final List<String> topics = asList("topic1", "topic2");

        final TaskId taskIdA0 = new TaskId(0, 0);
        final TaskId taskIdA1 = new TaskId(0, 1);
        final TaskId taskIdA2 = new TaskId(0, 2);
        final TaskId taskIdA3 = new TaskId(0, 3);

        final TaskId taskIdB0 = new TaskId(1, 0);
        final TaskId taskIdB1 = new TaskId(1, 1);
        final TaskId taskIdB2 = new TaskId(1, 2);
        final TaskId taskIdB3 = new TaskId(1, 3);

        final UUID uuid1 = UUID.randomUUID();

        createMockTaskManager(new HashSet<>(), new HashSet<>(), uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, new HashSet<>(), new HashSet<>()).encode()
                          ));
        subscriptions.put("consumer11",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, new HashSet<>(), new HashSet<>()).encode()
                          ));

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.assign(localMetadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partitions
        assertEquals(mkSet(mkSet(t2p2, t1p0, t1p2, t2p0), mkSet(t1p1, t2p1, t1p3, t2p3)),
                     mkSet(new HashSet<>(assignments.get("consumer10").partitions()), new HashSet<>(assignments.get("consumer11").partitions())));

        // the first consumer
        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());

        final List<TaskId> expectedInfo10TaskIds = asList(taskIdA0, taskIdA2, taskIdB0, taskIdB2);
        assertEquals(expectedInfo10TaskIds, info10.activeTasks());

        // the second consumer
        final AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        final List<TaskId> expectedInfo11TaskIds = asList(taskIdA1, taskIdA3, taskIdB1, taskIdB3);

        assertEquals(expectedInfo11TaskIds, info11.activeTasks());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testAssignWithPartialTopology() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store2", false), "processor2");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);

        final UUID uuid1 = UUID.randomUUID();

        createMockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, SingleGroupPartitionGrouperStub.class));

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        // will throw exception if it fails
        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, emptyTasks, emptyTasks).encode()
                          ));
        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assignment info
        final AssignmentInfo info10 = checkAssignment(mkSet("topic1"), assignments.get("consumer10"));
        final Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));
    }


    @Test
    public void testAssignEmptyMetadata() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);

        final Set<TaskId> prevTasks10 = mkSet(task0_0);
        final Set<TaskId> standbyTasks10 = mkSet(task0_1);
        final Cluster emptyMetadata = new Cluster("cluster", Collections.singletonList(Node.noNode()),
                                                  Collections.emptySet(),
                                                  Collections.emptySet(),
                                                  Collections.emptySet());
        final UUID uuid1 = UUID.randomUUID();

        createMockTaskManager(prevTasks10, standbyTasks10, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, prevTasks10, standbyTasks10).encode()
                          ));

        // initially metadata is empty
        Map<String, ConsumerPartitionAssignor.Assignment> assignments =
            partitionAssignor.assign(emptyMetadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check assigned partitions
        assertEquals(Collections.emptySet(),
                     new HashSet<>(assignments.get("consumer10").partitions()));

        // check assignment info
        AssignmentInfo info10 = checkAssignment(Collections.emptySet(), assignments.get("consumer10"));
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
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2", "source3");
        final List<String> topics = asList("topic1", "topic2", "topic3");
        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2, task0_3);

        // assuming that previous tasks do not have topic3
        final Set<TaskId> prevTasks10 = mkSet(task0_0);
        final Set<TaskId> prevTasks11 = mkSet(task0_1);
        final Set<TaskId> prevTasks20 = mkSet(task0_2);

        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        createMockTaskManager(prevTasks10, emptyTasks, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, prevTasks10, emptyTasks).encode()));
        subscriptions.put("consumer11",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, prevTasks11, emptyTasks).encode()));
        subscriptions.put("consumer20",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid2, prevTasks20, emptyTasks).encode()));

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

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
        builder.setApplicationId(APPLICATION_ID);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source1");
        builder.addStateStore(new MockKeyValueStoreBuilder("store1", false), "processor-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store2", false), "processor-2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store3", false), "processor-2");

        final List<String> topics = asList("topic1", "topic2");

        final TaskId task00 = new TaskId(0, 0);
        final TaskId task01 = new TaskId(0, 1);
        final TaskId task02 = new TaskId(0, 2);
        final TaskId task10 = new TaskId(1, 0);
        final TaskId task11 = new TaskId(1, 1);
        final TaskId task12 = new TaskId(1, 2);
        final List<TaskId> tasks = asList(task00, task01, task02, task10, task11, task12);

        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();

        createMockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(topics,
                                                                     getInfo(uuid1, emptyTasks, emptyTasks).encode()));
        subscriptions.put("consumer11",
                          new ConsumerPartitionAssignor.Subscription(topics,
                                                                     getInfo(uuid1, emptyTasks, emptyTasks).encode()));
        subscriptions.put("consumer20",
                          new ConsumerPartitionAssignor.Subscription(topics,
                                                                     getInfo(uuid2, emptyTasks, emptyTasks).encode()));

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

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

        assertEquals(mkSet(task00, task01, task02), tasksForState("store1", tasks, topicGroups));
        assertEquals(mkSet(task10, task11, task12), tasksForState("store2", tasks, topicGroups));
        assertEquals(mkSet(task10, task11, task12), tasksForState("store3", tasks, topicGroups));
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
    public void testAssignWithStandbyReplicas() {
        final Map<String, Object> props = configProps();
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
        final List<String> topics = asList("topic1", "topic2");
        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);


        final Set<TaskId> prevTasks00 = mkSet(task0_0);
        final Set<TaskId> prevTasks01 = mkSet(task0_1);
        final Set<TaskId> prevTasks02 = mkSet(task0_2);
        final Set<TaskId> standbyTasks01 = mkSet(task0_1);
        final Set<TaskId> standbyTasks02 = mkSet(task0_2);
        final Set<TaskId> standbyTasks00 = mkSet(task0_0);

        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();

        createMockTaskManager(prevTasks00, standbyTasks01, uuid1, builder);
        EasyMock.replay(taskManager);

        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, prevTasks00, standbyTasks01).encode()));
        subscriptions.put("consumer11",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, prevTasks01, standbyTasks02).encode()));
        subscriptions.put("consumer20",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid2, prevTasks02, standbyTasks00, "any:9097").encode()));

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments =
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
        assertEquals(mkSet(task0_0, task0_1), new HashSet<>(allActiveTasks));
        assertEquals(mkSet(task0_2), new HashSet<>(allStandbyTasks));

        // the third consumer
        final AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks());
        allStandbyTasks.addAll(info20.standbyTasks().keySet());

        // all task ids are in the active tasks and also in the standby tasks

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);

        assertEquals(3, allStandbyTasks.size());
        assertEquals(allTasks, allStandbyTasks);
    }

    @Test
    public void testOnAssignment() {
        createMockTaskManager();
        final Map<HostInfo, Set<TopicPartition>> hostState = Collections.singletonMap(
            new HostInfo("localhost", 9090),
            mkSet(t3p0, t3p3));
        taskManager.setPartitionsByHostState(hostState);
        EasyMock.expectLastCall();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(task0_0, mkSet(t3p0));
        activeTasks.put(task0_3, mkSet(t3p3));
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        standbyTasks.put(task0_1, mkSet(t3p1));
        standbyTasks.put(task0_2, mkSet(t3p2));
        taskManager.setAssignmentMetadata(activeTasks, standbyTasks);
        EasyMock.expectLastCall();

        final Capture<Cluster> capturedCluster = EasyMock.newCapture();
        taskManager.setClusterMetadata(EasyMock.capture(capturedCluster));
        EasyMock.expectLastCall();

        EasyMock.replay(taskManager);

        configurePartitionAssignor(emptyMap());
        final List<TaskId> activeTaskList = asList(task0_0, task0_3);
        final AssignmentInfo info = new AssignmentInfo(LATEST_SUPPORTED_VERSION, activeTaskList, standbyTasks, hostState, 0);
        final ConsumerPartitionAssignor.Assignment assignment = new ConsumerPartitionAssignor.Assignment(asList(t3p0, t3p3), info.encode());

        partitionAssignor.onAssignment(assignment, null);

        EasyMock.verify(taskManager);

        assertEquals(Collections.singleton(t3p0.topic()), capturedCluster.getValue().topics());
        assertEquals(2, capturedCluster.getValue().partitionsForTopic(t3p0.topic()).size());
    }

    @Test
    public void testAssignWithInternalTopics() {
        builder.setApplicationId(APPLICATION_ID);
        builder.addInternalTopic("topicX");
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addSink("sink1", "topicX", null, null, null, "processor1");
        builder.addSource(null, "source2", null, null, null, "topicX");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        final List<String> topics = asList("topic1", APPLICATION_ID + "-topicX");
        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);

        final UUID uuid1 = UUID.randomUUID();
        createMockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());
        final MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, emptyTasks, emptyTasks).encode())
        );
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check prepared internal topics
        assertEquals(1, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get(APPLICATION_ID + "-topicX"));
    }

    @Test
    public void testAssignWithInternalTopicThatsSourceIsAnotherInternalTopic() {
        final String applicationId = "test";
        builder.setApplicationId(applicationId);
        builder.addInternalTopic("topicX");
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addSink("sink1", "topicX", null, null, null, "processor1");
        builder.addSource(null, "source2", null, null, null, "topicX");
        builder.addInternalTopic("topicZ");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        builder.addSink("sink2", "topicZ", null, null, null, "processor2");
        builder.addSource(null, "source3", null, null, null, "topicZ");
        final List<String> topics = asList("topic1", "test-topicX", "test-topicZ");
        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);

        final UUID uuid1 = UUID.randomUUID();
        createMockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
        EasyMock.replay(taskManager);

        configurePartitionAssignor(emptyMap());
        final MockInternalTopicManager internalTopicManager =
            new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        subscriptions.put("consumer10",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, emptyTasks, emptyTasks).encode())
        );
        partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        // check prepared internal topics
        assertEquals(2, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get("test-topicZ"));
    }

    @Test
    public void shouldGenerateTasksForAllCreatedPartitions() {
        final StreamsBuilder builder = new StreamsBuilder();

        // KStream with 3 partitions
        final KStream<Object, Object> stream1 = builder
            .stream("topic1")
            // force creation of internal repartition topic
            .map((KeyValueMapper<Object, Object, KeyValue<Object, Object>>) KeyValue::new);

        // KTable with 4 partitions
        final KTable<Object, Long> table1 = builder
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

        final UUID uuid = UUID.randomUUID();
        final String client = "client1";
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(APPLICATION_ID);

        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), internalTopologyBuilder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            streamsConfig,
            mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        subscriptions.put(client,
                          new ConsumerPartitionAssignor.Subscription(
                              asList("topic1", "topic3"),
                              getInfo(uuid, emptyTasks, emptyTasks).encode())
        );
        final Map<String, ConsumerPartitionAssignor.Assignment> assignment =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions))
                             .groupAssignment();

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
    public void shouldAddUserDefinedEndPointToSubscription() {
        builder.setApplicationId(APPLICATION_ID);
        builder.addSource(null, "source", null, null, null, "input");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        final UUID uuid1 = UUID.randomUUID();
        createMockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT));
        final Set<String> topics = mkSet("input");
        final ByteBuffer userData = partitionAssignor.subscriptionUserData(topics);
        final ConsumerPartitionAssignor.Subscription subscription =
            new ConsumerPartitionAssignor.Subscription(new ArrayList<>(topics), userData);
        final SubscriptionInfo subscriptionInfo = SubscriptionInfo.decode(subscription.userData());
        assertEquals("localhost:8080", subscriptionInfo.userEndPoint());
    }

    @Test
    public void shouldMapUserEndPointToTopicPartitions() {
        builder.setApplicationId(APPLICATION_ID);
        builder.addSource(null, "source", null, null, null, "topic1");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        final List<String> topics = Collections.singletonList("topic1");

        final UUID uuid1 = UUID.randomUUID();

        createMockTaskManager(emptyTasks, emptyTasks, uuid1, builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT));

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        subscriptions.put("consumer1",
                          new ConsumerPartitionAssignor.Subscription(
                              topics,
                              getInfo(uuid1, emptyTasks, emptyTasks).encode())
        );
        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        final ConsumerPartitionAssignor.Assignment consumerAssignment = assignments.get("consumer1");
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
        builder.setApplicationId(APPLICATION_ID);

        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        try {
            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost"));
            fail("expected to an exception due to invalid config");
        } catch (final ConfigException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowExceptionIfApplicationServerConfigPortIsNotAnInteger() {
        builder.setApplicationId(APPLICATION_ID);

        try {
            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:j87yhk"));
            fail("expected to an exception due to invalid config");
        } catch (final ConfigException e) {
            // pass
        }
    }

    @Test
    public void shouldNotLoopInfinitelyOnMissingMetadataAndShouldNotCreateRelatedTasks() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Object, Object> stream1 = builder

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

        builder
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

        final UUID uuid = UUID.randomUUID();
        final String client = "client1";

        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(APPLICATION_ID);

        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), internalTopologyBuilder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            streamsConfig,
            mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        subscriptions.put(client,
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("unknownTopic"),
                              getInfo(uuid, emptyTasks, emptyTasks).encode())
        );
        final Map<String, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(mockInternalTopicManager.readyTopics.isEmpty(), equalTo(true));

        assertThat(assignment.get(client).partitions().isEmpty(), equalTo(true));
    }

    @Test
    public void shouldUpdateClusterMetadataAndHostInfoOnAssignment() {
        final TopicPartition partitionOne = new TopicPartition("topic", 1);
        final TopicPartition partitionTwo = new TopicPartition("topic", 2);
        final Map<HostInfo, Set<TopicPartition>> hostState = Collections.singletonMap(
            new HostInfo("localhost", 9090), mkSet(partitionOne, partitionTwo));

        final StreamsBuilder builder = new StreamsBuilder();
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(APPLICATION_ID);
        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), internalTopologyBuilder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(emptyMap());

        partitionAssignor.onAssignment(createAssignment(hostState), null);

        EasyMock.verify(taskManager);
    }

    @Test
    public void shouldNotAddStandbyTaskPartitionsToPartitionsForHost() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("topic1").groupByKey().count();
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(APPLICATION_ID);


        final UUID uuid = UUID.randomUUID();
        createMockTaskManager(emptyTasks, emptyTasks, uuid, internalTopologyBuilder);
        EasyMock.replay(taskManager);

        final Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, USER_END_POINT);
        configurePartitionAssignor(props);
        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(
            streamsConfig,
            mockClientSupplier.restoreConsumer));

        subscriptions.put("consumer1",
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(uuid, emptyTasks, emptyTasks).encode())
        );
        subscriptions.put("consumer2",
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(UUID.randomUUID(), emptyTasks, emptyTasks, "other:9090").encode())
        );
        final Set<TopicPartition> allPartitions = mkSet(t1p0, t1p1, t1p2);
        final Map<String, ConsumerPartitionAssignor.Assignment> assign = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
        final ConsumerPartitionAssignor.Assignment consumer1Assignment = assign.get("consumer1");
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumer1Assignment.userData());
        final Set<TopicPartition> consumer1partitions = assignmentInfo.partitionsByHost().get(new HostInfo("localhost", 8080));
        final Set<TopicPartition> consumer2Partitions = assignmentInfo.partitionsByHost().get(new HostInfo("other", 9090));
        final HashSet<TopicPartition> allAssignedPartitions = new HashSet<>(consumer1partitions);
        allAssignedPartitions.addAll(consumer2Partitions);
        assertThat(consumer1partitions, not(allPartitions));
        assertThat(consumer2Partitions, not(allPartitions));
        assertThat(allAssignedPartitions, equalTo(allPartitions));
    }

    @Test
    public void shouldThrowKafkaExceptionIfTaskMangerNotConfigured() {
        final Map<String, Object> config = configProps();
        config.remove(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR);

        try {
            partitionAssignor.configure(config);
            fail("Should have thrown KafkaException");
        } catch (final KafkaException expected) {
            assertThat(expected.getMessage(), equalTo("TaskManager is not specified"));
        }
    }

    @Test
    public void shouldThrowKafkaExceptionIfTaskMangerConfigIsNotTaskManagerInstance() {
        final Map<String, Object> config = configProps();
        config.put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, "i am not a task manager");

        try {
            partitionAssignor.configure(config);
            fail("Should have thrown KafkaException");
        } catch (final KafkaException expected) {
            assertThat(expected.getMessage(),
                       equalTo("java.lang.String is not an instance of org.apache.kafka.streams.processor.internals.TaskManager"));
        }
    }

    @Test
    public void shouldThrowKafkaExceptionAssignmentErrorCodeNotConfigured() {
        createMockTaskManager();
        final Map<String, Object> config = configProps();
        config.remove(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE);

        try {
            partitionAssignor.configure(config);
            fail("Should have thrown KafkaException");
        } catch (final KafkaException expected) {
            assertThat(expected.getMessage(), equalTo("assignmentErrorCode is not specified"));
        }
    }

    @Test
    public void shouldThrowKafkaExceptionIfVersionProbingFlagConfigIsNotAtomicInteger() {
        createMockTaskManager();
        final Map<String, Object> config = configProps();
        config.put(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE, "i am not an AtomicInteger");

        try {
            partitionAssignor.configure(config);
            fail("Should have thrown KafkaException");
        } catch (final KafkaException expected) {
            assertThat(expected.getMessage(),
                       equalTo("java.lang.String is not an instance of java.util.concurrent.atomic.AtomicInteger"));
        }
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
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(smallestVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode())
        );
        subscriptions.put("consumer2",
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(otherVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode()
                          )
        );

        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        partitionAssignor.configure(configProps());
        final Map<String, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));
        assertThat(AssignmentInfo.decode(assignment.get("consumer1").userData()).version(), equalTo(smallestVersion));
        assertThat(AssignmentInfo.decode(assignment.get("consumer2").userData()).version(), equalTo(smallestVersion));
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion1() {
        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_0100));

        final Set<String> topics = mkSet("topic1");
        final ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

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
        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFromValue));

        final Set<String> topics = mkSet("topic1");
        final ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(new ArrayList<>(topics), partitionAssignor.subscriptionUserData(topics));

        assertThat(SubscriptionInfo.decode(subscription.userData()).version(), equalTo(2));
    }

    @Test
    public void shouldReturnInterleavedAssignmentWithUnrevokedPartitionsRemovedWhenNewConsumerJoins() {
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);
        final Map<TaskId, Integer> allTaskLags = mkMap(
            mkEntry(task0_0, 0),
            mkEntry(task0_1, 0),
            mkEntry(task0_2, 0)
        );

        subscriptions.put(CONSUMER_1,
                          new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList("topic1"),
                getInfo(UUID.randomUUID(), allTasks, Collections.emptySet(), null).encode(),
                asList(t1p0, t1p1, t1p2))
        );
        subscriptions.put(CONSUMER_2,
                          new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList("topic1"),
                getInfo(UUID.randomUUID(), Collections.emptySet(), Collections.emptySet(), null).encode(),
                emptyList())
        );

        createMockTaskManager(allTasks, allTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        partitionAssignor.configure(configProps());

        final Map<String, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));

        assertThat(assignment.get(CONSUMER_1).partitions(), equalTo(asList(t1p0, t1p2)));
        final AssignmentInfo decode = AssignmentInfo.decode(assignment.get(CONSUMER_1).userData());
        assertThat(
            decode,
            equalTo(new AssignmentInfo(LATEST_SUPPORTED_VERSION, asList(task0_0, task0_2), emptyMap(), emptyMap(), 0)));

        // The new consumer's assignment should be empty until c1 has the chance to revoke its partitions/tasks
        assertThat(assignment.get(CONSUMER_2).partitions(), equalTo(emptyList()));
        assertThat(
            AssignmentInfo.decode(assignment.get(CONSUMER_2).userData()),
            equalTo(new AssignmentInfo(LATEST_SUPPORTED_VERSION, emptyList(), emptyMap(), emptyMap(), 0)));
    }

    @Test
    public void shouldReturnNormalAssignmentForOldAndFutureInstancesDuringVersionProbing() {
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);

        final Set<TaskId> activeTasks = mkSet(task0_0, task0_1);
        final Set<TaskId> standbyTasks = mkSet(task0_2);
        final Map<TaskId, Set<TopicPartition>> standbyTaskMap = mkMap(
            mkEntry(task0_2, Collections.singleton(t1p2))
        );
        final Map<TaskId, Set<TopicPartition>> futureStandbyTaskMap = mkMap(
            mkEntry(task0_0, Collections.singleton(t1p0)),
            mkEntry(task0_1, Collections.singleton(t1p1))
        );

        subscriptions.put("consumer1",
                new ConsumerPartitionAssignor.Subscription(
                        Collections.singletonList("topic1"),
                        getInfo(UUID.randomUUID(), activeTasks, standbyTasks, null).encode(),
                        asList(t1p0, t1p1))
        );
        subscriptions.put("future-consumer",
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription(),
                              Collections.singletonList(t1p2))
        );

        createMockTaskManager(allTasks, allTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        final Map<String, Object> props = configProps();
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        partitionAssignor.configure(props);
        final Map<String, ConsumerPartitionAssignor.Assignment> assignment = partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));

        assertThat(assignment.get("consumer1").partitions(), equalTo(asList(t1p0, t1p1)));
        assertThat(
            AssignmentInfo.decode(assignment.get("consumer1").userData()),
            equalTo(
                new AssignmentInfo(
                    LATEST_SUPPORTED_VERSION,
                    new ArrayList<>(activeTasks),
                    standbyTaskMap,
                    emptyMap(),
                    0
                )
            )
        );


        assertThat(assignment.get("future-consumer").partitions(), equalTo(Collections.singletonList(t1p2)));
        assertThat(
            AssignmentInfo.decode(assignment.get("future-consumer").userData()),
            equalTo(
                new AssignmentInfo(
                    LATEST_SUPPORTED_VERSION,
                    Collections.singletonList(task0_2),
                    futureStandbyTaskMap,
                    emptyMap(),
                    0)
            )
        );
    }

    @Test
    public void shouldReturnInterleavedAssignmentForOnlyFutureInstancesDuringVersionProbing() {
        builder.addSource(null, "source1", null, null, null, "topic1");

        final Set<TaskId> allTasks = mkSet(task0_0, task0_1, task0_2);

        subscriptions.put(CONSUMER_1,
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription(),
                              emptyList())
        );
        subscriptions.put(CONSUMER_2,
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription(),
                              emptyList())
        );

        createMockTaskManager(allTasks, allTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        final Map<String, Object> props = configProps();
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        partitionAssignor.configure(props);
        final Map<String, ConsumerPartitionAssignor.Assignment> assignment =
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();

        assertThat(assignment.size(), equalTo(2));

        assertThat(assignment.get(CONSUMER_1).partitions(), equalTo(asList(t1p0, t1p2)));
        assertThat(
            AssignmentInfo.decode(assignment.get(CONSUMER_1).userData()),
            equalTo(new AssignmentInfo(LATEST_SUPPORTED_VERSION, asList(task0_0, task0_2), emptyMap(), emptyMap(), 0)));


        assertThat(assignment.get(CONSUMER_2).partitions(), equalTo(Collections.singletonList(t1p1)));
        assertThat(
            AssignmentInfo.decode(assignment.get(CONSUMER_2).userData()),
            equalTo(new AssignmentInfo(LATEST_SUPPORTED_VERSION, Collections.singletonList(task0_1), emptyMap(), emptyMap(), 0)));
    }

    @Test
    public void shouldThrowIfV1SubscriptionAndFutureSubscriptionIsMixed() {
        shouldThrowIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(1);
    }

    @Test
    public void shouldThrowIfV2SubscriptionAndFutureSubscriptionIsMixed() {
        shouldThrowIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(2);
    }

    private static ByteBuffer encodeFutureSubscription() {
        final ByteBuffer buf = ByteBuffer.allocate(4 /* used version */ + 4 /* supported version */);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        return buf;
    }

    private void shouldThrowIfPreVersionProbingSubscriptionAndFutureSubscriptionIsMixed(final int oldVersion) {
        subscriptions.put("consumer1",
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              getInfo(oldVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode())
        );
        subscriptions.put("future-consumer",
                          new ConsumerPartitionAssignor.Subscription(
                              Collections.singletonList("topic1"),
                              encodeFutureSubscription())
        );

        createMockTaskManager(emptyTasks, emptyTasks, UUID.randomUUID(), builder);
        EasyMock.replay(taskManager);
        partitionAssignor.configure(configProps());

        try {
            partitionAssignor.assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
            fail("Should have thrown IllegalStateException");
        } catch (final IllegalStateException expected) {
            // pass
        }
    }

    private static ConsumerPartitionAssignor.Assignment createAssignment(final Map<HostInfo, Set<TopicPartition>> firstHostState) {
        final AssignmentInfo info = new AssignmentInfo(LATEST_SUPPORTED_VERSION, emptyList(), emptyMap(), firstHostState, 0);

        return new ConsumerPartitionAssignor.Assignment(emptyList(), info.encode());
    }

    private static AssignmentInfo checkAssignment(final Set<String> expectedTopics,
                                                  final ConsumerPartitionAssignor.Assignment assignment) {

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

}
