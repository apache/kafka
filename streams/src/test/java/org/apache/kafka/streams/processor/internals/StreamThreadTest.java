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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilderTest;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static java.util.Collections.EMPTY_SET;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamThreadTest {

    private final String clientId = "clientId";
    private final String applicationId = "stream-thread-test";
    private final MockTime mockTime = new MockTime();
    private final Metrics metrics = new Metrics();
    private MockClientSupplier clientSupplier = new MockClientSupplier();
    private UUID processId = UUID.randomUUID();
    private final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());
    private InternalTopologyBuilder internalTopologyBuilder;
    private final StreamsConfig config = new StreamsConfig(configProps(false));
    private final String stateDir = TestUtils.tempDirectory().getPath();
    private final StateDirectory stateDirectory  = new StateDirectory("applicationId", stateDir, mockTime);
    private StreamsMetadataState streamsMetadataState;
    private final ConsumedInternal<Object, Object> consumed = new ConsumedInternal<>();

    @Before
    public void setUp() {
        processId = UUID.randomUUID();

        internalTopologyBuilder = InternalStreamsBuilderTest.internalTopologyBuilder(internalStreamsBuilder);
        internalTopologyBuilder.setApplicationId(applicationId);
        streamsMetadataState = new StreamsMetadataState(internalTopologyBuilder, StreamsMetadataState.UNKNOWN_HOST);
    }

    private final TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private final TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private final TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private final TopicPartition t2p2 = new TopicPartition("topic2", 2);
    private final TopicPartition t3p1 = new TopicPartition("topic3", 1);
    private final TopicPartition t3p2 = new TopicPartition("topic3", 2);

    // task0 is unused
    private final TaskId task1 = new TaskId(0, 1);
    private final TaskId task2 = new TaskId(0, 2);
    private final TaskId task3 = new TaskId(1, 1);
    private final TaskId task4 = new TaskId(1, 2);

    private Properties configProps(final boolean enableEos) {
        return new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
                setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
                if (enableEos) {
                    setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
                }
            }
        };
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChangeForSingleGroup() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, "topic1");

        final StreamThread thread = getStreamThread();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        thread.setThreadMetadataProvider(new StreamPartitionAssignor() {
            @Override
            public Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.CREATED);

        final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;
        thread.setState(StreamThread.State.RUNNING);
        assertTrue(thread.tasks().isEmpty());

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;
        Set<TopicPartition> expectedGroup1;
        Set<TopicPartition> expectedGroup2;

        // revoke nothing
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);

        // assign single partition
        assignedPartitions = Collections.singletonList(t1p1);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        activeTasks.put(new TaskId(0, 1), expectedGroup1);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        Assert.assertEquals(4, stateListener.numChanges);
        Assert.assertEquals(StreamThread.State.PARTITIONS_ASSIGNED, stateListener.oldState);
        assertTrue(thread.tasks().containsKey(task1));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(1, thread.tasks().size());

        // revoke single partition
        revokedPartitions = assignedPartitions;
        activeTasks.clear();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertFalse(thread.tasks().containsKey(task1));
        assertEquals(0, thread.tasks().size());

        // assign different single partition
        assignedPartitions = Collections.singletonList(t1p2);
        expectedGroup2 = new HashSet<>(Collections.singleton(t1p2));
        activeTasks.put(new TaskId(0, 2), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);
        assertTrue(thread.tasks().containsKey(task2));
        assertEquals(expectedGroup2, thread.tasks().get(task2).partitions());
        assertEquals(1, thread.tasks().size());

        // revoke different single partition and assign both partitions
        revokedPartitions = assignedPartitions;
        activeTasks.clear();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Arrays.asList(t1p1, t1p2);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Collections.singleton(t1p2));
        activeTasks.put(new TaskId(0, 1), expectedGroup1);
        activeTasks.put(new TaskId(0, 2), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);
        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task2));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task2).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke all partitions and assign nothing
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);
        assertTrue(thread.tasks().isEmpty());

        thread.shutdown();
        assertTrue(thread.state() == StreamThread.State.PENDING_SHUTDOWN);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChangeForMultipleGroups() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, "topic1");
        internalTopologyBuilder.addSource(null, "source2", null, null, null, "topic2");
        internalTopologyBuilder.addSource(null, "source3", null, null, null, "topic3");
        internalTopologyBuilder.addProcessor("processor", new MockProcessorSupplier(), "source2", "source3");

        final StreamThread thread = getStreamThread();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        thread.setThreadMetadataProvider(new StreamPartitionAssignor() {
            @Override
            public Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.CREATED);

        final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;
        thread.setState(StreamThread.State.RUNNING);
        assertTrue(thread.tasks().isEmpty());

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;
        Set<TopicPartition> expectedGroup1;
        Set<TopicPartition> expectedGroup2;

        // revoke nothing
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);

        // assign four new partitions of second subtopology
        assignedPartitions = Arrays.asList(t2p1, t2p2, t3p1, t3p2);
        expectedGroup1 = new HashSet<>(Arrays.asList(t2p1, t3p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p2, t3p2));
        activeTasks.put(new TaskId(1, 1), expectedGroup1);
        activeTasks.put(new TaskId(1, 2), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);

        assertTrue(thread.tasks().containsKey(task3));
        assertTrue(thread.tasks().containsKey(task4));
        assertEquals(expectedGroup1, thread.tasks().get(task3).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task4).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke four partitions and assign three partitions of both subtopologies
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assignedPartitions = Arrays.asList(t1p1, t2p1, t3p1);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p1, t3p1));
        activeTasks.put(new TaskId(0, 1), expectedGroup1);
        activeTasks.put(new TaskId(1, 1), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task3));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task3).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke all three partitons and reassign the same three partitions (from different subtopologies)
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Arrays.asList(t1p1, t2p1, t3p1);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p1, t3p1));
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task3));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task3).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke all partitions and assign nothing
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);

        assertTrue(thread.tasks().isEmpty());

        thread.shutdown();
        assertEquals(thread.state(), StreamThread.State.PENDING_SHUTDOWN);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStateChangeStartClose() throws InterruptedException {

        final StreamThread thread = createStreamThread(clientId, config, false);

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        thread.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return thread.state() == StreamThread.State.RUNNING;
            }
        }, 10 * 1000, "Thread never started.");
        thread.shutdown();
        assertEquals(thread.state(), StreamThread.State.PENDING_SHUTDOWN);
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return thread.state() == StreamThread.State.DEAD;
            }
        }, 10 * 1000, "Thread never shut down.");
        thread.shutdown();
        assertEquals(thread.state(), StreamThread.State.DEAD);
    }

    private StreamThread createStreamThread(final String clientId, final StreamsConfig config, final boolean eosEnabled) {
        if (eosEnabled) {
            clientSupplier = new MockClientSupplier(applicationId);
        }
        return StreamThread.create(internalTopologyBuilder,
                                   config,
                                   clientSupplier,
                                   processId,
                                   clientId,
                                   metrics,
                                   mockTime,
                                   streamsMetadataState,
                                   0,
                                   stateDirectory,
                                   new MockStateRestoreListener());
    }

    private final static String TOPIC = "topic";
    private final Set<TopicPartition> task0Assignment = Collections.singleton(new TopicPartition(TOPIC, 0));
    private final Set<TopicPartition> task1Assignment = Collections.singleton(new TopicPartition(TOPIC, 1));

    @SuppressWarnings("unchecked")
    @Test
    public void testHandingOverTaskFromOneToAnotherThread() throws InterruptedException {
        internalTopologyBuilder.addStateStore(
            Stores
                .create("store")
                .withByteArrayKeys()
                .withByteArrayValues()
                .persistent()
                .build()
        );
        internalTopologyBuilder.addSource(null, "source", null, null, null, TOPIC);

        TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        TopicPartition tp1 = new TopicPartition(TOPIC, 1);
        clientSupplier.consumer.assign(Arrays.asList(tp0, tp1));
        final Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp0, 0L);
        offsets.put(tp1, 0L);
        clientSupplier.consumer.updateBeginningOffsets(offsets);

        final StreamThread thread1 = createStreamThread(clientId + 1, config, false);
        final StreamThread thread2 = createStreamThread(clientId + 2, config, false);


        final Map<TaskId, Set<TopicPartition>> task0 = Collections.singletonMap(new TaskId(0, 0), task0Assignment);
        final Map<TaskId, Set<TopicPartition>> task1 = Collections.singletonMap(new TaskId(0, 1), task1Assignment);

        final Map<TaskId, Set<TopicPartition>> thread1Assignment = new HashMap<>(task0);
        final Map<TaskId, Set<TopicPartition>> thread2Assignment = new HashMap<>(task1);

        thread1.setThreadMetadataProvider(new MockStreamsPartitionAssignor(thread1Assignment));
        thread2.setThreadMetadataProvider(new MockStreamsPartitionAssignor(thread2Assignment));

        // revoke (to get threads in correct state)
        thread1.setState(StreamThread.State.RUNNING);
        thread2.setState(StreamThread.State.RUNNING);
        thread1.rebalanceListener.onPartitionsRevoked(EMPTY_SET);
        thread2.rebalanceListener.onPartitionsRevoked(EMPTY_SET);

        // assign
        thread1.rebalanceListener.onPartitionsAssigned(task0Assignment);
        thread1.runOnce(-1);
        thread2.rebalanceListener.onPartitionsAssigned(task1Assignment);
        thread2.runOnce(-1);

        final Set<TaskId> originalTaskAssignmentThread1 = new HashSet<>();
        originalTaskAssignmentThread1.addAll(thread1.tasks().keySet());
        final Set<TaskId> originalTaskAssignmentThread2 = new HashSet<>();
        originalTaskAssignmentThread2.addAll(thread2.tasks().keySet());

        // revoke (task will be suspended)
        thread1.rebalanceListener.onPartitionsRevoked(task0Assignment);
        thread2.rebalanceListener.onPartitionsRevoked(task1Assignment);

        assertThat(thread1.prevActiveTasks(), equalTo(originalTaskAssignmentThread1));
        assertThat(thread2.prevActiveTasks(), equalTo(originalTaskAssignmentThread2));

        // assign reverted
        thread1Assignment.clear();
        thread1Assignment.putAll(task1);

        thread2Assignment.clear();
        thread2Assignment.putAll(task0);

        final Thread runIt = new Thread(new Runnable() {
            @Override
            public void run() {
                thread1.rebalanceListener.onPartitionsAssigned(task1Assignment);
                thread1.runOnce(-1);
            }
        });
        runIt.start();

        thread2.rebalanceListener.onPartitionsAssigned(task0Assignment);
        thread2.runOnce(-1);

        runIt.join();

        assertThat(thread1.tasks().keySet(), equalTo(originalTaskAssignmentThread2));
        assertThat(thread2.tasks().keySet(), equalTo(originalTaskAssignmentThread1));
    }

    private class MockStreamsPartitionAssignor extends StreamPartitionAssignor {

        private final Map<TaskId, Set<TopicPartition>> activeTaskAssignment;
        private final Map<TaskId, Set<TopicPartition>> standbyTaskAssignment;

        MockStreamsPartitionAssignor(final Map<TaskId, Set<TopicPartition>> activeTaskAssignment) {
            this(activeTaskAssignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        }

        MockStreamsPartitionAssignor(final Map<TaskId, Set<TopicPartition>> activeTaskAssignment,
                                     final Map<TaskId, Set<TopicPartition>> standbyTaskAssignment) {
            this.activeTaskAssignment = activeTaskAssignment;
            this.standbyTaskAssignment = standbyTaskAssignment;
        }

        @Override
        public Map<TaskId, Set<TopicPartition>> activeTasks() {
            return activeTaskAssignment;
        }

        @Override
        public Map<TaskId, Set<TopicPartition>> standbyTasks() {
            return standbyTaskAssignment;
        }

        @Override
        public void close() {}
    }

    @Test
    public void testMetrics() {
        final StreamThread thread = createStreamThread(clientId, config, false);
        final String defaultGroupName = "stream-metrics";
        final String defaultPrefix = "thread." + thread.threadClientId();
        final Map<String, String> defaultTags = Collections.singletonMap("client-id", thread.threadClientId());

        assertNotNull(metrics.getSensor(defaultPrefix + ".commit-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".poll-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".process-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".punctuate-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".task-created"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".task-closed"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".skipped-records"));

        assertNotNull(metrics.metrics().get(metrics.metricName("commit-latency-avg", defaultGroupName, "The average commit time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("commit-latency-max", defaultGroupName, "The maximum commit time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("commit-rate", defaultGroupName, "The average per-second number of commit calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-latency-avg", defaultGroupName, "The average poll time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-latency-max", defaultGroupName, "The maximum poll time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-rate", defaultGroupName, "The average per-second number of record-poll calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-latency-avg", defaultGroupName, "The average process time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-latency-max", defaultGroupName, "The maximum process time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-rate", defaultGroupName, "The average per-second number of process calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-latency-avg", defaultGroupName, "The average punctuate time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-latency-max", defaultGroupName, "The maximum punctuate time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-rate", defaultGroupName, "The average per-second number of punctuate calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-created-rate", defaultGroupName, "The average per-second number of newly created tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-closed-rate", defaultGroupName, "The average per-second number of closed tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("skipped-records-rate", defaultGroupName, "The average per-second number of skipped records.", defaultTags)));
    }


    @SuppressWarnings({"unchecked", "ThrowableNotThrown"})
    @Test
    public void shouldNotCommitBeforeTheCommitInterval() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 1);

        StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "", "", Collections.<String, String>emptyMap());
        final StreamThread thread = new StreamThread(internalTopologyBuilder,
                                                     clientId,
                                                     "",
                                                     config,
                                                     processId,
                                                     mockTime,
                                                     streamsMetadataState,
                                                     taskManager,
                                                     streamsMetrics,
                                                     clientSupplier,
                                                     consumer,
                                                     stateDirectory);
        thread.maybeCommit(mockTime.milliseconds());
        mockTime.sleep(commitInterval - 10L);
        thread.maybeCommit(mockTime.milliseconds());

        EasyMock.verify(taskManager);
    }

    @SuppressWarnings({"unchecked", "ThrowableNotThrown"})
    @Test
    public void shouldNotCauseExceptionIfNothingCommited() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);

        StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "", "", Collections.<String, String>emptyMap());
        final StreamThread thread = new StreamThread(internalTopologyBuilder,
                                                     clientId,
                                                     "",
                                                     config,
                                                     processId,
                                                     mockTime,
                                                     streamsMetadataState,
                                                     taskManager,
                                                     streamsMetrics,
                                                     clientSupplier,
                                                     consumer,
                                                     stateDirectory);
        thread.maybeCommit(mockTime.milliseconds());
        mockTime.sleep(commitInterval - 10L);
        thread.maybeCommit(mockTime.milliseconds());

        EasyMock.verify(taskManager);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void shouldCommitAfterTheCommitInterval() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = mockTaskManagerCommit(consumer, 2, 1);

        StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "", "", Collections.<String, String>emptyMap());
        final StreamThread thread = new StreamThread(internalTopologyBuilder,
                                                     clientId,
                                                     "",
                                                     config,
                                                     processId,
                                                     mockTime,
                                                     streamsMetadataState,
                                                     taskManager,
                                                     streamsMetrics,
                                                     clientSupplier,
                                                     consumer,
                                                     stateDirectory);
        thread.maybeCommit(mockTime.milliseconds());
        mockTime.sleep(commitInterval + 1);
        thread.maybeCommit(mockTime.milliseconds());

        EasyMock.verify(taskManager);
    }

    @SuppressWarnings({"ThrowableNotThrown", "unchecked"})
    private TaskManager mockTaskManagerCommit(final Consumer<byte[], byte[]> consumer, final int numberOfCommits, final int commits) {
        final TaskManager taskManager = EasyMock.createMock(TaskManager.class);
        taskManager.setConsumer(EasyMock.anyObject(Consumer.class));
        EasyMock.expectLastCall();
        EasyMock.expect(taskManager.commitAll()).andReturn(commits).times(numberOfCommits);
        EasyMock.replay(taskManager, consumer);
        return taskManager;
    }

    @Test
    public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, "someTopic");

        final StreamThread thread = createStreamThread(clientId, config, false);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(new TaskId(0, 0), Collections.singleton(new TopicPartition("someTopic", 0)));
        assignment.put(new TaskId(0, 1), Collections.singleton(new TopicPartition("someTopic", 1)));
        thread.setThreadMetadataProvider(new MockStreamsPartitionAssignor(assignment));

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Collections.singleton(new TopicPartition("someTopic", 0)));

        assertEquals(1, clientSupplier.producers.size());
        final Producer globalProducer = clientSupplier.producers.get(0);
        for (final Task task : thread.tasks().values()) {
            assertSame(globalProducer, ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    @Test
    public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, "someTopic");

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(new TaskId(0, 0), Collections.singleton(new TopicPartition("someTopic", 0)));
        assignment.put(new TaskId(0, 1), Collections.singleton(new TopicPartition("someTopic", 1)));
        assignment.put(new TaskId(0, 2), Collections.singleton(new TopicPartition("someTopic", 2)));
        thread.setThreadMetadataProvider(new MockStreamsPartitionAssignor(assignment));

        final Set<TopicPartition> assignedPartitions = new HashSet<>();
        Collections.addAll(assignedPartitions, new TopicPartition("someTopic", 0), new TopicPartition("someTopic", 2));
        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);

        assertEquals(thread.tasks().size(), clientSupplier.producers.size());
        final Iterator it = clientSupplier.producers.iterator();
        for (final Task task : thread.tasks().values()) {
            assertSame(it.next(), ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    @Test
    public void shouldCloseAllTaskProducersOnCloseIfEosEnabled() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, "someTopic");

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(new TaskId(0, 0), Collections.singleton(new TopicPartition("someTopic", 0)));
        assignment.put(new TaskId(0, 1), Collections.singleton(new TopicPartition("someTopic", 1)));
        thread.setThreadMetadataProvider(new MockStreamsPartitionAssignor(assignment));

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Collections.singleton(new TopicPartition("someTopic", 0)));

        thread.shutdown();
        thread.run();

        for (final Task task : thread.tasks().values()) {
            assertTrue(((MockProducer) ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer()).closed());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldShutdownTaskManagerOnClose() throws InterruptedException {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        taskManager.setConsumer(EasyMock.anyObject(Consumer.class));
        EasyMock.expectLastCall();
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "", "", Collections.<String, String>emptyMap());
        final StreamThread thread = new StreamThread(internalTopologyBuilder,
                                                     clientId,
                                                     "",
                                                     config,
                                                     processId,
                                                     mockTime,
                                                     streamsMetadataState,
                                                     taskManager,
                                                     streamsMetrics,
                                                     clientSupplier,
                                                     consumer,
                                                     stateDirectory);
        thread.setState(StreamThread.State.RUNNING);
        thread.shutdown();
        thread.run();
        EasyMock.verify(taskManager);
    }

    @Test
    public void shouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "name", null, null, null, "topic");
        internalTopologyBuilder.addSink("out", "output", null, null, null);

        final StreamThread thread = createStreamThread(clientId, config, false);

        thread.setThreadMetadataProvider(new StreamPartitionAssignor() {
            @Override
            public Map<TaskId, Set<TopicPartition>> standbyTasks() {
                return Collections.singletonMap(new TaskId(0, 0), Utils.mkSet(new TopicPartition("topic", 0)));
            }
        });

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());
    }

    @Test
    public void shouldCloseSuspendedTasksThatAreNoLongerAssignedToThisStreamThreadBeforeCreatingNewTasks() {
        internalStreamsBuilder.stream(Collections.singleton("t1"), consumed).groupByKey().count("count-one");
        internalStreamsBuilder.stream(Collections.singleton("t2"), consumed).groupByKey().count("count-two");

        final StreamThread thread = createStreamThread(clientId, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                                         Collections.singletonList(new PartitionInfo("stream-thread-test-count-one-changelog",
                                                                                     0,
                                                                                     null,
                                                                                     new Node[0],
                                                                                     new Node[0])));
        restoreConsumer.updatePartitions("stream-thread-test-count-two-changelog",
                                         Collections.singletonList(new PartitionInfo("stream-thread-test-count-two-changelog",
                                                                                     0,
                                                                                     null,
                                                                                     new Node[0],
                                                                                     new Node[0])));


        final HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L);
        offsets.put(new TopicPartition("stream-thread-test-count-two-changelog", 0), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        final TopicPartition t1 = new TopicPartition("t1", 0);
        Set<TopicPartition> partitionsT1 = Utils.mkSet(t1);
        standbyTasks.put(new TaskId(0, 0), partitionsT1);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final TopicPartition t2 = new TopicPartition("t2", 0);
        Set<TopicPartition> partitionsT2 = Utils.mkSet(t2);
        activeTasks.put(new TaskId(1, 0), partitionsT2);
        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t2, 0L));

        thread.setThreadMetadataProvider(new StreamPartitionAssignor() {
            @Override
            public Map<TaskId, Set<TopicPartition>> standbyTasks() {
                return standbyTasks;
            }

            @Override
            public Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        clientSupplier.consumer.assign(partitionsT2);
        thread.rebalanceListener.onPartitionsAssigned(Utils.mkSet(t2));
        thread.runOnce(-1);
        // swap the assignment around and make sure we don't get any exceptions
        standbyTasks.clear();
        activeTasks.clear();
        standbyTasks.put(new TaskId(1, 0), Utils.mkSet(t2));
        activeTasks.put(new TaskId(0, 0), Utils.mkSet(t1));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        clientSupplier.consumer.assign(partitionsT1);
        thread.rebalanceListener.onPartitionsAssigned(Utils.mkSet(t1));
    }

    @Test
    public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerWasFencedWhileProcessing() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source", null, null, null, TOPIC);
        internalTopologyBuilder.addSink("sink", "dummyTopic", null, null, null, "source");

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        final MockConsumer consumer = clientSupplier.consumer;
        consumer.updatePartitions(TOPIC, Collections.singletonList(new PartitionInfo(TOPIC, 0, null, null, null)));

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(task1, task0Assignment);

        thread.setThreadMetadataProvider(new MockStreamsPartitionAssignor(activeTasks));

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(null);
        thread.rebalanceListener.onPartitionsAssigned(task0Assignment);
        thread.runOnce(-1);
        assertThat(thread.tasks().size(), equalTo(1));
        final MockProducer producer = clientSupplier.producers.get(0);



        // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
        consumer.updateBeginningOffsets(Collections.singletonMap(task0Assignment.iterator().next(), 0L));
        consumer.unsubscribe();
        consumer.assign(task0Assignment);

        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[0], new byte[0]));
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1);
        thread.runOnce(-1);
        assertThat(producer.history().size(), equalTo(1));

        assertFalse(producer.transactionCommitted());
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        TestUtils.waitForCondition(
            new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return producer.commitCount() == 1;
                }
            },
            "StreamsThread did not commit transaction.");

        producer.fenceProducer();
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[0], new byte[0]));
        try {
            thread.runOnce(-1);
            fail("Should have thrown TaskMigratedException");
        } catch (final TaskMigratedException expected) { /* ignore */ }
        TestUtils.waitForCondition(
            new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return thread.tasks().isEmpty();
                }
            },
            "StreamsThread did not remove fenced zombie task.");

        assertThat(producer.commitCount(), equalTo(1L));
    }

    @Test
    public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedAtBeginTransactionWhenTaskIsResumed() {
        internalTopologyBuilder.addSource(null, "name", null, null, null, "topic");
        internalTopologyBuilder.addSink("out", "output", null, null, null);

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(task1, task0Assignment);

        thread.setThreadMetadataProvider(new MockStreamsPartitionAssignor(activeTasks));

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(null);
        thread.rebalanceListener.onPartitionsAssigned(task0Assignment);
        thread.runOnce(-1);

        assertThat(thread.tasks().size(), equalTo(1));

        thread.rebalanceListener.onPartitionsRevoked(null);
        clientSupplier.producers.get(0).fenceProducer();
        thread.rebalanceListener.onPartitionsAssigned(task0Assignment);
        try {
            thread.runOnce(-1);
            fail("Should have thrown TaskMigratedException");
        } catch (final TaskMigratedException expected) { /* ignore */ }

        assertTrue(thread.tasks().isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldAlwaysUpdateWithLatestTopicsFromStreamPartitionAssignor() throws Exception {
        internalTopologyBuilder.addSource(null, "source", null, null, null, Pattern.compile("t.*"));
        internalTopologyBuilder.addProcessor("processor", new MockProcessorSupplier(), "source");

        final StreamThread thread = createStreamThread(clientId, config, false);

        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        final Map<String, Object> configurationMap = new HashMap<>();

        configurationMap.put(StreamsConfig.InternalConfig.STREAM_THREAD_INSTANCE, thread);
        configurationMap.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        partitionAssignor.configure(configurationMap);

        thread.setThreadMetadataProvider(partitionAssignor);

        final Field nodeToSourceTopicsField =
            internalTopologyBuilder.getClass().getDeclaredField("nodeToSourceTopics");
        nodeToSourceTopicsField.setAccessible(true);
        final Map<String, List<String>>
            nodeToSourceTopics =
            (Map<String, List<String>>) nodeToSourceTopicsField.get(internalTopologyBuilder);
        final List<TopicPartition> topicPartitions = new ArrayList<>();

        final TopicPartition topicPartition1 = new TopicPartition("topic-1", 0);
        final TopicPartition topicPartition2 = new TopicPartition("topic-2", 0);
        final TopicPartition topicPartition3 = new TopicPartition("topic-3", 0);

        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 0);
        final TaskId taskId3 = new TaskId(0, 0);

        List<TaskId> activeTasks = Utils.mkList(taskId1);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        AssignmentInfo info = new AssignmentInfo(activeTasks, standbyTasks, new HashMap<HostInfo, Set<TopicPartition>>());

        topicPartitions.addAll(Utils.mkList(topicPartition1));
        PartitionAssignor.Assignment assignment = new PartitionAssignor.Assignment(topicPartitions, info.encode());
        partitionAssignor.onAssignment(assignment);

        assertTrue(nodeToSourceTopics.get("source").size() == 1);
        assertTrue(nodeToSourceTopics.get("source").contains("topic-1"));

        topicPartitions.clear();

        activeTasks = Arrays.asList(taskId1, taskId2);
        info = new AssignmentInfo(activeTasks, standbyTasks, new HashMap<HostInfo, Set<TopicPartition>>());
        topicPartitions.addAll(Arrays.asList(topicPartition1, topicPartition2));
        assignment = new PartitionAssignor.Assignment(topicPartitions, info.encode());
        partitionAssignor.onAssignment(assignment);

        assertTrue(nodeToSourceTopics.get("source").size() == 2);
        assertTrue(nodeToSourceTopics.get("source").contains("topic-1"));
        assertTrue(nodeToSourceTopics.get("source").contains("topic-2"));

        topicPartitions.clear();

        activeTasks = Arrays.asList(taskId1, taskId2, taskId3);
        info = new AssignmentInfo(activeTasks, standbyTasks,
                               new HashMap<HostInfo, Set<TopicPartition>>());
        topicPartitions.addAll(Arrays.asList(topicPartition1, topicPartition2, topicPartition3));
        assignment = new PartitionAssignor.Assignment(topicPartitions, info.encode());
        partitionAssignor.onAssignment(assignment);

        assertTrue(nodeToSourceTopics.get("source").size() == 3);
        assertTrue(nodeToSourceTopics.get("source").contains("topic-1"));
        assertTrue(nodeToSourceTopics.get("source").contains("topic-2"));
        assertTrue(nodeToSourceTopics.get("source").contains("topic-3"));

    }

    private static class StateListenerStub implements StreamThread.StateListener {
        int numChanges = 0;
        ThreadStateTransitionValidator oldState = null;
        ThreadStateTransitionValidator newState = null;

        @Override
        public void onChange(final Thread thread, final ThreadStateTransitionValidator newState,
                             final ThreadStateTransitionValidator oldState) {
            ++numChanges;
            if (this.newState != null) {
                if (this.newState != oldState) {
                    throw new RuntimeException("State mismatch " + oldState + " different from " + this.newState);
                }
            }
            this.oldState = oldState;
            this.newState = newState;
        }
    }

    private StreamThread getStreamThread() {
        return createStreamThread(clientId, config, false);
    }


    @Test
    public void shouldReturnActiveTaskMetadataWhileRunningState() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source", null, null, null, TOPIC);

        final TaskId taskId = new TaskId(0, 0);
        final StreamThread thread = createStreamThread(clientId, config, false);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(taskId, task0Assignment);
        thread.setThreadMetadataProvider(new MockStreamsPartitionAssignor(assignment));

        thread.setState(StreamThread.State.RUNNING);

        thread.rebalanceListener.onPartitionsRevoked(null);
        thread.rebalanceListener.onPartitionsAssigned(task0Assignment);
        thread.runOnce(-1);

        ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.activeTasks().contains(new TaskMetadata(taskId.toString(), task0Assignment)));
        assertTrue(threadMetadata.standbyTasks().isEmpty());
    }

    @Test
    public void shouldReturnStandbyTaskMetadataWhileRunningState() throws InterruptedException {
        internalStreamsBuilder.stream(Collections.singleton("t1"), consumed).groupByKey().count("count-one");

        final StreamThread thread = createStreamThread(clientId, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                Collections.singletonList(new PartitionInfo("stream-thread-test-count-one-changelog",
                        0,
                        null,
                        new Node[0],
                        new Node[0])));

        final HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        final TaskId taskId = new TaskId(0, 0);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        final TopicPartition t1 = new TopicPartition("t1", 0);
        Set<TopicPartition> partitionsT1 = Utils.mkSet(t1);
        standbyTasks.put(taskId, partitionsT1);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();

        thread.setThreadMetadataProvider(new MockStreamsPartitionAssignor(activeTasks, standbyTasks));

        thread.setState(StreamThread.State.RUNNING);

        thread.rebalanceListener.onPartitionsRevoked(task0Assignment);
        thread.rebalanceListener.onPartitionsAssigned(null);
        thread.runOnce(-1);

        ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.standbyTasks().contains(new TaskMetadata(taskId.toString(), partitionsT1)));
        assertTrue(threadMetadata.activeTasks().isEmpty());
    }

    @Test
    public void shouldAlwaysUpdateTasksMetadataAfterChangingState() throws InterruptedException {
        final StreamThread thread = createStreamThread(clientId, config, false);
        ThreadMetadata metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.CREATED.name(), metadata.threadState());

        thread.setState(StreamThread.State.RUNNING);
        metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), metadata.threadState());
    }

    @Test
    public void shouldAlwaysReturnEmptyTasksMetadataWhileRebalancingStateAndTasksNotRunning() throws InterruptedException {
        internalStreamsBuilder.stream(Collections.singleton("t1"), consumed).groupByKey().count("count-one");

        final StreamThread thread = createStreamThread(clientId, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                Utils.mkList(
                        new PartitionInfo("stream-thread-test-count-one-changelog",
                                0,
                                null,
                                new Node[0],
                                new Node[0]),
                        new PartitionInfo("stream-thread-test-count-one-changelog",
                                1,
                                null,
                                new Node[0],
                                new Node[0])
                        ));
        final HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L);
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        final TopicPartition t1p0 = new TopicPartition("t1", 0);
        Set<TopicPartition> partitionsT1P0 = Utils.mkSet(t1p0);
        standbyTasks.put(new TaskId(0, 0), partitionsT1P0);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final TopicPartition t1p1 = new TopicPartition("t1", 1);
        Set<TopicPartition> partitionsT1P1 = Utils.mkSet(t1p1);
        activeTasks.put(new TaskId(0, 1), partitionsT1P1);
        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.setThreadMetadataProvider(new StreamPartitionAssignor() {
            @Override
            public Map<TaskId, Set<TopicPartition>> standbyTasks() {
                return standbyTasks;
            }

            @Override
            public Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        thread.setState(StreamThread.State.RUNNING);

        thread.rebalanceListener.onPartitionsRevoked(partitionsT1P0);
        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_REVOKED);

        clientSupplier.consumer.assign(partitionsT1P1);
        thread.rebalanceListener.onPartitionsAssigned(partitionsT1P1);
        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_ASSIGNED);
        thread.runOnce(-1);

        standbyTasks.clear();
        activeTasks.clear();
        standbyTasks.put(new TaskId(0, 1), Utils.mkSet(t1p1));
        activeTasks.put(new TaskId(0, 0), Utils.mkSet(t1p0));

        assertFalse(thread.threadMetadata().activeTasks().isEmpty());
        assertFalse(thread.threadMetadata().standbyTasks().isEmpty());

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_REVOKED);
    }

    private void assertThreadMetadataHasEmptyTasksWithState(ThreadMetadata metadata, StreamThread.State state) {
        assertEquals(state.name(), metadata.threadState());
        assertTrue(metadata.activeTasks().isEmpty());
        assertTrue(metadata.standbyTasks().isEmpty());
    }
}
