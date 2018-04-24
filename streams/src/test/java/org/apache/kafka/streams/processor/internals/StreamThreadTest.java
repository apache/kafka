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
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilderTest;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    private final MockClientSupplier clientSupplier = new MockClientSupplier();
    private UUID processId = UUID.randomUUID();
    private final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());
    private InternalTopologyBuilder internalTopologyBuilder;
    private final StreamsConfig config = new StreamsConfig(configProps(false));
    private final String stateDir = TestUtils.tempDirectory().getPath();
    private final StateDirectory stateDirectory = new StateDirectory(config, mockTime);
    private StreamsMetadataState streamsMetadataState;
    private final ConsumedInternal<Object, Object> consumed = new ConsumedInternal<>();

    @Before
    public void setUp() {
        processId = UUID.randomUUID();

        internalTopologyBuilder = InternalStreamsBuilderTest.internalTopologyBuilder(internalStreamsBuilder);
        internalTopologyBuilder.setApplicationId(applicationId);
        streamsMetadataState = new StreamsMetadataState(internalTopologyBuilder, StreamsMetadataState.UNKNOWN_HOST);
    }

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";

    private final TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private final TopicPartition t2p1 = new TopicPartition(topic2, 1);

    // task0 is unused
    private final TaskId task1 = new TaskId(0, 1);
    private final TaskId task2 = new TaskId(0, 2);
    private final TaskId task3 = new TaskId(1, 1);

    private Properties configProps(final boolean enableEoS) {
        return mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName()),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE : StreamsConfig.AT_LEAST_ONCE)
        ));
    }

    @Test
    public void testPartitionAssignmentChangeForSingleGroup() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final StreamThread thread = createStreamThread(clientId, config, false);

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.CREATED);

        final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;
        thread.setState(StreamThread.State.RUNNING);

        final List<TopicPartition> revokedPartitions;
        final List<TopicPartition> assignedPartitions;

        // revoke nothing
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);

        // assign single partition
        assignedPartitions = singletonList(t1p1);
        thread.taskManager().setAssignmentMetadata(Collections.<TaskId, Set<TopicPartition>>emptyMap(), Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        Assert.assertEquals(4, stateListener.numChanges);
        Assert.assertEquals(StreamThread.State.PARTITIONS_ASSIGNED, stateListener.oldState);

        thread.shutdown();
        assertSame(StreamThread.State.PENDING_SHUTDOWN, thread.state());
    }

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
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return thread.state() == StreamThread.State.DEAD;
            }
        }, 10 * 1000, "Thread never shut down.");

        thread.shutdown();
        assertEquals(thread.state(), StreamThread.State.DEAD);
    }

    private Cluster createCluster() {
        final Node node = new Node(0, "localhost", 8121);
        return new Cluster(
            "mockClusterId",
            singletonList(node),
            Collections.<PartitionInfo>emptySet(),
            Collections.<String>emptySet(),
            Collections.<String>emptySet(),
            node
        );
    }

    private StreamThread createStreamThread(@SuppressWarnings("SameParameterValue") final String clientId,
                                            final StreamsConfig config,
                                            final boolean eosEnabled) {
        if (eosEnabled) {
            clientSupplier.setApplicationIdForProducer(applicationId);
        }

        clientSupplier.setClusterForAdminClient(createCluster());

        return StreamThread.create(
            internalTopologyBuilder,
            config,
            clientSupplier,
            clientSupplier.getAdminClient(config.getAdminConfigs(clientId)),
            processId,
            clientId,
            metrics,
            mockTime,
            streamsMetadataState,
            0,
            stateDirectory,
            new MockStateRestoreListener());
    }

    @Test
    public void testMetricsCreatedAtStartup() {
        final StreamThread thread = createStreamThread(clientId, config, false);
        final String defaultGroupName = "stream-metrics";
        final String defaultPrefix = "thread." + thread.getName();
        final Map<String, String> defaultTags = Collections.singletonMap("client-id", thread.getName());

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
        assertNotNull(metrics.metrics().get(metrics.metricName("commit-total", defaultGroupName, "The total number of commit calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-latency-avg", defaultGroupName, "The average poll time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-latency-max", defaultGroupName, "The maximum poll time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-rate", defaultGroupName, "The average per-second number of record-poll calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-total", defaultGroupName, "The total number of record-poll calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-latency-avg", defaultGroupName, "The average process time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-latency-max", defaultGroupName, "The maximum process time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-rate", defaultGroupName, "The average per-second number of process calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-total", defaultGroupName, "The total number of process calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-latency-avg", defaultGroupName, "The average punctuate time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-latency-max", defaultGroupName, "The maximum punctuate time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-rate", defaultGroupName, "The average per-second number of punctuate calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-total", defaultGroupName, "The total number of punctuate calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-created-rate", defaultGroupName, "The average per-second number of newly created tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-created-total", defaultGroupName, "The total number of newly created tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-closed-rate", defaultGroupName, "The average per-second number of closed tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-closed-total", defaultGroupName, "The total number of closed tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("skipped-records-rate", defaultGroupName, "The average per-second number of skipped records.", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("skipped-records-total", defaultGroupName, "The total number of skipped records.", defaultTags)));
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

        final StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "");
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext("")
        );
        thread.maybeCommit(mockTime.milliseconds());
        mockTime.sleep(commitInterval - 10L);
        thread.maybeCommit(mockTime.milliseconds());

        EasyMock.verify(taskManager);
    }

    @SuppressWarnings({"unchecked", "ThrowableNotThrown"})
    @Test
    public void shouldNotCauseExceptionIfNothingCommitted() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);

        final StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "");
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""));
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

        final StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "");
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""));
        thread.maybeCommit(mockTime.milliseconds());
        mockTime.sleep(commitInterval + 1);
        thread.maybeCommit(mockTime.milliseconds());

        EasyMock.verify(taskManager);
    }

    @SuppressWarnings({"ThrowableNotThrown", "unchecked"})
    private TaskManager mockTaskManagerCommit(final Consumer<byte[], byte[]> consumer, final int numberOfCommits, final int commits) {
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        EasyMock.expect(taskManager.commitAll()).andReturn(commits).times(numberOfCommits);
        EasyMock.replay(taskManager, consumer);
        return taskManager;
    }

    @Test
    public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final StreamThread thread = createStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        final Map<TopicPartition, Long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        thread.rebalanceListener.onPartitionsAssigned(new HashSet<>(assignedPartitions));

        assertEquals(1, clientSupplier.producers.size());
        final Producer globalProducer = clientSupplier.producers.get(0);
        for (final Task task : thread.tasks().values()) {
            assertSame(globalProducer, ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    @Test
    public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        final Map<TopicPartition, Long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        thread.rebalanceListener.onPartitionsAssigned(new HashSet<>(assignedPartitions));

        thread.runOnce(-1);

        assertEquals(thread.tasks().size(), clientSupplier.producers.size());
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    @Test
    public void shouldCloseAllTaskProducersOnCloseIfEosEnabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        final Map<TopicPartition, Long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);

        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.shutdown();
        thread.run();

        for (final Task task : thread.tasks().values()) {
            assertTrue(((MockProducer) ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer()).closed());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldShutdownTaskManagerOnClose() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        EasyMock.expect(taskManager.activeTasks()).andReturn(Collections.<TaskId, StreamTask>emptyMap());
        EasyMock.expect(taskManager.standbyTasks()).andReturn(Collections.<TaskId, StandbyTask>emptyMap());
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "");
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext("")
        );
        thread.setStateListener(
            new StreamThread.StateListener() {
                @Override
                public void onChange(final Thread t, final ThreadStateTransitionValidator newState, final ThreadStateTransitionValidator oldState) {
                    if (oldState == StreamThread.State.CREATED && newState == StreamThread.State.RUNNING) {
                        thread.shutdown();
                    }
                }
            });
        thread.run();
        EasyMock.verify(taskManager);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldShutdownTaskManagerOnCloseWithoutStart() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "");
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext("")
        );
        thread.shutdown();
        EasyMock.verify(taskManager);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldOnlyShutdownOnce() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "");
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""));
        thread.shutdown();
        // Execute the run method. Verification of the mock will check that shutdown was only done once
        thread.run();
        EasyMock.verify(taskManager);
    }

    @Test
    public void shouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology() {
        internalTopologyBuilder.addSource(null, "name", null, null, null, "topic");
        internalTopologyBuilder.addSink("out", "output", null, null, null);

        final StreamThread thread = createStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(Collections.<TaskId, Set<TopicPartition>>emptyMap(), standbyTasks);
        thread.taskManager().createTasks(Collections.<TopicPartition>emptyList());

        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());
    }

    @Test
    public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerWasFencedWhileProcessing() throws Exception {
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);
        internalTopologyBuilder.addSink("sink", "dummyTopic", null, null, null, "source");

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        final MockConsumer<byte[], byte[]> consumer = clientSupplier.consumer;

        consumer.updatePartitions(topic1, singletonList(new PartitionInfo(topic1, 1, null, null, null)));

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce(-1);
        assertThat(thread.tasks().size(), equalTo(1));
        final MockProducer producer = clientSupplier.producers.get(0);

        // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
        consumer.updateBeginningOffsets(Collections.singletonMap(assignedPartitions.iterator().next(), 0L));
        consumer.unsubscribe();
        consumer.assign(new HashSet<>(assignedPartitions));

        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 0, new byte[0], new byte[0]));
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
        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 0, new byte[0], new byte[0]));
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
        internalTopologyBuilder.addSource(null, "name", null, null, null, topic1);
        internalTopologyBuilder.addSink("out", "output", null, null, null);

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce(-1);

        assertThat(thread.tasks().size(), equalTo(1));

        thread.rebalanceListener.onPartitionsRevoked(null);
        clientSupplier.producers.get(0).fenceProducer();
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
        try {
            thread.runOnce(-1);
            fail("Should have thrown TaskMigratedException");
        } catch (final TaskMigratedException expected) { /* ignore */ }

        assertTrue(thread.tasks().isEmpty());
    }

    private static class StateListenerStub implements StreamThread.StateListener {
        int numChanges = 0;
        ThreadStateTransitionValidator oldState = null;
        ThreadStateTransitionValidator newState = null;

        @Override
        public void onChange(final Thread thread,
                             final ThreadStateTransitionValidator newState,
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

    @Test
    public void shouldReturnActiveTaskMetadataWhileRunningState() {
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);

        final StreamThread thread = createStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.RUNNING);

        thread.rebalanceListener.onPartitionsRevoked(null);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce(-1);

        final ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.activeTasks().contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
        assertTrue(threadMetadata.standbyTasks().isEmpty());
    }

    @Test
    public void shouldReturnStandbyTaskMetadataWhileRunningState() {
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
            .groupByKey().count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("count-one"));

        final StreamThread thread = createStreamThread(clientId, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions(
            "stream-thread-test-count-one-changelog",
            singletonList(
                new PartitionInfo("stream-thread-test-count-one-changelog",
                0,
                null,
                new Node[0],
                new Node[0])
            )
        );

        final HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        thread.setState(StreamThread.State.RUNNING);

        thread.rebalanceListener.onPartitionsRevoked(null);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(Collections.<TaskId, Set<TopicPartition>>emptyMap(), standbyTasks);

        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());

        thread.runOnce(-1);

        final ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.standbyTasks().contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
        assertTrue(threadMetadata.activeTasks().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldUpdateStandbyTask() {
        final String storeName1 = "count-one";
        final String storeName2 = "table-two";
        final String changelogName = applicationId + "-" + storeName1 + "-changelog";
        final TopicPartition partition1 = new TopicPartition(changelogName, 1);
        final TopicPartition partition2 = t2p1;
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
            .groupByKey().count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as(storeName1));
        internalStreamsBuilder.table(topic2, new ConsumedInternal(), new MaterializedInternal(Materialized.as(storeName2), internalStreamsBuilder, ""));

        final StreamThread thread = createStreamThread(clientId, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions(changelogName,
            singletonList(
                new PartitionInfo(
                    changelogName,
                    1,
                    null,
                    new Node[0],
                    new Node[0]
                )
            )
        );

        restoreConsumer.assign(Utils.mkSet(partition1, partition2));
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition1, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition1, 0L));
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition2, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition2, 0L));
        // let the store1 be restored from 0 to 10; store2 be restored from 0 to (committed offset) 5
        clientSupplier.consumer.assign(Utils.mkSet(partition2));
        clientSupplier.consumer.commitSync(Collections.singletonMap(partition2, new OffsetAndMetadata(5L, "")));

        for (long i = 0L; i < 10L; i++) {
            restoreConsumer.addRecord(new ConsumerRecord<>(changelogName, 1, i, ("K" + i).getBytes(), ("V" + i).getBytes()));
            restoreConsumer.addRecord(new ConsumerRecord<>(topic2, 1, i, ("K" + i).getBytes(), ("V" + i).getBytes()));
        }

        thread.setState(StreamThread.State.RUNNING);

        thread.rebalanceListener.onPartitionsRevoked(null);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));
        standbyTasks.put(task3, Collections.singleton(t2p1));

        thread.taskManager().setAssignmentMetadata(Collections.<TaskId, Set<TopicPartition>>emptyMap(), standbyTasks);

        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());

        thread.runOnce(-1);

        final StandbyTask standbyTask1 = thread.taskManager().standbyTask(partition1);
        final StandbyTask standbyTask2 = thread.taskManager().standbyTask(partition2);
        final KeyValueStore<Object, Long> store1 = (KeyValueStore<Object, Long>) standbyTask1.getStore(storeName1);
        final KeyValueStore<Object, Long> store2 = (KeyValueStore<Object, Long>) standbyTask2.getStore(storeName2);

        assertEquals(10L, store1.approximateNumEntries());
        assertEquals(5L, store2.approximateNumEntries());
        assertEquals(Collections.singleton(partition2), restoreConsumer.paused());
        assertEquals(1, thread.standbyRecords().size());
        assertEquals(5, thread.standbyRecords().get(partition2).size());
    }

    @Test
    public void shouldPunctuateActiveTask() {
        final List<Long> punctuatedStreamTime = new ArrayList<>();
        final List<Long> punctuatedWallClockTime = new ArrayList<>();
        final ProcessorSupplier<Object, Object> punctuateProcessor = new ProcessorSupplier<Object, Object>() {
            @Override
            public Processor<Object, Object> get() {
                return new Processor<Object, Object>() {
                    @Override
                    public void init(final ProcessorContext context) {
                        context.schedule(100L, PunctuationType.STREAM_TIME, new Punctuator() {
                            @Override
                            public void punctuate(final long timestamp) {
                                punctuatedStreamTime.add(timestamp);
                            }
                        });
                        context.schedule(100L, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                            @Override
                            public void punctuate(final long timestamp) {
                                punctuatedWallClockTime.add(timestamp);
                            }
                        });
                    }

                    @Override
                    public void process(final Object key, final Object value) {}

                    @SuppressWarnings("deprecation")
                    @Override
                    public void punctuate(final long timestamp) {}

                    @Override
                    public void close() {}
                };
            }
        };

        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed).process(punctuateProcessor);

        final StreamThread thread = createStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.RUNNING);

        thread.rebalanceListener.onPartitionsRevoked(null);
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

        clientSupplier.consumer.assign(assignedPartitions);
        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce(-1);

        assertEquals(0, punctuatedStreamTime.size());
        assertEquals(0, punctuatedWallClockTime.size());

        mockTime.sleep(100L);
        for (long i = 0L; i < 10L; i++) {
            clientSupplier.consumer.addRecord(new ConsumerRecord<>(topic1, 1, i, i * 100L, TimestampType.CREATE_TIME, ConsumerRecord.NULL_CHECKSUM, ("K" + i).getBytes().length, ("V" + i).getBytes().length, ("K" + i).getBytes(), ("V" + i).getBytes()));
        }

        thread.runOnce(-1);

        assertEquals(1, punctuatedStreamTime.size());
        assertEquals(1, punctuatedWallClockTime.size());

        mockTime.sleep(100L);

        thread.runOnce(-1);

        // we should skip stream time punctuation, only trigger wall-clock time punctuation
        assertEquals(1, punctuatedStreamTime.size());
        assertEquals(2, punctuatedWallClockTime.size());
    }

    @Test
    public void shouldAlwaysUpdateTasksMetadataAfterChangingState() {
        final StreamThread thread = createStreamThread(clientId, config, false);
        ThreadMetadata metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.CREATED.name(), metadata.threadState());

        thread.setState(StreamThread.State.RUNNING);
        metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), metadata.threadState());
    }

    @Test
    public void shouldAlwaysReturnEmptyTasksMetadataWhileRebalancingStateAndTasksNotRunning() {
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
            .groupByKey().count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("count-one"));

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

        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

        thread.setState(StreamThread.State.RUNNING);

        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        thread.rebalanceListener.onPartitionsRevoked(assignedPartitions);
        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_REVOKED);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));
        standbyTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, standbyTasks);

        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_ASSIGNED);
    }

    @Test
    public void shouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore() throws Exception {
        internalStreamsBuilder.stream(Collections.singleton("topic"), consumed)
            .groupByKey().count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("count"));

        final StreamThread thread = createStreamThread("clientId", config, false);
        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        final MockConsumer<byte[], byte[]> mockRestoreConsumer = (MockConsumer<byte[], byte[]>) thread.restoreConsumer;

        final TopicPartition topicPartition = new TopicPartition("topic", 0);
        final Set<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(new TaskId(0, 0), topicPartitionSet);
        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

        mockConsumer.updatePartitions(
            "topic",
            singletonList(
                new PartitionInfo(
                    "topic",
                    0,
                    null,
                    new Node[0],
                    new Node[0]
                )
            )
        );
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));

        mockRestoreConsumer.updatePartitions(
            "stream-thread-test-count-changelog",
            singletonList(
                new PartitionInfo(
                    "stream-thread-test-count-changelog",
                    0,
                    null,
                    new Node[0],
                    new Node[0]
                )
            )
        );

        final TopicPartition changelogPartition = new TopicPartition("stream-thread-test-count-changelog", 0);
        final Set<TopicPartition> changelogPartitionSet = Collections.singleton(changelogPartition);
        mockRestoreConsumer.updateBeginningOffsets(Collections.singletonMap(changelogPartition, 0L));
        mockRestoreConsumer.updateEndOffsets(Collections.singletonMap(changelogPartition, 2L));

        mockConsumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                thread.setState(StreamThread.State.PARTITIONS_REVOKED);
                thread.rebalanceListener.onPartitionsAssigned(topicPartitionSet);
            }
        });

        try {
            thread.start();

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return mockRestoreConsumer.assignment().size() == 1;
                }
            }, "Never restore first record");

            mockRestoreConsumer.addRecord(new ConsumerRecord<>("stream-thread-test-count-changelog", 0, 0L, "K1".getBytes(), "V1".getBytes()));

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return mockRestoreConsumer.position(changelogPartition) == 1L;
                }
            }, "Never restore first record");

            mockRestoreConsumer.setException(new InvalidOffsetException("Try Again!") {
                @Override
                public Set<TopicPartition> partitions() {
                    return changelogPartitionSet;
                }
            });

            mockRestoreConsumer.addRecord(new ConsumerRecord<>("stream-thread-test-count-changelog", 0, 0L, "K1".getBytes(), "V1".getBytes()));
            mockRestoreConsumer.addRecord(new ConsumerRecord<>("stream-thread-test-count-changelog", 0, 1L, "K2".getBytes(), "V2".getBytes()));

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    mockRestoreConsumer.assign(changelogPartitionSet);
                    return mockRestoreConsumer.position(changelogPartition) == 2L;
                }
            }, "Never finished restore");
        } finally {
            thread.shutdown();
            thread.join(10000);
        }
    }

    @Test
    public void shouldRecordSkippedMetricForDeserializationException() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties config = configProps(false);
        config.setProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

        thread.setState(StreamThread.State.RUNNING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
        thread.taskManager().setAssignmentMetadata(
            Collections.singletonMap(
                new TaskId(0, t1p1.partition()),
                assignedPartitions),
            Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);

        final MetricName skippedTotalMetric = metrics.metricName("skipped-records-total", "stream-metrics", Collections.singletonMap("client-id", thread.getName()));
        final MetricName skippedRateMetric = metrics.metricName("skipped-records-rate", "stream-metrics", Collections.singletonMap("client-id", thread.getName()));
        assertEquals(0.0, metrics.metric(skippedTotalMetric).metricValue());
        assertEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

        long offset = -1;
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], "I am not an integer.".getBytes()));
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], "I am not an integer.".getBytes()));
        thread.runOnce(-1);
        assertEquals(2.0, metrics.metric(skippedTotalMetric).metricValue());
        assertNotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

        LogCaptureAppender.unregister(appender);
        final List<String> strings = appender.getMessages();
        assertTrue(strings.contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]"));
        assertTrue(strings.contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]"));
    }

    @Test
    public void shouldReportSkippedRecordsForInvalidTimestamps() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties config = configProps(false);
        config.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LogAndSkipOnInvalidTimestamp.class.getName());
        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

        thread.setState(StreamThread.State.RUNNING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
        thread.taskManager().setAssignmentMetadata(
            Collections.singletonMap(
                new TaskId(0, t1p1.partition()),
                assignedPartitions),
            Collections.<TaskId, Set<TopicPartition>>emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);

        final MetricName skippedTotalMetric = metrics.metricName("skipped-records-total", "stream-metrics", Collections.singletonMap("client-id", thread.getName()));
        final MetricName skippedRateMetric = metrics.metricName("skipped-records-rate", "stream-metrics", Collections.singletonMap("client-id", thread.getName()));
        assertEquals(0.0, metrics.metric(skippedTotalMetric).metricValue());
        assertEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

        long offset = -1;
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        thread.runOnce(-1);
        assertEquals(2.0, metrics.metric(skippedTotalMetric).metricValue());
        assertNotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, -1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        thread.runOnce(-1);
        assertEquals(6.0, metrics.metric(skippedTotalMetric).metricValue());
        assertNotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, 1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        mockConsumer.addRecord(new ConsumerRecord<>(t1p1.topic(), t1p1.partition(), ++offset, 1, TimestampType.CREATE_TIME, -1, -1, -1, new byte[0], new byte[0]));
        thread.runOnce(-1);
        assertEquals(6.0, metrics.metric(skippedTotalMetric).metricValue());
        assertNotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

        LogCaptureAppender.unregister(appender);
        final List<String> strings = appender.getMessages();
        assertTrue(strings.contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[0] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        assertTrue(strings.contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[1] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        assertTrue(strings.contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[2] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        assertTrue(strings.contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[3] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        assertTrue(strings.contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[4] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        assertTrue(strings.contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[5] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
    }

    private void assertThreadMetadataHasEmptyTasksWithState(final ThreadMetadata metadata,
                                                            final StreamThread.State state) {
        assertEquals(state.name(), metadata.threadState());
        assertTrue(metadata.activeTasks().isEmpty());
        assertTrue(metadata.standbyTasks().isEmpty());
    }



}
