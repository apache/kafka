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
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

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
    private final MockClientSupplier clientSupplier = new MockClientSupplier();
    private UUID processId = UUID.randomUUID();
    private final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());
    private InternalTopologyBuilder internalTopologyBuilder;
    private final StreamsConfig config = new StreamsConfig(configProps(false));
    private final String stateDir = TestUtils.tempDirectory().getPath();
    private final StateDirectory stateDirectory  = new StateDirectory(config, mockTime);
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

    private final TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);

    // task0 is unused
    private final TaskId task1 = new TaskId(0, 1);
    private final TaskId task2 = new TaskId(0, 2);

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
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final StreamThread thread = getStreamThread();

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.CREATED);

        final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;
        thread.setState(StreamThread.State.RUNNING);

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;

        // revoke nothing
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);

        // assign single partition
        assignedPartitions = Collections.singletonList(t1p1);
        thread.taskManager().setAssignmentMetadata(Collections.<TaskId, Set<TopicPartition>>emptyMap(), Collections.<TaskId, Set<TopicPartition>>emptyMap());
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce(-1);
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        Assert.assertEquals(4, stateListener.numChanges);
        Assert.assertEquals(StreamThread.State.PARTITIONS_ASSIGNED, stateListener.oldState);

        thread.shutdown();
        assertTrue(thread.state() == StreamThread.State.PENDING_SHUTDOWN);
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
            clientSupplier.setApplicationIdForProducer(applicationId);
        }

        return StreamThread.create(internalTopologyBuilder,
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
    public void testMetrics() {
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
        final StreamThread thread = new StreamThread(mockTime,
                config,
                consumer,
                consumer,
                null,
                clientSupplier.getAdminClient(config.getAdminConfigs(clientId)),
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
    public void shouldNotCauseExceptionIfNothingCommited() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);

        StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "", "", Collections.<String, String>emptyMap());
        final StreamThread thread = new StreamThread(mockTime,
                config,
                consumer,
                consumer,
                null,
                clientSupplier.getAdminClient(config.getAdminConfigs(clientId)),
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

        StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "", "", Collections.<String, String>emptyMap());
        final StreamThread thread = new StreamThread(mockTime,
                config,
                consumer,
                consumer,
                null,
                clientSupplier.getAdminClient(config.getAdminConfigs(clientId)),
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
        final TaskManager taskManager = EasyMock.createMock(TaskManager.class);
        EasyMock.expect(taskManager.commitAll()).andReturn(commits).times(numberOfCommits);
        EasyMock.replay(taskManager, consumer);
        return taskManager;
    }

    @Test
    public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled() throws InterruptedException {
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
        thread.taskManager().createTasks(assignedPartitions);

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
    public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable() throws InterruptedException {
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

        thread.rebalanceListener.onPartitionsAssigned(new HashSet<>(assignedPartitions));

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
        thread.taskManager().createTasks(assignedPartitions);

        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

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
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        StreamThread.StreamsMetricsThreadImpl streamsMetrics = new StreamThread.StreamsMetricsThreadImpl(metrics, "", "", Collections.<String, String>emptyMap());
        final StreamThread thread = new StreamThread(mockTime,
                config,
                consumer,
                consumer,
                null,
                clientSupplier.getAdminClient(config.getAdminConfigs(clientId)),
                taskManager,
                streamsMetrics,
                internalTopologyBuilder,
                clientId,
                new LogContext(""));
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
    public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerWasFencedWhileProcessing() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);
        internalTopologyBuilder.addSink("sink", "dummyTopic", null, null, null, "source");

        final StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        final MockConsumer<byte[], byte[]> consumer = clientSupplier.consumer;

        consumer.updatePartitions(topic1, Collections.singletonList(new PartitionInfo(topic1, 1, null, null, null)));

        thread.setState(StreamThread.State.RUNNING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.<TaskId, Set<TopicPartition>>emptyMap());

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
        thread.taskManager().createTasks(assignedPartitions);

        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce(-1);

        ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.activeTasks().contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
        assertTrue(threadMetadata.standbyTasks().isEmpty());
    }

    @Test
    public void shouldReturnStandbyTaskMetadataWhileRunningState() throws InterruptedException {
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed).groupByKey().count("count-one");

        final StreamThread thread = createStreamThread(clientId, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                Collections.singletonList(new PartitionInfo("stream-thread-test-count-one-changelog",
                        0,
                        null,
                        new Node[0],
                        new Node[0])));

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

        ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.standbyTasks().contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
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
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed).groupByKey().count("count-one");

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

    private void assertThreadMetadataHasEmptyTasksWithState(ThreadMetadata metadata, StreamThread.State state) {
        assertEquals(state.name(), metadata.threadState());
        assertTrue(metadata.activeTasks().isEmpty());
        assertTrue(metadata.standbyTasks().isEmpty());
    }
}
