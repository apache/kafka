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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.MockRebalanceListener;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.tasks.DefaultTaskManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getSharedAdminClientId;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statelessTask;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class StreamThreadTest {

    @Parameter(0)
    public boolean stateUpdaterEnabled = true;

    @Parameter(1)
    public boolean processingThreadsEnabled = true;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {false, false}, {true, false}, {true, true}
        });
    }

    private final static String APPLICATION_ID = "stream-thread-test";
    private final static UUID PROCESS_ID = UUID.fromString("87bf53a8-54f2-485f-a4b6-acdbec0a8b3d");
    private final static String CLIENT_ID = APPLICATION_ID + "-" + PROCESS_ID;
    public static final String STREAM_THREAD_TEST_COUNT_ONE_CHANGELOG = "stream-thread-test-count-one-changelog";
    public static final String STREAM_THREAD_TEST_TABLE_TWO_CHANGELOG = "stream-thread-test-table-two-changelog";

    private final int threadIdx = 1;
    private final Metrics metrics = new Metrics();
    private final MockTime mockTime = new MockTime();
    private final String stateDir = TestUtils.tempDirectory().getPath();
    private final MockClientSupplier clientSupplier = new MockClientSupplier();
    private final ConsumedInternal<Object, Object> consumed = new ConsumedInternal<>();
    private final ChangelogReader changelogReader = new MockChangelogReader();
    private StateDirectory stateDirectory = null;
    private final InternalTopologyBuilder internalTopologyBuilder = new InternalTopologyBuilder();
    private final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(internalTopologyBuilder);

    private StreamThread thread = null;

    @SuppressWarnings("unchecked")
    private final Consumer<byte[], byte[]> mainConsumer = (Consumer<byte[], byte[]>) Mockito.mock(Consumer.class);

    private final static BiConsumer<Throwable, Boolean> HANDLER = (e, b) -> {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else if (e instanceof Error) {
            throw (Error) e;
        } else {
            throw new RuntimeException("Unexpected checked exception caught in the uncaught exception handler", e);
        }
    };

    @Before
    public void setUp() {
        Thread.currentThread().setName(CLIENT_ID + "-StreamThread-" + threadIdx);
        internalTopologyBuilder.setApplicationId(APPLICATION_ID);
    }

    @After
    public void tearDown() {
        if (thread != null) {
            thread.shutdown();
            thread = null;
        }
        stateDirectory = null;
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
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName()),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE_V2 : StreamsConfig.AT_LEAST_ONCE),
            mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName()),
            mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName()),
            mkEntry(InternalConfig.STATE_UPDATER_ENABLED, Boolean.toString(stateUpdaterEnabled)),
            mkEntry(InternalConfig.PROCESSING_THREADS_ENABLED, Boolean.toString(processingThreadsEnabled))
        ));
    }

    private Cluster createCluster() {
        final Node node = new Node(-1, "localhost", 8121);
        return new Cluster(
            "mockClusterId",
            Collections.singletonList(node),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            node
        );
    }

    private StreamThread createStreamThread(@SuppressWarnings("SameParameterValue") final String clientId) {
        return createStreamThread(clientId, mockTime);
    }

    private StreamThread createStreamThread(@SuppressWarnings("SameParameterValue") final String clientId,
                                            final Time time) {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        return createStreamThread(clientId, config, time);
    }

    private StreamThread createStreamThread(@SuppressWarnings("SameParameterValue") final String clientId,
                                            final StreamsConfig config) {
        return createStreamThread(clientId, config, mockTime);
    }

    private StreamThread createStreamThread(@SuppressWarnings("SameParameterValue") final String clientId,
                                            final StreamsConfig config,
                                            final Time time) {
        if (!StreamsConfig.AT_LEAST_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
            clientSupplier.setApplicationIdForProducer(APPLICATION_ID);
        }

        clientSupplier.setCluster(createCluster());

        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            APPLICATION_ID,
            config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            time
        );

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();

        stateDirectory = new StateDirectory(config, mockTime, true, false);
        final StreamsMetadataState streamsMetadataState = new StreamsMetadataState(
            new TopologyMetadata(internalTopologyBuilder, config),
            StreamsMetadataState.UNKNOWN_HOST,
            new LogContext(String.format("stream-client [%s] ", CLIENT_ID))
        );
        return StreamThread.create(
            topologyMetadata,
            config,
            clientSupplier,
            clientSupplier.getAdmin(config.getAdminConfigs(clientId)),
            PROCESS_ID,
            clientId,
            streamsMetrics,
            time,
            streamsMetadataState,
            0,
            stateDirectory,
            new MockStateRestoreListener(),
            threadIdx,
            null,
            HANDLER
        );
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
                if (!this.newState.equals(oldState)) {
                    throw new RuntimeException("State mismatch " + oldState + " different from " + this.newState);
                }
            }
            this.oldState = oldState;
            this.newState = newState;
        }
    }

    @Test
    public void shouldChangeStateInRebalanceListener() {
        thread = createStreamThread(CLIENT_ID);

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.CREATED);

        final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener();

        final List<TopicPartition> revokedPartitions;
        final List<TopicPartition> assignedPartitions;

        // revoke nothing
        thread.setState(StreamThread.State.STARTING);
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);

        // assign single partition
        assignedPartitions = Collections.singletonList(t1p1);

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        runOnce();
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        Assert.assertEquals(4, stateListener.numChanges);
        Assert.assertEquals(StreamThread.State.PARTITIONS_ASSIGNED, stateListener.oldState);

        thread.shutdown();
        assertSame(StreamThread.State.PENDING_SHUTDOWN, thread.state());
    }

    @Test
    public void shouldChangeStateAtStartClose() throws Exception {
        thread = createStreamThread(CLIENT_ID, new MockTime(1));

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);

        thread.start();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.STARTING,
            10 * 1000,
            "Thread never started.");

        thread.shutdown();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.DEAD,
            10 * 1000,
            "Thread never shut down.");

        thread.shutdown();
        assertEquals(thread.state(), StreamThread.State.DEAD);
    }

    @Test
    public void shouldCreateMetricsAtStartup() {
        thread = createStreamThread(CLIENT_ID, new MockTime(1));
        final String defaultGroupName = "stream-thread-metrics";
        final Map<String, String> defaultTags = Collections.singletonMap(
            "thread-id",
            thread.getName()
        );
        final String descriptionIsNotVerified = "";

        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-ratio", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-ratio", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-records-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-records-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-ratio", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-records-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-records-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-ratio", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-created-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-created-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-closed-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-closed-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));

        assertNull(metrics.metrics().get(metrics.metricName(
            "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNull(metrics.metrics().get(metrics.metricName(
            "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));

        final String taskGroupName = "stream-task-metrics";
        final Map<String, String> taskTags =
            mkMap(mkEntry("task-id", "all"), mkEntry("thread-id", thread.getName()));
        assertNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-avg", taskGroupName, descriptionIsNotVerified, taskTags)));
        assertNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-max", taskGroupName, descriptionIsNotVerified, taskTags)));
        assertNull(metrics.metrics().get(metrics.metricName(
            "commit-rate", taskGroupName, descriptionIsNotVerified, taskTags)));

        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        assertEquals(CLIENT_ID + "-StreamThread-1", thread.getName());
        assertTrue(reporter.containsMbean(String.format("kafka.streams:type=%s,%s=%s",
                                                        defaultGroupName,
                                                        "thread-id",
                                                        thread.getName())
        ));
        assertFalse(reporter.containsMbean(String.format(
            "kafka.streams:type=stream-task-metrics,%s=%s,task-id=all",
            "thread-id",
            thread.getName())));
    }

    @Test
    public void shouldNotCommitBeforeTheCommitInterval() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        final TaskManager taskManager = mockTaskManagerCommit(1);
        EasyMock.replay(consumer, consumerGroupMetadata);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        mockTime.sleep(commitInterval - 10L);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldNotPurgeBeforeThePurgeInterval() {
        final long commitInterval = 1000L;
        final long purgeInterval = 2000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));
        props.setProperty(StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG, Long.toString(purgeInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        final TaskManager taskManager = mockTaskManagerPurge(1);
        taskManager.maybePurgeCommittedRecords();
        EasyMock.replay(consumer, consumerGroupMetadata);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        mockTime.sleep(purgeInterval - 10L);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldAlsoPurgeWhenNothingGetsCommitted() {
        final long purgeInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(purgeInterval));
        props.setProperty(StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG, Long.toString(purgeInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        final TaskId taskId = new TaskId(0, 0);
        final Task runningTask = statelessTask(taskId)
            .inState(Task.State.RUNNING).build();
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.allOwnedTasks()).andStubReturn(Collections.singletonMap(taskId, runningTask));
        expect(taskManager.commit(Collections.singleton(runningTask))).andStubReturn(0);
        taskManager.maybePurgeCommittedRecords();
        EasyMock.replay(consumer, consumerGroupMetadata, taskManager);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);
        thread.setNow(mockTime.milliseconds());
        mockTime.sleep(purgeInterval + 10L);
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldNotProcessWhenPartitionRevoked() {
        Assume.assumeFalse(processingThreadsEnabled);

        final Properties props = configProps(false);

        final StreamsConfig config = new StreamsConfig(props);
        when(mainConsumer.poll(Mockito.any())).thenReturn(ConsumerRecords.empty());
        final ConsumerGroupMetadata consumerGroupMetadata = Mockito.mock(ConsumerGroupMetadata.class);
        when(mainConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        when(consumerGroupMetadata.groupInstanceId()).thenReturn(Optional.empty());
        final TaskManager taskManager = Mockito.mock(TaskManager.class);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(mainConsumer, taskManager, config, topologyMetadata);
        thread.setState(State.STARTING);
        thread.setState(State.PARTITIONS_REVOKED);
        thread.runOnceWithoutProcessingThreads();

        Mockito.verify(taskManager, Mockito.never()).process(Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void shouldProcessWhenRunning() {
        Assume.assumeFalse(processingThreadsEnabled);
        final Properties props = configProps(false);

        final StreamsConfig config = new StreamsConfig(props);
        when(mainConsumer.poll(Mockito.any())).thenReturn(ConsumerRecords.empty());
        final ConsumerGroupMetadata consumerGroupMetadata = Mockito.mock(ConsumerGroupMetadata.class);
        when(mainConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        when(consumerGroupMetadata.groupInstanceId()).thenReturn(Optional.empty());
        final TaskManager taskManager = Mockito.mock(TaskManager.class);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(mainConsumer, taskManager, config, topologyMetadata);
        thread.updateThreadMetadata("admin");
        thread.setState(State.STARTING);
        thread.setState(State.PARTITIONS_ASSIGNED);
        thread.setState(State.RUNNING);
        runOnce();

        Mockito.verify(taskManager).process(Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void shouldProcessWhenPartitionAssigned() {
        Assume.assumeTrue(stateUpdaterEnabled);
        Assume.assumeFalse(processingThreadsEnabled);
        final Properties props = configProps(false);

        final StreamsConfig config = new StreamsConfig(props);
        when(mainConsumer.poll(Mockito.any())).thenReturn(ConsumerRecords.empty());
        final ConsumerGroupMetadata consumerGroupMetadata = Mockito.mock(ConsumerGroupMetadata.class);
        when(mainConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        when(consumerGroupMetadata.groupInstanceId()).thenReturn(Optional.empty());
        final TaskManager taskManager = Mockito.mock(TaskManager.class);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(mainConsumer, taskManager, config, topologyMetadata);
        thread.updateThreadMetadata("admin");
        thread.setState(State.STARTING);
        thread.setState(State.PARTITIONS_ASSIGNED);
        runOnce();

        Mockito.verify(taskManager).process(Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void shouldProcessWhenStarting() {
        Assume.assumeTrue(stateUpdaterEnabled);
        Assume.assumeFalse(processingThreadsEnabled);
        final Properties props = configProps(false);
        props.setProperty(InternalConfig.STATE_UPDATER_ENABLED, Boolean.toString(true));

        final StreamsConfig config = new StreamsConfig(props);
        when(mainConsumer.poll(Mockito.any())).thenReturn(ConsumerRecords.empty());
        final ConsumerGroupMetadata consumerGroupMetadata = Mockito.mock(ConsumerGroupMetadata.class);
        when(mainConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        when(consumerGroupMetadata.groupInstanceId()).thenReturn(Optional.empty());
        final TaskManager taskManager = Mockito.mock(TaskManager.class);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(mainConsumer, taskManager, config, topologyMetadata);
        thread.updateThreadMetadata("admin");
        thread.setState(State.STARTING);
        runOnce();

        Mockito.verify(taskManager).process(Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void shouldEnforceRebalanceWhenScheduledAndNotCurrentlyRebalancing() throws InterruptedException {
        final Time mockTime = new MockTime(1);
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            APPLICATION_ID,
            config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            mockTime
        );
        
        final Consumer<byte[], byte[]> mockConsumer = EasyMock.createNiceMock(Consumer.class);
        expect(mockConsumer.poll(anyObject())).andStubReturn(ConsumerRecords.empty());
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(mockConsumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumerGroupMetadata, mockConsumer);
        final EasyMockConsumerClientSupplier mockClientSupplier = new EasyMockConsumerClientSupplier(mockConsumer);
        mockClientSupplier.setCluster(createCluster());

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        stateDirectory = new StateDirectory(config, mockTime, true, false);
        final StreamsMetadataState streamsMetadataState = new StreamsMetadataState(
            new TopologyMetadata(internalTopologyBuilder, config),
            StreamsMetadataState.UNKNOWN_HOST,
            new LogContext(String.format("stream-client [%s] ", CLIENT_ID))
        );
        thread = StreamThread.create(
            topologyMetadata,
            config,
            mockClientSupplier,
            mockClientSupplier.getAdmin(config.getAdminConfigs(CLIENT_ID)),
            PROCESS_ID,
            CLIENT_ID,
            streamsMetrics,
            mockTime,
            streamsMetadataState,
            0,
            stateDirectory,
            new MockStateRestoreListener(),
            threadIdx,
            null,
            null
        );

        mockConsumer.enforceRebalance("Scheduled probing rebalance");
        mockClientSupplier.nextRebalanceMs().set(mockTime.milliseconds() - 1L);

        thread.start();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.STARTING,
            10 * 1000,
            "Thread never started.");

        TestUtils.retryOnExceptionWithTimeout(
            () -> verify(mockConsumer)
        );

        thread.shutdown();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.DEAD,
            10 * 1000,
            "Thread never shut down.");

    }

    @Test
    public void shouldNotEnforceRebalanceWhenCurrentlyRebalancing() throws InterruptedException {
        final Time mockTime = new MockTime(1);
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            APPLICATION_ID,
            config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            mockTime
        );

        final Consumer<byte[], byte[]> mockConsumer = EasyMock.createNiceMock(Consumer.class);
        expect(mockConsumer.poll(anyObject())).andStubReturn(ConsumerRecords.empty());
        expect(mockConsumer.assignment()).andStubReturn(emptySet());
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(mockConsumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumerGroupMetadata, mockConsumer);
        final EasyMockConsumerClientSupplier mockClientSupplier = new EasyMockConsumerClientSupplier(mockConsumer);
        mockClientSupplier.setCluster(createCluster());

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        stateDirectory = new StateDirectory(config, mockTime, true, false);
        final StreamsMetadataState streamsMetadataState = new StreamsMetadataState(
            new TopologyMetadata(internalTopologyBuilder, config),
            StreamsMetadataState.UNKNOWN_HOST,
            new LogContext(String.format("stream-client [%s] ", CLIENT_ID))
        );
        thread = StreamThread.create(
            topologyMetadata,
            config,
            mockClientSupplier,
            mockClientSupplier.getAdmin(config.getAdminConfigs(CLIENT_ID)),
            PROCESS_ID,
            CLIENT_ID,
            streamsMetrics,
            mockTime,
            streamsMetadataState,
            0,
            stateDirectory,
            new MockStateRestoreListener(),
            threadIdx,
            null,
            null
        );

        mockClientSupplier.nextRebalanceMs().set(mockTime.milliseconds() - 1L);
        thread.taskManager().handleRebalanceStart(emptySet());

        thread.start();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.STARTING,
            10 * 1000,
            "Thread never started.");

        TestUtils.retryOnExceptionWithTimeout(
            () -> verify(mockConsumer)
        );

        thread.shutdown();

        // Validate that the scheduled rebalance wasn't reset then set to MAX_VALUE so we
        // don't trigger one before we can shut down, since the rebalance must be ended
        // for the thread to fully shut down
        assertThat(mockClientSupplier.nextRebalanceMs().get(), not(0L));

        thread.taskManager().handleRebalanceComplete();

        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.DEAD,
            10 * 1000,
            "Thread never shut down.");

    }

    private static class EasyMockConsumerClientSupplier extends MockClientSupplier {
        final Consumer<byte[], byte[]> mockConsumer;
        final Map<String, Object> consumerConfigs = new HashMap<>();

        EasyMockConsumerClientSupplier(final Consumer<byte[], byte[]> mockConsumer) {
            this.mockConsumer = mockConsumer;
        }

        @Override
        public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
            consumerConfigs.putAll(config);
            return mockConsumer;
        }

        AtomicLong nextRebalanceMs() {
            return ((ReferenceContainer) consumerConfigs.get(
                    StreamsConfig.InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR)
                ).nextScheduledRebalanceMs;
        }
    }

    @Test
    public void shouldRespectNumIterationsInMainLoopWithoutProcessingThreads() {
        // With processing threads, there is no guarantee how many iterations will be performed
        Assume.assumeFalse(processingThreadsEnabled);

        final List<MockApiProcessor<byte[], byte[], Object, Object>> mockProcessors = new LinkedList<>();
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalTopologyBuilder.addProcessor(
            "processor1",
            (ProcessorSupplier<byte[], byte[], ?, ?>) () -> {
                final MockApiProcessor<byte[], byte[], Object, Object> processor = new MockApiProcessor<>(PunctuationType.WALL_CLOCK_TIME, 10L);
                mockProcessors.add(processor);
                return processor;
            },
            "source1"
        );
        internalTopologyBuilder.addProcessor(
            "processor2",
            (ProcessorSupplier<byte[], byte[], ?, ?>) () -> new MockApiProcessor<>(PunctuationType.STREAM_TIME, 10L),
            "source1"
        );

        final Properties properties = new Properties();
        properties.put(InternalConfig.STATE_UPDATER_ENABLED, stateUpdaterEnabled);
        properties.put(InternalConfig.PROCESSING_THREADS_ENABLED, processingThreadsEnabled);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig(APPLICATION_ID,
                                                                                         "localhost:2171",
                                                                                         Serdes.ByteArraySerde.class.getName(),
                                                                                         Serdes.ByteArraySerde.class.getName(),
                                                                                         properties));
        thread = createStreamThread(CLIENT_ID, config);

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        final TaskId task1 = new TaskId(0, t1p1.partition());
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);

        thread.taskManager().handleAssignment(Collections.singletonMap(task1, assignedPartitions), emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);
        runOnce();

        // processed one record, punctuated after the first record, and hence num.iterations is still 1
        long offset = -1;
        addRecord(mockConsumer, ++offset, 0L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed one more record without punctuation, and bump num.iterations to 2
        addRecord(mockConsumer, ++offset, 1L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(2));

        // processed zero records, early exit and iterations stays as 2
        runOnce();
        assertThat(thread.currentNumIterations(), equalTo(2));

        // system time based punctutation without processing any record, iteration stays as 2
        mockTime.sleep(11L);

        runOnce();
        assertThat(thread.currentNumIterations(), equalTo(2));

        // system time based punctutation after processing a record, half iteration to 1
        mockTime.sleep(11L);
        addRecord(mockConsumer, ++offset, 5L);

        runOnce();
        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed two records, bumping up iterations to 3 (1 + 2)
        addRecord(mockConsumer, ++offset, 5L);
        addRecord(mockConsumer, ++offset, 6L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        // stream time based punctutation halves to 1
        addRecord(mockConsumer, ++offset, 11L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed three records, bumping up iterations to 3 (1 + 2)
        addRecord(mockConsumer, ++offset, 12L);
        addRecord(mockConsumer, ++offset, 13L);
        addRecord(mockConsumer, ++offset, 14L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        mockProcessors.forEach(MockApiProcessor::requestCommit);
        addRecord(mockConsumer, ++offset, 15L);
        runOnce();

        // user requested commit should half iteration to 1
        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed three records, bumping up iterations to 3 (1 + 2)
        addRecord(mockConsumer, ++offset, 15L);
        addRecord(mockConsumer, ++offset, 16L);
        addRecord(mockConsumer, ++offset, 17L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        // time based commit without processing, should keep the iteration as 3
        mockTime.sleep(90L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        // time based commit without processing, should half the iteration to 1
        mockTime.sleep(90L);
        addRecord(mockConsumer, ++offset, 18L);
        runOnce();

        assertThat(thread.currentNumIterations(), equalTo(1));
    }

    @Test
    public void shouldNotCauseExceptionIfNothingCommitted() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumer, consumerGroupMetadata);
        final TaskManager taskManager = mockTaskManagerCommit(0);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();

        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        mockTime.sleep(commitInterval - 10L);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldCommitAfterCommitInterval() {
        final long commitInterval = 100L;
        final long commitLatency = 10L;

        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumer, consumerGroupMetadata);

        final AtomicBoolean committed = new AtomicBoolean(false);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        final TaskManager taskManager = new TaskManager(
            null,
            null,
            null,
            null,
            null,
            null,
            new Tasks(new LogContext()),
            topologyMetadata,
            null,
            null,
            null,
            null
        ) {
            @Override
            int commit(final Collection<Task> tasksToCommit) {
                committed.set(true);
                // we advance time to make sure the commit delay is considered when computing the next commit timestamp
                mockTime.sleep(commitLatency);
                return 1;
            }
        };
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);

        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        assertTrue(committed.get());

        mockTime.sleep(commitInterval);

        committed.set(false);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        assertFalse(committed.get());

        mockTime.sleep(1);

        committed.set(false);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        assertTrue(committed.get());
    }

    @Test
    public void shouldPurgeAfterPurgeInterval() {
        final long commitInterval = 100L;
        final long purgeInterval = 200L;

        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));
        props.setProperty(StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG, Long.toString(purgeInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());

        final TaskManager taskManager = mockTaskManagerPurge(2);

        EasyMock.replay(consumer, consumerGroupMetadata);

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);

        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        mockTime.sleep(purgeInterval + 1);

        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldRecordCommitLatency() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        expect(consumer.poll(anyObject())).andStubReturn(new ConsumerRecords<>(Collections.emptyMap()));
        expect(consumer.assignment()).andStubReturn(emptySet());
        final Task task = niceMock(Task.class);
        expect(task.id()).andStubReturn(task1);
        expect(task.inputPartitions()).andStubReturn(Collections.singleton(t1p1));
        expect(task.committedOffsets()).andStubReturn(Collections.emptyMap());
        expect(task.highWaterMark()).andStubReturn(Collections.emptyMap());
        final ActiveTaskCreator activeTaskCreator = mock(ActiveTaskCreator.class);
        expect(activeTaskCreator.createTasks(anyObject(), anyObject())).andStubReturn(Collections.singleton(task));
        expect(activeTaskCreator.producerClientIds()).andStubReturn(Collections.singleton("producerClientId"));

        final StandbyTaskCreator standbyTaskCreator = mock(StandbyTaskCreator.class);
        expect(standbyTaskCreator.createTasks(anyObject())).andStubReturn(Collections.emptySet());

        EasyMock.replay(consumer, consumerGroupMetadata, activeTaskCreator, standbyTaskCreator);

        final StateUpdater stateUpdater = Mockito.mock(StateUpdater.class);
        final DefaultTaskManager schedulingTaskManager;
        if (processingThreadsEnabled) {
            schedulingTaskManager = Mockito.mock(DefaultTaskManager.class);
            final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            future.complete(null);
            when(schedulingTaskManager.lockTasks(any())).thenReturn(future);
        } else {
            schedulingTaskManager = null;
        }

        final StreamsConfig config = new StreamsConfig(configProps(false));
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();

        final TaskManager taskManager = new TaskManager(
            null,
            changelogReader,
            null,
            null,
            activeTaskCreator,
            standbyTaskCreator,
            new Tasks(new LogContext()),
            topologyMetadata,
            null,
            null,
            stateUpdater,
            schedulingTaskManager
        ) {
            @Override
            int commit(final Collection<Task> tasksToCommit) {
                mockTime.sleep(10L);
                return 1;
            }
        };
        taskManager.setMainConsumer(consumer);

        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);
        thread.updateThreadMetadata("adminClientId");
        thread.setState(StreamThread.State.STARTING);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(task1, Collections.singleton(t1p1));
        thread.taskManager().handleAssignment(activeTasks, emptyMap());
        thread.rebalanceListener().onPartitionsAssigned(Collections.singleton(t1p1));

        assertTrue(
            Double.isNaN(
                (Double) streamsMetrics.metrics().get(new MetricName(
                    "commit-latency-max",
                    "stream-thread-metrics",
                    "",
                    Collections.singletonMap("thread-id", CLIENT_ID))
                ).metricValue()
            )
        );
        assertTrue(
            Double.isNaN(
                (Double) streamsMetrics.metrics().get(new MetricName(
                    "commit-latency-avg",
                    "stream-thread-metrics",
                    "",
                    Collections.singletonMap("thread-id", CLIENT_ID))
                ).metricValue()
            )
        );

        runOnce();

        assertThat(
            streamsMetrics.metrics().get(
                new MetricName(
                    "commit-latency-max",
                    "stream-thread-metrics",
                    "",
                    Collections.singletonMap("thread-id", CLIENT_ID)
                )
            ).metricValue(),
            equalTo(10.0)
        );
        assertThat(
            streamsMetrics.metrics().get(
                new MetricName(
                    "commit-latency-avg",
                    "stream-thread-metrics",
                    "",
                    Collections.singletonMap("thread-id", CLIENT_ID)
                )
            ).metricValue(),
            equalTo(10.0)
        );
    }

    @Test
    public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalStreamsBuilder.buildAndOptimizeTopology();

        final StreamsConfig config = new StreamsConfig(configProps(false));
        thread = createStreamThread(CLIENT_ID, config);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptyList());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        final Map<TopicPartition, Long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        thread.rebalanceListener().onPartitionsAssigned(new HashSet<>(assignedPartitions));

        assertEquals(1, clientSupplier.producers.size());
        final Producer<byte[], byte[]> globalProducer = clientSupplier.producers.get(0);
        for (final Task task : thread.readOnlyActiveTasks()) {
            assertSame(globalProducer, ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.mainConsumer());
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosAlphaEnabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties props = configProps(true);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        thread = createStreamThread(CLIENT_ID, new StreamsConfig(props));

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptyList());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        final Map<TopicPartition, Long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        thread.rebalanceListener().onPartitionsAssigned(new HashSet<>(assignedPartitions));

        runOnce();

        assertEquals(thread.readOnlyActiveTasks().size(), clientSupplier.producers.size());
        assertSame(clientSupplier.consumer, thread.mainConsumer());
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer());
    }

    @Test
    public void shouldInjectProducerPerThreadUsingClientSupplierOnCreateIfEosV2Enabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties props = configProps(true);
        thread = createStreamThread(CLIENT_ID, new StreamsConfig(props));

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptyList());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        final Map<TopicPartition, Long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        thread.rebalanceListener().onPartitionsAssigned(new HashSet<>(assignedPartitions));

        runOnce();

        assertThat(clientSupplier.producers.size(), is(1));
        assertSame(clientSupplier.consumer, thread.mainConsumer());
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer());
    }

    @Test
    public void shouldOnlyCompleteShutdownAfterRebalanceNotInProgress() throws InterruptedException {
        // The state updater is disabled for this test because this test relies on the fact the mainConsumer.resume()
        // is not called. This is not true when the state updater is enabled which leads to
        // java.lang.IllegalStateException: No current assignment for partition topic1-2.
        // Since this tests verifies an aspect that is independent from the state updater, it is OK to disable
        // the state updater and leave the rewriting of the test to later, when the code path for disabled state updater
        // is removed.
        Assume.assumeFalse(stateUpdaterEnabled);
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties props = configProps(true);
        thread =
            createStreamThread(CLIENT_ID, new StreamsConfig(props), new MockTime(1));

        thread.taskManager().handleRebalanceStart(Collections.singleton(topic1));

        // assign single partition
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        thread.start();
        TestUtils.waitForCondition(
                () -> thread.state() == StreamThread.State.STARTING,
                10 * 1000,
                "Thread never started.");

        thread.shutdown();

        // even if thread is no longer running, it should still be polling
        // as long as the rebalance is still ongoing
        assertFalse(thread.isRunning());
        assertTrue(thread.isAlive());

        Thread.sleep(1000);
        assertEquals(Utils.mkSet(task1, task2), thread.taskManager().activeTaskIds());
        assertEquals(StreamThread.State.PENDING_SHUTDOWN, thread.state());

        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.DEAD,
            10 * 1000,
            "Thread never shut down.");
        assertEquals(Collections.emptySet(), thread.taskManager().activeTaskIds());
    }

    @Test
    public void shouldCloseAllTaskProducersOnCloseIfEosEnabled() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        thread = createStreamThread(
            CLIENT_ID,
            new StreamsConfig(configProps(true)),
            new MockTime(1)
        );

        thread.start();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.STARTING,
            10 * 1000,
            "Thread never started.");

        thread.rebalanceListener().onPartitionsRevoked(Collections.emptyList());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        thread.shutdown();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.DEAD,
            10 * 1000,
            "Thread never shut down.");

        for (final Task task : thread.readOnlyActiveTasks()) {
            assertTrue(((MockProducer<byte[], byte[]>) ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer()).closed());
        }
    }

    @Test
    public void shouldShutdownTaskManagerOnClose() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumerGroupMetadata);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamsConfig config = new StreamsConfig(configProps(false));
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata)
            .updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));
        thread.setStateListener(
            (t, newState, oldState) -> {
                if (oldState == StreamThread.State.CREATED && newState == StreamThread.State.STARTING) {
                    thread.shutdown();
                }
            });
        thread.run();
        verify(taskManager);
    }

    @Test
    public void shouldNotReturnDataAfterTaskMigrated() {
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        expect(taskManager.allOwnedTasks()).andStubReturn(emptyMap());
        final InternalTopologyBuilder internalTopologyBuilder = EasyMock.createNiceMock(InternalTopologyBuilder.class);
        expect(internalTopologyBuilder.fullSourceTopicNames()).andReturn(Collections.singletonList(topic1)).times(2);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        final MockConsumer<byte[], byte[]> restoreConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        consumer.subscribe(Collections.singletonList(topic1), new MockRebalanceListener());
        consumer.rebalance(Collections.singletonList(t1p1));
        consumer.updateEndOffsets(Collections.singletonMap(t1p1, 10L));
        consumer.seekToEnd(Collections.singletonList(t1p1));

        final TaskMigratedException taskMigratedException = new TaskMigratedException(
            "Changelog restore found task migrated", new RuntimeException("restore task migrated"));
        ChangelogReader changelogReader = this.changelogReader;
        if (stateUpdaterEnabled) {
            expect(taskManager.checkStateUpdater(anyLong(), anyObject())).andAnswer(() -> {
                consumer.addRecord(new ConsumerRecord<>(topic1, 1, 11, new byte[0], new byte[0]));
                consumer.addRecord(new ConsumerRecord<>(topic1, 1, 12, new byte[1], new byte[0]));

                throw taskMigratedException;
            });
        } else {
            changelogReader = new MockChangelogReader() {
                @Override
                public long restore(final Map<TaskId, Task> tasks) {
                    consumer.addRecord(new ConsumerRecord<>(topic1, 1, 11, new byte[0], new byte[0]));
                    consumer.addRecord(new ConsumerRecord<>(topic1, 1, 12, new byte[1], new byte[0]));

                    throw taskMigratedException;
                }
            };
        }

        taskManager.handleLostAll();

        EasyMock.replay(taskManager, internalTopologyBuilder);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);

        final Properties props = configProps(false);
        final StreamsConfig config = new StreamsConfig(props);
        thread = new StreamThread(
            new MockTime(1),
            config,
            null,
            consumer,
            restoreConsumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            new TopologyMetadata(internalTopologyBuilder, config),
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        final StreamsException thrown = assertThrows(StreamsException.class, thread::run);

        verify(taskManager);

        assertThat(thrown.getCause(), isA(IllegalStateException.class));
        // The Mock consumer shall throw as the assignment has been wiped out, but records are assigned.
        assertEquals("No current assignment for partition topic1-1", thrown.getCause().getMessage());
        assertFalse(consumer.shouldRebalance());
    }

    @Test
    public void shouldShutdownTaskManagerOnCloseWithoutStart() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumerGroupMetadata);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamsConfig config = new StreamsConfig(configProps(false));
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata)
            .updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));
        thread.shutdown();
        verify(taskManager);
    }

    @Test
    public void shouldOnlyShutdownOnce() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumerGroupMetadata);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamsConfig config = new StreamsConfig(configProps(false));
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata)
            .updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));
        thread.shutdown();
        // Execute the run method. Verification of the mock will check that shutdown was only done once
        thread.run();
        verify(taskManager);
    }

    @Test
    public void shouldNotThrowWhenStandbyTasksAssignedAndNoStateStoresForTopology() {
        internalTopologyBuilder.addSource(null, "name", null, null, null, "topic");
        internalTopologyBuilder.addSink("out", "output", null, null, null, "name");

        final StreamsConfig config = new StreamsConfig(configProps(false));
        thread = createStreamThread(CLIENT_ID, config);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptyList());

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(emptyMap(), standbyTasks);

        thread.rebalanceListener().onPartitionsAssigned(Collections.emptyList());
    }

    @Test
    public void shouldNotCloseTaskAndRemoveFromTaskManagerIfProducerWasFencedWhileProcessing() throws Exception {
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);
        internalTopologyBuilder.addSink("sink", "dummyTopic", null, null, null, "source");

        final StreamsConfig config = new StreamsConfig(configProps(true));
        thread = createStreamThread(CLIENT_ID, config);

        final MockConsumer<byte[], byte[]> consumer = clientSupplier.consumer;

        consumer.updatePartitions(topic1, Collections.singletonList(new PartitionInfo(topic1, 1, null, null, null)));

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        runOnce();
        assertThat(thread.readOnlyActiveTasks().size(), equalTo(1));
        final MockProducer<byte[], byte[]> producer = clientSupplier.producers.get(0);

        // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
        consumer.updateBeginningOffsets(Collections.singletonMap(assignedPartitions.iterator().next(), 0L));
        consumer.unsubscribe();
        consumer.assign(new HashSet<>(assignedPartitions));

        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 0, new byte[0], new byte[0]));
        if (processingThreadsEnabled) {
            assertTrue(runUntilTimeoutOrCondition(this::runOnce, () -> !producer.history().isEmpty()));
        } else {
            mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
            runOnce();
            assertThat(producer.history().size(), equalTo(1));
        }

        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        TestUtils.waitForCondition(
            () -> producer.commitCount() == 1,
            "StreamsThread did not commit transaction.");

        producer.fenceProducer();
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 1, new byte[0], new byte[0]));

        try {
            if (processingThreadsEnabled) {
                runUntilTimeoutOrException(this::runOnce);
            } else {
                runOnce();
            }
            fail("Should have thrown TaskMigratedException");
        } catch (final KafkaException expected) {
            assertTrue(String.format("Expected TaskMigratedException but got %s", expected), expected instanceof TaskMigratedException);
            assertTrue("StreamsThread removed the fenced zombie task already, should wait for rebalance to close all zombies together.",
                thread.readOnlyActiveTasks().stream().anyMatch(task -> task.id().equals(task1)));
        }

        assertThat(producer.commitCount(), equalTo(1L));
    }

    @Test
    public void shouldNotCloseTaskAndRemoveFromTaskManagerIfProducerGotFencedInCommitTransactionWhenSuspendingTasks() throws Exception {
        final StreamsConfig config = new StreamsConfig(configProps(true));
        thread = createStreamThread(CLIENT_ID, config);

        internalTopologyBuilder.addSource(null, "name", null, null, null, topic1);
        internalTopologyBuilder.addSink("out", "output", null, null, null, "name");

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        runOnce();

        assertThat(thread.readOnlyActiveTasks().size(), equalTo(1));

        // need to process a record to enable committing
        addRecord(mockConsumer, 0L);
        final MockProducer<byte[], byte[]> producer = clientSupplier.producers.get(0);
        runOnce();
        if (processingThreadsEnabled) {
            waitForCondition(() -> !producer.uncommittedRecords().isEmpty(), "Processing threads to process record");
        }

        producer.commitTransactionException = new ProducerFencedException("Producer is fenced");
        assertThrows(TaskMigratedException.class, () -> thread.rebalanceListener().onPartitionsRevoked(assignedPartitions));
        assertFalse(producer.transactionCommitted());
        assertFalse(producer.closed());
        assertEquals(1, thread.readOnlyActiveTasks().size());
    }

    @Test
    public void shouldReinitializeRevivedTasksInAnyState() throws Exception {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        thread = createStreamThread(CLIENT_ID, config, new MockTime(1));

        final String storeName = "store";
        final String storeChangelog = "stream-thread-test-store-changelog";
        final TopicPartition storeChangelogTopicPartition = new TopicPartition(storeChangelog, 1);

        internalTopologyBuilder.addSource(null, "name", null, null, null, topic1);
        final AtomicBoolean shouldThrow = new AtomicBoolean(false);
        final AtomicBoolean processed = new AtomicBoolean(false);
        // TODO check if needs to be extended
        internalTopologyBuilder.addProcessor(
            "proc",
            (ProcessorSupplier<Object, Object, Object, Object>) () -> record -> {
                if (shouldThrow.get()) {
                    throw new TaskCorruptedException(singleton(task1));
                } else {
                    processed.set(true);
                }
            },
            "name"
        );
        internalTopologyBuilder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.String()
                ),
                "proc"
        );
        internalTopologyBuilder.buildTopology();

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(mkMap(
                mkEntry(t1p1, 0L)
        ));

        final MockConsumer<byte[], byte[]> restoreConsumer = (MockConsumer<byte[], byte[]>) thread.restoreConsumer();
        restoreConsumer.updateBeginningOffsets(mkMap(
                mkEntry(storeChangelogTopicPartition, 0L)
        ));
        final MockAdminClient admin = (MockAdminClient) thread.adminClient();
        admin.updateEndOffsets(singletonMap(storeChangelogTopicPartition, 0L));

        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);


        // the first iteration completes the restoration
        runOnce();
        assertThat(thread.readOnlyActiveTasks().size(), equalTo(1));

        // the second transits to running and unpause the input
        runOnce();

        // the third actually polls, processes the record, and throws the corruption exception
        if (stateUpdaterEnabled) {
            TestUtils.waitForCondition(
                () -> thread.taskManager().checkStateUpdater(
                    mockTime.milliseconds(),
                    topicPartitions -> mockConsumer.seekToBeginning(singleton(t1p1))
                ),
                10 * 1000,
                "State updater never returned tasks.");
        }
        addRecord(mockConsumer, 0L);
        shouldThrow.set(true);
        final TaskCorruptedException taskCorruptedException;
        if (processingThreadsEnabled) {
            taskCorruptedException = assertThrows(TaskCorruptedException.class, () -> runUntilTimeoutOrException(this::runOnce));
        } else {
            taskCorruptedException = assertThrows(TaskCorruptedException.class, this::runOnce);
        }

        // Now, we can handle the corruption
        thread.taskManager().handleCorruption(taskCorruptedException.corruptedTasks());
        if (stateUpdaterEnabled) {
            TestUtils.waitForCondition(
                () -> thread.taskManager().checkStateUpdater(
                    mockTime.milliseconds(),
                    topicPartitions -> mockConsumer.seekToBeginning(singleton(t1p1))
                ),
                10 * 1000,
                "State updater never returned tasks.");
        }

        // again, complete the restoration
        runOnce();
        // transit to running and unpause
        runOnce();
        // process the record
        addRecord(mockConsumer, 0L);
        shouldThrow.set(false);
        assertThat(processed.get(), is(false));
        if (processingThreadsEnabled) {
            assertTrue(runUntilTimeoutOrCondition(this::runOnce, processed::get));
        } else {
            runOnce();
            runOnce();
            assertThat(processed.get(), is(true));
        }
    }

    @Test
    public void shouldNotCloseTaskAndRemoveFromTaskManagerIfProducerGotFencedInCommitTransactionWhenCommitting() {
        // only have source but no sink so that we would not get fenced in producer.send
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);

        final StreamsConfig config = new StreamsConfig(configProps(true));
        thread = createStreamThread(CLIENT_ID, config);

        final MockConsumer<byte[], byte[]> consumer = clientSupplier.consumer;

        consumer.updatePartitions(topic1, Collections.singletonList(new PartitionInfo(topic1, 1, null, null, null)));

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        runOnce();
        assertThat(thread.readOnlyActiveTasks().size(), equalTo(1));
        final MockProducer<byte[], byte[]> producer = clientSupplier.producers.get(0);

        producer.commitTransactionException = new ProducerFencedException("Producer is fenced");
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 1, new byte[0], new byte[0]));
        try {
            if (processingThreadsEnabled) {
                runUntilTimeoutOrException(this::runOnce);
            } else {
                runOnce();
            }
            fail("Should have thrown TaskMigratedException");
        } catch (final KafkaException expected) {
            assertTrue(expected instanceof TaskMigratedException);
            assertTrue("StreamsThread removed the fenced zombie task already, should wait for rebalance to close all zombies together.",
                thread.readOnlyActiveTasks().stream().anyMatch(task -> task.id().equals(task1)));
        }

        assertThat(producer.commitCount(), equalTo(0L));

        assertTrue(clientSupplier.producers.get(0).transactionInFlight());
        assertFalse(clientSupplier.producers.get(0).transactionCommitted());
        assertFalse(clientSupplier.producers.get(0).closed());
        assertEquals(1, thread.readOnlyActiveTasks().size());
    }

    @Test
    public void shouldNotCloseTaskProducerWhenSuspending() throws Exception {
        final StreamsConfig config = new StreamsConfig(configProps(true));
        thread = createStreamThread(CLIENT_ID, config);

        internalTopologyBuilder.addSource(null, "name", null, null, null, topic1);
        internalTopologyBuilder.addSink("out", "output", null, null, null, "name");

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        runOnce();

        assertThat(thread.readOnlyActiveTasks().size(), equalTo(1));

        // need to process a record to enable committing
        addRecord(mockConsumer, 0L);
        final MockProducer<byte[], byte[]> producer = clientSupplier.producers.get(0);

        if (processingThreadsEnabled) {
            assertTrue(runUntilTimeoutOrCondition(this::runOnce, () -> !producer.history().isEmpty()));
        } else {
            runOnce();
        }

        thread.rebalanceListener().onPartitionsRevoked(assignedPartitions);
        assertTrue(producer.transactionCommitted());
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.closed());
        assertEquals(1, thread.readOnlyActiveTasks().size());
    }

    @Test
    public void shouldReturnActiveTaskMetadataWhileRunningState() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);

        clientSupplier.setCluster(createCluster());

        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            APPLICATION_ID,
            config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            mockTime
        );

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();

        stateDirectory = new StateDirectory(config, mockTime, true, false);
        final StreamsMetadataState streamsMetadataState = new StreamsMetadataState(
            new TopologyMetadata(internalTopologyBuilder, config),
            StreamsMetadataState.UNKNOWN_HOST,
            new LogContext(String.format("stream-client [%s] ", CLIENT_ID))
        );
        thread = StreamThread.create(
            topologyMetadata,
            config,
            clientSupplier,
            clientSupplier.getAdmin(config.getAdminConfigs(CLIENT_ID)),
            PROCESS_ID,
            CLIENT_ID,
            streamsMetrics,
            mockTime,
            streamsMetadataState,
            0,
            stateDirectory,
            new MockStateRestoreListener(),
            threadIdx,
            null,
            HANDLER
        );

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        runOnce();

        final ThreadMetadata metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), metadata.threadState());
        assertTrue(metadata.activeTasks().contains(new TaskMetadataImpl(task1, Utils.mkSet(t1p1), new HashMap<>(), new HashMap<>(), Optional.empty())));
        assertTrue(metadata.standbyTasks().isEmpty());

        assertTrue("#threadState() was: " + metadata.threadState() + "; expected either RUNNING, STARTING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED",
                   Arrays.asList("RUNNING", "STARTING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED").contains(metadata.threadState()));
        final String threadName = metadata.threadName();
        assertThat(threadName, startsWith(CLIENT_ID + "-StreamThread-" + threadIdx));
        assertEquals(threadName + "-consumer", metadata.consumerClientId());
        assertEquals(threadName + "-restore-consumer", metadata.restoreConsumerClientId());
        assertEquals(Collections.singleton(threadName + "-producer"), metadata.producerClientIds());
        assertEquals(CLIENT_ID + "-admin", metadata.adminClientId());
    }

    @Test
    public void shouldReturnStandbyTaskMetadataWhileRunningState() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
                              .groupByKey().count(Materialized.as("count-one"));

        internalStreamsBuilder.buildAndOptimizeTopology();
        thread = createStreamThread(CLIENT_ID, config, new MockTime(1));
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions(
            STREAM_THREAD_TEST_COUNT_ONE_CHANGELOG,
            Collections.singletonList(
                new PartitionInfo(STREAM_THREAD_TEST_COUNT_ONE_CHANGELOG,
                                  0,
                                  null,
                                  new Node[0],
                                  new Node[0])
            )
        );

        final HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(STREAM_THREAD_TEST_COUNT_ONE_CHANGELOG, 1), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(emptyMap(), standbyTasks);

        thread.rebalanceListener().onPartitionsAssigned(Collections.emptyList());

        runOnce();

        final ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.standbyTasks().contains(new TaskMetadataImpl(task1, Utils.mkSet(t1p1), new HashMap<>(), new HashMap<>(), Optional.empty())));
        assertTrue(threadMetadata.activeTasks().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldUpdateStandbyTask() throws Exception {
        // Updating standby tasks on the stream thread only happens when the state updater is disabled
        Assume.assumeFalse(stateUpdaterEnabled);

        final String storeName1 = "count-one";
        final String storeName2 = "table-two";
        final String changelogName1 = APPLICATION_ID + "-" + storeName1 + "-changelog";
        final String changelogName2 = APPLICATION_ID + "-" + storeName2 + "-changelog";
        final Properties props = configProps(false);
        final StreamsConfig config = new StreamsConfig(props);
        thread = createStreamThread(CLIENT_ID, config);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;

        setupThread(storeName1, storeName2, changelogName1, changelogName2, restoreConsumer, false);

        runOnce();

        final StandbyTask standbyTask1 = standbyTask(thread.taskManager(), t1p1);
        final StandbyTask standbyTask2 = standbyTask(thread.taskManager(), t2p1);
        assertEquals(task1, standbyTask1.id());
        assertEquals(task3, standbyTask2.id());

        final KeyValueStore<Object, Long> store1 = (KeyValueStore<Object, Long>) standbyTask1.getStore(storeName1);
        final KeyValueStore<Object, Long> store2 = (KeyValueStore<Object, Long>) standbyTask2.getStore(storeName2);

        assertEquals(0L, store1.approximateNumEntries());
        assertEquals(0L, store2.approximateNumEntries());

        addStandbyRecordsToRestoreConsumer(restoreConsumer);

        runOnce();

        assertEquals(10L, store1.approximateNumEntries());
        assertEquals(4L, store2.approximateNumEntries());
    }

    private void addActiveRecordsToRestoreConsumer(final MockConsumer<byte[], byte[]> restoreConsumer) {
        for (long i = 0L; i < 10L; i++) {
            restoreConsumer.addRecord(new ConsumerRecord<>(
                STREAM_THREAD_TEST_COUNT_ONE_CHANGELOG,
                2,
                i,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
        }
    }

    private void addStandbyRecordsToRestoreConsumer(final MockConsumer<byte[], byte[]> restoreConsumer) {
        // let the store1 be restored from 0 to 10; store2 be restored from 5 (checkpointed) to 10
        for (long i = 0L; i < 10L; i++) {
            restoreConsumer.addRecord(new ConsumerRecord<>(
                STREAM_THREAD_TEST_COUNT_ONE_CHANGELOG,
                1,
                i,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
            restoreConsumer.addRecord(new ConsumerRecord<>(
                STREAM_THREAD_TEST_TABLE_TWO_CHANGELOG,
                1,
                i,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
        }
    }

    private void setupThread(final String storeName1,
                             final String storeName2,
                             final String changelogName1,
                             final String changelogName2,
                             final MockConsumer<byte[], byte[]> restoreConsumer,
                             final boolean addActiveTask) throws IOException {
        final TopicPartition activePartition = new TopicPartition(changelogName1, 2);
        final TopicPartition partition1 = new TopicPartition(changelogName1, 1);
        final TopicPartition partition2 = new TopicPartition(changelogName2, 1);

        internalStreamsBuilder
            .stream(Collections.singleton(topic1), consumed)
            .groupByKey()
            .count(Materialized.as(storeName1));
        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized
            = new MaterializedInternal<>(Materialized.as(storeName2), internalStreamsBuilder, "");
        internalStreamsBuilder.table(topic2, new ConsumedInternal<>(), materialized);

        internalStreamsBuilder.buildAndOptimizeTopology();
        restoreConsumer.updatePartitions(changelogName1,
            Collections.singletonList(new PartitionInfo(changelogName1, 1, null, new Node[0], new Node[0]))
        );

        restoreConsumer.updateEndOffsets(Collections.singletonMap(activePartition, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(activePartition, 0L));
        ((MockAdminClient) (thread.adminClient())).updateBeginningOffsets(Collections.singletonMap(activePartition, 0L));
        ((MockAdminClient) (thread.adminClient())).updateEndOffsets(Collections.singletonMap(activePartition, 10L));

        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition1, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition1, 0L));
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition2, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition2, 0L));
        final OffsetCheckpoint checkpoint
            = new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(task3), CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(partition2, 5L));

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        if (addActiveTask) {
            activeTasks.put(task2, Collections.singleton(t1p2));
        }

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));
        standbyTasks.put(task3, Collections.singleton(t2p1));

        thread.taskManager().handleAssignment(activeTasks, standbyTasks);
        thread.taskManager().tryToCompleteRestoration(mockTime.milliseconds(), null);

        thread.rebalanceListener().onPartitionsAssigned(Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotUpdateStandbyTaskWhenPaused() throws Exception {
        // Updating standby tasks on the stream thread only happens when the state updater is disabled
        Assume.assumeFalse(stateUpdaterEnabled);

        final StreamsConfig config = new StreamsConfig(configProps(false));
        final String storeName1 = "count-one";
        final String storeName2 = "table-two";
        final String changelogName1 = APPLICATION_ID + "-" + storeName1 + "-changelog";
        final String changelogName2 = APPLICATION_ID + "-" + storeName2 + "-changelog";
        thread = createStreamThread(CLIENT_ID, config);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;

        setupThread(storeName1, storeName2, changelogName1, changelogName2, restoreConsumer, true);

        runOnce();

        final StreamTask activeTask1 = activeTask(thread.taskManager(), t1p2);
        final StandbyTask standbyTask1 = standbyTask(thread.taskManager(), t1p1);
        final StandbyTask standbyTask2 = standbyTask(thread.taskManager(), t2p1);
        assertEquals(task1, standbyTask1.id());
        assertEquals(task3, standbyTask2.id());

        final KeyValueStore<Object, Long> activeStore = (KeyValueStore<Object, Long>) activeTask1.getStore(storeName1);

        final KeyValueStore<Object, Long> store1 = (KeyValueStore<Object, Long>) standbyTask1.getStore(storeName1);
        final KeyValueStore<Object, Long> store2 = (KeyValueStore<Object, Long>) standbyTask2.getStore(storeName2);

        assertEquals(0L, activeStore.approximateNumEntries());
        assertEquals(0L, store1.approximateNumEntries());
        assertEquals(0L, store2.approximateNumEntries());

        // Add some records that the active task would handle
        addActiveRecordsToRestoreConsumer(restoreConsumer);
        // let the store1 be restored from 0 to 10; store2 be restored from 5 (checkpointed) to 10
        addStandbyRecordsToRestoreConsumer(restoreConsumer);

        // Simulate pause
        thread.taskManager().topologyMetadata().pauseTopology(TopologyMetadata.UNNAMED_TOPOLOGY);
        runOnce();

        assertEquals(0L, activeStore.approximateNumEntries());
        assertEquals(0L, store1.approximateNumEntries());
        assertEquals(0L, store2.approximateNumEntries());

        // Simulate resume
        thread.taskManager().topologyMetadata().resumeTopology(TopologyMetadata.UNNAMED_TOPOLOGY);
        runOnce();

        assertEquals(10L, activeStore.approximateNumEntries());
        assertEquals(0L, store1.approximateNumEntries());
        assertEquals(0L, store2.approximateNumEntries());

        runOnce();
        assertEquals(10L, activeStore.approximateNumEntries());
        assertEquals(10L, store1.approximateNumEntries());
        assertEquals(4L, store2.approximateNumEntries());
    }

    @Test
    public void shouldCreateStandbyTask() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        setupInternalTopologyWithoutState(config);
        internalTopologyBuilder.addStateStore(new MockKeyValueStoreBuilder("myStore", true), "processor1");

        assertThat(createStandbyTask(config), not(empty()));
    }

    @Test
    public void shouldNotCreateStandbyTaskWithoutStateStores() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        setupInternalTopologyWithoutState(config);

        assertThat(createStandbyTask(config), empty());
    }

    @Test
    public void shouldNotCreateStandbyTaskIfStateStoresHaveLoggingDisabled() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        setupInternalTopologyWithoutState(config);
        final StoreBuilder<KeyValueStore<Object, Object>> storeBuilder =
            new MockKeyValueStoreBuilder("myStore", true);
        storeBuilder.withLoggingDisabled();
        internalTopologyBuilder.addStateStore(storeBuilder, "processor1");

        assertThat(createStandbyTask(config), empty());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldPunctuateActiveTask() {
        Assume.assumeFalse(processingThreadsEnabled);

        final List<Long> punctuatedStreamTime = new ArrayList<>();
        final List<Long> punctuatedWallClockTime = new ArrayList<>();
        final ProcessorSupplier<Object, Object, Void, Void> punctuateProcessor =
            () -> new ContextualProcessor<Object, Object, Void, Void>() {
                @Override
                public void init(final ProcessorContext<Void, Void> context) {
                    context.schedule(Duration.ofMillis(100L), PunctuationType.STREAM_TIME, punctuatedStreamTime::add);
                    context.schedule(Duration.ofMillis(100L), PunctuationType.WALL_CLOCK_TIME, punctuatedWallClockTime::add);
                }

                @Override
                public void process(final Record<Object, Object> record) {}
            };

        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed).process(punctuateProcessor);
        internalStreamsBuilder.buildAndOptimizeTopology();

        final StreamsConfig config = new StreamsConfig(configProps(false));
        thread = createStreamThread(CLIENT_ID, config);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        clientSupplier.consumer.assign(assignedPartitions);
        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        runOnce();

        assertEquals(0, punctuatedStreamTime.size());
        assertEquals(0, punctuatedWallClockTime.size());

        mockTime.sleep(100L);
        clientSupplier.consumer.addRecord(new ConsumerRecord<>(
            topic1,
            1,
            100L,
            100L,
            TimestampType.CREATE_TIME,
            "K".getBytes().length,
            "V".getBytes().length,
            "K".getBytes(),
            "V".getBytes(),
            new RecordHeaders(),
            Optional.empty()));

        runOnce();

        assertEquals(1, punctuatedStreamTime.size());
        assertEquals(1, punctuatedWallClockTime.size());

        mockTime.sleep(100L);

        runOnce();

        // we should skip stream time punctuation, only trigger wall-clock time punctuation
        assertEquals(1, punctuatedStreamTime.size());
        assertEquals(2, punctuatedWallClockTime.size());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldPunctuateWithTimestampPreservedInProcessorContext() {
        Assume.assumeFalse(processingThreadsEnabled);

        final org.apache.kafka.streams.kstream.TransformerSupplier<Object, Object, KeyValue<Object, Object>> punctuateProcessor =
            () -> new org.apache.kafka.streams.kstream.Transformer<Object, Object, KeyValue<Object, Object>>() {
                @Override
                public void init(final org.apache.kafka.streams.processor.ProcessorContext context) {
                    context.schedule(Duration.ofMillis(100L), PunctuationType.WALL_CLOCK_TIME, timestamp -> context.forward("key", "value"));
                    context.schedule(Duration.ofMillis(100L), PunctuationType.STREAM_TIME, timestamp -> context.forward("key", "value"));
                }

                @Override
                public KeyValue<Object, Object> transform(final Object key, final Object value) {
                        return null;
                    }

                @Override
                public void close() {}
            };

        final List<Long> peekedContextTime = new ArrayList<>();
        final ProcessorSupplier<Object, Object, Void, Void> peekProcessor =
            () -> (Processor<Object, Object, Void, Void>) record -> peekedContextTime.add(record.timestamp());

        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
            .transform(punctuateProcessor)
            .process(peekProcessor);
        internalStreamsBuilder.buildAndOptimizeTopology();

        final long currTime = mockTime.milliseconds();
        thread = createStreamThread(CLIENT_ID);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        clientSupplier.consumer.assign(assignedPartitions);
        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        runOnce();
        assertEquals(0, peekedContextTime.size());

        mockTime.sleep(100L);
        runOnce();

        assertEquals(1, peekedContextTime.size());
        assertEquals(currTime + 100L, peekedContextTime.get(0).longValue());

        clientSupplier.consumer.addRecord(new ConsumerRecord<>(
            topic1,
            1,
            110L,
            110L,
            TimestampType.CREATE_TIME,
            "K".getBytes().length,
            "V".getBytes().length,
            "K".getBytes(),
            "V".getBytes(),
            new RecordHeaders(),
            Optional.empty()));

        runOnce();

        assertEquals(2, peekedContextTime.size());
        assertEquals(110L, peekedContextTime.get(1).longValue());
    }

    @Test
    public void shouldAlwaysUpdateTasksMetadataAfterChangingState() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        thread = createStreamThread(CLIENT_ID, config);
        ThreadMetadata metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.CREATED.name(), metadata.threadState());

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);
        thread.setState(StreamThread.State.PARTITIONS_ASSIGNED);
        thread.setState(StreamThread.State.RUNNING);
        metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), metadata.threadState());
    }

    @Test
    public void shouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore() throws Exception {
        internalStreamsBuilder.stream(Collections.singleton("topic"), consumed)
            .groupByKey()
            .count(Materialized.as("count"));
        internalStreamsBuilder.buildAndOptimizeTopology();

        thread = createStreamThread("clientId", new MockTime(1));
        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        final MockConsumer<byte[], byte[]> mockRestoreConsumer = (MockConsumer<byte[], byte[]>) thread.restoreConsumer();
        final MockAdminClient mockAdminClient = (MockAdminClient) thread.adminClient();

        final TopicPartition topicPartition = new TopicPartition("topic", 0);
        final Set<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final TaskId task0 = new TaskId(0, 0);
        activeTasks.put(task0, topicPartitionSet);

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        mockConsumer.updatePartitions(
            "topic",
            Collections.singletonList(
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
        mockConsumer.subscribe(mkSet(topicPartition.topic()));
        mockConsumer.rebalance(Collections.singleton(topicPartition));

        mockRestoreConsumer.updatePartitions(
            "stream-thread-test-count-changelog",
            Collections.singletonList(
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
        mockAdminClient.updateEndOffsets(Collections.singletonMap(changelogPartition, 2L));

        mockConsumer.schedulePollTask(() -> {
            thread.setState(StreamThread.State.PARTITIONS_REVOKED);
            thread.rebalanceListener().onPartitionsAssigned(topicPartitionSet);
        });

        thread.start();

        TestUtils.waitForCondition(
            () -> mockRestoreConsumer.assignment().size() == 1,
            "Never get the assignment");

        mockRestoreConsumer.addRecord(new ConsumerRecord<>(
            "stream-thread-test-count-changelog",
            0,
            0L,
            "K1".getBytes(),
            "V1".getBytes()));

        TestUtils.waitForCondition(
            () -> mockRestoreConsumer.position(changelogPartition) == 1L,
            "Never restore first record");

        mockRestoreConsumer.setPollException(new InvalidOffsetException("Try Again!") {
            @Override
            public Set<TopicPartition> partitions() {
                return changelogPartitionSet;
            }
        });

        // after handling the exception and reviving the task, with the state updater the changelog topic is
        // registered again with the changelog reader
        TestUtils.waitForCondition(
            () -> mockRestoreConsumer.assignment().size() == 1,
            "Never get the assignment");

        // after handling the exception and reviving the task, the position
        // should be reset to the beginning.
        TestUtils.waitForCondition(
            () -> mockRestoreConsumer.position(changelogPartition) == 0L,
            "Never restore first record");

        mockRestoreConsumer.addRecord(new ConsumerRecord<>(
            "stream-thread-test-count-changelog",
            0,
            0L,
            "K1".getBytes(),
            "V1".getBytes()));
        mockRestoreConsumer.addRecord(new ConsumerRecord<>(
            "stream-thread-test-count-changelog",
            0,
            1L,
            "K2".getBytes(),
            "V2".getBytes()));

        if (stateUpdaterEnabled) {
            TestUtils.waitForCondition(
                () -> mockRestoreConsumer.assignment().size() == 0,
                "Never get the assignment");
        } else {
            TestUtils.waitForCondition(
                () -> {
                    mockRestoreConsumer.assign(changelogPartitionSet);
                    return mockRestoreConsumer.position(changelogPartition) == 2L;
                },
                "Never finished restore");
        }
    }

    @Test
    public void shouldLogAndRecordSkippedMetricForDeserializationException() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties properties = configProps(false);
        properties.setProperty(
            StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class.getName()
        );
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        thread = createStreamThread(CLIENT_ID, new StreamsConfig(properties));

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        final TaskId task1 = new TaskId(0, t1p1.partition());
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
        thread.taskManager().handleAssignment(
            Collections.singletonMap(task1, assignedPartitions),
            emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);
        runOnce();

        long offset = -1;
        mockConsumer.addRecord(new ConsumerRecord<>(
            t1p1.topic(),
            t1p1.partition(),
            ++offset,
            -1,
            TimestampType.CREATE_TIME,
            -1,
            -1,
            new byte[0],
            "I am not an integer.".getBytes(),
            new RecordHeaders(),
            Optional.empty()));
        mockConsumer.addRecord(new ConsumerRecord<>(
            t1p1.topic(),
            t1p1.partition(),
            ++offset,
            -1,
            TimestampType.CREATE_TIME,
            -1,
            -1,
            new byte[0],
            "I am not an integer.".getBytes(),
            new RecordHeaders(),
            Optional.empty()));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RecordDeserializer.class)) {
            runOnce();

            final List<String> strings = appender.getMessages();
            assertTrue(strings.contains("stream-thread [" + Thread.currentThread().getName() + "] task [0_1]" +
                " Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]"));
            assertTrue(strings.contains("stream-thread [" + Thread.currentThread().getName() + "] task [0_1]" +
                " Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]"));
        }
    }

    @Test
    public void shouldThrowTaskMigratedExceptionHandlingTaskLost() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);

        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        consumer.assign(assignedPartitions);
        consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(t1p1, 10L));

        taskManager.handleLostAll();
        EasyMock.expectLastCall()
            .andThrow(new TaskMigratedException("Task lost exception", new RuntimeException()));

        EasyMock.replay(taskManager);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata)
            .updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        consumer.schedulePollTask(() -> {
            thread.setState(StreamThread.State.PARTITIONS_REVOKED);
            thread.rebalanceListener().onPartitionsLost(assignedPartitions);
        });

        thread.setState(StreamThread.State.STARTING);
        assertThrows(TaskMigratedException.class, this::runOnce);
    }

    @Test
    public void shouldThrowTaskMigratedExceptionHandlingRevocation() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);

        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        consumer.assign(assignedPartitions);
        consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(t1p1, 10L));

        taskManager.handleRevocation(assignedPartitions);
        EasyMock.expectLastCall()
            .andThrow(new TaskMigratedException("Revocation non fatal exception", new RuntimeException()));

        EasyMock.replay(taskManager);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata)
            .updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        consumer.schedulePollTask(() -> {
            thread.setState(StreamThread.State.PARTITIONS_REVOKED);
            thread.rebalanceListener().onPartitionsRevoked(assignedPartitions);
        });

        thread.setState(StreamThread.State.STARTING);
        assertThrows(TaskMigratedException.class, this::runOnce);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCatchHandleCorruptionOnTaskCorruptedExceptionPath() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        consumer.subscribe((Collection<String>) anyObject(), anyObject());
        EasyMock.expectLastCall().anyTimes();
        consumer.unsubscribe();
        EasyMock.expectLastCall().anyTimes();
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumerGroupMetadata);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);
        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 2);

        final Set<TaskId> corruptedTasks = singleton(taskId1);

        expect(task1.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task1.id()).andReturn(taskId1).anyTimes();
        expect(task2.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task2.id()).andReturn(taskId2).anyTimes();

        expect(taskManager.handleCorruption(corruptedTasks)).andReturn(true);

        EasyMock.replay(task1, task2, taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        ) {
            @Override
            void runOnceWithProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
            @Override
            void runOnceWithoutProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
        }.updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        thread.run();

        verify(taskManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCatchTimeoutExceptionFromHandleCorruptionAndInvokeExceptionHandler() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        consumer.subscribe((Collection<String>) anyObject(), anyObject());
        EasyMock.expectLastCall().anyTimes();
        consumer.unsubscribe();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(consumerGroupMetadata);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);
        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 2);

        final Set<TaskId> corruptedTasks = singleton(taskId1);

        expect(task1.state()).andStubReturn(Task.State.RUNNING);
        expect(task1.id()).andStubReturn(taskId1);
        expect(task2.state()).andStubReturn(Task.State.RUNNING);
        expect(task2.id()).andStubReturn(taskId2);

        taskManager.handleCorruption(corruptedTasks);
        expectLastCall().andThrow(new TimeoutException());

        EasyMock.replay(task1, task2, taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        ) {
            @Override
            void runOnceWithProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
            @Override
            void runOnceWithoutProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
        }.updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        final AtomicBoolean exceptionHandlerInvoked = new AtomicBoolean(false);

        thread.setStreamsUncaughtExceptionHandler((e, b) -> exceptionHandlerInvoked.set(true));
        thread.run();

        verify(taskManager);
        assertThat(exceptionHandlerInvoked.get(), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCatchTaskMigratedExceptionOnOnTaskCorruptedExceptionPath() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        consumer.subscribe((Collection<String>) anyObject(), anyObject());
        EasyMock.expectLastCall().anyTimes();
        consumer.unsubscribe();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(consumerGroupMetadata);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);
        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 2);

        final Set<TaskId> corruptedTasks = singleton(taskId1);

        expect(task1.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task1.id()).andReturn(taskId1).anyTimes();
        expect(task2.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task2.id()).andReturn(taskId2).anyTimes();

        taskManager.handleCorruption(corruptedTasks);
        expectLastCall().andThrow(new TaskMigratedException("Task migrated",
                                                            new RuntimeException("non-corrupted task migrated")));

        taskManager.handleLostAll();
        expectLastCall();

        EasyMock.replay(task1, task2, taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        ) {
            @Override
            void runOnceWithProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
            @Override
            void runOnceWithoutProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
        }.updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        thread.setState(StreamThread.State.STARTING);
        thread.runLoop();

        verify(taskManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldEnforceRebalanceWhenTaskCorruptedExceptionIsThrownForAnActiveTask() {
        final StreamsConfig config = new StreamsConfig(configProps(true));

        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        consumer.subscribe((Collection<String>) anyObject(), anyObject());
        EasyMock.expectLastCall().anyTimes();
        consumer.unsubscribe();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(consumerGroupMetadata);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);

        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 2);

        final Set<TaskId> corruptedTasks = singleton(taskId1);

        expect(task1.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task1.id()).andReturn(taskId1).anyTimes();
        expect(task2.state()).andReturn(Task.State.CREATED).anyTimes();
        expect(task2.id()).andReturn(taskId2).anyTimes();
        expect(taskManager.handleCorruption(corruptedTasks)).andReturn(true);

        consumer.enforceRebalance("Active tasks corrupted");
        expectLastCall();

        EasyMock.replay(task1, task2, taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        ) {
            @Override
            void runOnceWithProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
            @Override
            void runOnceWithoutProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
        }.updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        thread.setState(StreamThread.State.STARTING);
        thread.runLoop();

        verify(taskManager);
        verify(consumer);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotEnforceRebalanceWhenTaskCorruptedExceptionIsThrownForAnInactiveTask() {
        final StreamsConfig config = new StreamsConfig(configProps(true));

        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        consumer.subscribe((Collection<String>) anyObject(), anyObject());
        EasyMock.expectLastCall().anyTimes();
        consumer.unsubscribe();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(consumerGroupMetadata);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);

        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 2);

        final Set<TaskId> corruptedTasks = singleton(taskId1);

        expect(task1.state()).andReturn(Task.State.CLOSED).anyTimes();
        expect(task1.id()).andReturn(taskId1).anyTimes();
        expect(task2.state()).andReturn(Task.State.CLOSED).anyTimes();
        expect(task2.id()).andReturn(taskId2).anyTimes();
        expect(taskManager.handleCorruption(corruptedTasks)).andReturn(false);

        EasyMock.replay(task1, task2, taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        ) {
            @Override
            void runOnceWithProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
            @Override
            void runOnceWithoutProcessingThreads() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasks);
            }
        }.updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        thread.setState(StreamThread.State.STARTING);
        thread.runLoop();

        verify(taskManager);
        verify(consumer);
    }

    @Test
    public void shouldNotCommitNonRunningNonRestoringTasks() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumer, consumerGroupMetadata);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);
        final Task task3 = mock(Task.class);

        final TaskId taskId1 = new TaskId(0, 1);
        final TaskId taskId2 = new TaskId(0, 2);
        final TaskId taskId3 = new TaskId(0, 3);

        expect(task1.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task1.id()).andReturn(taskId1).anyTimes();
        expect(task2.state()).andReturn(Task.State.RESTORING).anyTimes();
        expect(task2.id()).andReturn(taskId2).anyTimes();
        expect(task3.state()).andReturn(Task.State.CREATED).anyTimes();
        expect(task3.id()).andReturn(taskId3).anyTimes();

        expect(taskManager.allOwnedTasks()).andReturn(mkMap(
            mkEntry(taskId1, task1),
            mkEntry(taskId2, task2),
            mkEntry(taskId3, task3)
        )).anyTimes();

        // expect not to try and commit task3, because it's not running.
        expect(taskManager.commit(mkSet(task1, task2))).andReturn(2).times(1);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);

        EasyMock.replay(task1, task2, task3, taskManager);

        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldLogAndRecordSkippedRecordsForInvalidTimestamps() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties properties = configProps(false);
        properties.setProperty(
            StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            LogAndSkipOnInvalidTimestamp.class.getName()
        );
        final StreamsConfig config = new StreamsConfig(properties);
        thread = createStreamThread(CLIENT_ID, config);

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        final TaskId task1 = new TaskId(0, t1p1.partition());
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
        thread.taskManager().handleAssignment(
            Collections.singletonMap(
                task1,
                assignedPartitions),
            emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);
        runOnce();

        final MetricName skippedTotalMetric = metrics.metricName(
            "skipped-records-total",
            "stream-metrics",
            Collections.singletonMap("client-id", thread.getName())
        );
        final MetricName skippedRateMetric = metrics.metricName(
            "skipped-records-rate",
            "stream-metrics",
            Collections.singletonMap("client-id", thread.getName())
        );

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RecordQueue.class)) {
            long offset = -1;
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            runOnce();

            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            runOnce();

            addRecord(mockConsumer, ++offset, 1L);
            addRecord(mockConsumer, ++offset, 1L);
            runOnce();

            final List<String> strings = appender.getMessages();

            final String threadTaskPrefix = "stream-thread [" + Thread.currentThread().getName() + "] task [0_1] ";
            assertTrue(strings.contains(
                threadTaskPrefix + "Skipping record due to negative extracted timestamp. " +
                    "topic=[topic1] partition=[1] offset=[0] extractedTimestamp=[-1] " +
                    "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
            ));
            assertTrue(strings.contains(
                threadTaskPrefix + "Skipping record due to negative extracted timestamp. " +
                    "topic=[topic1] partition=[1] offset=[1] extractedTimestamp=[-1] " +
                    "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
            ));
            assertTrue(strings.contains(
                threadTaskPrefix + "Skipping record due to negative extracted timestamp. " +
                    "topic=[topic1] partition=[1] offset=[2] extractedTimestamp=[-1] " +
                    "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
            ));
            assertTrue(strings.contains(
                threadTaskPrefix + "Skipping record due to negative extracted timestamp. " +
                    "topic=[topic1] partition=[1] offset=[3] extractedTimestamp=[-1] " +
                    "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
            ));
            assertTrue(strings.contains(
                threadTaskPrefix + "Skipping record due to negative extracted timestamp. " +
                    "topic=[topic1] partition=[1] offset=[4] extractedTimestamp=[-1] " +
                    "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
            ));
            assertTrue(strings.contains(
                threadTaskPrefix + "Skipping record due to negative extracted timestamp. " +
                    "topic=[topic1] partition=[1] offset=[5] extractedTimestamp=[-1] " +
                    "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
            ));
        }
    }

    @Test
    public void shouldTransmitTaskManagerMetrics() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumer, consumerGroupMetadata);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);

        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (c, now) -> 0,
            null,
            new MockTime());
        final Map<MetricName, Metric> dummyProducerMetrics = singletonMap(testMetricName, testMetric);

        expect(taskManager.producerMetrics()).andReturn(dummyProducerMetrics);
        EasyMock.replay(taskManager);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = buildStreamThread(consumer, taskManager, config, topologyMetadata);

        assertThat(dummyProducerMetrics, is(thread.producerMetrics()));
    }

    @Test
    public void shouldConstructAdminMetrics() {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        final List<Node> cluster = Arrays.asList(broker1, broker2);

        final MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(cluster).clusterId(null).build();

        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumer, consumerGroupMetadata);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = new StreamThread(
            mockTime,
            config,
            adminClient,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        );
        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (c, now) -> 0,
            null,
            new MockTime());

        EasyMock.replay(taskManager);

        adminClient.setMockMetrics(testMetricName, testMetric);
        final Map<MetricName, Metric> adminClientMetrics = thread.adminClientMetrics();
        assertEquals(testMetricName, adminClientMetrics.get(testMetricName).metricName());
    }

    @Test
    public void shouldNotRecordFailedStreamThread() {
        runAndVerifyFailedStreamThreadRecording(false);
    }

    @Test
    public void shouldRecordFailedStreamThread() {
        runAndVerifyFailedStreamThreadRecording(true);
    }

    public void runAndVerifyFailedStreamThreadRecording(final boolean shouldFail) {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        expect(consumer.groupMetadata()).andStubReturn(consumerGroupMetadata);
        expect(consumerGroupMetadata.groupInstanceId()).andReturn(Optional.empty());
        EasyMock.replay(consumer, consumerGroupMetadata);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.producerClientIds()).andStubReturn(Collections.emptySet());
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            (e, b) -> { },
            null
        ) {
            @Override
            void runOnceWithProcessingThreads() {
                setState(StreamThread.State.PENDING_SHUTDOWN);
                if (shouldFail) {
                    throw new StreamsException(Thread.currentThread().getName());
                }
            }
            @Override
            void runOnceWithoutProcessingThreads() {
                setState(StreamThread.State.PENDING_SHUTDOWN);
                if (shouldFail) {
                    throw new StreamsException(Thread.currentThread().getName());
                }
            }
        };
        EasyMock.replay(taskManager);
        thread.updateThreadMetadata("metadata");

        thread.run();

        final Metric failedThreads = StreamsTestUtils.getMetricByName(metrics.metrics(), "failed-stream-threads", "stream-metrics");
        assertThat(failedThreads.metricValue(), is(shouldFail ? 1.0 : 0.0));
    }

    @Test
    public void shouldCheckStateUpdater() {
        Assume.assumeTrue(stateUpdaterEnabled);
        final Properties streamsConfigProps = configProps(false);
        thread = setUpThread(streamsConfigProps);
        final TaskManager taskManager = thread.taskManager();
        thread.setState(State.STARTING);

        runOnce();

        Mockito.verify(taskManager).checkStateUpdater(Mockito.anyLong(), Mockito.any());

        if (!processingThreadsEnabled) {
            Mockito.verify(taskManager).process(Mockito.anyInt(), Mockito.any());
        }
    }

    @Test
    public void shouldCheckStateUpdaterInBetweenProcessCalls() {
        Assume.assumeTrue(stateUpdaterEnabled);
        Assume.assumeFalse(processingThreadsEnabled);

        final Properties streamsConfigProps = configProps(false);
        thread = setUpThread(streamsConfigProps);
        final TaskManager taskManager = thread.taskManager();
        thread.setState(State.STARTING);
        // non-zero return of process will cause a second call to process
        when(taskManager.process(Mockito.anyInt(), Mockito.any())).thenReturn(1).thenReturn(0);

        runOnce();

        Mockito.verify(taskManager, times(2)).checkStateUpdater(Mockito.anyLong(), Mockito.any());
    }

    @Test
    public void shouldUpdateLagsAfterPolling() {
        final Properties streamsConfigProps = configProps(false);
        thread = setUpThread(streamsConfigProps);
        thread.setState(State.STARTING);
        thread.setState(State.PARTITIONS_ASSIGNED);

        runOnce();

        final InOrder inOrder = Mockito.inOrder(mainConsumer, thread.taskManager());
        inOrder.verify(mainConsumer).poll(Mockito.any());
        inOrder.verify(thread.taskManager()).updateLags();
    }


    @Test
    public void shouldResumePollingForPartitionsWithAvailableSpaceBeforePolling() {
        final Properties streamsConfigProps = configProps(false);
        thread = setUpThread(streamsConfigProps);
        thread.setState(State.STARTING);
        thread.setState(State.PARTITIONS_ASSIGNED);

        runOnce();

        final InOrder inOrder = Mockito.inOrder(thread.taskManager(), mainConsumer);
        inOrder.verify(thread.taskManager()).resumePollingForPartitionsWithAvailableSpace();
        inOrder.verify(mainConsumer).poll(Mockito.any());
    }

    @Test
    public void shouldRespectPollTimeInPartitionsAssignedStateWithStateUpdater() {
        Assume.assumeTrue(stateUpdaterEnabled);
        final Properties streamsConfigProps = configProps(false);
        final StreamsConfig config = new StreamsConfig(streamsConfigProps);
        final Duration pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        thread = setUpThread(streamsConfigProps);
        thread.setState(State.STARTING);
        thread.setState(State.PARTITIONS_ASSIGNED);

        runOnce();

        Mockito.verify(mainConsumer).poll(pollTime);
    }

    @Test
    public void shouldNotBlockWhenPollingInPartitionsAssignedStateWithoutStateUpdater() {
        Assume.assumeFalse(stateUpdaterEnabled);
        final Properties streamsConfigProps = configProps(false);
        thread = setUpThread(streamsConfigProps);
        thread.setState(State.STARTING);
        thread.setState(State.PARTITIONS_ASSIGNED);

        runOnce();

        Mockito.verify(mainConsumer).poll(Duration.ZERO);
    }

    private StreamThread setUpThread(final Properties streamsConfigProps) {
        final StreamsConfig config = new StreamsConfig(streamsConfigProps);
        final ConsumerGroupMetadata consumerGroupMetadata = Mockito.mock(ConsumerGroupMetadata.class);
        when(consumerGroupMetadata.groupInstanceId()).thenReturn(Optional.empty());
        when(mainConsumer.poll(Mockito.any(Duration.class))).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
        when(mainConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        final TaskManager taskManager = Mockito.mock(TaskManager.class);
        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder, config);
        topologyMetadata.buildAndRewriteTopology();
        return new StreamThread(
            mockTime,
            new StreamsConfig(streamsConfigProps.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
            null,
            mainConsumer,
            null,
            changelogReader,
            "",
            taskManager,
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime),
            topologyMetadata,
            "thread-id",
            new LogContext(),
            null,
            null,
            new LinkedList<>(),
            null,
            null,
            null
        );
    }

    private TaskManager mockTaskManager(final Task runningTask) {
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        final TaskId taskId = new TaskId(0, 0);

        expect(runningTask.state()).andStubReturn(Task.State.RUNNING);
        expect(runningTask.id()).andStubReturn(taskId);
        expect(taskManager.allOwnedTasks()).andStubReturn(Collections.singletonMap(taskId, runningTask));
        expect(taskManager.commit(Collections.singleton(runningTask))).andStubReturn(1);
        return taskManager;
    }

    private TaskManager mockTaskManagerPurge(final int numberOfPurges) {
        final Task runningTask = mock(Task.class);
        final TaskManager taskManager = mockTaskManager(runningTask);

        taskManager.maybePurgeCommittedRecords();
        EasyMock.expectLastCall().times(numberOfPurges);
        EasyMock.replay(taskManager, runningTask);
        return taskManager;
    }

    private TaskManager mockTaskManagerCommit(final int commits) {
        final Task runningTask = mock(Task.class);
        final TaskManager taskManager = mockTaskManager(runningTask);

        expect(taskManager.commit(Collections.singleton(runningTask))).andReturn(commits).times(1);
        EasyMock.replay(taskManager, runningTask);
        return taskManager;
    }

    private void setupInternalTopologyWithoutState(final StreamsConfig config) {
        stateDirectory = new StateDirectory(config, mockTime, true, false);
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalTopologyBuilder.addProcessor(
            "processor1",
            (ProcessorSupplier<byte[], byte[], ?, ?>) MockApiProcessor::new,
            "source1"
        );
        internalTopologyBuilder.setStreamsConfig(config);
    }

    // TODO: change return type to `StandbyTask`
    private Collection<Task> createStandbyTask(final StreamsConfig config) {
        final LogContext logContext = new LogContext("test");
        final Logger log = logContext.logger(StreamThreadTest.class);
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StandbyTaskCreator standbyTaskCreator = new StandbyTaskCreator(
            new TopologyMetadata(internalTopologyBuilder, config),
            config,
            streamsMetrics,
            stateDirectory,
            new MockChangelogReader(),
            CLIENT_ID,
            log,
            false);
        return standbyTaskCreator.createTasks(singletonMap(new TaskId(1, 2), emptySet()));
    }

    private void addRecord(final MockConsumer<byte[], byte[]> mockConsumer,
                           final long offset) {
        addRecord(mockConsumer, offset, -1L);
    }

    private void addRecord(final MockConsumer<byte[], byte[]> mockConsumer,
                           final long offset,
                           final long timestamp) {
        mockConsumer.addRecord(new ConsumerRecord<>(
            t1p1.topic(),
            t1p1.partition(),
            offset,
            timestamp,
            TimestampType.CREATE_TIME,
            -1,
            -1,
            new byte[0],
            new byte[0],
            new RecordHeaders(),
            Optional.empty()));
    }

    StreamTask activeTask(final TaskManager taskManager, final TopicPartition partition) {
        final Stream<Task> standbys = taskManager.allTasks().values().stream().filter(Task::isActive);
        for (final Task task : (Iterable<Task>) standbys::iterator) {
            if (task.inputPartitions().contains(partition)) {
                return (StreamTask) task;
            }
        }
        return null;
    }
    StandbyTask standbyTask(final TaskManager taskManager, final TopicPartition partition) {
        final Stream<Task> standbys = taskManager.standbyTaskMap().values().stream();
        for (final Task task : (Iterable<Task>) standbys::iterator) {
            if (task.inputPartitions().contains(partition)) {
                return (StandbyTask) task;
            }
        }
        return null;
    }

    private StreamThread buildStreamThread(final Consumer<byte[], byte[]> consumer,
                                           final TaskManager taskManager,
                                           final StreamsConfig config,
                                           final TopologyMetadata topologyMetadata) {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);

        return new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            topologyMetadata,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            new LinkedList<>(),
            null,
            HANDLER,
            null
        );
    }
    
    private void runOnce() {
        if (processingThreadsEnabled) {
            thread.runOnceWithProcessingThreads();
        } else {
            thread.runOnceWithoutProcessingThreads();
        }
    }

    private void runUntilTimeoutOrException(final Runnable action) {
        final long expectedEnd = System.currentTimeMillis() + DEFAULT_MAX_WAIT_MS;
        while (System.currentTimeMillis() < expectedEnd) {
            action.run();
            mockTime.sleep(10);
        }
    }

    private boolean runUntilTimeoutOrCondition(final Runnable action, final TestCondition testCondition) throws Exception {
        final long expectedEnd = System.currentTimeMillis() + DEFAULT_MAX_WAIT_MS;
        while (System.currentTimeMillis() < expectedEnd) {
            action.run();
            if (testCondition.conditionMet()) {
                return true;
            }
            mockTime.sleep(10);
        }
        return false;
    }

}
