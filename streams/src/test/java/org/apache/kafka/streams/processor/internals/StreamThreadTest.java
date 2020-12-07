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
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
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
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
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
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
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
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamThreadTest {

    private final static String APPLICATION_ID = "stream-thread-test";
    private final static UUID PROCESS_ID = UUID.fromString("87bf53a8-54f2-485f-a4b6-acdbec0a8b3d");
    private final static String CLIENT_ID = APPLICATION_ID + "-" + PROCESS_ID;

    private final int threadIdx = 1;
    private final Metrics metrics = new Metrics();
    private final MockTime mockTime = new MockTime();
    private final String stateDir = TestUtils.tempDirectory().getPath();
    private final MockClientSupplier clientSupplier = new MockClientSupplier();
    private final StreamsConfig config = new StreamsConfig(configProps(false));
    private final ConsumedInternal<Object, Object> consumed = new ConsumedInternal<>();
    private final ChangelogReader changelogReader = new MockChangelogReader();
    private final StateDirectory stateDirectory = new StateDirectory(config, mockTime, true);
    private final InternalTopologyBuilder internalTopologyBuilder = new InternalTopologyBuilder();
    private final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(internalTopologyBuilder);

    private StreamsMetadataState streamsMetadataState;
    private final static java.util.function.Consumer<Throwable> HANDLER = e -> {
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
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName()),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE : StreamsConfig.AT_LEAST_ONCE)
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

    private StreamThread createStreamThread(@SuppressWarnings("SameParameterValue") final String clientId,
                                            final StreamsConfig config,
                                            final boolean eosEnabled) {
        if (eosEnabled) {
            clientSupplier.setApplicationIdForProducer(APPLICATION_ID);
        }

        clientSupplier.setCluster(createCluster());

        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            APPLICATION_ID,
            config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            mockTime
        );

        internalTopologyBuilder.buildTopology();

        return StreamThread.create(
            internalTopologyBuilder,
            config,
            clientSupplier,
            clientSupplier.getAdmin(config.getAdminConfigs(clientId)),
            PROCESS_ID,
            clientId,
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
    public void shouldChangeStateInRebalanceListener() {
        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);

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
        thread.runOnce();
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        Assert.assertEquals(4, stateListener.numChanges);
        Assert.assertEquals(StreamThread.State.PARTITIONS_ASSIGNED, stateListener.oldState);

        thread.shutdown();
        assertSame(StreamThread.State.PENDING_SHUTDOWN, thread.state());
    }

    @Test
    public void shouldChangeStateAtStartClose() throws Exception {
        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);

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
    public void shouldCreateMetricsAtStartupWithBuiltInMetricsVersionLatest() {
        shouldCreateMetricsAtStartup(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldCreateMetricsAtStartupWithBuiltInMetricsVersion0100To24() {
        shouldCreateMetricsAtStartup(StreamsConfig.METRICS_0100_TO_24);
    }

    private void shouldCreateMetricsAtStartup(final String builtInMetricsVersion) {
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);
        final StreamsConfig config = new StreamsConfig(props);
        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);
        final String defaultGroupName = getGroupName(builtInMetricsVersion);
        final Map<String, String> defaultTags = Collections.singletonMap(
            getThreadTagKey(builtInMetricsVersion),
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

        if (builtInMetricsVersion.equals(StreamsConfig.METRICS_0100_TO_24)) {
            assertNotNull(metrics.metrics().get(metrics.metricName(
                "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            assertNotNull(metrics.metrics().get(metrics.metricName(
                "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        } else {
            assertNull(metrics.metrics().get(metrics.metricName(
                "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            assertNull(metrics.metrics().get(metrics.metricName(
                "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        }

        final String taskGroupName = "stream-task-metrics";
        final Map<String, String> taskTags =
            mkMap(mkEntry("task-id", "all"), mkEntry(getThreadTagKey(builtInMetricsVersion), thread.getName()));
        if (builtInMetricsVersion.equals(StreamsConfig.METRICS_0100_TO_24)) {
            assertNotNull(metrics.metrics().get(metrics.metricName(
                "commit-rate", taskGroupName, descriptionIsNotVerified, taskTags)));
        } else {
            assertNull(metrics.metrics().get(metrics.metricName(
                "commit-latency-avg", taskGroupName, descriptionIsNotVerified, taskTags)));
            assertNull(metrics.metrics().get(metrics.metricName(
                "commit-latency-max", taskGroupName, descriptionIsNotVerified, taskTags)));
            assertNull(metrics.metrics().get(metrics.metricName(
                "commit-rate", taskGroupName, descriptionIsNotVerified, taskTags)));
        }

        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        assertEquals(CLIENT_ID + "-StreamThread-1", thread.getName());
        assertTrue(reporter.containsMbean(String.format("kafka.streams:type=%s,%s=%s",
                                                        defaultGroupName,
                                                        getThreadTagKey(builtInMetricsVersion),
                                                        thread.getName())
        ));
        if (builtInMetricsVersion.equals(StreamsConfig.METRICS_0100_TO_24)) {
            assertTrue(reporter.containsMbean(String.format(
                "kafka.streams:type=stream-task-metrics,%s=%s,task-id=all",
                getThreadTagKey(builtInMetricsVersion),
                thread.getName())));
        } else {
            assertFalse(reporter.containsMbean(String.format(
                "kafka.streams:type=stream-task-metrics,%s=%s,task-id=all",
                getThreadTagKey(builtInMetricsVersion),
                thread.getName())));
        }
    }

    private String getGroupName(final String builtInMetricsVersion) {
        return builtInMetricsVersion.equals(StreamsConfig.METRICS_0100_TO_24) ? "stream-metrics"
            : "stream-thread-metrics";
    }

    private String getThreadTagKey(final String builtInMetricsVersion) {
        return builtInMetricsVersion.equals(StreamsConfig.METRICS_0100_TO_24) ? "client-id" : "thread-id";
    }

    @Test
    public void shouldNotCommitBeforeTheCommitInterval() {
        final long commitInterval = 1000L;
        final Properties props = configProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

        final StreamsConfig config = new StreamsConfig(props);
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 1);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        );
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        mockTime.sleep(commitInterval - 10L);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldEnforceRebalanceAfterNextScheduledProbingRebalanceTime() throws InterruptedException {
        final StreamsConfig config = new StreamsConfig(configProps(false));
        internalTopologyBuilder.buildTopology();
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            APPLICATION_ID,
            config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            mockTime
        );
        
        final Consumer<byte[], byte[]> mockConsumer = EasyMock.createNiceMock(Consumer.class);
        expect(mockConsumer.poll(anyObject())).andStubReturn(ConsumerRecords.empty());
        final EasyMockConsumerClientSupplier mockClientSupplier = new EasyMockConsumerClientSupplier(mockConsumer);

        mockClientSupplier.setCluster(createCluster());
        final StreamThread thread = StreamThread.create(
            internalTopologyBuilder,
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

        mockConsumer.enforceRebalance();
        EasyMock.replay(mockConsumer);
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
    public void shouldRespectNumIterationsInMainLoop() {
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
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig(APPLICATION_ID,
                                                                                         "localhost:2171",
                                                                                         Serdes.ByteArraySerde.class.getName(),
                                                                                         Serdes.ByteArraySerde.class.getName(),
                                                                                         properties));
        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        final TaskId task1 = new TaskId(0, t1p1.partition());
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);

        thread.taskManager().handleAssignment(Collections.singletonMap(task1, assignedPartitions), emptyMap());

        final MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.mainConsumer();
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);
        thread.runOnce();

        // processed one record, punctuated after the first record, and hence num.iterations is still 1
        long offset = -1;
        addRecord(mockConsumer, ++offset, 0L);
        thread.runOnce();

        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed one more record without punctuation, and bump num.iterations to 2
        addRecord(mockConsumer, ++offset, 1L);
        thread.runOnce();

        assertThat(thread.currentNumIterations(), equalTo(2));

        // processed zero records, early exit and iterations stays as 2
        thread.runOnce();
        assertThat(thread.currentNumIterations(), equalTo(2));

        // system time based punctutation without processing any record, iteration stays as 2
        mockTime.sleep(11L);

        thread.runOnce();
        assertThat(thread.currentNumIterations(), equalTo(2));

        // system time based punctutation after processing a record, half iteration to 1
        mockTime.sleep(11L);
        addRecord(mockConsumer, ++offset, 5L);

        thread.runOnce();
        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed two records, bumping up iterations to 3 (1 + 2)
        addRecord(mockConsumer, ++offset, 5L);
        addRecord(mockConsumer, ++offset, 6L);
        thread.runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        // stream time based punctutation halves to 1
        addRecord(mockConsumer, ++offset, 11L);
        thread.runOnce();

        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed three records, bumping up iterations to 3 (1 + 2)
        addRecord(mockConsumer, ++offset, 12L);
        addRecord(mockConsumer, ++offset, 13L);
        addRecord(mockConsumer, ++offset, 14L);
        thread.runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        mockProcessors.forEach(MockApiProcessor::requestCommit);
        addRecord(mockConsumer, ++offset, 15L);
        thread.runOnce();

        // user requested commit should half iteration to 1
        assertThat(thread.currentNumIterations(), equalTo(1));

        // processed three records, bumping up iterations to 3 (1 + 2)
        addRecord(mockConsumer, ++offset, 15L);
        addRecord(mockConsumer, ++offset, 16L);
        addRecord(mockConsumer, ++offset, 17L);
        thread.runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        // time based commit without processing, should keep the iteration as 3
        mockTime.sleep(90L);
        thread.runOnce();

        assertThat(thread.currentNumIterations(), equalTo(3));

        // time based commit without processing, should half the iteration to 1
        mockTime.sleep(90L);
        addRecord(mockConsumer, ++offset, 18L);
        thread.runOnce();

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
        final TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        );
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

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);

        final AtomicBoolean committed = new AtomicBoolean(false);
        final TaskManager taskManager = new TaskManager(
            null,
            null,
            null,
            null,
            null,
            null,
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

        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        );

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
    public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalStreamsBuilder.buildAndOptimizeTopology();

        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);

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
        for (final Task task : thread.activeTasks()) {
            assertSame(globalProducer, ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.mainConsumer());
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer());
    }

    @Test
    public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(true)), true);

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

        thread.runOnce();

        assertEquals(thread.activeTasks().size(), clientSupplier.producers.size());
        assertSame(clientSupplier.consumer, thread.mainConsumer());
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer());
    }

    @Test
    public void shouldOnlyCompleteShutdownAfterRebalanceNotInProgress() throws InterruptedException {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(true)), true);

        thread.start();
        TestUtils.waitForCondition(
            () -> thread.state() == StreamThread.State.STARTING,
            10 * 1000,
            "Thread never started.");

        thread.rebalanceListener().onPartitionsRevoked(Collections.emptyList());
        thread.taskManager().handleRebalanceStart(Collections.singleton(topic1));

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().handleAssignment(activeTasks, emptyMap());

        thread.shutdown();

        // even if thread is no longer running, it should still be polling
        // as long as the rebalance is still ongoing
        assertFalse(thread.isRunning());

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

        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(true)), true);

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

        for (final Task task : thread.activeTasks()) {
            assertTrue(((MockProducer<byte[], byte[]>) ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer()).closed());
        }
    }

    @Test
    public void shouldShutdownTaskManagerOnClose() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));
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

        final InternalTopologyBuilder internalTopologyBuilder = EasyMock.createNiceMock(InternalTopologyBuilder.class);

        expect(internalTopologyBuilder.sourceTopicCollection()).andReturn(Collections.singletonList(topic1)).times(2);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        consumer.subscribe(Collections.singletonList(topic1), new MockRebalanceListener());
        consumer.rebalance(Collections.singletonList(t1p1));
        consumer.updateEndOffsets(Collections.singletonMap(t1p1, 10L));
        consumer.seekToEnd(Collections.singletonList(t1p1));

        final ChangelogReader changelogReader = new MockChangelogReader() {
            @Override
            public void restore(final Map<TaskId, Task> tasks) {
                consumer.addRecord(new ConsumerRecord<>(topic1, 1, 11, new byte[0], new byte[0]));
                consumer.addRecord(new ConsumerRecord<>(topic1, 1, 12, new byte[1], new byte[0]));

                throw new TaskMigratedException(
                    "Changelog restore found task migrated", new RuntimeException("restore task migrated"));
            }
        };

        taskManager.handleLostAll();

        EasyMock.replay(taskManager, internalTopologyBuilder);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);

        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            changelogReader,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class, thread::run);

        verify(taskManager);

        // The Mock consumer shall throw as the assignment has been wiped out, but records are assigned.
        assertEquals("No current assignment for partition topic1-1", thrown.getMessage());
        assertFalse(consumer.shouldRebalance());
    }

    @Test
    public void shouldShutdownTaskManagerOnCloseWithoutStart() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));
        thread.shutdown();
        verify(taskManager);
    }

    @Test
    public void shouldOnlyShutdownOnce() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));
        thread.shutdown();
        // Execute the run method. Verification of the mock will check that shutdown was only done once
        thread.run();
        verify(taskManager);
    }

    @Test
    public void shouldNotThrowWhenStandbyTasksAssignedAndNoStateStoresForTopology() {
        internalTopologyBuilder.addSource(null, "name", null, null, null, "topic");
        internalTopologyBuilder.addSink("out", "output", null, null, null, "name");

        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);

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

        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(true)), true);

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

        thread.runOnce();
        assertThat(thread.activeTasks().size(), equalTo(1));
        final MockProducer<byte[], byte[]> producer = clientSupplier.producers.get(0);

        // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
        consumer.updateBeginningOffsets(Collections.singletonMap(assignedPartitions.iterator().next(), 0L));
        consumer.unsubscribe();
        consumer.assign(new HashSet<>(assignedPartitions));

        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 0, new byte[0], new byte[0]));
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1);
        thread.runOnce();
        assertThat(producer.history().size(), equalTo(1));

        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        TestUtils.waitForCondition(
            () -> producer.commitCount() == 1,
            "StreamsThread did not commit transaction.");

        producer.fenceProducer();
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 1, new byte[0], new byte[0]));
        try {
            thread.runOnce();
            fail("Should have thrown TaskMigratedException");
        } catch (final KafkaException expected) {
            assertTrue(expected instanceof TaskMigratedException);
            assertTrue("StreamsThread removed the fenced zombie task already, should wait for rebalance to close all zombies together.",
                thread.activeTasks().stream().anyMatch(task -> task.id().equals(task1)));
        }

        assertThat(producer.commitCount(), equalTo(1L));
    }

    @Test
    public void shouldNotCloseTaskAndRemoveFromTaskManagerIfProducerGotFencedInCommitTransactionWhenSuspendingTasks() {
        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(true)), true);

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

        thread.runOnce();

        assertThat(thread.activeTasks().size(), equalTo(1));

        // need to process a record to enable committing
        addRecord(mockConsumer, 0L);
        thread.runOnce();

        clientSupplier.producers.get(0).commitTransactionException = new ProducerFencedException("Producer is fenced");
        assertThrows(TaskMigratedException.class, () -> thread.rebalanceListener().onPartitionsRevoked(assignedPartitions));
        assertFalse(clientSupplier.producers.get(0).transactionCommitted());
        assertFalse(clientSupplier.producers.get(0).closed());
        assertEquals(1, thread.activeTasks().size());
    }

    @Test
    public void shouldReinitializeRevivedTasksInAnyState() {
        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(false)), false);

        final String storeName = "store";
        final String storeChangelog = "stream-thread-test-store-changelog";
        final TopicPartition storeChangelogTopicPartition = new TopicPartition(storeChangelog, 1);

        internalTopologyBuilder.addSource(null, "name", null, null, null, topic1);
        final AtomicBoolean shouldThrow = new AtomicBoolean(false);
        final AtomicBoolean processed = new AtomicBoolean(false);
        internalTopologyBuilder.addProcessor(
            "proc",
            () -> new Processor<Object, Object, Object, Object>() {
                @Override
                public void process(final Record<Object, Object> record) {
                    if (shouldThrow.get()) {
                        throw new TaskCorruptedException(singletonMap(task1, new HashSet<>(singleton(storeChangelogTopicPartition))));
                    } else {
                        processed.set(true);
                    }
                }
            },
            "name");
        internalTopologyBuilder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.String()
                ),
                "proc"
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
        thread.runOnce();
        assertThat(thread.activeTasks().size(), equalTo(1));

        // the second transits to running and unpause the input
        thread.runOnce();

        // the third actually polls, processes the record, and throws the corruption exception
        addRecord(mockConsumer, 0L);
        shouldThrow.set(true);
        final TaskCorruptedException taskCorruptedException = assertThrows(TaskCorruptedException.class, thread::runOnce);

        // Now, we can handle the corruption
        thread.taskManager().handleCorruption(taskCorruptedException.corruptedTaskWithChangelogs());

        // again, complete the restoration
        thread.runOnce();
        // transit to running and unpause
        thread.runOnce();
        // process the record
        addRecord(mockConsumer, 0L);
        shouldThrow.set(false);
        assertThat(processed.get(), is(false));
        thread.runOnce();
        assertThat(processed.get(), is(true));
    }

    @Test
    public void shouldNotCloseTaskAndRemoveFromTaskManagerIfProducerGotFencedInCommitTransactionWhenCommitting() {
        // only have source but no sink so that we would not get fenced in producer.send
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);

        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(true)), true);

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

        thread.runOnce();
        assertThat(thread.activeTasks().size(), equalTo(1));
        final MockProducer<byte[], byte[]> producer = clientSupplier.producers.get(0);

        producer.commitTransactionException = new ProducerFencedException("Producer is fenced");
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        consumer.addRecord(new ConsumerRecord<>(topic1, 1, 1, new byte[0], new byte[0]));
        try {
            thread.runOnce();
            fail("Should have thrown TaskMigratedException");
        } catch (final KafkaException expected) {
            assertTrue(expected instanceof TaskMigratedException);
            assertTrue("StreamsThread removed the fenced zombie task already, should wait for rebalance to close all zombies together.",
                thread.activeTasks().stream().anyMatch(task -> task.id().equals(task1)));
        }

        assertThat(producer.commitCount(), equalTo(0L));

        assertTrue(clientSupplier.producers.get(0).transactionInFlight());
        assertFalse(clientSupplier.producers.get(0).transactionCommitted());
        assertFalse(clientSupplier.producers.get(0).closed());
        assertEquals(1, thread.activeTasks().size());
    }

    @Test
    public void shouldNotCloseTaskProducerWhenSuspending() {
        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(configProps(true)), true);

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

        thread.runOnce();

        assertThat(thread.activeTasks().size(), equalTo(1));

        // need to process a record to enable committing
        addRecord(mockConsumer, 0L);
        thread.runOnce();

        thread.rebalanceListener().onPartitionsRevoked(assignedPartitions);
        assertTrue(clientSupplier.producers.get(0).transactionCommitted());
        assertFalse(clientSupplier.producers.get(0).closed());
        assertEquals(1, thread.activeTasks().size());
    }

    @Test
    public void shouldReturnActiveTaskMetadataWhileRunningState() {
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);

        clientSupplier.setCluster(createCluster());

        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            APPLICATION_ID,
            config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            mockTime
        );

        internalTopologyBuilder.buildTopology();

        final StreamThread thread = StreamThread.create(
            internalTopologyBuilder,
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

        thread.runOnce();

        final ThreadMetadata metadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), metadata.threadState());
        assertTrue(metadata.activeTasks().contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
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
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
                              .groupByKey().count(Materialized.as("count-one"));

        internalStreamsBuilder.buildAndOptimizeTopology();
        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions(
            "stream-thread-test-count-one-changelog",
            Collections.singletonList(
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

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().handleAssignment(emptyMap(), standbyTasks);

        thread.rebalanceListener().onPartitionsAssigned(Collections.emptyList());

        thread.runOnce();

        final ThreadMetadata threadMetadata = thread.threadMetadata();
        assertEquals(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        assertTrue(threadMetadata.standbyTasks().contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
        assertTrue(threadMetadata.activeTasks().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldUpdateStandbyTask() throws Exception {
        final String storeName1 = "count-one";
        final String storeName2 = "table-two";
        final String changelogName1 = APPLICATION_ID + "-" + storeName1 + "-changelog";
        final String changelogName2 = APPLICATION_ID + "-" + storeName2 + "-changelog";
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
        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions(changelogName1,
            Collections.singletonList(new PartitionInfo(changelogName1, 1, null, new Node[0], new Node[0]))
        );

        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition1, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition1, 0L));
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition2, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition2, 0L));
        final OffsetCheckpoint checkpoint
            = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(task3), CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(partition2, 5L));

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(Collections.emptySet());

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));
        standbyTasks.put(task3, Collections.singleton(t2p1));

        thread.taskManager().handleAssignment(emptyMap(), standbyTasks);
        thread.taskManager().tryToCompleteRestoration(mockTime.milliseconds());

        thread.rebalanceListener().onPartitionsAssigned(Collections.emptyList());

        thread.runOnce();

        final StandbyTask standbyTask1 = standbyTask(thread.taskManager(), t1p1);
        final StandbyTask standbyTask2 = standbyTask(thread.taskManager(), t2p1);
        assertEquals(task1, standbyTask1.id());
        assertEquals(task3, standbyTask2.id());

        final KeyValueStore<Object, Long> store1 = (KeyValueStore<Object, Long>) standbyTask1.getStore(storeName1);
        final KeyValueStore<Object, Long> store2 = (KeyValueStore<Object, Long>) standbyTask2.getStore(storeName2);
        assertEquals(0L, store1.approximateNumEntries());
        assertEquals(0L, store2.approximateNumEntries());

        // let the store1 be restored from 0 to 10; store2 be restored from 5 (checkpointed) to 10
        for (long i = 0L; i < 10L; i++) {
            restoreConsumer.addRecord(new ConsumerRecord<>(
                changelogName1,
                1,
                i,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
            restoreConsumer.addRecord(new ConsumerRecord<>(
                changelogName2,
                1,
                i,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
        }

        thread.runOnce();

        assertEquals(10L, store1.approximateNumEntries());
        assertEquals(4L, store2.approximateNumEntries());
    }

    @Test
    public void shouldCreateStandbyTask() {
        setupInternalTopologyWithoutState();
        internalTopologyBuilder.addStateStore(new MockKeyValueStoreBuilder("myStore", true), "processor1");

        assertThat(createStandbyTask(), not(empty()));
    }

    @Test
    public void shouldNotCreateStandbyTaskWithoutStateStores() {
        setupInternalTopologyWithoutState();

        assertThat(createStandbyTask(), empty());
    }

    @Test
    public void shouldNotCreateStandbyTaskIfStateStoresHaveLoggingDisabled() {
        setupInternalTopologyWithoutState();
        final StoreBuilder<KeyValueStore<Object, Object>> storeBuilder =
            new MockKeyValueStoreBuilder("myStore", true);
        storeBuilder.withLoggingDisabled();
        internalTopologyBuilder.addStateStore(storeBuilder, "processor1");

        assertThat(createStandbyTask(), empty());
    }

    @Test
    public void shouldPunctuateActiveTask() {
        final List<Long> punctuatedStreamTime = new ArrayList<>();
        final List<Long> punctuatedWallClockTime = new ArrayList<>();
        final org.apache.kafka.streams.processor.ProcessorSupplier<Object, Object> punctuateProcessor =
            () -> new org.apache.kafka.streams.processor.Processor<Object, Object>() {
                @Override
                public void init(final org.apache.kafka.streams.processor.ProcessorContext context) {
                    context.schedule(Duration.ofMillis(100L), PunctuationType.STREAM_TIME, punctuatedStreamTime::add);
                    context.schedule(Duration.ofMillis(100L), PunctuationType.WALL_CLOCK_TIME, punctuatedWallClockTime::add);
                }

                @Override
                public void process(final Object key, final Object value) {}

                @Override
                public void close() {}
            };

        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed).process(punctuateProcessor);
        internalStreamsBuilder.buildAndOptimizeTopology();

        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);

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

        thread.runOnce();

        assertEquals(0, punctuatedStreamTime.size());
        assertEquals(0, punctuatedWallClockTime.size());

        mockTime.sleep(100L);
        for (long i = 0L; i < 10L; i++) {
            clientSupplier.consumer.addRecord(new ConsumerRecord<>(
                topic1,
                1,
                i,
                i * 100L,
                TimestampType.CREATE_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ("K" + i).getBytes().length,
                ("V" + i).getBytes().length,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
        }

        thread.runOnce();

        assertEquals(1, punctuatedStreamTime.size());
        assertEquals(1, punctuatedWallClockTime.size());

        mockTime.sleep(100L);

        thread.runOnce();

        // we should skip stream time punctuation, only trigger wall-clock time punctuation
        assertEquals(1, punctuatedStreamTime.size());
        assertEquals(2, punctuatedWallClockTime.size());
    }

    @Test
    public void shouldAlwaysUpdateTasksMetadataAfterChangingState() {
        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);
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
    public void shouldAlwaysReturnEmptyTasksMetadataWhileRebalancingStateAndTasksNotRunning() {
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
                              .groupByKey().count(Materialized.as("count-one"));
        internalStreamsBuilder.buildAndOptimizeTopology();

        final StreamThread thread = createStreamThread(CLIENT_ID, config, false);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                                         Arrays.asList(
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

        final List<TopicPartition> assignedPartitions = new ArrayList<>();

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener().onPartitionsRevoked(assignedPartitions);
        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_REVOKED);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));
        standbyTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().handleAssignment(activeTasks, standbyTasks);

        thread.rebalanceListener().onPartitionsAssigned(assignedPartitions);

        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_ASSIGNED);
    }

    private void assertThreadMetadataHasEmptyTasksWithState(final ThreadMetadata metadata, final StreamThread.State state) {
        assertEquals(state.name(), metadata.threadState());
        assertTrue(metadata.activeTasks().isEmpty());
        assertTrue(metadata.standbyTasks().isEmpty());
    }

    @Test
    public void shouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore() throws Exception {
        internalStreamsBuilder.stream(Collections.singleton("topic"), consumed)
            .groupByKey()
            .count(Materialized.as("count"));
        internalStreamsBuilder.buildAndOptimizeTopology();

        final StreamThread thread = createStreamThread("clientId", config, false);
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

        try {
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

            TestUtils.waitForCondition(
                () -> {
                    mockRestoreConsumer.assign(changelogPartitionSet);
                    return mockRestoreConsumer.position(changelogPartition) == 2L;
                },
                "Never finished restore");
        } finally {
            thread.shutdown();
            thread.join(10000);
        }
    }

    @Test
    public void shouldLogAndNotRecordSkippedMetricForDeserializationExceptionWithBuiltInMetricsVersionLatest() {
        shouldLogAndRecordSkippedMetricForDeserializationException(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldLogAndRecordSkippedMetricForDeserializationExceptionWithBuiltInMetricsVersion0100To24() {
        shouldLogAndRecordSkippedMetricForDeserializationException(StreamsConfig.METRICS_0100_TO_24);
    }

    private void shouldLogAndRecordSkippedMetricForDeserializationException(final String builtInMetricsVersion) {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties config = configProps(false);
        config.setProperty(
            StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class.getName()
        );
        config.setProperty(
            StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG,
            builtInMetricsVersion
        );
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(config), false);

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
        thread.runOnce();

        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
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
            assertEquals(0.0, metrics.metric(skippedTotalMetric).metricValue());
            assertEquals(0.0, metrics.metric(skippedRateMetric).metricValue());
        }

        long offset = -1;
        mockConsumer.addRecord(new ConsumerRecord<>(
            t1p1.topic(),
            t1p1.partition(),
            ++offset,
            -1,
            TimestampType.CREATE_TIME,
            ConsumerRecord.NULL_CHECKSUM,
            -1,
            -1,
            new byte[0],
            "I am not an integer.".getBytes()));
        mockConsumer.addRecord(new ConsumerRecord<>(
            t1p1.topic(),
            t1p1.partition(),
            ++offset,
            -1,
            TimestampType.CREATE_TIME,
            ConsumerRecord.NULL_CHECKSUM,
            -1,
            -1,
            new byte[0],
            "I am not an integer.".getBytes()));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RecordDeserializer.class)) {
            thread.runOnce();

            final List<String> strings = appender.getMessages();
            assertTrue(strings.contains("stream-thread [" + Thread.currentThread().getName() + "] task [0_1]" +
                " Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]"));
            assertTrue(strings.contains("stream-thread [" + Thread.currentThread().getName() + "] task [0_1]" +
                " Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]"));
        }
    }

    @Test
    public void shouldThrowTaskMigratedExceptionHandlingTaskLost() {
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);

        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
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
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        consumer.schedulePollTask(() -> {
            thread.setState(StreamThread.State.PARTITIONS_REVOKED);
            thread.rebalanceListener().onPartitionsLost(assignedPartitions);
        });

        thread.setState(StreamThread.State.STARTING);
        assertThrows(TaskMigratedException.class, thread::runOnce);
    }

    @Test
    public void shouldThrowTaskMigratedExceptionHandlingRevocation() {
        final Set<TopicPartition> assignedPartitions = Collections.singleton(t1p1);

        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
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
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        consumer.schedulePollTask(() -> {
            thread.setState(StreamThread.State.PARTITIONS_REVOKED);
            thread.rebalanceListener().onPartitionsRevoked(assignedPartitions);
        });

        thread.setState(StreamThread.State.STARTING);
        assertThrows(TaskMigratedException.class, thread::runOnce);
    }

    @Test
    public void shouldCatchHandleCorruptionOnTaskCorruptedExceptionPath() {
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);
        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 2);

        final Map<TaskId, Collection<TopicPartition>> corruptedTasksWithChangelogs = mkMap(
            mkEntry(taskId1, emptySet())
        );

        expect(task1.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task1.id()).andReturn(taskId1).anyTimes();
        expect(task2.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task2.id()).andReturn(taskId2).anyTimes();

        taskManager.handleCorruption(corruptedTasksWithChangelogs);

        EasyMock.replay(task1, task2, taskManager);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ) {
            @Override
            void runOnce() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasksWithChangelogs);
            }
        }.updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        thread.setState(StreamThread.State.STARTING);
        thread.runLoop();

        verify(taskManager);
    }

    @Test
    public void shouldCatchTaskMigratedExceptionOnOnTaskCorruptedExceptionPath() {
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        final Task task1 = mock(Task.class);
        final Task task2 = mock(Task.class);
        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 2);

        final Map<TaskId, Collection<TopicPartition>> corruptedTasksWithChangelogs = mkMap(
            mkEntry(taskId1, emptySet())
        );

        expect(task1.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task1.id()).andReturn(taskId1).anyTimes();
        expect(task2.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(task2.id()).andReturn(taskId2).anyTimes();

        taskManager.handleCorruption(corruptedTasksWithChangelogs);
        expectLastCall().andThrow(new TaskMigratedException("Task migrated",
                                                            new RuntimeException("non-corrupted task migrated")));

        taskManager.handleLostAll();
        expectLastCall();

        EasyMock.replay(task1, task2, taskManager);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        ) {
            @Override
            void runOnce() {
                setState(State.PENDING_SHUTDOWN);
                throw new TaskCorruptedException(corruptedTasksWithChangelogs);
            }
        }.updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));

        thread.setState(StreamThread.State.STARTING);
        thread.runLoop();

        verify(taskManager);
    }

    @Test
    public void shouldNotCommitNonRunningNonRestoringTasks() {
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
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

        expect(taskManager.tasks()).andReturn(mkMap(
            mkEntry(taskId1, task1),
            mkEntry(taskId2, task2),
            mkEntry(taskId3, task3)
        )).anyTimes();

        // expect not to try and commit task3, because it's not running.
        expect(taskManager.commit(mkSet(task1, task2))).andReturn(2).times(1);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        );

        EasyMock.replay(task1, task2, task3, taskManager);

        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        verify(taskManager);
    }

    @Test
    public void shouldLogAndRecordSkippedRecordsForInvalidTimestampsWithBuiltInMetricsVersion0100To24() {
        shouldLogAndRecordSkippedRecordsForInvalidTimestamps(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void shouldLogAndNotRecordSkippedRecordsForInvalidTimestampsWithBuiltInMetricsVersionLatest() {
        shouldLogAndRecordSkippedRecordsForInvalidTimestamps(StreamsConfig.METRICS_LATEST);
    }

    private void shouldLogAndRecordSkippedRecordsForInvalidTimestamps(final String builtInMetricsVersion) {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        final Properties config = configProps(false);
        config.setProperty(
            StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            LogAndSkipOnInvalidTimestamp.class.getName()
        );
        config.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);
        final StreamThread thread = createStreamThread(CLIENT_ID, new StreamsConfig(config), false);

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
        thread.runOnce();

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

        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(0.0, metrics.metric(skippedTotalMetric).metricValue());
            assertEquals(0.0, metrics.metric(skippedRateMetric).metricValue());
        }

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RecordQueue.class)) {
            long offset = -1;
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            thread.runOnce();

            if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
                assertEquals(2.0, metrics.metric(skippedTotalMetric).metricValue());
                assertNotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());
            }

            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            thread.runOnce();

            if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
                assertEquals(6.0, metrics.metric(skippedTotalMetric).metricValue());
                assertNotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());
            }

            addRecord(mockConsumer, ++offset, 1L);
            addRecord(mockConsumer, ++offset, 1L);
            thread.runOnce();

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

        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(6.0, metrics.metric(skippedTotalMetric).metricValue());
            assertNotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());
        }
    }

    @Test
    public void shouldTransmitTaskManagerMetrics() {
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);

        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);

        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());
        final Map<MetricName, Metric> dummyProducerMetrics = singletonMap(testMetricName, testMetric);

        expect(taskManager.producerMetrics()).andReturn(dummyProducerMetrics);
        EasyMock.replay(taskManager, consumer);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            new StreamsConfig(configProps(true)),
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        );

        assertThat(dummyProducerMetrics, is(thread.producerMetrics()));
    }

    @Test
    public void shouldConstructAdminMetrics() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        final List<Node> cluster = Arrays.asList(broker1, broker2);

        final MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(cluster).clusterId(null).build();

        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);

        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            adminClient,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            HANDLER,
            null
        );
        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());

        EasyMock.replay(taskManager, consumer);

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
        final Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            CLIENT_ID,
            new LogContext(""),
            new AtomicInteger(),
            new AtomicLong(Long.MAX_VALUE),
            null,
            e -> { },
            null
        ) {
            @Override
            void runOnce() {
                setState(StreamThread.State.PENDING_SHUTDOWN);
                if (shouldFail) {
                    throw new StreamsException(Thread.currentThread().getName());
                }
            }
        };
        EasyMock.replay(taskManager);
        thread.updateThreadMetadata("metadata");
        thread.setState(StreamThread.State.STARTING);

        thread.runLoop();

        final Metric failedThreads = StreamsTestUtils.getMetricByName(metrics.metrics(), "failed-stream-threads", "stream-metrics");
        assertThat(failedThreads.metricValue(), is(shouldFail ? 1.0 : 0.0));
    }

    private TaskManager mockTaskManagerCommit(final Consumer<byte[], byte[]> consumer,
                                              final int numberOfCommits,
                                              final int commits) {
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        final Task runningTask = mock(Task.class);
        final TaskId taskId = new TaskId(0, 0);

        expect(runningTask.state()).andReturn(Task.State.RUNNING).anyTimes();
        expect(runningTask.id()).andReturn(taskId).anyTimes();
        expect(taskManager.tasks())
            .andReturn(Collections.singletonMap(taskId, runningTask)).times(numberOfCommits);
        expect(taskManager.commit(Collections.singleton(runningTask))).andReturn(commits).times(numberOfCommits);
        EasyMock.replay(taskManager, consumer, runningTask);
        return taskManager;
    }

    private void setupInternalTopologyWithoutState() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalTopologyBuilder.addProcessor(
            "processor1",
            (ProcessorSupplier<byte[], byte[], ?, ?>) () -> new MockApiProcessor<>(),
            "source1"
        );
    }

    private Collection<Task> createStandbyTask() {
        final LogContext logContext = new LogContext("test");
        final Logger log = logContext.logger(StreamThreadTest.class);
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST, mockTime);
        final StandbyTaskCreator standbyTaskCreator = new StandbyTaskCreator(
            internalTopologyBuilder,
            config,
            streamsMetrics,
            stateDirectory,
            new MockChangelogReader(),
            CLIENT_ID,
            log);
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
            ConsumerRecord.NULL_CHECKSUM,
            -1,
            -1,
            new byte[0],
            new byte[0]));
    }

    StandbyTask standbyTask(final TaskManager taskManager, final TopicPartition partition) {
        final Stream<Task> standbys = taskManager.tasks().values().stream().filter(t -> !t.isActive());
        for (final Task task : (Iterable<Task>) standbys::iterator) {
            if (task.inputPartitions().contains(partition)) {
                return (StandbyTask) task;
            }
        }
        return null;
    }
}
