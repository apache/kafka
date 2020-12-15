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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.IMocksControl;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.StreamTask.encodeTimestamp;
import static org.apache.kafka.streams.processor.internals.Task.State.CREATED;
import static org.apache.kafka.streams.processor.internals.Task.State.RESTORING;
import static org.apache.kafka.streams.processor.internals.Task.State.RUNNING;
import static org.apache.kafka.streams.processor.internals.Task.State.SUSPENDED;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG_0100_TO_24;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByNameFilterByTags;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EasyMockRunner.class)
public class StreamTaskTest {

    private static final String APPLICATION_ID = "stream-task-test";
    private static final File BASE_DIR = TestUtils.tempDirectory();
    private static final long DEFAULT_TIMESTAMP = 1000;

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final TopicPartition partition1 = new TopicPartition(topic1, 1);
    private final TopicPartition partition2 = new TopicPartition(topic2, 1);
    private final Set<TopicPartition> partitions = mkSet(partition1, partition2);
    private final Serializer<Integer> intSerializer = Serdes.Integer().serializer();
    private final Deserializer<Integer> intDeserializer = Serdes.Integer().deserializer();

    private final MockSourceNode<Integer, Integer, Integer, Integer> source1 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer, Integer, Integer> source2 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer, ?, ?> source3 = new MockSourceNode<Integer, Integer, Object, Object>(intDeserializer, intDeserializer) {
        @Override
        public void process(final Record<Integer, Integer> record) {
            throw new RuntimeException("KABOOM!");
        }

        @Override
        public void close() {
            throw new RuntimeException("KABOOM!");
        }
    };
    private final MockProcessorNode<Integer, Integer, ?, ?> processorStreamTime = new MockProcessorNode<>(10L);
    private final MockProcessorNode<Integer, Integer, ?, ?> processorSystemTime = new MockProcessorNode<>(10L, PunctuationType.WALL_CLOCK_TIME);

    private final String storeName = "store";
    private final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, false);
    private final TopicPartition changelogPartition = new TopicPartition("store-changelog", 1);

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);
    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0);

    private MockTime time = new MockTime();
    private Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG), time);
    private final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);

    private StateDirectory stateDirectory;
    private StreamTask task;
    private long punctuatedAt;

    @Mock(type = MockType.NICE)
    private ProcessorStateManager stateManager;
    @Mock(type = MockType.NICE)
    private RecordCollector recordCollector;
    @Mock(type = MockType.NICE)
    private ThreadCache cache;

    private final Punctuator punctuator = new Punctuator() {
        @Override
        public void punctuate(final long timestamp) {
            punctuatedAt = timestamp;
        }
    };

    private static ProcessorTopology withRepartitionTopics(final List<ProcessorNode<?, ?, ?, ?>> processorNodes,
                                                           final Map<String, SourceNode<?, ?, ?, ?>> sourcesByTopic,
                                                           final Set<String> repartitionTopics) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     repartitionTopics);
    }

    private static ProcessorTopology withSources(final List<ProcessorNode<?, ?, ?, ?>> processorNodes,
                                                 final Map<String, SourceNode<?, ?, ?, ?>> sourcesByTopic) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     Collections.emptySet());
    }

    private static StreamsConfig createConfig(final boolean enableEoS, final String enforcedProcessingValue) {
        final String canonicalPath;
        try {
            canonicalPath = BASE_DIR.getCanonicalPath();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, canonicalPath),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE : StreamsConfig.AT_LEAST_ONCE),
            mkEntry(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, enforcedProcessingValue)
        )));
    }

    @Before
    public void setup() {
        EasyMock.expect(stateManager.taskId()).andStubReturn(taskId);
        EasyMock.expect(stateManager.taskType()).andStubReturn(TaskType.ACTIVE);

        consumer.assign(asList(partition1, partition2));
        consumer.updateBeginningOffsets(mkMap(mkEntry(partition1, 0L), mkEntry(partition2, 0L)));
        stateDirectory = new StateDirectory(createConfig(false, "100"), new MockTime(), true);
    }

    @After
    public void cleanup() throws IOException {
        if (task != null) {
            try {
                task.suspend();
            } catch (final IllegalStateException maybeSwallow) {
                if (!maybeSwallow.getMessage().startsWith("Illegal state CLOSED")) {
                    throw maybeSwallow;
                }
            } catch (final RuntimeException swallow) {
                // suspend dirty case
            }
            task.closeDirty();
            task = null;
        }
        Utils.delete(BASE_DIR);
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectory() throws IOException {
        stateDirectory = EasyMock.createNiceMock(StateDirectory.class);
        EasyMock.expect(stateDirectory.lock(taskId)).andReturn(false);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet());
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);
        EasyMock.expectLastCall();
        EasyMock.replay(stateDirectory, stateManager);

        task = createStatefulTask(createConfig(false, "100"), false);

        assertThrows(LockException.class, task::initializeIfNeeded);
    }

    @Test
    public void shouldNotAttemptToLockIfNoStores() {
        stateDirectory = EasyMock.createNiceMock(StateDirectory.class);
        EasyMock.replay(stateDirectory);

        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);

        task.initializeIfNeeded();

        // should fail if lock is called
        EasyMock.verify(stateDirectory);
    }

    @Test
    public void shouldAttemptToDeleteStateDirectoryWhenCloseDirtyAndEosEnabled() throws IOException {
        final IMocksControl ctrl = EasyMock.createStrictControl();
        final ProcessorStateManager stateManager = ctrl.createMock(ProcessorStateManager.class);
        EasyMock.expect(stateManager.taskType()).andStubReturn(TaskType.ACTIVE);
        stateDirectory = ctrl.createMock(StateDirectory.class);

        stateManager.registerGlobalStateStores(Collections.emptyList());
        EasyMock.expectLastCall();

        EasyMock.expect(stateManager.taskId()).andReturn(taskId);

        EasyMock.expect(stateDirectory.lock(taskId)).andReturn(true);

        stateManager.close();
        EasyMock.expectLastCall();

        // The `baseDir` will be accessed when attempting to delete the state store.
        EasyMock.expect(stateManager.baseDir()).andReturn(TestUtils.tempDirectory("state_store"));

        stateDirectory.unlock(taskId);
        EasyMock.expectLastCall();

        ctrl.checkOrder(true);
        ctrl.replay();

        task = createStatefulTask(createConfig(true, "100"), true, stateManager);
        task.suspend();
        task.closeDirty();
        task = null;

        ctrl.verify();
    }

    @Test
    public void shouldReadCommittedStreamTimeOnInitialize() {
        stateDirectory = EasyMock.createNiceMock(StateDirectory.class);
        EasyMock.replay(stateDirectory);

        consumer.commitSync(partitions.stream()
            .collect(Collectors.toMap(Function.identity(), tp -> new OffsetAndMetadata(0L, encodeTimestamp(10L)))));

        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);

        assertEquals(RecordQueue.UNKNOWN, task.streamTime());

        task.initializeIfNeeded();
        task.completeRestoration();

        assertEquals(10L, task.streamTime());
    }

    @Test
    public void shouldTransitToRestoringThenRunningAfterCreation() throws IOException {
        stateDirectory = EasyMock.createNiceMock(StateDirectory.class);
        EasyMock.expect(stateDirectory.lock(taskId)).andReturn(true);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(changelogPartition));
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.singletonMap(changelogPartition, 10L));
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);
        EasyMock.expectLastCall();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateDirectory, stateManager, recordCollector);

        task = createStatefulTask(createConfig(false, "100"), true);

        assertEquals(Task.State.CREATED, task.state());

        task.initializeIfNeeded();

        assertEquals(RESTORING, task.state());
        assertFalse(source1.initialized);
        assertFalse(source2.initialized);

        // initialize should be idempotent
        task.initializeIfNeeded();

        assertEquals(RESTORING, task.state());

        task.completeRestoration();

        assertEquals(Task.State.RUNNING, task.state());
        assertTrue(source1.initialized);
        assertTrue(source2.initialized);

        EasyMock.verify(stateDirectory);
    }

    @Test
    public void shouldProcessInOrder() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 30)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45)
        ));

        assertTrue(task.process(0L));
        assertEquals(5, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process(0L));
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process(0L));
        assertEquals(3, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process(0L));
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process(0L));
        assertEquals(1, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.process(0L));
        assertEquals(0, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
    }

    @Test
    public void shouldRecordBufferedRecords() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);

        final KafkaMetric metric = getMetric("active-buffer", "%s-count", task.id().toString(), StreamsConfig.METRICS_LATEST);

        assertThat(metric.metricValue(), equalTo(0.0));

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20)
        ));
        task.recordProcessTimeRatioAndBufferSize(100L, time.milliseconds());

        assertThat(metric.metricValue(), equalTo(2.0));

        task.process(0L);
        task.recordProcessTimeRatioAndBufferSize(100L, time.milliseconds());

        assertThat(metric.metricValue(), equalTo(1.0));
    }

    @Test
    public void shouldRecordProcessRatio() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);

        final KafkaMetric metric = getMetric("active-process", "%s-ratio", task.id().toString(), StreamsConfig.METRICS_LATEST);

        assertThat(metric.metricValue(), equalTo(0.0));

        task.recordProcessBatchTime(10L);
        task.recordProcessBatchTime(15L);
        task.recordProcessTimeRatioAndBufferSize(100L, time.milliseconds());

        assertThat(metric.metricValue(), equalTo(0.25));

        task.recordProcessBatchTime(10L);

        assertThat(metric.metricValue(), equalTo(0.25));

        task.recordProcessBatchTime(10L);
        task.recordProcessTimeRatioAndBufferSize(20L, time.milliseconds());

        assertThat(metric.metricValue(), equalTo(1.0));
    }

    @Test
    public void shouldRecordE2ELatencyOnSourceNodeAndTerminalNodes() {
        time = new MockTime(0L, 0L, 0L);
        metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.INFO), time);

        // Create a processor that only forwards even keys to test the metrics at the source and terminal nodes
        final MockSourceNode<Integer, Integer, Integer, Integer> evenKeyForwardingSourceNode = new MockSourceNode<Integer, Integer, Integer, Integer>(intDeserializer, intDeserializer) {
            InternalProcessorContext context;

            @Override
            public void init(final InternalProcessorContext context) {
                this.context = context;
                super.init(context);
            }

            @Override
            public void process(final Record<Integer, Integer> record) {
                if (record.key() % 2 == 0) {
                    context.forward(record);
                }
            }
        };

        task = createStatelessTaskWithForwardingTopology(evenKeyForwardingSourceNode);
        task.initializeIfNeeded();
        task.completeRestoration();

        final String sourceNodeName = evenKeyForwardingSourceNode.name();
        final String terminalNodeName = processorStreamTime.name();

        final Metric sourceAvg = getProcessorMetric("record-e2e-latency", "%s-avg", task.id().toString(), sourceNodeName, StreamsConfig.METRICS_LATEST);
        final Metric sourceMin = getProcessorMetric("record-e2e-latency", "%s-min", task.id().toString(), sourceNodeName, StreamsConfig.METRICS_LATEST);
        final Metric sourceMax = getProcessorMetric("record-e2e-latency", "%s-max", task.id().toString(), sourceNodeName, StreamsConfig.METRICS_LATEST);

        final Metric terminalAvg = getProcessorMetric("record-e2e-latency", "%s-avg", task.id().toString(), terminalNodeName, StreamsConfig.METRICS_LATEST);
        final Metric terminalMin = getProcessorMetric("record-e2e-latency", "%s-min", task.id().toString(), terminalNodeName, StreamsConfig.METRICS_LATEST);
        final Metric terminalMax = getProcessorMetric("record-e2e-latency", "%s-max", task.id().toString(), terminalNodeName, StreamsConfig.METRICS_LATEST);

        // e2e latency = 10
        task.addRecords(partition1, singletonList(getConsumerRecord(0, 0L)));
        task.process(10L);

        assertThat(sourceAvg.metricValue(), equalTo(10.0));
        assertThat(sourceMin.metricValue(), equalTo(10.0));
        assertThat(sourceMax.metricValue(), equalTo(10.0));

        // key 0: reaches terminal node
        assertThat(terminalAvg.metricValue(), equalTo(10.0));
        assertThat(terminalMin.metricValue(), equalTo(10.0));
        assertThat(terminalMax.metricValue(), equalTo(10.0));


        // e2e latency = 15
        task.addRecords(partition1, singletonList(getConsumerRecord(1, 0L)));
        task.process(15L);

        assertThat(sourceAvg.metricValue(), equalTo(12.5));
        assertThat(sourceMin.metricValue(), equalTo(10.0));
        assertThat(sourceMax.metricValue(), equalTo(15.0));

        // key 1: stops at source, doesn't affect terminal node metrics
        assertThat(terminalAvg.metricValue(), equalTo(10.0));
        assertThat(terminalMin.metricValue(), equalTo(10.0));
        assertThat(terminalMax.metricValue(), equalTo(10.0));


        // e2e latency = 23
        task.addRecords(partition1, singletonList(getConsumerRecord(2, 0L)));
        task.process(23L);

        assertThat(sourceAvg.metricValue(), equalTo(16.0));
        assertThat(sourceMin.metricValue(), equalTo(10.0));
        assertThat(sourceMax.metricValue(), equalTo(23.0));

        // key 2: reaches terminal node
        assertThat(terminalAvg.metricValue(), equalTo(16.5));
        assertThat(terminalMin.metricValue(), equalTo(10.0));
        assertThat(terminalMax.metricValue(), equalTo(23.0));


        // e2e latency = 5
        task.addRecords(partition1, singletonList(getConsumerRecord(3, 0L)));
        task.process(5L);

        assertThat(sourceAvg.metricValue(), equalTo(13.25));
        assertThat(sourceMin.metricValue(), equalTo(5.0));
        assertThat(sourceMax.metricValue(), equalTo(23.0));

        // key 3: stops at source, doesn't affect terminal node metrics
        assertThat(terminalAvg.metricValue(), equalTo(16.5));
        assertThat(terminalMin.metricValue(), equalTo(10.0));
        assertThat(terminalMax.metricValue(), equalTo(23.0));
    }

    @Test
    public void shouldConstructMetricsWithBuiltInMetricsVersion0100To24() {
        testMetrics(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void shouldConstructMetricsWithBuiltInMetricsVersionLatest() {
        testMetrics(StreamsConfig.METRICS_LATEST);
    }

    private void testMetrics(final String builtInMetricsVersion) {
        task = createStatelessTask(createConfig(false, "100"), builtInMetricsVersion);

        assertNotNull(getMetric(
            "enforced-processing",
            "%s-rate",
            task.id().toString(),
            builtInMetricsVersion
        ));
        assertNotNull(getMetric(
            "enforced-processing",
            "%s-total",
            task.id().toString(),
            builtInMetricsVersion
        ));

        assertNotNull(getMetric(
            "record-lateness",
            "%s-avg",
            task.id().toString(),
            builtInMetricsVersion
        ));
        assertNotNull(getMetric(
            "record-lateness",
            "%s-max",
            task.id().toString(),
            builtInMetricsVersion
        ));

        assertNotNull(getMetric(
            "active-process",
            "%s-ratio",
            task.id().toString(),
            builtInMetricsVersion
        ));

        assertNotNull(getMetric(
            "active-buffer",
            "%s-count",
            task.id().toString(),
            builtInMetricsVersion
        ));

        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            testMetricsForBuiltInMetricsVersion0100To24();
        } else {
            testMetricsForBuiltInMetricsVersionLatest();
        }

        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        final String threadIdTag =
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? THREAD_ID_TAG : THREAD_ID_TAG_0100_TO_24;
        assertTrue(reporter.containsMbean(String.format(
            "kafka.streams:type=stream-task-metrics,%s=%s,task-id=%s",
            threadIdTag,
            threadId,
            task.id()
        )));
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertTrue(reporter.containsMbean(String.format(
                "kafka.streams:type=stream-task-metrics,%s=%s,task-id=all",
                threadIdTag,
                threadId
            )));
        }
    }

    private void testMetricsForBuiltInMetricsVersionLatest() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_LATEST;
        assertNull(getMetric("commit", "%s-latency-avg", "all", builtInMetricsVersion));
        assertNull(getMetric("commit", "%s-latency-max", "all", builtInMetricsVersion));
        assertNull(getMetric("commit", "%s-rate", "all", builtInMetricsVersion));
        assertNull(getMetric("commit", "%s-total", "all", builtInMetricsVersion));

        assertNotNull(getMetric("process", "%s-latency-max", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("process", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));

        assertNotNull(getMetric("punctuate", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("punctuate", "%s-latency-max", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("punctuate", "%s-rate", task.id().toString(), builtInMetricsVersion));
        assertNotNull(getMetric("punctuate", "%s-total", task.id().toString(), builtInMetricsVersion));
    }

    private void testMetricsForBuiltInMetricsVersion0100To24() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_0100_TO_24;
        assertNotNull(getMetric("commit", "%s-rate", "all", builtInMetricsVersion));

        assertNull(getMetric("process", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("process", "%s-latency-max", task.id().toString(), builtInMetricsVersion));

        assertNull(getMetric("punctuate", "%s-latency-avg", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("punctuate", "%s-latency-max", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("punctuate", "%s-rate", task.id().toString(), builtInMetricsVersion));
        assertNull(getMetric("punctuate", "%s-total", task.id().toString(), builtInMetricsVersion));
    }

    private KafkaMetric getMetric(final String operation,
                                  final String nameFormat,
                                  final String taskId,
                                  final String builtInMetricsVersion) {
        final String descriptionIsNotVerified = "";
        return metrics.metrics().get(metrics.metricName(
            String.format(nameFormat, operation),
            "stream-task-metrics",
            descriptionIsNotVerified,
            mkMap(
                mkEntry("task-id", taskId),
                mkEntry(
                    StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? THREAD_ID_TAG
                        : THREAD_ID_TAG_0100_TO_24,
                    Thread.currentThread().getName()
                )
            )
        ));
    }

    private Metric getProcessorMetric(final String operation,
                                      final String nameFormat,
                                      final String taskId,
                                      final String processorNodeId,
                                      final String builtInMetricsVersion) {

        return getMetricByNameFilterByTags(
            metrics.metrics(),
            String.format(nameFormat, operation),
            "stream-processor-node-metrics",
            mkMap(
                mkEntry("task-id", taskId),
                mkEntry("processor-node-id", processorNodeId),
                mkEntry(
                    StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? THREAD_ID_TAG
                        : THREAD_ID_TAG_0100_TO_24,
                    Thread.currentThread().getName()
                )
            )
        );
    }

    @Test
    public void shouldPauseAndResumeBasedOnBufferedRecords() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45),
            getConsumerRecord(partition2, 55),
            getConsumerRecord(partition2, 65)
        ));

        assertTrue(task.process(0L));
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 30),
            getConsumerRecord(partition1, 40),
            getConsumerRecord(partition1, 50)
        ));

        assertEquals(2, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition1));
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process(0L));
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process(0L));
        assertEquals(3, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process(0L));
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertEquals(0, consumer.paused().size());
    }

    @Test
    public void shouldPunctuateOnceStreamTimeAfterGap() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 142),
            getConsumerRecord(partition1, 155),
            getConsumerRecord(partition1, 160)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 145),
            getConsumerRecord(partition2, 159),
            getConsumerRecord(partition2, 161)
        ));

        // st: -1
        assertFalse(task.maybePunctuateStreamTime()); // punctuate at 20

        // st: 20
        assertTrue(task.process(0L));
        assertEquals(7, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 25
        assertTrue(task.process(0L));
        assertEquals(6, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 142
        // punctuate at 142
        assertTrue(task.process(0L));
        assertEquals(5, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 145
        // only one punctuation after 100ms gap
        assertTrue(task.process(0L));
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 155
        // punctuate at 155
        assertTrue(task.process(0L));
        assertEquals(3, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 159
        assertTrue(task.process(0L));
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 160, aligned at 0
        assertTrue(task.process(0L));
        assertEquals(1, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 161
        assertTrue(task.process(0L));
        assertEquals(0, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(4, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 142L, 155L, 160L);
    }

    @Test
    public void shouldRespectPunctuateCancellationStreamTime() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 30),
            getConsumerRecord(partition1, 40)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45)
        ));

        assertFalse(task.maybePunctuateStreamTime());

        // st is now 20
        assertTrue(task.process(0L));

        assertTrue(task.maybePunctuateStreamTime());

        // st is now 25
        assertTrue(task.process(0L));

        assertFalse(task.maybePunctuateStreamTime());

        // st is now 30
        assertTrue(task.process(0L));

        processorStreamTime.mockProcessor.scheduleCancellable().cancel();

        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L);
    }

    @Test
    public void shouldRespectPunctuateCancellationSystemTime() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();
        final long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.scheduleCancellable().cancel();
        time.sleep(10);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
    }

    @Test
    public void shouldRespectCommitNeeded() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        assertFalse(task.commitNeeded());

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        assertTrue(task.process(0L));
        assertTrue(task.commitNeeded());

        task.prepareCommit();
        assertTrue(task.commitNeeded());

        task.postCommit(true);
        assertFalse(task.commitNeeded());

        assertTrue(task.maybePunctuateStreamTime());
        assertTrue(task.commitNeeded());

        task.prepareCommit();
        assertTrue(task.commitNeeded());

        task.postCommit(true);
        assertFalse(task.commitNeeded());

        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        assertTrue(task.commitNeeded());

        task.prepareCommit();
        assertTrue(task.commitNeeded());

        task.postCommit(true);
        assertFalse(task.commitNeeded());
    }

    @Test
    public void shouldCommitNextOffsetFromQueueIfAvailable() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, Arrays.asList(
            getConsumerRecord(partition1, 0L),
            getConsumerRecord(partition1, 3L),
            getConsumerRecord(partition1, 5L)));

        task.process(0L);
        task.process(0L);

        final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = task.prepareCommit();

        assertThat(offsetsAndMetadata, equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(5L, encodeTimestamp(3L))))));
    }

    @Test
    public void shouldCommitConsumerPositionIfRecordQueueIsEmpty() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        consumer.addRecord(getConsumerRecord(partition1, 0L));
        consumer.addRecord(getConsumerRecord(partition1, 1L));
        consumer.addRecord(getConsumerRecord(partition1, 2L));
        consumer.poll(Duration.ZERO);

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0L)));
        task.process(0L);
        final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = task.prepareCommit();

        assertThat(offsetsAndMetadata, equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(3L, encodeTimestamp(0L))))));
    }

    @Test
    public void shouldFailOnCommitIfTaskIsClosed() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        task.suspend();
        task.transitionTo(Task.State.CLOSED);

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            task::prepareCommit
        );

        assertThat(thrown.getMessage(), is("Illegal state CLOSED while preparing active task 0_0 for committing"));
    }

    @Test
    public void shouldRespectCommitRequested() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.requestCommit();
        assertTrue(task.commitRequested());
    }

    @Test
    public void shouldEncodeAndDecodeMetadata() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        assertEquals(DEFAULT_TIMESTAMP, task.decodeTimestamp(encodeTimestamp(DEFAULT_TIMESTAMP)));
    }

    @Test
    public void shouldReturnUnknownTimestampIfUnknownVersion() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);

        final byte[] emptyMessage = {StreamTask.LATEST_MAGIC_BYTE + 1};
        final String encodedString = Base64.getEncoder().encodeToString(emptyMessage);
        assertEquals(RecordQueue.UNKNOWN, task.decodeTimestamp(encodedString));
    }

    @Test
    public void shouldReturnUnknownTimestampIfEmptyMessage() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);

        assertEquals(RecordQueue.UNKNOWN, task.decodeTimestamp(""));
    }

    @Test
    public void shouldBeProcessableIfAllPartitionsBuffered() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        assertFalse(task.process(0L));

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.process(0L));

        task.addRecords(partition2, Collections.singleton(new ConsumerRecord<>(topic2, 1, 0, bytes, bytes)));

        assertTrue(task.process(0L));
    }

    @Test
    public void shouldBeProcessableIfWaitedForTooLong() {
        // max idle time is 100ms
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        final MetricName enforcedProcessMetric = metrics.metricName(
            "enforced-processing-total",
            "stream-task-metrics",
            mkMap(mkEntry("thread-id", Thread.currentThread().getName()), mkEntry("task-id", taskId.toString()))
        );

        assertFalse(task.process(time.milliseconds()));
        assertEquals(0.0, metrics.metric(enforcedProcessMetric).metricValue());

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1,
                        asList(
                            new ConsumerRecord<>(topic1, 1, 0, bytes, bytes),
                            new ConsumerRecord<>(topic1, 1, 1, bytes, bytes),
                            new ConsumerRecord<>(topic1, 1, 2, bytes, bytes)
                        )
        );

        assertFalse(task.process(time.milliseconds()));

        assertFalse(task.process(time.milliseconds() + 99L));

        assertTrue(task.process(time.milliseconds() + 100L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // once decided to enforce, continue doing that
        assertTrue(task.process(time.milliseconds() + 101L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        task.addRecords(partition2, Collections.singleton(new ConsumerRecord<>(topic2, 1, 0, bytes, bytes)));

        assertTrue(task.process(time.milliseconds() + 130L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        // one resumed to normal processing, the timer should be reset

        assertFalse(task.process(time.milliseconds() + 150L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertFalse(task.process(time.milliseconds() + 249L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertTrue(task.process(time.milliseconds() + 250L));
        assertEquals(3.0, metrics.metric(enforcedProcessMetric).metricValue());
    }

    @Test
    public void shouldNotBeProcessableIfNoDataAvailble() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        final MetricName enforcedProcessMetric = metrics.metricName(
            "enforced-processing-total",
            "stream-task-metrics",
            mkMap(mkEntry("thread-id", Thread.currentThread().getName()), mkEntry("task-id", taskId.toString()))
        );

        assertFalse(task.process(0L));
        assertEquals(0.0, metrics.metric(enforcedProcessMetric).metricValue());

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.process(time.milliseconds()));

        assertFalse(task.process(time.milliseconds() + 99L));

        assertTrue(task.process(time.milliseconds() + 100L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // once the buffer is drained and no new records coming, the timer should be reset

        assertFalse(task.process(time.milliseconds() + 110L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // check that after time is reset, we only falls into enforced processing after the
        // whole timeout has elapsed again
        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.process(time.milliseconds() + 150L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertFalse(task.process(time.milliseconds() + 249L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertTrue(task.process(time.milliseconds() + 250L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());
    }


    @Test
    public void shouldPunctuateSystemTimeWhenIntervalElapsed() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();
        final long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(20);
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10, now + 20, now + 30, now + 50);
    }

    @Test
    public void shouldNotPunctuateSystemTimeWhenIntervalNotElapsed() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME);
    }

    @Test
    public void shouldPunctuateOnceSystemTimeAfterGap() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();
        final long now = time.milliseconds();
        time.sleep(100);
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(12);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(7);
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1); // punctuate at now + 130
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(105); // punctuate at now + 235
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(5); // punctuate at now + 240, still aligned on the initial punctuation
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 100, now + 110, now + 122, now + 130, now + 235, now + 240);
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        try {
            task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorStreamTime.name() + "'"));
            assertThat(task.processorContext().currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingWallClockTimeTime() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        try {
            task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorSystemTime.name() + "'"));
            assertThat(task.processorContext().currentNode(), nullValue());
        }
    }

    @Test
    public void shouldNotShareHeadersBetweenPunctuateIterations() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            task.processorContext().headers().add("dummy", (byte[]) null);
        });
        task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            assertFalse(task.processorContext().headers().iterator().hasNext());
        });
    }

    @Test
    public void shouldWrapKafkaExceptionWithStreamsExceptionWhenProcess() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createFaultyStatefulTask(createConfig(false, "100"));

        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 30)
        ));
        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 5),  // this is the first record to process
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45)
        ));

        assertThrows(StreamsException.class, () -> task.process(0L));
    }

    @Test
    public void shouldReadCommittedOffsetAndRethrowTimeoutWhenCompleteRestoration() throws IOException {
        stateDirectory = EasyMock.createNiceMock(StateDirectory.class);
        EasyMock.expect(stateDirectory.lock(taskId)).andReturn(true);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();

        EasyMock.replay(recordCollector, stateDirectory, stateManager);

        task = createDisconnectedTask(createConfig(false, "100"));

        task.initializeIfNeeded();

        assertThrows(TimeoutException.class, task::completeRestoration);
    }

    @Test
    public void shouldReInitializeTopologyWhenResuming() throws IOException {
        stateDirectory = EasyMock.createNiceMock(StateDirectory.class);
        EasyMock.expect(stateDirectory.lock(taskId)).andReturn(true);
        EasyMock.expect(recordCollector.offsets()).andThrow(new AssertionError("Should not try to read offsets")).anyTimes();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();

        EasyMock.replay(recordCollector, stateDirectory, stateManager);

        task = createStatefulTask(createConfig(false, "100"), true);

        task.initializeIfNeeded();

        task.suspend();

        assertEquals(Task.State.SUSPENDED, task.state());
        assertFalse(source1.initialized);
        assertFalse(source2.initialized);

        task.resume();

        assertEquals(RESTORING, task.state());
        assertFalse(source1.initialized);
        assertFalse(source2.initialized);

        task.completeRestoration();

        assertEquals(Task.State.RUNNING, task.state());
        assertTrue(source1.initialized);
        assertTrue(source2.initialized);

        EasyMock.verify(stateManager, recordCollector);

        EasyMock.reset(recordCollector);
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap());
        EasyMock.replay(recordCollector);
    }

    @Test
    public void shouldNotCheckpointOffsetsAgainOnCommitIfSnapshotNotChangedMuch() {
        final Long offset = 543L;

        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.singletonMap(changelogPartition, offset)).anyTimes();
        stateManager.checkpoint();
        EasyMock.expectLastCall().once();
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(Collections.singletonMap(changelogPartition, 10L))
                .andReturn(Collections.singletonMap(changelogPartition, 20L));
        EasyMock.expectLastCall();
        EasyMock.replay(stateManager, recordCollector);

        task = createStatefulTask(createConfig(false, "100"), true);

        task.initializeIfNeeded();
        task.completeRestoration();

        task.prepareCommit();
        task.postCommit(true);   // should checkpoint

        task.prepareCommit();
        task.postCommit(false);   // should not checkpoint

        EasyMock.verify(stateManager, recordCollector);
    }

    @Test
    public void shouldCheckpointOffsetsOnCommitIfSnapshotMuchChanged() {
        final Long offset = 543L;

        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.singletonMap(changelogPartition, offset)).anyTimes();
        stateManager.checkpoint();
        EasyMock.expectLastCall().times(2);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(changelogPartition));
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(Collections.singletonMap(changelogPartition, 0L))
                .andReturn(Collections.singletonMap(changelogPartition, 10L))
                .andReturn(Collections.singletonMap(changelogPartition, 12000L));
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);
        EasyMock.expectLastCall();
        EasyMock.replay(stateManager, recordCollector);

        task = createStatefulTask(createConfig(false, "100"), true);

        task.initializeIfNeeded();
        task.completeRestoration();
        task.prepareCommit();
        task.postCommit(true);

        task.prepareCommit();
        task.postCommit(false);

        EasyMock.verify(recordCollector);
    }

    @Test
    public void shouldNotCheckpointOffsetsOnCommitIfEosIsEnabled() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(changelogPartition));
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);
        EasyMock.expectLastCall();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createStatefulTask(createConfig(true, "100"), true);

        task.initializeIfNeeded();
        task.completeRestoration();
        task.prepareCommit();
        task.postCommit(false);
        final File checkpointFile = new File(
            stateDirectory.directoryForTask(taskId),
            StateManagerUtil.CHECKPOINT_FILE_NAME
        );

        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();
        task.processorContext().setCurrentNode(processorStreamTime);
        try {
            task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
            fail("Should throw illegal state exception as current node is not null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldCallPunctuateOnPassedInProcessorNode() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(5L));
        task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(10L));
    }

    @Test
    public void shouldSetProcessorNodeOnContextBackToNullAfterSuccessfulPunctuate() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(task.processorContext().currentNode(), nullValue());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.schedule(1, PunctuationType.STREAM_TIME, timestamp -> { });
    }

    @Test
    public void shouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);
        task.processorContext().setCurrentNode(processorStreamTime);
        task.schedule(1, PunctuationType.STREAM_TIME, timestamp -> { });
    }

    @Test
    public void shouldCloseStateManagerEvenDuringFailureOnUncleanTaskClose() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expectLastCall();
        stateManager.close();
        EasyMock.expectLastCall();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createFaultyStatefulTask(createConfig(false, "100"));

        task.initializeIfNeeded();
        task.completeRestoration();

        assertThrows(RuntimeException.class, () -> task.suspend());
        task.closeDirty();

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldReturnOffsetsForRepartitionTopicsForPurging() {
        final TopicPartition repartition = new TopicPartition("repartition", 1);

        final ProcessorTopology topology = withRepartitionTopics(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(repartition.topic(), source2)),
            Collections.singleton(repartition.topic())
        );
        consumer.assign(asList(partition1, repartition));
        consumer.updateBeginningOffsets(mkMap(mkEntry(repartition, 0L)));

        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        final StreamsConfig config = createConfig(false, "0");
        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        task = new StreamTask(
            taskId,
            mkSet(partition1, repartition),
            topology,
            consumer,
            config,
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context);

        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 5L)));
        task.addRecords(repartition, singletonList(getConsumerRecord(repartition, 10L)));

        assertTrue(task.process(0L));
        assertTrue(task.process(0L));

        task.prepareCommit();

        final Map<TopicPartition, Long> map = task.purgeableOffsets();

        assertThat(map, equalTo(Collections.singletonMap(repartition, 11L)));
    }

    @Test
    public void shouldThrowStreamsExceptionWhenFetchCommittedFailed() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(partition1));
        EasyMock.replay(stateManager);

        final Consumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw new KafkaException("KABOOM!");
            }
        };

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.initializeIfNeeded();

        assertThrows(StreamsException.class, task::completeRestoration);
    }

    @Test
    public void shouldThrowIfCommittingOnIllegalState() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);

        task.transitionTo(Task.State.SUSPENDED);
        task.transitionTo(Task.State.CLOSED);
        assertThrows(IllegalStateException.class, task::prepareCommit);
    }

    @Test
    public void shouldThrowIfPostCommittingOnIllegalState() {
        task = createStatelessTask(createConfig(false, "100"), StreamsConfig.METRICS_LATEST);

        task.transitionTo(Task.State.SUSPENDED);
        task.transitionTo(Task.State.CLOSED);
        assertThrows(IllegalStateException.class, () -> task.postCommit(true));
    }

    @Test
    public void shouldSkipCheckpointingSuspendedCreatedTask() {
        stateManager.checkpoint();
        EasyMock.expectLastCall().andThrow(new AssertionError("Should not have tried to checkpoint"));
        EasyMock.replay(stateManager);

        task = createStatefulTask(createConfig(false, "100"), true);
        task.suspend();
        task.postCommit(true);
    }

    @Test
    public void shouldCheckpointForSuspendedTask() {
        stateManager.checkpoint();
        EasyMock.expectLastCall().once();
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(Collections.singletonMap(partition1, 1L));
        EasyMock.replay(stateManager);

        task = createStatefulTask(createConfig(false, "100"), true);
        task.initializeIfNeeded();
        task.suspend();
        task.postCommit(true);
        EasyMock.verify(stateManager);
    }
    
    @Test
    public void shouldNotCheckpointForSuspendedRunningTaskWithSmallProgress() {
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(Collections.singletonMap(partition1, 1L))
                .andReturn(Collections.singletonMap(partition1, 2L));
        stateManager.checkpoint();
        EasyMock.expectLastCall().andThrow(new AssertionError("Checkpoint should not be called")).anyTimes();
        EasyMock.replay(stateManager);

        task = createStatefulTask(createConfig(false, "100"), true);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.prepareCommit();
        task.postCommit(false);

        task.suspend();
        task.postCommit(false);
        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldCheckpointForSuspendedRunningTaskWithLargeProgress() {
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(Collections.singletonMap(partition1, 12000L))
                .andReturn(Collections.singletonMap(partition1, 24000L));
        stateManager.checkpoint();
        EasyMock.expectLastCall().times(2);
        EasyMock.replay(stateManager);

        task = createStatefulTask(createConfig(false, "100"), true);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.prepareCommit();
        task.postCommit(false);

        task.suspend();
        task.postCommit(false);
        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldCheckpointWhileUpdateSnapshotWithTheConsumedOffsetsForSuspendedRunningTask() {
        final Map<TopicPartition, Long> checkpointableOffsets = singletonMap(partition1, 1L);
        stateManager.checkpoint();
        EasyMock.expectLastCall().once();
        stateManager.updateChangelogOffsets(EasyMock.eq(checkpointableOffsets));
        EasyMock.expectLastCall().once();
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(checkpointableOffsets);
        EasyMock.expect(recordCollector.offsets()).andReturn(checkpointableOffsets).once();
        EasyMock.replay(stateManager, recordCollector);

        task = createStatefulTask(createConfig(false, "0"), true);
        task.initializeIfNeeded();
        task.completeRestoration();
        task.addRecords(partition1, singleton(getConsumerRecord(partition1, 10)));
        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.postCommit(true);
        EasyMock.verify(stateManager, recordCollector);
    }

    @Test
    public void shouldReturnStateManagerChangelogOffsets() {
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.singletonMap(partition1, 50L)).anyTimes();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(partition1)).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.initializeIfNeeded();

        assertEquals(Collections.singletonMap(partition1, 50L), task.changelogOffsets());

        task.completeRestoration();

        assertEquals(Collections.singletonMap(partition1, Task.LATEST_OFFSET), task.changelogOffsets());
    }

    @Test
    public void shouldNotCheckpointOnCloseCreated() {
        stateManager.flush();
        EasyMock.expectLastCall().andThrow(new AssertionError("Flush should not be called")).anyTimes();
        stateManager.checkpoint();
        EasyMock.expectLastCall().andThrow(new AssertionError("Checkpoint should not be called")).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.suspend();
        task.closeClean();

        assertEquals(Task.State.CLOSED, task.state());
        assertFalse(source1.initialized);
        assertFalse(source1.closed);

        EasyMock.verify(stateManager, recordCollector);

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);
    }

    @Test
    public void shouldCheckpointOnCloseRestoringIfNoProgress() {
        stateManager.flush();
        EasyMock.expectLastCall().once();
        stateManager.checkpoint();
        EasyMock.expectLastCall().once();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.initializeIfNeeded();
        task.completeRestoration();
        task.suspend();
        task.prepareCommit();
        task.postCommit(true);
        task.closeClean();

        assertEquals(Task.State.CLOSED, task.state());

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldCheckpointOffsetsOnPostCommit() {
        final long offset = 543L;
        final long consumedOffset = 345L;

        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.singletonMap(changelogPartition, offset)).anyTimes();
        EasyMock.expectLastCall();
        stateManager.checkpoint();
        EasyMock.expectLastCall().once();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(Collections.singletonMap(partition1, offset + 12000L));
        EasyMock.replay(recordCollector, stateManager);

        task = createOptimizedStatefulTask(createConfig(false, "0"), consumer);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, consumedOffset)));
        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.prepareCommit();
        task.postCommit(false);

        assertEquals(Task.State.SUSPENDED, task.state());

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldThrowExceptionOnCloseCleanError() {
        final long offset = 543L;

        EasyMock.expect(recordCollector.offsets()).andReturn(emptyMap()).anyTimes();
        stateManager.checkpoint();
        EasyMock.expectLastCall().once();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(changelogPartition)).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.singletonMap(changelogPartition, offset)).anyTimes();
        stateManager.close();
        EasyMock.expectLastCall().andThrow(new ProcessorStateException("KABOOM!")).anyTimes();
        EasyMock.replay(recordCollector, stateManager);
        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, offset)));
        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.prepareCommit();
        task.postCommit(true);
        assertThrows(ProcessorStateException.class, () -> task.closeClean());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);
        EasyMock.reset(stateManager);
        EasyMock.expect(stateManager.changelogPartitions()).andStubReturn(Collections.singleton(changelogPartition));
        stateManager.close();
        EasyMock.expectLastCall();
        EasyMock.replay(stateManager);
    }

    @Test
    public void shouldThrowOnCloseCleanFlushError() {
        final long offset = 543L;

        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.singletonMap(changelogPartition, offset));
        stateManager.flushCache();
        EasyMock.expectLastCall().andThrow(new ProcessorStateException("KABOOM!")).anyTimes();
        stateManager.flush();
        EasyMock.expectLastCall().andThrow(new AssertionError("Flush should not be called")).anyTimes();
        stateManager.checkpoint();
        EasyMock.expectLastCall().andThrow(new AssertionError("Checkpoint should not be called")).anyTimes();
        stateManager.close();
        EasyMock.expectLastCall().andThrow(new AssertionError("Close should not be called!")).anyTimes();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(recordCollector, stateManager);
        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);
        task.initializeIfNeeded();
        task.completeRestoration();

        // process one record to make commit needed
        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, offset)));
        task.process(100L);

        assertThrows(ProcessorStateException.class, task::prepareCommit);

        assertEquals(RUNNING, task.state());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);
        EasyMock.reset(stateManager);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.replay(stateManager);
    }

    @Test
    public void shouldThrowOnCloseCleanCheckpointError() {
        final long offset = 54300L;
        EasyMock.expect(recordCollector.offsets()).andReturn(emptyMap());
        stateManager.checkpoint();
        EasyMock.expectLastCall().andThrow(new ProcessorStateException("KABOOM!")).anyTimes();
        stateManager.close();
        EasyMock.expectLastCall().andThrow(new AssertionError("Close should not be called!")).anyTimes();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets())
                .andReturn(Collections.singletonMap(partition1, offset));
        EasyMock.replay(recordCollector, stateManager);
        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);
        task.initializeIfNeeded();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, offset)));
        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.prepareCommit();
        assertThrows(ProcessorStateException.class, () -> task.postCommit(true));

        assertEquals(Task.State.SUSPENDED, task.state());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);
        EasyMock.reset(stateManager);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        stateManager.close();
        EasyMock.expectLastCall();
        EasyMock.replay(stateManager);
    }

    @Test
    public void shouldNotThrowFromStateManagerCloseInCloseDirty() {
        stateManager.close();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!")).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager);

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);
        task.initializeIfNeeded();

        task.suspend();
        task.closeDirty();

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldUnregisterMetricsInCloseClean() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.suspend();
        assertThat(getTaskMetrics(), not(empty()));
        task.closeClean();
        assertThat(getTaskMetrics(), empty());
    }

    @Test
    public void shouldUnregisterMetricsInCloseDirty() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.suspend();
        assertThat(getTaskMetrics(), not(empty()));
        task.closeDirty();
        assertThat(getTaskMetrics(), empty());
    }

    @Test
    public void shouldUnregisterMetricsInCloseCleanAndRecycleState() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.suspend();
        assertThat(getTaskMetrics(), not(empty()));
        task.closeCleanAndRecycleState();
        assertThat(getTaskMetrics(), empty());
    }

    @Test
    public void closeShouldBeIdempotent() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createOptimizedStatefulTask(createConfig(false, "100"), consumer);

        task.suspend();
        task.closeClean();

        // close calls are idempotent since we are already in closed
        task.closeClean();
        task.closeDirty();

        EasyMock.reset(stateManager);
        EasyMock.replay(stateManager);
    }

    @Test
    public void shouldUpdatePartitions() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        final Set<TopicPartition> newPartitions = new HashSet<>(task.inputPartitions());
        newPartitions.add(new TopicPartition("newTopic", 0));

        task.update(newPartitions, mkMap(
            mkEntry(source1.name(), asList(topic1, "newTopic")),
            mkEntry(source2.name(), singletonList(topic2)))
        );

        assertThat(task.inputPartitions(), equalTo(newPartitions));
    }

    @Test
    public void shouldThrowIfCleanClosingDirtyTask() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process(0L);
        assertTrue(task.commitNeeded());

        assertThrows(TaskMigratedException.class, () -> task.closeClean());
    }

    @Test
    public void shouldThrowIfRecyclingDirtyTask() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);
        task.initializeIfNeeded();
        task.completeRestoration();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process(0L);
        assertTrue(task.commitNeeded());

        assertThrows(TaskMigratedException.class, () -> task.closeCleanAndRecycleState());
    }

    @Test
    public void shouldOnlyRecycleSuspendedTasks() {
        stateManager.recycle();
        recordCollector.closeClean();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        task = createStatefulTask(createConfig(false, "100"), true);
        assertThrows(IllegalStateException.class, () -> task.closeCleanAndRecycleState()); // CREATED

        task.initializeIfNeeded();
        assertThrows(IllegalStateException.class, () -> task.closeCleanAndRecycleState()); // RESTORING

        task.completeRestoration();
        assertThrows(IllegalStateException.class, () -> task.closeCleanAndRecycleState()); // RUNNING

        task.suspend();
        task.closeCleanAndRecycleState(); // SUSPENDED

        EasyMock.verify(stateManager, recordCollector);
    }

    @Test
    public void shouldAlwaysSuspendCreatedTasks() {
        EasyMock.replay(stateManager);
        task = createStatefulTask(createConfig(false, "100"), true);
        assertThat(task.state(), equalTo(CREATED));
        task.suspend();
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldAlwaysSuspendRestoringTasks() {
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager);
        task = createStatefulTask(createConfig(false, "100"), true);
        task.initializeIfNeeded();
        assertThat(task.state(), equalTo(RESTORING));
        task.suspend();
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldAlwaysSuspendRunningTasks() {
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager);
        task = createFaultyStatefulTask(createConfig(false, "100"));
        task.initializeIfNeeded();
        task.completeRestoration();
        assertThat(task.state(), equalTo(RUNNING));
        assertThrows(RuntimeException.class, () -> task.suspend());
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldThrowTopologyExceptionIfTaskCreatedForUnknownTopic() {
        final InternalProcessorContext context = new ProcessorContextImpl(
                taskId,
                createConfig(false, "100"),
                stateManager,
                streamsMetrics,
                null
        );
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(this.metrics, "test", StreamsConfig.METRICS_LATEST, time);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.replay(stateManager);

        // The processor topology is missing the topics
        final ProcessorTopology topology = withSources(asList(), mkMap());

        final TopologyException  exception = assertThrows(
            TopologyException.class,
            () -> new StreamTask(
                taskId,
                partitions,
                topology,
                consumer,
                createConfig(false, "100"),
                metrics,
                stateDirectory,
                cache,
                time,
                stateManager,
                recordCollector,
                context
            )
        );

        assertThat(exception.getMessage(), equalTo("Invalid topology: " +
                "Topic is unknown to the topology. This may happen if different KafkaStreams instances of the same " +
                "application execute different Topologies. Note that Topologies are only identical if all operators " +
                "are added in the same order."));
    }

    @Test
    public void shouldInitTaskTimeoutAndEventuallyThrow() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);

        task.maybeInitTaskTimeoutOrThrow(0L, null);
        task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).toMillis(), null);

        assertThrows(
            TimeoutException.class,
            () -> task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).plus(Duration.ofMillis(1L)).toMillis(), null)
        );
    }

    @Test
    public void shouldCLearTaskTimeout() {
        task = createStatelessTask(createConfig(false, "0"), StreamsConfig.METRICS_LATEST);

        task.maybeInitTaskTimeoutOrThrow(0L, null);
        task.clearTaskTimeout();
        task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).plus(Duration.ofMillis(1L)).toMillis(), null);
    }

    private List<MetricName> getTaskMetrics() {
        return metrics.metrics().keySet().stream().filter(m -> m.tags().containsKey("task-id")).collect(Collectors.toList());
    }

    private StreamTask createOptimizedStatefulTask(final StreamsConfig config, final Consumer<byte[], byte[]> consumer) {
        final StateStore stateStore = new MockKeyValueStore(storeName, true);

        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            singletonList(source1),
            mkMap(mkEntry(topic1, source1)),
            singletonList(stateStore),
            Collections.singletonMap(storeName, topic1));

        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            mkSet(partition1),
            topology,
            consumer,
            config,
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context);
    }

    private StreamTask createDisconnectedTask(final StreamsConfig config) {
        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, false);

        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
            singletonList(stateStore),
            Collections.emptyMap());

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw new TimeoutException("KABOOM!");
            }
        };

        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            partitions,
            topology,
            consumer,
            config,
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context);
    }

    private StreamTask createFaultyStatefulTask(final StreamsConfig config) {
        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source3),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3)),
            singletonList(stateStore),
            Collections.emptyMap());

        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            partitions,
            topology,
            consumer,
            config,
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context);
    }

    private StreamTask createStatefulTask(final StreamsConfig config, final boolean logged) {
        return createStatefulTask(config, logged, stateManager);
    }

    private StreamTask createStatefulTask(final StreamsConfig config, final boolean logged, final ProcessorStateManager stateManager) {
        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, logged);

        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
            singletonList(stateStore),
            logged ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.emptyMap());

        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            partitions,
            topology,
            consumer,
            config,
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context);
    }

    private StreamTask createStatelessTask(final StreamsConfig config,
                                           final String builtInMetricsVersion) {
        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            partitions,
            topology,
            consumer,
            config,
            new StreamsMetricsImpl(metrics, "test", builtInMetricsVersion, time),
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context);
    }

    private StreamTask createStatelessTaskWithForwardingTopology(final SourceNode<Integer, Integer, Integer, Integer> sourceNode) {
        final ProcessorTopology topology = withSources(
            asList(sourceNode, processorStreamTime),
            singletonMap(topic1, sourceNode)
        );

        sourceNode.addChild(processorStreamTime);

        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.expect(recordCollector.offsets()).andReturn(Collections.emptyMap()).anyTimes();
        EasyMock.replay(stateManager, recordCollector);

        final StreamsConfig config = createConfig(false, "0");

        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            singleton(partition1),
            topology,
            consumer,
            config,
            new StreamsMetricsImpl(metrics, "test", StreamsConfig.METRICS_LATEST, time),
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context);
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecord(final TopicPartition topicPartition, final long offset) {
        return new ConsumerRecord<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            recordKey,
            recordValue
        );
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecord(final Integer key, final long offset) {
        return new ConsumerRecord<>(
            topic1,
            1,
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            new IntegerSerializer().serialize(topic1, key),
            recordValue
        );
    }

    private MetricName setupCloseTaskMetric() {
        final MetricName metricName = new MetricName("name", "group", "description", Collections.emptyMap());
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, "task-closed", Sensor.RecordingLevel.INFO);
        sensor.add(metricName, new CumulativeSum());
        return metricName;
    }

    private void verifyCloseTaskMetric(final double expected, final StreamsMetricsImpl streamsMetrics, final MetricName metricName) {
        final KafkaMetric metric = (KafkaMetric) streamsMetrics.metrics().get(metricName);
        final double totalCloses = metric.measurable().measure(metric.config(), System.currentTimeMillis());
        assertThat(totalCloses, equalTo(expected));
    }
}
