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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamTaskTest {

    private final Serializer<Integer> intSerializer = Serdes.Integer().serializer();
    private final Serializer<byte[]> bytesSerializer = Serdes.ByteArray().serializer();
    private final Deserializer<Integer> intDeserializer = Serdes.Integer().deserializer();
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final TopicPartition partition1 = new TopicPartition(topic1, 1);
    private final TopicPartition partition2 = new TopicPartition(topic2, 1);
    private final Set<TopicPartition> partitions = Utils.mkSet(partition1, partition2);

    private final MockSourceNode<Integer, Integer> source1 = new MockSourceNode<>(new String[]{topic1}, intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source2 = new MockSourceNode<>(new String[]{topic2}, intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source3 = new MockSourceNode<Integer, Integer>(new String[]{topic2}, intDeserializer, intDeserializer) {
        @Override
        public void process(final Integer key, final Integer value) {
            throw new RuntimeException("KABOOM!");
        }

        @Override
        public void close() {
            throw new RuntimeException("KABOOM!");
        }
    };
    private final MockProcessorNode<Integer, Integer> processorStreamTime = new MockProcessorNode<>(10L);
    private final MockProcessorNode<Integer, Integer> processorSystemTime = new MockProcessorNode<>(10L, PunctuationType.WALL_CLOCK_TIME);

    private final String storeName = "store";
    private final StateStore stateStore = new MockKeyValueStore(storeName, false);
    private final TopicPartition changelogPartition = new TopicPartition("store-changelog", 0);
    private final Long offset = 543L;

    private final ProcessorTopology topology = withSources(
        asList(source1, source2, processorStreamTime, processorSystemTime),
        mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
    );

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private MockProducer<byte[], byte[]> producer;
    private final MockConsumer<byte[], byte[]> restoreStateConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, Duration.ZERO, stateRestoreListener, new LogContext("stream-task-test ")) {
        @Override
        public Map<TopicPartition, Long> restoredOffsets() {
            return Collections.singletonMap(changelogPartition, offset);
        }
    };
    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);
    private final TaskId taskId00 = new TaskId(0, 0);
    private final MockTime time = new MockTime();
    private final File baseDir = TestUtils.tempDirectory();
    private StateDirectory stateDirectory;
    private StreamTask task;
    private long punctuatedAt;

    private final Punctuator punctuator = new Punctuator() {
        @Override
        public void punctuate(final long timestamp) {
            punctuatedAt = timestamp;
        }
    };

    static ProcessorTopology withRepartitionTopics(final List<ProcessorNode> processorNodes,
                                                   final Map<String, SourceNode> sourcesByTopic,
                                                   final Set<String> repartitionTopics) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     repartitionTopics);
    }

    static ProcessorTopology withSources(final List<ProcessorNode> processorNodes,
                                         final Map<String, SourceNode> sourcesByTopic) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     Collections.emptySet());
    }

    private StreamsConfig createConfig(final boolean enableEoS) {
        final String canonicalPath;
        try {
            canonicalPath = baseDir.getCanonicalPath();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "stream-task-test"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, canonicalPath),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE : StreamsConfig.AT_LEAST_ONCE),
            mkEntry(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "100")
        )));
    }

    @Before
    public void setup() {
        consumer.assign(asList(partition1, partition2));
        stateDirectory = new StateDirectory(createConfig(false), new MockTime(), true);
    }

    @After
    public void cleanup() throws IOException {
        try {
            if (task != null) {
                try {
                    task.close(true, false);
                } catch (final Exception e) {
                    // swallow
                }
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void shouldHandleInitTransactionsTimeoutExceptionOnCreation() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, (SourceNode) source1), mkEntry(topic2, (SourceNode) source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        try {
            new StreamTask(
                taskId00,
                partitions,
                topology,
                consumer,
                changelogReader,
                createConfig(true),
                streamsMetrics,
                stateDirectory,
                null,
                time,
                () -> producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer) {
                    @Override
                    public void initTransactions() {
                        throw new TimeoutException("test");
                    }
                },
                null,
                null
            );
            fail("Expected an exception");
        } catch (final StreamsException expected) {
            // make sure we log the explanation as an ERROR
            assertTimeoutErrorLog(appender);

            // make sure we report the correct message
            assertThat(expected.getMessage(), is("task [0_0] Failed to initialize task 0_0 due to timeout."));

            // make sure we preserve the cause
            assertEquals(expected.getCause().getClass(), TimeoutException.class);
            assertThat(expected.getCause().getMessage(), is("test"));
        }
        LogCaptureAppender.unregister(appender);
    }

    @Test
    public void shouldHandleInitTransactionsTimeoutExceptionOnResume() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, (SourceNode) source1), mkEntry(topic2, (SourceNode) source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        final AtomicBoolean timeOut = new AtomicBoolean(false);

        final StreamTask testTask = new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer) {
                @Override
                public void initTransactions() {
                    if (timeOut.get()) {
                        throw new TimeoutException("test");
                    } else {
                        super.initTransactions();
                    }
                }
            },
            null,
            null
        );
        testTask.initializeTopology();
        testTask.suspend();
        timeOut.set(true);
        try {
            testTask.resume();
            fail("Expected an exception");
        } catch (final StreamsException expected) {
            // make sure we log the explanation as an ERROR
            assertTimeoutErrorLog(appender);

            // make sure we report the correct message
            assertThat(expected.getMessage(), is("task [0_0] Failed to initialize task 0_0 due to timeout."));

            // make sure we preserve the cause
            assertEquals(expected.getCause().getClass(), TimeoutException.class);
            assertThat(expected.getCause().getMessage(), is("test"));
        }
        LogCaptureAppender.unregister(appender);
    }

    private void assertTimeoutErrorLog(final LogCaptureAppender appender) {

        final String expectedErrorLogMessage =
            "task [0_0] Timeout exception caught when initializing transactions for task 0_0. " +
                "This might happen if the broker is slow to respond, if the network " +
                "connection to the broker was interrupted, or if similar circumstances arise. " +
                "You can increase producer parameter `max.block.ms` to increase this timeout.";

        final List<String> expectedError =
            appender
                .getEvents()
                .stream()
                .filter(event -> event.getMessage().equals(expectedErrorLogMessage))
                .map(LogCaptureAppender.Event::getLevel)
                .collect(Collectors.toList());
        assertThat(expectedError, is(singletonList("ERROR")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessOrder() {
        task = createStatelessTask(createConfig(false));

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

        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
    }


    @Test
    public void testMetrics() {
        task = createStatelessTask(createConfig(false));

        assertNotNull(getMetric("%s-latency-avg", "The average latency of %s operation.", task.id().toString()));
        assertNotNull(getMetric("%s-latency-max", "The max latency of %s operation.", task.id().toString()));
        assertNotNull(getMetric("%s-rate", "The average number of occurrence of %s operation per second.", task.id().toString()));

        assertNotNull(getMetric("%s-latency-avg", "The average latency of %s operation.", "all"));
        assertNotNull(getMetric("%s-latency-max", "The max latency of %s operation.", "all"));
        assertNotNull(getMetric("%s-rate", "The average number of occurrence of %s operation per second.", "all"));

        final JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format("kafka.streams:type=stream-task-metrics,client-id=test,task-id=%s", task.id.toString())));
        assertTrue(reporter.containsMbean("kafka.streams:type=stream-task-metrics,client-id=test,task-id=all"));
    }

    private KafkaMetric getMetric(final String nameFormat, final String descriptionFormat, final String taskId) {
        return metrics.metrics().get(metrics.metricName(
            String.format(nameFormat, "commit"),
            "stream-task-metrics",
            String.format(descriptionFormat, "commit"),
            mkMap(mkEntry("task-id", taskId), mkEntry("client-id", "test"))
        ));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPauseResume() {
        task = createStatelessTask(createConfig(false));

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

        assertTrue(task.process());
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

        assertTrue(task.process());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(3, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertEquals(0, consumer.paused().size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPunctuateOnceStreamTimeAfterGap() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

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
        assertTrue(task.process());
        assertEquals(7, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 25
        assertTrue(task.process());
        assertEquals(6, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 142
        // punctuate at 142
        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 145
        // only one punctuation after 100ms gap
        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 155
        // punctuate at 155
        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 159
        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        // st: 160, aligned at 0
        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertTrue(task.maybePunctuateStreamTime());

        // st: 161
        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(4, source2.numReceived);
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 142L, 155L, 160L);
    }

    @Test
    public void shouldRespectPunctuateCancellationStreamTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

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
        assertTrue(task.process());

        assertTrue(task.maybePunctuateStreamTime());

        // st is now 25
        assertTrue(task.process());

        assertFalse(task.maybePunctuateStreamTime());

        // st is now 30
        assertTrue(task.process());

        processorStreamTime.mockProcessor.scheduleCancellable.cancel();

        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L);
    }

    @Test
    public void shouldRespectPunctuateCancellationSystemTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        final long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.scheduleCancellable.cancel();
        time.sleep(10);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
    }

    @Test
    public void shouldRespectCommitNeeded() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        assertFalse(task.commitNeeded());

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        assertTrue(task.process());
        assertTrue(task.commitNeeded());

        task.commit();
        assertFalse(task.commitNeeded());

        assertTrue(task.maybePunctuateStreamTime());
        assertTrue(task.commitNeeded());

        task.commit();
        assertFalse(task.commitNeeded());

        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        assertTrue(task.commitNeeded());

        task.commit();
        assertFalse(task.commitNeeded());
    }

    @Test
    public void shouldRespectCommitRequested() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        task.requestCommit();
        assertTrue(task.commitRequested());
    }

    @Test
    public void shouldBeProcessableIfAllPartitionsBuffered() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        assertFalse(task.isProcessable(0L));

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.isProcessable(0L));

        task.addRecords(partition2, Collections.singleton(new ConsumerRecord<>(topic2, 1, 0, bytes, bytes)));

        assertTrue(task.isProcessable(0L));
    }

    @Test
    public void shouldBeProcessableIfWaitedForTooLong() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        final MetricName enforcedProcessMetric = metrics.metricName("enforced-processing-total", "stream-task-metrics", mkMap(mkEntry("client-id", "test"), mkEntry("task-id", taskId00.toString())));

        assertFalse(task.isProcessable(0L));
        assertEquals(0.0, metrics.metric(enforcedProcessMetric).metricValue());

        final byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumerRecord<>(topic1, 1, 0, bytes, bytes)));

        assertFalse(task.isProcessable(time.milliseconds()));

        assertFalse(task.isProcessable(time.milliseconds() + 50L));

        assertTrue(task.isProcessable(time.milliseconds() + 100L));
        assertEquals(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // once decided to enforce, continue doing that
        assertTrue(task.isProcessable(time.milliseconds() + 101L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        task.addRecords(partition2, Collections.singleton(new ConsumerRecord<>(topic2, 1, 0, bytes, bytes)));

        assertTrue(task.isProcessable(time.milliseconds() + 130L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        // one resumed to normal processing, the timer should be reset
        task.process();

        assertFalse(task.isProcessable(time.milliseconds() + 150L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertFalse(task.isProcessable(time.milliseconds() + 249L));
        assertEquals(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        assertTrue(task.isProcessable(time.milliseconds() + 250L));
        assertEquals(3.0, metrics.metric(enforcedProcessMetric).metricValue());
    }


    @Test
    public void shouldPunctuateSystemTimeWhenIntervalElapsed() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
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
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME);
    }

    @Test
    public void shouldPunctuateOnceSystemTimeAfterGap() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
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
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContext() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();
        task.addRecords(partition2, singletonList(getConsumerRecord(partition2, 0)));

        try {
            task.process();
            fail("Should've thrown StreamsException");
        } catch (final Exception e) {
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, new Punctuator() {
                @Override
                public void punctuate(final long timestamp) {
                    throw new KafkaException("KABOOM!");
                }
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorStreamTime.name() + "'"));
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingWallClockTimeTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                @Override
                public void punctuate(final long timestamp) {
                    throw new KafkaException("KABOOM!");
                }
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorSystemTime.name() + "'"));
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldFlushRecordCollectorOnFlushState() {
        final AtomicBoolean flushed = new AtomicBoolean(false);
        final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(new Metrics());
        final StreamTask streamTask = new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(false),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            new NoOpRecordCollector() {
                @Override
                public void flush() {
                    flushed.set(true);
                }
            },
            metrics.sensor("dummy"));
        streamTask.flushState();
        assertTrue(flushed.get());
    }

    @Test
    public void shouldCheckpointOffsetsOnCommit() throws IOException {
        task = createStatefulTask(createConfig(false), true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(
            new File(stateDirectory.directoryForTask(taskId00), ProcessorStateManager.CHECKPOINT_FILE_NAME)
        );

        assertThat(checkpoint.read(), equalTo(Collections.singletonMap(changelogPartition, offset)));
    }

    @Test
    public void shouldNotCheckpointOffsetsOnCommitIfEosIsEnabled() {
        task = createStatefulTask(createConfig(true), true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        final File checkpointFile = new File(
            stateDirectory.directoryForTask(taskId00),
            ProcessorStateManager.CHECKPOINT_FILE_NAME
        );

        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        task.processorContext.setCurrentNode(processorStreamTime);
        try {
            task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
            fail("Should throw illegal state exception as current node is not null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldCallPunctuateOnPassedInProcessorNode() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(5L));
        task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(10L));
    }

    @Test
    public void shouldSetProcessorNodeOnContextBackToNullAfterSuccessfulPunctuate() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(((ProcessorContextImpl) task.context()).currentNode(), nullValue());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() {
        task = createStatelessTask(createConfig(false));
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            @Override
            public void punctuate(final long timestamp) {
                // no-op
            }
        });
    }

    @Test
    public void shouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull() {
        task = createStatelessTask(createConfig(false));
        task.processorContext.setCurrentNode(processorStreamTime);
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            @Override
            public void punctuate(final long timestamp) {
                // no-op
            }
        });
    }

    @Test
    public void shouldNotCloseProducerOnCleanCloseWithEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.close(true, false);
        task = null;

        assertFalse(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerOnUncleanCloseWithEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.close(false, false);
        task = null;

        assertFalse(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerOnErrorDuringCleanCloseWithEosDisabled() {
        task = createTaskThatThrowsException(false);

        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException expected) {
            task = null;
        }

        assertFalse(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerOnErrorDuringUncleanCloseWithEosDisabled() {
        task = createTaskThatThrowsException(false);

        task.close(false, false);
        task = null;

        assertFalse(producer.closed());
    }

    @Test
    public void shouldCommitTransactionAndCloseProducerOnCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.close(true, false);
        task = null;

        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldNotAbortTransactionAndNotCloseProducerOnErrorDuringCleanCloseWithEosEnabled() {
        task = createTaskThatThrowsException(true);
        task.initializeTopology();

        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException expected) {
            task = null;
        }

        assertTrue(producer.transactionInFlight());
        assertFalse(producer.closed());
    }

    @Test
    public void shouldOnlyCloseProducerIfFencedOnCommitDuringCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducer();

        try {
            task.close(true, false);
            fail("should have thrown TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            task = null;
            assertTrue(expected.getCause() instanceof ProducerFencedException);
        }

        assertFalse(producer.transactionCommitted());
        assertTrue(producer.transactionInFlight());
        assertFalse(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldNotCloseProducerIfFencedOnCloseDuringCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducerOnClose();

        try {
            task.close(true, false);
            fail("should have thrown TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            task = null;
            assertTrue(expected.getCause() instanceof ProducerFencedException);
        }

        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
        assertFalse(producer.closed());
    }

    @Test
    public void shouldAbortTransactionAndCloseProducerOnUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.close(false, false);
        task = null;

        assertTrue(producer.transactionAborted());
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldAbortTransactionAndCloseProducerOnErrorDuringUncleanCloseWithEosEnabled() {
        task = createTaskThatThrowsException(true);
        task.initializeTopology();

        task.close(false, false);

        assertTrue(producer.transactionAborted());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldOnlyCloseProducerIfFencedOnAbortDuringUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducer();

        task.close(false, false);
        task = null;

        assertTrue(producer.transactionInFlight());
        assertFalse(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
        assertTrue(producer.closed());
    }

    @Test
    public void shouldAbortTransactionButNotCloseProducerIfFencedOnCloseDuringUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducerOnClose();

        task.close(false, false);
        task = null;

        assertTrue(producer.transactionAborted());
        assertFalse(producer.closed());
    }

    @Test
    public void shouldThrowExceptionIfAnyExceptionsRaisedDuringCloseButStillCloseAllProcessorNodesTopology() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException expected) {
            task = null;
        }
        assertTrue(processorSystemTime.closed);
        assertTrue(processorStreamTime.closed);
        assertTrue(source1.closed);
    }

    @Test
    public void shouldInitAndBeginTransactionOnCreateIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        assertTrue(producer.transactionInitialized());
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldWrapProducerFencedExceptionWithTaskMigratedExceptionForBeginTransaction() {
        task = createStatelessTask(createConfig(true));
        producer.fenceProducer();

        try {
            task.initializeTopology();
            fail("Should have throws TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertTrue(expected.getCause() instanceof ProducerFencedException);
        }
    }

    @Test
    public void shouldNotThrowOnCloseIfTaskWasNotInitializedWithEosEnabled() {
        task = createStatelessTask(createConfig(true));

        assertTrue(!producer.transactionInFlight());
        task.close(false, false);
    }

    @Test
    public void shouldNotInitOrBeginTransactionOnCreateIfEosDisabled() {
        task = createStatelessTask(createConfig(false));

        assertFalse(producer.transactionInitialized());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldSendOffsetsAndCommitTransactionButNotStartNewTransactionOnSuspendIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.suspend();
        assertTrue(producer.sentOffsets());
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldCommitTransactionOnSuspendEvenIfTransactionIsEmptyIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        task.suspend();

        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldNotSendOffsetsAndCommitTransactionNorStartNewTransactionOnSuspendIfEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        assertFalse(producer.sentOffsets());
        assertFalse(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldWrapProducerFencedExceptionWithTaskMigragedExceptionInSuspendWhenCommitting() {
        task = createStatelessTask(createConfig(true));
        producer.fenceProducer();

        try {
            task.suspend();
            fail("Should have throws TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertTrue(expected.getCause() instanceof ProducerFencedException);
        }
        task = null;

        assertFalse(producer.transactionCommitted());
    }

    @Test
    public void shouldWrapProducerFencedExceptionWithTaskMigragedExceptionInSuspendWhenClosingProducer() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        producer.fenceProducerOnClose();
        try {
            task.suspend();
            fail("Should have throws TaskMigratedException");
        } catch (final TaskMigratedException expected) {
            assertTrue(expected.getCause() instanceof ProducerFencedException);
        }

        assertTrue(producer.transactionCommitted());
    }

    @Test
    public void shouldStartNewTransactionOnResumeIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        task.resume();
        task.initializeTopology();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnResumeIfEosDisabled() {
        task = createStatelessTask(createConfig(false));

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        task.resume();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldStartNewTransactionOnCommitIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.commit();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnCommitIfEosDisabled() {
        task = createStatelessTask(createConfig(false));

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.commit();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldNotAbortTransactionOnZombieClosedIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.close(false, true);
        task = null;

        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldNotAbortTransactionOnDirtyClosedIfEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.close(false, false);
        task = null;

        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldCloseProducerOnCloseWhenEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        task.close(true, false);
        task = null;

        assertTrue(producer.closed());
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushing() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.commit();
            fail("should have thrown an exception");
        } catch (final Exception e) {
            // all good
        }
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringTaskSuspension() {
        final StreamTask task = createTaskThatThrowsException(false);

        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.suspend();
            fail("should have thrown an exception");
        } catch (final Exception e) {
            // all good
        }
    }

    @Test
    public void shouldCloseStateManagerIfFailureOnTaskClose() {
        task = createStatefulTaskThatThrowsExceptionOnClose();
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.close(true, false);
            fail("should have thrown an exception");
        } catch (final Exception e) {
            // all good
        }

        task = null;
        assertFalse(stateStore.isOpen());
    }

    @Test
    public void shouldNotCloseTopologyProcessorNodesIfNotInitialized() {
        final StreamTask task = createTaskThatThrowsException(false);
        try {
            task.close(false, false);
        } catch (final Exception e) {
            fail("should have not closed non-initialized topology");
        }
    }

    @Test
    public void shouldBeInitializedIfChangelogPartitionsIsEmpty() {
        final StreamTask task = createStatefulTask(createConfig(false), false);

        assertTrue(task.initializeStateStores());
    }

    @Test
    public void shouldNotBeInitializedIfChangelogPartitionsIsNonEmpty() {
        final StreamTask task = createStatefulTask(createConfig(false), true);

        assertFalse(task.initializeStateStores());
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

        task = new StreamTask(
            taskId00,
            Utils.mkSet(partition1, repartition),
            topology,
            consumer,
            changelogReader,
            createConfig(false),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            metrics.sensor("dummy"));
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 5L)));
        task.addRecords(repartition, singletonList(getConsumerRecord(repartition, 10L)));

        assertTrue(task.process());
        assertTrue(task.process());

        task.commit();

        final Map<TopicPartition, Long> map = task.purgableOffsets();

        assertThat(map, equalTo(Collections.singletonMap(repartition, 11L)));
    }

    @Test
    public void shouldThrowOnCleanCloseTaskWhenEosEnabledIfTransactionInFlight() {
        task = createStatelessTask(createConfig(true));
        try {
            task.close(true, false);
            fail("should have throw IllegalStateException");
        } catch (final IllegalStateException expected) {
            // pass
        }
        task = null;

        assertTrue(producer.closed());
    }

    @Test
    public void shouldAlwaysCommitIfEosEnabled() {
        task = createStatelessTask(createConfig(true));

        final RecordCollectorImpl recordCollector =  new RecordCollectorImpl("StreamTask",
                new LogContext("StreamTaskTest "), new DefaultProductionExceptionHandler(), new Metrics().sensor("skipped-records"));
        recordCollector.init(producer);

        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorSystemTime, 5, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(final long timestamp) {
                recordCollector.send("result-topic1", 3, 5, null, 0, time.milliseconds(),
                        new IntegerSerializer(),  new IntegerSerializer());
            }
        });
        task.commit();
        assertEquals(1, producer.history().size());
    }

    private StreamTask createStatefulTask(final StreamsConfig config, final boolean logged) {
        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
            singletonList(stateStore),
            logged ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.emptyMap());

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            config,
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            metrics.sensor("dummy"));
    }

    private StreamTask createStatefulTaskThatThrowsExceptionOnClose() {
        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source3),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3)),
            singletonList(stateStore),
            Collections.emptyMap());

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            metrics.sensor("dummy"));
    }

    private StreamTask createStatelessTask(final StreamsConfig streamsConfig) {
        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            streamsConfig,
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            metrics.sensor("dummy"));
    }

    // this task will throw exception when processing (on partition2), flushing, suspending and closing
    private StreamTask createTaskThatThrowsException(final boolean enableEos) {
        final ProcessorTopology topology = withSources(
            asList(source1, source3, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3))
        );

        source1.addChild(processorStreamTime);
        source3.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source3.addChild(processorSystemTime);

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(enableEos),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            metrics.sensor("dummy")) {
            @Override
            protected void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };
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
}
