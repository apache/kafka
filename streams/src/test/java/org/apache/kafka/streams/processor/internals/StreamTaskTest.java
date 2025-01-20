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
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.errors.internals.FailedProcessingException;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.DEBUG;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.processor.internals.Task.State.CREATED;
import static org.apache.kafka.streams.processor.internals.Task.State.RESTORING;
import static org.apache.kafka.streams.processor.internals.Task.State.RUNNING;
import static org.apache.kafka.streams.processor.internals.Task.State.SUSPENDED;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByNameFilterByTags;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StreamTaskTest {

    private static final String APPLICATION_ID = "stream-task-test";
    private static final File BASE_DIR = TestUtils.tempDirectory();

    private final LogContext logContext = new LogContext("[test] ");
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final TopicPartition partition1 = new TopicPartition(topic1, 0);
    private final TopicPartition partition2 = new TopicPartition(topic2, 0);
    private final Set<TopicPartition> partitions = new HashSet<>(List.of(partition1, partition2));
    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();

    private final MockSourceNode<Integer, Integer> source1 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source2 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source3 = new MockSourceNode<>(intDeserializer, intDeserializer) {
        @Override
        public void process(final Record<Integer, Integer> record) {
            throw new RuntimeException("KABOOM!");
        }

        @Override
        public void close() {
            throw new RuntimeException("KABOOM!");
        }
    };
    private final MockSourceNode<Integer, Integer> timeoutSource = new MockSourceNode<>(intDeserializer, intDeserializer) {
        @Override
        public void process(final Record<Integer, Integer> record) {
            throw new TimeoutException("Kaboom!");
        }
    };
    private final MockProcessorNode<Integer, Integer, ?, ?> processorStreamTime = new MockProcessorNode<>(10L);
    private final MockProcessorNode<Integer, Integer, ?, ?> processorSystemTime = new MockProcessorNode<>(10L, PunctuationType.WALL_CLOCK_TIME);

    private final String storeName = "store";
    private final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, false);
    private final TopicPartition changelogPartition = new TopicPartition("store-changelog", 1);

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
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

    @Mock
    private ProcessorStateManager stateManager;
    @Mock
    private RecordCollector recordCollector;
    @Mock
    private ThreadCache cache;

    private final Punctuator punctuator = new Punctuator() {
        @Override
        public void punctuate(final long timestamp) {
            punctuatedAt = timestamp;
        }
    };

    private static ProcessorTopology withRepartitionTopics(final List<ProcessorNode<?, ?, ?, ?>> processorNodes,
                                                           final Map<String, SourceNode<?, ?>> sourcesByTopic,
                                                           final Set<String> repartitionTopics) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     emptyMap(),
                                     emptyList(),
                                     emptyList(),
                                     emptyMap(),
                                     repartitionTopics,
                                     emptyMap());
    }

    private static ProcessorTopology withSources(final List<ProcessorNode<?, ?, ?, ?>> processorNodes,
                                                 final Map<String, SourceNode<?, ?>> sourcesByTopic) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     emptyMap(),
                                     emptyList(),
                                     emptyList(),
                                     emptyMap(),
                                     Collections.emptySet(),
                                     emptyMap());
    }

    private static StreamsConfig createConfig() {
        return createConfig("0");
    }

    private static StreamsConfig createConfig(final String enforcedProcessingValue) {
        return createConfig(AT_LEAST_ONCE, enforcedProcessingValue);
    }

    private static StreamsConfig createConfig(final String eosConfig, final String enforcedProcessingValue) {
        return createConfig(
            eosConfig,
            enforcedProcessingValue,
            LogAndFailExceptionHandler.class,
            LogAndFailProcessingExceptionHandler.class,
            FailOnInvalidTimestamp.class
        );
    }

    private static StreamsConfig createConfig(final Class<? extends DeserializationExceptionHandler> deserializationExceptionHandler) {
        return createConfig(
            AT_LEAST_ONCE,
            "0", // max.task.idle.ms
            deserializationExceptionHandler,
            LogAndFailProcessingExceptionHandler.class,
            FailOnInvalidTimestamp.class
        );
    }

    private static StreamsConfig createConfigWithTsExtractor(final Class<? extends TimestampExtractor> timestampExtractor) {
        return createConfig(
            AT_LEAST_ONCE,
            "0", // max.task.idle.ms
            LogAndFailExceptionHandler.class,
            LogAndFailProcessingExceptionHandler.class,
            timestampExtractor
        );
    }

    private static StreamsConfig createConfig(
        final String enforcedProcessingValue,
        final Class<? extends ProcessingExceptionHandler> processingExceptionHandler
    ) {
        return createConfig(
            AT_LEAST_ONCE,
            enforcedProcessingValue,
            LogAndFailExceptionHandler.class,
            processingExceptionHandler,
            FailOnInvalidTimestamp.class
        );
    }

    private static StreamsConfig createConfig(
        final String eosConfig,
        final String enforcedProcessingValue,
        final Class<? extends DeserializationExceptionHandler> deserializationExceptionHandler,
        final Class<? extends ProcessingExceptionHandler> processingExceptionHandler,
        final Class<? extends TimestampExtractor> timestampExtractor) {
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
            mkEntry(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, DEBUG.name),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig),
            mkEntry(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, enforcedProcessingValue),
            mkEntry(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, deserializationExceptionHandler.getName()),
            mkEntry(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, processingExceptionHandler.getName()),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, timestampExtractor.getName())
        )));
    }

    @BeforeEach
    public void setup() {
        consumer.assign(asList(partition1, partition2));
        consumer.updateBeginningOffsets(mkMap(mkEntry(partition1, 0L), mkEntry(partition2, 0L)));
        stateDirectory = new StateDirectory(createConfig("100"), new MockTime(), true, false);
        // Unless we initialise a lock on the state directory we cannot unlock it successfully during teardown
        stateDirectory.initializeProcessId();
    }

    @AfterEach
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
        stateDirectory.close();
        Utils.delete(BASE_DIR);
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectory() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);
        when(stateDirectory.lock(taskId)).thenReturn(false);

        task = createStatefulTask(createConfig("100"), false);

        assertThrows(LockException.class, () -> task.initializeIfNeeded());
    }

    @Test
    public void shouldNotAttemptToLockIfNoStores() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);

        task = createStatelessTask(createConfig("100"));

        task.initializeIfNeeded();

        // should fail if lock is called
        verify(stateDirectory, never()).lock(any());
    }

    @Test
    public void shouldAttemptToDeleteStateDirectoryWhenCloseDirtyAndEosEnabled() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);

        when(stateDirectory.lock(taskId)).thenReturn(true);

        // The `baseDir` will be accessed when attempting to delete the state store.
        when(stateManager.baseDir()).thenReturn(TestUtils.tempDirectory("state_store"));

        final InOrder inOrder = inOrder(stateManager, stateDirectory);

        task = createStatefulTask(createConfig(StreamsConfig.EXACTLY_ONCE_V2, "100"), true, stateManager);
        task.suspend();
        task.closeDirty();
        task = null;

        inOrder.verify(stateManager).taskType();
        inOrder.verify(stateManager).registerGlobalStateStores(emptyList());
        inOrder.verify(stateManager).taskId();
        inOrder.verify(stateDirectory).lock(taskId);
        inOrder.verify(stateManager).close();
        inOrder.verify(stateManager).baseDir();
        inOrder.verify(stateDirectory).unlock(taskId);
    }

    @Test
    public void shouldResetOffsetsToLastCommittedForSpecifiedPartitions() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.addPartitionsForOffsetReset(Collections.singleton(partition1));

        consumer.seek(partition1, 5L);
        consumer.commitSync();

        consumer.seek(partition1, 10L);
        consumer.seek(partition2, 15L);

        @SuppressWarnings("unchecked")
        final java.util.function.Consumer<Set<TopicPartition>> resetter =
            mock(java.util.function.Consumer.class);

        task.initializeIfNeeded();
        task.completeRestoration(resetter);

        assertThat(consumer.position(partition1), equalTo(5L));
        assertThat(consumer.position(partition2), equalTo(15L));
        verify(resetter).accept(Collections.emptySet());
    }

    @Test
    public void shouldAutoOffsetResetIfNoCommittedOffsetFound() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.addPartitionsForOffsetReset(Collections.singleton(partition1));

        final AtomicReference<AssertionError> shouldNotSeek = new AtomicReference<>();
        try (final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
                @Override
                public void seek(final TopicPartition partition, final long offset) {
                    final AssertionError error = shouldNotSeek.get();
                    if (error != null) {
                        throw error;
                    }
                    super.seek(partition, offset);
                }
            }) {

            consumer.assign(asList(partition1, partition2));
            consumer.updateBeginningOffsets(mkMap(mkEntry(partition1, 0L), mkEntry(partition2, 0L)));

            consumer.seek(partition1, 5L);
            consumer.seek(partition2, 15L);

            shouldNotSeek.set(new AssertionError("Should not seek"));

            // We need to keep a separate reference to the arguments of Consumer#accept
            // because the underlying data-structure is emptied and on verification time
            // it is reported as empty.
            final Set<TopicPartition> partitionsAtCall = new HashSet<>();

            task.initializeIfNeeded();
            task.completeRestoration(partitionsAtCall::addAll);

            // because we mocked the `resetter` positions don't change
            assertThat(consumer.position(partition1), equalTo(5L));
            assertThat(consumer.position(partition2), equalTo(15L));
            assertThat(partitionsAtCall, equalTo(Collections.singleton(partition1)));
        }
    }

    @Test
    public void shouldReadCommittedStreamTimeAndProcessorMetadataOnInitialize() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);

        final ProcessorMetadata processorMetadata = new ProcessorMetadata(mkMap(
            mkEntry("key1", 1L),
            mkEntry("key2", 2L)
        ));

        consumer.commitSync(partitions.stream()
            .collect(Collectors.toMap(Function.identity(), tp -> new OffsetAndMetadata(0L, new TopicPartitionMetadata(10L, processorMetadata).encode()))));

        task = createStatelessTask(createConfig("100"));

        assertEquals(RecordQueue.UNKNOWN, task.streamTime());

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        assertEquals(10L, task.streamTime());
        assertEquals(1L, task.processorContext().processorMetadataForKey("key1").longValue());
        assertEquals(2L, task.processorContext().processorMetadataForKey("key2").longValue());
    }

    @Test
    public void shouldReadCommittedStreamTimeAndMergeProcessorMetadataOnInitialize() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);

        final ProcessorMetadata processorMetadata1 = new ProcessorMetadata(mkMap(
            mkEntry("key1", 1L),
            mkEntry("key2", 2L)
        ));

        final Map<TopicPartition, OffsetAndMetadata> meta1 = mkMap(
            mkEntry(partition1, new OffsetAndMetadata(0L, new TopicPartitionMetadata(10L, processorMetadata1).encode())
            )
        );

        final ProcessorMetadata processorMetadata2 = new ProcessorMetadata(mkMap(
            mkEntry("key1", 10L),
            mkEntry("key3", 30L)
        ));

        final Map<TopicPartition, OffsetAndMetadata> meta2 = mkMap(
            mkEntry(partition2, new OffsetAndMetadata(0L, new TopicPartitionMetadata(20L, processorMetadata2).encode())
            )
        );

        consumer.commitSync(meta1);
        consumer.commitSync(meta2);

        task = createStatelessTask(createConfig("100"));

        assertEquals(RecordQueue.UNKNOWN, task.streamTime());

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        assertEquals(20L, task.streamTime());
        assertEquals(10L, task.processorContext().processorMetadataForKey("key1").longValue());
        assertEquals(2L, task.processorContext().processorMetadataForKey("key2").longValue());
        assertEquals(30L, task.processorContext().processorMetadataForKey("key3").longValue());
    }

    @Test
    public void shouldTransitToRestoringThenRunningAfterCreation() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);
        when(stateDirectory.lock(taskId)).thenReturn(true);
        when(stateManager.changelogOffsets()).thenReturn(singletonMap(changelogPartition, 10L));

        task = createStatefulTask(createConfig("100"), true);

        assertEquals(CREATED, task.state());

        task.initializeIfNeeded();

        assertEquals(RESTORING, task.state());
        assertFalse(source1.initialized);
        assertFalse(source2.initialized);

        // initialize should be idempotent
        task.initializeIfNeeded();

        assertEquals(RESTORING, task.state());

        task.completeRestoration(noOpResetter -> { });

        assertEquals(RUNNING, task.state());
        assertTrue(source1.initialized);
        assertTrue(source2.initialized);
    }

    @Test
    public void shouldProcessInOrder() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.resumePollingForPartitionsWithAvailableSpace();

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 10, 101),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20, 102),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 30, 103)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition2, 25, 201),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 35, 202),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 45, 203)
        ));

        task.updateLags();

        assertTrue(task.process(0L));
        assertEquals(5, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);
        assertEquals(singletonList(101), source1.values);
        assertEquals(emptyList(), source2.values);

        assertTrue(task.process(0L));
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);
        assertEquals(asList(101, 102), source1.values);
        assertEquals(emptyList(), source2.values);

        assertTrue(task.process(0L));
        assertEquals(3, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertEquals(asList(101, 102), source1.values);
        assertEquals(singletonList(201), source2.values);

        assertTrue(task.process(0L));
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertEquals(asList(101, 102, 103), source1.values);
        assertEquals(singletonList(201), source2.values);

        assertTrue(task.process(0L));
        assertEquals(1, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertEquals(asList(101, 102, 103), source1.values);
        assertEquals(asList(201, 202), source2.values);

        assertTrue(task.process(0L));
        assertEquals(0, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertEquals(asList(101, 102, 103), source1.values);
        assertEquals(asList(201, 202, 203), source2.values);
    }

    @Test
    public void shouldProcessRecordsAfterPrepareCommitWhenEosDisabled() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig());

        assertFalse(task.process(time.milliseconds()));

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 10),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 30)
        ));

        assertTrue(task.process(time.milliseconds()));
        task.prepareCommit();
        assertTrue(task.process(time.milliseconds()));
        task.postCommit(false);
        assertTrue(task.process(time.milliseconds()));

        assertFalse(task.process(time.milliseconds()));
    }

    @Test
    public void shouldNotProcessRecordsAfterPrepareCommitWhenEosV2Enabled() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig(StreamsConfig.EXACTLY_ONCE_V2, "0"));

        assertFalse(task.process(time.milliseconds()));

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 10),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 30)
        ));

        assertTrue(task.process(time.milliseconds()));
        task.prepareCommit();
        assertFalse(task.process(time.milliseconds()));
        task.postCommit(false);
        assertTrue(task.process(time.milliseconds()));
        assertTrue(task.process(time.milliseconds()));

        assertFalse(task.process(time.milliseconds()));
    }

    @Test
    public void shouldRecordBufferedRecords() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig(AT_LEAST_ONCE, "0"));

        final KafkaMetric metric = getMetric("active-buffer", "%s-count", task.id().toString());

        assertThat(metric.metricValue(), equalTo(0.0));

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 10),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20)
        ));
        task.recordProcessTimeRatioAndBufferSize(100L, time.milliseconds());

        assertThat(metric.metricValue(), equalTo(2.0));

        assertTrue(task.process(0L));
        task.recordProcessTimeRatioAndBufferSize(100L, time.milliseconds());

        assertThat(metric.metricValue(), equalTo(1.0));
    }

    @Test
    public void shouldRecordProcessRatio() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());

        final KafkaMetric metric = getMetric("active-process", "%s-ratio", task.id().toString());

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
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        time = new MockTime(0L, 0L, 0L);
        metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.INFO), time);

        // Create a processor that only forwards even keys to test the metrics at the source and terminal nodes
        final MockSourceNode<Integer, Integer> evenKeyForwardingSourceNode = new MockSourceNode<Integer, Integer>(intDeserializer, intDeserializer) {
            InternalProcessorContext<Integer, Integer> context;

            @Override
            public void init(final InternalProcessorContext<Integer, Integer> context) {
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
        task.completeRestoration(noOpResetter -> { });

        final String sourceNodeName = evenKeyForwardingSourceNode.name();
        final String terminalNodeName = processorStreamTime.name();

        final Metric sourceAvg = getProcessorMetric("record-e2e-latency", "%s-avg", task.id().toString(), sourceNodeName);
        final Metric sourceMin = getProcessorMetric("record-e2e-latency", "%s-min", task.id().toString(), sourceNodeName);
        final Metric sourceMax = getProcessorMetric("record-e2e-latency", "%s-max", task.id().toString(), sourceNodeName);

        final Metric terminalAvg = getProcessorMetric("record-e2e-latency", "%s-avg", task.id().toString(), terminalNodeName);
        final Metric terminalMin = getProcessorMetric("record-e2e-latency", "%s-min", task.id().toString(), terminalNodeName);
        final Metric terminalMax = getProcessorMetric("record-e2e-latency", "%s-max", task.id().toString(), terminalNodeName);

        // e2e latency = 10
        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(0, 0L)));
        task.process(10L);

        assertThat(sourceAvg.metricValue(), equalTo(10.0));
        assertThat(sourceMin.metricValue(), equalTo(10.0));
        assertThat(sourceMax.metricValue(), equalTo(10.0));

        // key 0: reaches terminal node
        assertThat(terminalAvg.metricValue(), equalTo(10.0));
        assertThat(terminalMin.metricValue(), equalTo(10.0));
        assertThat(terminalMax.metricValue(), equalTo(10.0));


        // e2e latency = 15
        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(1, 0L)));
        task.process(15L);

        assertThat(sourceAvg.metricValue(), equalTo(12.5));
        assertThat(sourceMin.metricValue(), equalTo(10.0));
        assertThat(sourceMax.metricValue(), equalTo(15.0));

        // key 1: stops at source, doesn't affect terminal node metrics
        assertThat(terminalAvg.metricValue(), equalTo(10.0));
        assertThat(terminalMin.metricValue(), equalTo(10.0));
        assertThat(terminalMax.metricValue(), equalTo(10.0));


        // e2e latency = 23
        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(2, 0L)));
        task.process(23L);

        assertThat(sourceAvg.metricValue(), equalTo(16.0));
        assertThat(sourceMin.metricValue(), equalTo(10.0));
        assertThat(sourceMax.metricValue(), equalTo(23.0));

        // key 2: reaches terminal node
        assertThat(terminalAvg.metricValue(), equalTo(16.5));
        assertThat(terminalMin.metricValue(), equalTo(10.0));
        assertThat(terminalMax.metricValue(), equalTo(23.0));


        // e2e latency = 5
        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(3, 0L)));
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
    public void shouldRecordRestoredRecords() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig(AT_LEAST_ONCE, "0"));

        final KafkaMetric totalMetric = getMetric("restore", "%s-total", task.id().toString());
        final KafkaMetric rateMetric = getMetric("restore", "%s-rate", task.id().toString());
        final KafkaMetric remainMetric = getMetric("restore", "%s-remaining-records-total", task.id().toString());

        assertThat(totalMetric.metricValue(), equalTo(0.0));
        assertThat(rateMetric.metricValue(), equalTo(0.0));
        assertThat(remainMetric.metricValue(), equalTo(0.0));

        task.recordRestoration(time, 100L, true);

        assertThat(remainMetric.metricValue(), equalTo(100.0));

        task.recordRestoration(time, 25L, false);

        assertThat(totalMetric.metricValue(), equalTo(25.0));
        assertThat(rateMetric.metricValue(), not(0.0));
        assertThat(remainMetric.metricValue(), equalTo(75.0));

        task.recordRestoration(time, 50L, false);

        assertThat(totalMetric.metricValue(), equalTo(75.0));
        assertThat(rateMetric.metricValue(), not(0.0));
        assertThat(remainMetric.metricValue(), equalTo(25.0));
    }

    @Test
    public void shouldThrowOnTimeoutExceptionAndBufferRecordForRetryIfEosDisabled() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        createTimeoutTask(AT_LEAST_ONCE);

        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(0, 0L)));

        final TimeoutException exception = assertThrows(
            TimeoutException.class,
            () -> task.process(0)
        );
        assertThat(exception.getMessage(), equalTo("Kaboom!"));

        // we have only a single record that was not successfully processed
        // however, the record should not be in the record buffer any longer, but should be cached within the task itself
        assertThat(task.commitNeeded(), equalTo(false));
        assertThat(task.hasRecordsQueued(), equalTo(false));

        // -> thus the task should try process the cached record now (that thus throw again)
        final TimeoutException nextException = assertThrows(
            TimeoutException.class,
            () -> task.process(0)
        );
        assertThat(nextException.getMessage(), equalTo("Kaboom!"));
    }

    @Test
    public void shouldThrowTaskCorruptedExceptionOnTimeoutExceptionIfEosEnabled() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        createTimeoutTask(StreamsConfig.EXACTLY_ONCE_V2);

        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(0, 0L)));

        assertThrows(
            TaskCorruptedException.class,
            () -> task.process(0)
        );
    }

    @Test
    public void testMetrics() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));

        assertNotNull(getMetric(
            "enforced-processing",
            "%s-rate",
            task.id().toString()
        ));
        assertNotNull(getMetric(
            "enforced-processing",
            "%s-total",
            task.id().toString()
        ));

        assertNotNull(getMetric(
            "record-lateness",
            "%s-avg",
            task.id().toString()
        ));
        assertNotNull(getMetric(
            "record-lateness",
            "%s-max",
            task.id().toString()
        ));

        assertNotNull(getMetric(
            "active-process",
            "%s-ratio",
            task.id().toString()
        ));

        assertNotNull(getMetric(
            "active-buffer",
            "%s-count",
            task.id().toString()
        ));

        testMetricsForBuiltInMetricsVersionLatest();

        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format(
            "kafka.streams:type=stream-task-metrics,%s=%s,task-id=%s",
            THREAD_ID_TAG,
            threadId,
            task.id()
        )));
    }

    private void testMetricsForBuiltInMetricsVersionLatest() {
        assertNull(getMetric("commit", "%s-latency-avg", "all"));
        assertNull(getMetric("commit", "%s-latency-max", "all"));
        assertNull(getMetric("commit", "%s-rate", "all"));
        assertNull(getMetric("commit", "%s-total", "all"));

        assertNotNull(getMetric("process", "%s-latency-max", task.id().toString()));
        assertNotNull(getMetric("process", "%s-latency-avg", task.id().toString()));

        assertNotNull(getMetric("punctuate", "%s-latency-avg", task.id().toString()));
        assertNotNull(getMetric("punctuate", "%s-latency-max", task.id().toString()));
        assertNotNull(getMetric("punctuate", "%s-rate", task.id().toString()));
        assertNotNull(getMetric("punctuate", "%s-total", task.id().toString()));
    }

    private KafkaMetric getMetric(final String operation,
                                  final String nameFormat,
                                  final String taskId) {
        final String descriptionIsNotVerified = "";
        return metrics.metrics().get(metrics.metricName(
            String.format(nameFormat, operation),
            "stream-task-metrics",
            descriptionIsNotVerified,
            mkMap(
                mkEntry("task-id", taskId),
                mkEntry(THREAD_ID_TAG, Thread.currentThread().getName())
            )
        ));
    }

    private Metric getProcessorMetric(final String operation,
                                      final String nameFormat,
                                      final String taskId,
                                      final String processorNodeId) {

        return getMetricByNameFilterByTags(
            metrics.metrics(),
            String.format(nameFormat, operation),
            "stream-processor-node-metrics",
            mkMap(
                mkEntry("task-id", taskId),
                mkEntry("processor-node-id", processorNodeId),
                mkEntry(THREAD_ID_TAG, Thread.currentThread().getName()
                )
            )
        );
    }

    @Test
    public void shouldPauseAndResumeBasedOnBufferedRecords() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 10),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition2, 35),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 45),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 55),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 65)
        ));

        assertTrue(task.process(0L));
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 30),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 40),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 50)
        ));

        assertEquals(2, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition1));
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process(0L));
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(2, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition1));
        assertTrue(consumer.paused().contains(partition2));

        task.resumePollingForPartitionsWithAvailableSpace();

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

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        task.resumePollingForPartitionsWithAvailableSpace();

        assertEquals(0, consumer.paused().size());
    }

    @Test
    public void shouldResumePartitionWhenSkippingOverRecordsWithInvalidTs() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            StreamsConfig.AT_LEAST_ONCE,
            "-1",
            LogAndContinueExceptionHandler.class,
            LogAndFailProcessingExceptionHandler.class,
            LogAndSkipOnInvalidTimestamp.class
        ));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 10),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20),
            getConsumerRecordWithInvalidTimestamp(30),
            getConsumerRecordWithInvalidTimestamp(40),
            getConsumerRecordWithInvalidTimestamp(50)
        ));
        assertTrue(consumer.paused().contains(partition1));

        assertTrue(task.process(0L));

        task.resumePollingForPartitionsWithAvailableSpace();
        assertTrue(consumer.paused().contains(partition1));

        assertTrue(task.process(0L));

        task.resumePollingForPartitionsWithAvailableSpace();
        assertEquals(0, consumer.paused().size());

        assertTrue(task.process(0L)); // drain head record (ie, last invalid record)
        assertFalse(task.process(0L));
        assertFalse(task.hasRecordsQueued());


        // repeat test for deserialization error
        task.resumePollingForPartitionsWithAvailableSpace();
        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 110),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 120),
            getCorruptedConsumerRecordWithOffsetAsTimestamp(130),
            getCorruptedConsumerRecordWithOffsetAsTimestamp(140),
            getCorruptedConsumerRecordWithOffsetAsTimestamp(150)
        ));
        assertTrue(consumer.paused().contains(partition1));

        assertTrue(task.process(0L));

        task.resumePollingForPartitionsWithAvailableSpace();
        assertTrue(consumer.paused().contains(partition1));

        assertTrue(task.process(0L));

        task.resumePollingForPartitionsWithAvailableSpace();
        assertEquals(0, consumer.paused().size());

        assertTrue(task.process(0L)); // drain head record (ie, last corrupted record)
        assertFalse(task.process(0L));
        assertFalse(task.hasRecordsQueued());
    }

    @Test
    public void shouldPunctuateOnceStreamTimeAfterGap() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.resumePollingForPartitionsWithAvailableSpace();

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 142),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 155),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 160)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition2, 25),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 145),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 159),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 161)
        ));

        task.updateLags();

        // st: -1
        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime()); // punctuate at 20

        // st: 20
        assertTrue(task.process(0L));
        assertEquals(7, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);
        assertTrue(task.canPunctuateStreamTime());
        assertTrue(task.maybePunctuateStreamTime());

        // st: 25
        assertTrue(task.process(0L));
        assertEquals(6, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime());

        // st: 142
        // punctuate at 142
        assertTrue(task.process(0L));
        assertEquals(5, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);
        assertTrue(task.canPunctuateStreamTime());
        assertTrue(task.maybePunctuateStreamTime());

        // st: 145
        // only one punctuation after 100ms gap
        assertTrue(task.process(0L));
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime());

        // st: 155
        // punctuate at 155
        assertTrue(task.process(0L));
        assertEquals(3, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);
        assertTrue(task.canPunctuateStreamTime());
        assertTrue(task.maybePunctuateStreamTime());

        // st: 159
        assertTrue(task.process(0L));
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime());

        // st: 160, aligned at 0
        assertTrue(task.process(0L));
        assertEquals(1, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(3, source2.numReceived);
        assertTrue(task.canPunctuateStreamTime());
        assertTrue(task.maybePunctuateStreamTime());

        // st: 161
        assertTrue(task.process(0L));
        assertEquals(0, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(4, source2.numReceived);
        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 142L, 155L, 160L);
    }

    @Test
    public void shouldRespectPunctuateCancellationStreamTime() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 30),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 40)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition2, 25),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 35),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 45)
        ));

        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime());

        // st is now 20
        assertTrue(task.process(0L));

        assertTrue(task.canPunctuateStreamTime());
        assertTrue(task.maybePunctuateStreamTime());

        // st is now 25
        assertTrue(task.process(0L));

        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime());

        // st is now 30
        assertTrue(task.process(0L));

        processorStreamTime.mockProcessor.scheduleCancellable().cancel();

        assertFalse(task.canPunctuateStreamTime());
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L);
    }

    @Test
    public void shouldRespectPunctuateCancellationSystemTime() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        final long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.scheduleCancellable().cancel();
        time.sleep(10);
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
    }

    @Test
    public void shouldRespectCommitNeeded() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig(AT_LEAST_ONCE, "0"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        assertFalse(task.commitNeeded());

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> record = mkMap(
                mkEntry(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, 0)))
        );
        task.addRecords(partition1, record.get(partition1));
        task.updateNextOffsets(partition1, new OffsetAndMetadata(1, Optional.empty(), ""));
        assertTrue(task.process(0L));
        assertTrue(task.commitNeeded());

        task.prepareCommit();
        assertTrue(task.commitNeeded());

        task.postCommit(true);
        assertFalse(task.commitNeeded());

        assertTrue(task.canPunctuateStreamTime());
        assertTrue(task.maybePunctuateStreamTime());
        assertTrue(task.commitNeeded());

        task.prepareCommit();
        assertTrue(task.commitNeeded());

        task.postCommit(true);
        assertFalse(task.commitNeeded());

        time.sleep(10);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        assertTrue(task.commitNeeded());

        task.prepareCommit();
        assertTrue(task.commitNeeded());

        task.postCommit(true);
        assertFalse(task.commitNeeded());
    }

    @Test
    public void shouldCommitNextOffsetAndProcessorMetadataFromQueueIfAvailable() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig(AT_LEAST_ONCE, "0"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 0L, 1),
            getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 3L, 1),
            getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 5L, 2)));

        task.process(0L);
        processorStreamTime.mockProcessor.addProcessorMetadata("key1", 100L);
        task.process(0L);
        processorSystemTime.mockProcessor.addProcessorMetadata("key2", 200L);

        final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = task.prepareCommit();
        final TopicPartitionMetadata expected = new TopicPartitionMetadata(3L,
            new ProcessorMetadata(
                mkMap(
                    mkEntry("key1", 100L),
                    mkEntry("key2", 200L)
                )
            )
        );

        assertThat(offsetsAndMetadata, equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(5L, Optional.of(2), expected.encode())))));
    }

    @Test
    public void shouldCommitFetchedNextOffsetIfRecordQueueIsEmpty() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 0L, 0));
        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 1L, 1));
        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 2L, 2));
        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition2, 0L, 0));
        consumer.poll(Duration.ZERO);

        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 0L, 0)));
        task.addRecords(partition2, singletonList(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition2, 0L, 0)));

        task.updateNextOffsets(partition1, new OffsetAndMetadata(3, Optional.of(2), ""));
        task.updateNextOffsets(partition2, new OffsetAndMetadata(1, Optional.of(0), ""));

        task.process(0L);


        final TopicPartitionMetadata metadata = new TopicPartitionMetadata(0, new ProcessorMetadata());

        assertTrue(task.commitNeeded());
        assertThat(task.prepareCommit(), equalTo(
                mkMap(
                        mkEntry(partition1, new OffsetAndMetadata(3L, Optional.of(2), metadata.encode()))
                )
        ));

        task.postCommit(false);

        // the task should still be committed since the processed records have not reached the consumer position
        assertTrue(task.commitNeeded());

        task.resumePollingForPartitionsWithAvailableSpace();
        consumer.poll(Duration.ZERO);
        task.updateLags();
        task.process(0L);

        assertTrue(task.commitNeeded());
        assertThat(task.prepareCommit(), equalTo(
            mkMap(
                mkEntry(partition1, new OffsetAndMetadata(3L, Optional.of(2), metadata.encode())),
                mkEntry(partition2, new OffsetAndMetadata(1L, Optional.of(0), metadata.encode()))
            )
        ));
        task.postCommit(false);

        assertFalse(task.commitNeeded());
    }

    @Test
    public void shouldCommitOldProcessorMetadataWhenNotDirty() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.resumePollingForPartitionsWithAvailableSpace();

        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 0L, 0));
        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 1L, 1));
        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition2, 0L, 0));
        consumer.addRecord(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition2, 1L, 1));
        consumer.poll(Duration.ZERO);

        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 0L, 0)));
        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(partition1, 1L, 1)));

        task.updateNextOffsets(partition1, new OffsetAndMetadata(2, Optional.of(1), ""));
        task.updateNextOffsets(partition2, new OffsetAndMetadata(2, Optional.of(1), ""));

        task.updateLags();

        task.process(0L);
        processorStreamTime.mockProcessor.addProcessorMetadata("key1", 100L);

        final TopicPartitionMetadata expectedMetadata1 = new TopicPartitionMetadata(0L,
            new ProcessorMetadata(
                mkMap(
                    mkEntry("key1", 100L)
                )
            )
        );

        final TopicPartitionMetadata expectedMetadata2 = new TopicPartitionMetadata(RecordQueue.UNKNOWN,
            new ProcessorMetadata(
                mkMap(
                    mkEntry("key1", 100L)
                )
            )
        );

        assertTrue(task.commitNeeded());

        assertThat(task.prepareCommit(), equalTo(
            mkMap(
                mkEntry(partition1, new OffsetAndMetadata(1L,  Optional.of(1), expectedMetadata1.encode())),
                mkEntry(partition2, new OffsetAndMetadata(2L, Optional.of(1), expectedMetadata2.encode()))
            )));
        task.postCommit(false);

        // the task should still be committed since the processed records have not reached the consumer position
        assertTrue(task.commitNeeded());

        consumer.poll(Duration.ZERO);
        task.process(0L);

        final TopicPartitionMetadata expectedMetadata3 = new TopicPartitionMetadata(1L,
            new ProcessorMetadata(
                mkMap(
                    mkEntry("key1", 100L)
                )
            )
        );
        assertTrue(task.commitNeeded());

        // Processor metadata not updated, we just need to commit to partition1 again with new offset
        assertThat(task.prepareCommit(), equalTo(
                mkMap(mkEntry(partition1, new OffsetAndMetadata(2L, Optional.of(1), expectedMetadata3.encode())))
        ));
        task.postCommit(false);

        assertFalse(task.commitNeeded());
    }

    @Test
    public void shouldFailOnCommitIfTaskIsClosed() {
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());
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
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.requestCommit();
        assertTrue(task.commitRequested());
    }

    @Test
    public void shouldBeProcessableIfAllPartitionsBuffered() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        assertThat("task is not idling", task.timeCurrentIdlingStarted().isEmpty());

        assertFalse(task.process(0L));

        task.addRecords(partition1, singleton(getConsumerRecordWithOffsetAsTimestamp(partition1, 0)));

        assertFalse(task.process(0L));
        assertThat("task is idling", task.timeCurrentIdlingStarted().isPresent());

        task.addRecords(partition2, singleton(getConsumerRecordWithOffsetAsTimestamp(partition2, 0)));

        assertTrue(task.process(0L));
        assertThat("task is not idling", task.timeCurrentIdlingStarted().isEmpty());
    }

    @Test
    public void shouldBeRecordIdlingTimeIfSuspended() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        task.suspend();

        assertThat("task is idling", task.timeCurrentIdlingStarted().isPresent());

        task.resume();

        assertThat("task is not idling", task.timeCurrentIdlingStarted().isEmpty());
    }

    @Test
    public void shouldPunctuateSystemTimeWhenIntervalElapsed() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        final long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(20);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10, now + 20, now + 30, now + 50);
    }

    @Test
    public void shouldNotPunctuateSystemTimeWhenIntervalNotElapsed() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME);
    }

    @Test
    public void shouldPunctuateOnceSystemTimeAfterGap() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        final long now = time.milliseconds();
        time.sleep(100);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(12);
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(7);
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1); // punctuate at now + 130
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(105); // punctuate at now + 235
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(5); // punctuate at now + 240, still aligned on the initial punctuation
        assertTrue(task.canPunctuateSystemTime());
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.canPunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 100, now + 110, now + 122, now + 130, now + 235, now + 240);
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        try {
            task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue(message.contains("processor '" + processorStreamTime.name() + "'"), "message=" + message + " should contain processor");
            assertThat(task.processorContext().currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingWallClockTimeTime() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        try {
            task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue(message.contains("processor '" + processorSystemTime.name() + "'"), "message=" + message + " should contain processor");
            assertThat(task.processorContext().currentNode(), nullValue());
        }
    }

    @Test
    public void shouldNotShareHeadersBetweenPunctuateIterations() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.punctuate(
            processorSystemTime,
            1L,
            PunctuationType.WALL_CLOCK_TIME,
            timestamp -> task.processorContext().headers().add("dummy", null)
        );
        task.punctuate(
            processorSystemTime,
            1L,
            PunctuationType.WALL_CLOCK_TIME,
            timestamp -> assertFalse(task.processorContext().headers().iterator().hasNext())
        );
    }

    @Test
    public void shouldWrapKafkaExceptionWithStreamsExceptionWhenProcess() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createFaultyStatefulTask(createConfig("100"));

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, 10),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 20),
            getConsumerRecordWithOffsetAsTimestamp(partition1, 30)
        ));
        task.addRecords(partition2, asList(
            getConsumerRecordWithOffsetAsTimestamp(partition2, 5),  // this is the first record to process
            getConsumerRecordWithOffsetAsTimestamp(partition2, 35),
            getConsumerRecordWithOffsetAsTimestamp(partition2, 45)
        ));

        assertThat("Map did not contain the partitions", task.highWaterMark().containsKey(partition1)
                && task.highWaterMark().containsKey(partition2));
        assertThrows(StreamsException.class, () -> task.process(0L));
    }

    @Test
    public void shouldReadCommittedOffsetAndRethrowTimeoutWhenCompleteRestoration() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);
        when(stateDirectory.lock(taskId)).thenReturn(true);

        task = createDisconnectedTask(createConfig("100"));

        task.transitionTo(RESTORING);

        assertThrows(TimeoutException.class, () -> task.completeRestoration(noOpResetter -> { }));
    }

    @Test
    public void shouldReInitializeTopologyWhenResuming() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        // Clean up state directory created as part of setup
        stateDirectory.close();
        stateDirectory = mock(StateDirectory.class);
        when(stateDirectory.lock(taskId)).thenReturn(true);

        task = createStatefulTask(createConfig("100"), true);

        task.initializeIfNeeded();

        task.suspend();

        assertEquals(SUSPENDED, task.state());
        assertFalse(source1.initialized);
        assertFalse(source2.initialized);

        task.resume();

        assertEquals(RESTORING, task.state());
        assertFalse(source1.initialized);
        assertFalse(source2.initialized);

        task.completeRestoration(noOpResetter -> { });

        assertEquals(RUNNING, task.state());
        assertTrue(source1.initialized);
        assertTrue(source2.initialized);

        assertThat("Map did not contain the partition", task.highWaterMark().containsKey(partition1));

        verify(recordCollector).offsets();
    }

    @Test
    public void shouldNotCheckpointOffsetsAgainOnCommitIfSnapshotNotChangedMuch() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final Long offset = 543L;

        when(recordCollector.offsets()).thenReturn(singletonMap(changelogPartition, offset));
        when(stateManager.changelogOffsets())
            .thenReturn(singletonMap(changelogPartition, 10L)) // restoration checkpoint
            .thenReturn(singletonMap(changelogPartition, 10L))
            .thenReturn(singletonMap(changelogPartition, 20L));

        task = createStatefulTask(createConfig("100"), true);

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { }); // should checkpoint

        task.prepareCommit();
        task.postCommit(true); // should checkpoint

        task.prepareCommit();
        task.postCommit(false); // should not checkpoint

        assertThat("Map was empty", task.highWaterMark().size() == 2);

        verify(stateManager, times(2)).checkpoint();
    }

    @Test
    public void shouldCheckpointOffsetsOnCommitIfSnapshotMuchChanged() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final Long offset = 543L;

        when(recordCollector.offsets()).thenReturn(singletonMap(changelogPartition, offset));
        when(stateManager.changelogOffsets())
            .thenReturn(singletonMap(changelogPartition, 0L))
            .thenReturn(singletonMap(changelogPartition, 10L))
            .thenReturn(singletonMap(changelogPartition, 12000L));

        task = createStatefulTask(createConfig("100"), true);

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { }); // should checkpoint
        task.prepareCommit();
        task.postCommit(true); // should checkpoint

        task.prepareCommit();
        task.postCommit(false); // should checkpoint since the offset delta is greater than the threshold

        assertThat("Map was empty", task.highWaterMark().size() == 2);

        verify(stateManager, times(3)).checkpoint();
    }

    @Test
    public void shouldNotCheckpointOffsetsOnCommitIfEosIsEnabled() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatefulTask(createConfig(StreamsConfig.EXACTLY_ONCE_V2, "100"), true);

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        task.prepareCommit();
        task.postCommit(false);
        final File checkpointFile = new File(
            stateDirectory.getOrCreateDirectoryForTask(taskId),
            StateManagerUtil.CHECKPOINT_FILE_NAME
        );

        assertFalse(checkpointFile.exists());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
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
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(5L));
        task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(10L));
    }

    @Test
    public void shouldSetProcessorNodeOnContextBackToNullAfterSuccessfulPunctuate() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(task.processorContext().currentNode(), nullValue());
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        assertThrows(IllegalStateException.class, () -> task.schedule(1, PunctuationType.STREAM_TIME, timestamp -> { }));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));
        task.processorContext().setCurrentNode(processorStreamTime);
        task.schedule(1, PunctuationType.STREAM_TIME, timestamp -> { });
    }

    @Test
    public void shouldCloseStateManagerEvenDuringFailureOnUncleanTaskClose() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createFaultyStatefulTask(createConfig("100"));

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        assertThrows(RuntimeException.class, () -> task.suspend());
        task.closeDirty();
        verify(stateManager).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMaybeReturnOffsetsForRepartitionTopicsForPurging(final boolean doCommit) {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final TopicPartition repartition = new TopicPartition("repartition", 1);

        final ProcessorTopology topology = withRepartitionTopics(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(repartition.topic(), source2)),
            singleton(repartition.topic())
        );
        consumer.assign(asList(partition1, repartition));
        consumer.updateBeginningOffsets(mkMap(mkEntry(repartition, 0L)));

        final StreamsConfig config = createConfig();
        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        task = new StreamTask(
            taskId,
            new HashSet<>(List.of(partition1, repartition)),
            topology,
            consumer,
            new TopologyConfig(null, config, new Properties()).getTaskConfig(),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
            );

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });


        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = mkMap(
                mkEntry(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, 5L))),
                mkEntry(repartition, singletonList(getConsumerRecordWithOffsetAsTimestamp(repartition, 10L)))
        );

        task.addRecords(partition1, records.get(partition1));
        task.addRecords(repartition, records.get(repartition));

        task.updateNextOffsets(partition1, new OffsetAndMetadata(6, Optional.empty(), ""));
        task.updateNextOffsets(repartition, new OffsetAndMetadata(11, Optional.empty(), ""));

        task.resumePollingForPartitionsWithAvailableSpace();
        task.updateLags();

        assertTrue(task.process(0L));
        assertTrue(task.process(0L));

        task.prepareCommit();
        if (doCommit) {
            task.updateCommittedOffsets(repartition, 10L);
        }

        final Map<TopicPartition, Long> map = task.purgeableOffsets();

        if (doCommit) {
            assertThat(map, equalTo(singletonMap(repartition, 10L)));
        } else {
            assertThat(map, equalTo(Collections.emptyMap()));
        }
    }

    @Test
    public void shouldThrowStreamsExceptionWhenFetchCommittedFailed() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final Consumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw new KafkaException("KABOOM!");
            }
        };

        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.transitionTo(RESTORING);

        assertThrows(StreamsException.class, () -> task.completeRestoration(noOpResetter -> { }));
    }

    @Test
    public void shouldThrowIfCommittingOnIllegalState() {
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));

        task.transitionTo(SUSPENDED);
        task.transitionTo(Task.State.CLOSED);
        assertThrows(IllegalStateException.class, task::prepareCommit);
    }

    @Test
    public void shouldThrowIfPostCommittingOnIllegalState() {
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig("100"));

        task.transitionTo(SUSPENDED);
        task.transitionTo(Task.State.CLOSED);
        assertThrows(IllegalStateException.class, () -> task.postCommit(true));
    }

    @Test
    public void shouldSkipCheckpointingSuspendedCreatedTask() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatefulTask(createConfig("100"), true);
        task.suspend();
        task.postCommit(true);

        verify(stateManager, never()).checkpoint();
    }

    @Test
    public void shouldCheckpointForSuspendedTask() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.changelogOffsets()).thenReturn(singletonMap(partition1, 1L));

        task = createStatefulTask(createConfig("100"), true);
        task.initializeIfNeeded();
        task.suspend();
        task.postCommit(true);

        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldNotCheckpointForSuspendedRunningTaskWithSmallProgress() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.changelogOffsets())
                .thenReturn(singletonMap(partition1, 0L)) // restoration checkpoint
                .thenReturn(singletonMap(partition1, 1L))
                .thenReturn(singletonMap(partition1, 2L));

        task = createStatefulTask(createConfig("100"), true);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.prepareCommit();
        task.postCommit(false);

        task.suspend();
        task.postCommit(false);

        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldCheckpointForSuspendedRunningTaskWithLargeProgress() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.changelogOffsets())
                .thenReturn(singletonMap(partition1, 0L))
                .thenReturn(singletonMap(partition1, 12000L))
                .thenReturn(singletonMap(partition1, 24000L));

        task = createStatefulTask(createConfig("100"), true);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { }); // should checkpoint

        task.prepareCommit();
        task.postCommit(false); // should checkpoint since the offset delta is greater than the threshold

        task.suspend();
        task.postCommit(false); // should checkpoint since the offset delta is greater than the threshold

        verify(stateManager, times(3)).checkpoint();
    }

    @Test
    public void shouldCheckpointWhileUpdateSnapshotWithTheConsumedOffsetsForSuspendedRunningTask() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final Map<TopicPartition, Long> checkpointableOffsets = singletonMap(partition1, 1L);
        when(stateManager.changelogOffsets())
                .thenReturn(Collections.emptyMap()) // restoration checkpoint
                .thenReturn(checkpointableOffsets);
        when(recordCollector.offsets()).thenReturn(checkpointableOffsets);

        task = createStatefulTask(createConfig(), true);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { }); // should checkpoint
        task.addRecords(partition1, singleton(getConsumerRecordWithOffsetAsTimestamp(partition1, 10)));
        task.addRecords(partition2, singleton(getConsumerRecordWithOffsetAsTimestamp(partition2, 10)));
        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.postCommit(true); // should checkpoint

        verify(stateManager, times(2)).checkpoint();
        verify(stateManager, times(2)).updateChangelogOffsets(checkpointableOffsets);
        verify(recordCollector, times(2)).offsets();
    }

    @Test
    public void shouldReturnStateManagerChangelogOffsets() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.changelogOffsets()).thenReturn(singletonMap(partition1, 50L));
        when(stateManager.changelogPartitions()).thenReturn(singleton(partition1));

        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.initializeIfNeeded();

        assertEquals(singletonMap(partition1, 50L), task.changelogOffsets());

        task.completeRestoration(noOpResetter -> { });

        assertEquals(singletonMap(partition1, Task.LATEST_OFFSET), task.changelogOffsets());
    }

    @Test
    public void shouldNotCheckpointOnCloseCreated() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.suspend();
        task.closeClean();

        assertEquals(Task.State.CLOSED, task.state());
        assertFalse(source1.initialized);
        assertFalse(source1.closed);

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        verify(stateManager, never()).flush();
        verify(stateManager, never()).checkpoint();
    }

    @Test
    public void shouldCheckpointOnCloseRestoringIfNoProgress() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { }); // should flush and checkpoint
        task.suspend();
        task.prepareCommit();
        task.postCommit(true); // should flush and checkpoint
        task.closeClean();

        assertEquals(Task.State.CLOSED, task.state());

        verify(stateManager, times(2)).flush();
        verify(stateManager, times(2)).checkpoint();
    }

    @Test
    public void shouldAlwaysCheckpointStateIfEnforced() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.initializeIfNeeded();
        task.maybeCheckpoint(true);

        verify(stateManager).flush();
        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldOnlyCheckpointStateWithBigAdvanceIfNotEnforced() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.changelogOffsets())
                .thenReturn(Collections.singletonMap(partition1, 50L))
                .thenReturn(Collections.singletonMap(partition1, 11000L))
                .thenReturn(Collections.singletonMap(partition1, 12000L));

        task = createOptimizedStatefulTask(createConfig("100"), consumer);
        task.initializeIfNeeded();

        task.maybeCheckpoint(false);  // this should not checkpoint
        assertTrue(task.offsetSnapshotSinceLastFlush.isEmpty());
        task.maybeCheckpoint(false);  // this should checkpoint
        assertEquals(Collections.singletonMap(partition1, 11000L), task.offsetSnapshotSinceLastFlush);
        task.maybeCheckpoint(false);  // this should not checkpoint
        assertEquals(Collections.singletonMap(partition1, 11000L), task.offsetSnapshotSinceLastFlush);

        verify(stateManager).flush();
        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldCheckpointOffsetsOnPostCommit() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final long offset = 543L;

        when(recordCollector.offsets()).thenReturn(singletonMap(changelogPartition, offset));
        when(stateManager.changelogOffsets())
                .thenReturn(singletonMap(partition1, offset + 10000L)) // restoration checkpoint
                .thenReturn(singletonMap(partition1, offset + 12000L));

        task = createOptimizedStatefulTask(createConfig(), consumer);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> record = mkMap(
                mkEntry(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, 5L)))
        );
        task.addRecords(partition1, record.get(partition1));
        task.updateNextOffsets(partition1, new OffsetAndMetadata(6, Optional.empty(), ""));
        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.prepareCommit();
        task.postCommit(false);

        assertEquals(SUSPENDED, task.state());

        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldThrowExceptionOnCloseCleanError() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final long offset = 543L;

        when(stateManager.changelogOffsets()).thenReturn(singletonMap(changelogPartition, offset));
        doThrow(new ProcessorStateException("KABOOM!")).when(stateManager).close();
        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig("100"), consumer);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { }); // should checkpoint

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> record = mkMap(
                mkEntry(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, offset))));
        task.addRecords(partition1, record.get(partition1));
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));
        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.prepareCommit();
        task.postCommit(true); // should checkpoint
        assertThrows(ProcessorStateException.class, () -> task.closeClean());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        verify(stateManager, times(2)).checkpoint();
        verify(stateManager).close();
    }

    @Test
    public void shouldThrowOnCloseCleanFlushError() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final long offset = 543L;

        when(recordCollector.offsets()).thenReturn(singletonMap(changelogPartition, offset));
        doThrow(new ProcessorStateException("KABOOM!")).when(stateManager).flushCache();
        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig("100"), consumer);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        // process one record to make commit needed
        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, offset)));
        task.process(100L);

        assertThrows(ProcessorStateException.class, task::prepareCommit);

        assertEquals(RUNNING, task.state());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        verify(stateManager).flush();
        verify(stateManager).checkpoint();
        verify(stateManager, never()).close();
    }

    @Test
    public void shouldThrowOnCloseCleanCheckpointError() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final long offset = 54300L;
        doThrow(new ProcessorStateException("KABOOM!")).when(stateManager).checkpoint();
        when(stateManager.changelogOffsets()).thenReturn(singletonMap(partition1, offset));
        final MetricName metricName = setupCloseTaskMetric();

        task = createOptimizedStatefulTask(createConfig("100"), consumer);
        task.initializeIfNeeded();

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> record = mkMap(
                mkEntry(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, offset))));
        task.addRecords(partition1, record.get(partition1));
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));

        task.process(100L);
        assertTrue(task.commitNeeded());

        task.suspend();
        task.prepareCommit();
        assertThrows(ProcessorStateException.class, () -> task.postCommit(true));

        assertEquals(Task.State.SUSPENDED, task.state());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        verify(stateManager, never()).close();
    }

    @Test
    public void shouldNotThrowFromStateManagerCloseInCloseDirty() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        doThrow(new RuntimeException("KABOOM!")).when(stateManager).close();

        task = createOptimizedStatefulTask(createConfig("100"), consumer);
        task.initializeIfNeeded();

        task.suspend();
        assertDoesNotThrow(() -> task.closeDirty());
    }

    @Test
    public void shouldUnregisterMetricsInCloseClean() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.suspend();
        assertThat(getTaskMetrics(), not(empty()));
        task.closeClean();
        assertThat(getTaskMetrics(), empty());
    }

    @Test
    public void shouldUnregisterMetricsInCloseDirty() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.suspend();
        assertThat(getTaskMetrics(), not(empty()));
        task.closeDirty();
        assertThat(getTaskMetrics(), empty());
    }

    @Test
    public void shouldUnregisterMetricsAndCloseInPrepareRecycle() {
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.suspend();
        assertThat(getTaskMetrics(), not(empty()));
        task.prepareRecycle();
        assertThat(getTaskMetrics(), empty());
        assertThat(task.state(), is(Task.State.CLOSED));

        verify(stateManager).recycle();
    }

    @Test
    public void shouldFlushStateManagerAndRecordCollector() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatefulTask(createConfig("100"), false);

        task.flush();

        verify(stateManager).flushCache();
        verify(recordCollector).flush();
    }

    @Test
    public void shouldClearCommitStatusesInCloseDirty() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig(AT_LEAST_ONCE, "0"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, 0)));
        assertTrue(task.process(0L));
        task.requestCommit();

        task.suspend();
        assertThat(task.commitNeeded(), is(true));
        assertThat(task.commitRequested(), is(true));
        task.closeDirty();
        assertThat(task.commitNeeded(), is(false));
        assertThat(task.commitRequested(), is(false));
    }

    @Test
    public void closeShouldBeIdempotent() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createOptimizedStatefulTask(createConfig("100"), consumer);

        task.suspend();
        task.closeClean();

        // close calls are idempotent since we are already in closed
        task.closeClean();
        task.closeDirty();
    }

    @Test
    public void shouldUpdatePartitions() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());
        final Set<TopicPartition> newPartitions = new HashSet<>(task.inputPartitions());
        newPartitions.add(new TopicPartition("newTopic", 0));

        task.updateInputPartitions(newPartitions, mkMap(
            mkEntry(source1.name(), asList(topic1, "newTopic")),
            mkEntry(source2.name(), singletonList(topic2)))
        );

        assertThat(task.inputPartitions(), equalTo(newPartitions));
    }

    @Test
    public void shouldThrowIfCleanClosingDirtyTask() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createSingleSourceStateless(createConfig(AT_LEAST_ONCE, "0"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, 0)));
        assertTrue(task.process(0L));
        assertTrue(task.commitNeeded());

        assertThrows(TaskMigratedException.class, () -> task.closeClean());
    }

    @Test
    public void shouldThrowIfRecyclingDirtyTask() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        task.addRecords(partition1, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition1, 0)));
        task.addRecords(partition2, singletonList(getConsumerRecordWithOffsetAsTimestamp(partition2, 0)));
        task.process(0L);
        assertTrue(task.commitNeeded());

        assertThrows(TaskMigratedException.class, () -> task.prepareRecycle());
    }

    @Test
    public void shouldPrepareRecycleSuspendedTask() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatefulTask(createConfig("100"), true);
        assertThrows(IllegalStateException.class, () -> task.prepareRecycle()); // CREATED

        task.initializeIfNeeded();
        assertThrows(IllegalStateException.class, () -> task.prepareRecycle()); // RESTORING

        task.completeRestoration(noOpResetter -> { });
        assertThrows(IllegalStateException.class, () -> task.prepareRecycle()); // RUNNING

        task.suspend();
        task.prepareRecycle(); // SUSPENDED
        assertThat(task.state(), is(Task.State.CLOSED));

        verify(stateManager).recycle();
        verify(recordCollector).closeClean();
    }

    @Test
    public void shouldAlwaysSuspendCreatedTasks() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatefulTask(createConfig("100"), true);
        assertThat(task.state(), equalTo(CREATED));
        task.suspend();
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldAlwaysSuspendRestoringTasks() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatefulTask(createConfig("100"), true);
        task.initializeIfNeeded();
        assertThat(task.state(), equalTo(RESTORING));
        task.suspend();
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldAlwaysSuspendRunningTasks() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createFaultyStatefulTask(createConfig("100"));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        assertThat(task.state(), equalTo(RUNNING));
        assertThrows(RuntimeException.class, () -> task.suspend());
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldThrowTopologyExceptionIfTaskCreatedForUnknownTopic() {
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
                taskId,
                createConfig("100"),
                stateManager,
                streamsMetrics,
                null
        );
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(this.metrics, "test", "processId", time);

        // The processor topology is missing the topics
        final ProcessorTopology topology = withSources(emptyList(), mkMap());

        final TopologyException exception = assertThrows(
            TopologyException.class,
            () -> new StreamTask(
                taskId,
                partitions,
                topology,
                consumer,
                new TopologyConfig(null, createConfig("100"), new Properties()).getTaskConfig(),
                metrics,
                stateDirectory,
                cache,
                time,
                stateManager,
                recordCollector,
                context,
                logContext,
                false
            )
        );

        assertThat(exception.getMessage(), equalTo("Invalid topology: " +
                "Topic " + topic1 + " is unknown to the topology. This may happen if different KafkaStreams instances of the same " +
                "application execute different Topologies. Note that Topologies are only identical if all operators " +
                "are added in the same order."));
    }

    @Test
    public void shouldInitTaskTimeoutAndEventuallyThrow() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());

        task.maybeInitTaskTimeoutOrThrow(0L, null);
        task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).toMillis(), null);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).plus(Duration.ofMillis(1L)).toMillis(), null)
        );

        assertThat(thrown.getCause(), isA(TimeoutException.class));
    }

    @Test
    public void shouldClearTaskTimeout() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig());

        task.maybeInitTaskTimeoutOrThrow(0L, null);
        task.clearTaskTimeout();
        task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).plus(Duration.ofMillis(1L)).toMillis(), null);
    }

    @Test
    public void shouldUpdateOffsetIfAllRecordsHaveInvalidTimestamp() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfigWithTsExtractor(LogAndSkipOnInvalidTimestamp.class));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        long offset = -1L;

        final List<ConsumerRecord<byte[], byte[]>> records = asList(
            getConsumerRecordWithInvalidTimestamp(++offset),
            getConsumerRecordWithInvalidTimestamp(++offset)
        );
        consumer.addRecord(records.get(0));
        consumer.addRecord(records.get(1));
        task.resumePollingForPartitionsWithAvailableSpace();
        consumer.poll(Duration.ZERO);
        task.addRecords(partition1, records);
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));
        task.updateLags();

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1,
                new OffsetAndMetadata(offset + 1,
                    new TopicPartitionMetadata(RecordQueue.UNKNOWN, new ProcessorMetadata()).encode()))))
        );
    }

    @Test
    public void shouldUpdateOffsetIfValidRecordFollowsInvalidTimestamp() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfigWithTsExtractor(LogAndSkipOnInvalidTimestamp.class));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        long offset = -1L;

        final List<ConsumerRecord<byte[], byte[]>> records = asList(
            getConsumerRecordWithInvalidTimestamp(++offset),
            getConsumerRecordWithOffsetAsTimestamp(partition1, ++offset)
        );
        consumer.addRecord(records.get(0));
        consumer.addRecord(records.get(1));
        task.resumePollingForPartitionsWithAvailableSpace();
        consumer.poll(Duration.ZERO);
        task.addRecords(partition1, records);
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));
        task.updateLags();

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(offset + 1, new TopicPartitionMetadata(offset, new ProcessorMetadata()).encode()))))
        );
    }

    @Test
    public void shouldUpdateOffsetIfInvalidTimestampeRecordFollowsValid() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfigWithTsExtractor(LogAndSkipOnInvalidTimestamp.class));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        long offset = -1;

        final List<ConsumerRecord<byte[], byte[]>> records = asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, ++offset),
            getConsumerRecordWithInvalidTimestamp(++offset));
        consumer.addRecord(records.get(0));
        consumer.addRecord(records.get(1));
        task.resumePollingForPartitionsWithAvailableSpace();
        consumer.poll(Duration.ZERO);
        task.addRecords(partition1, records);
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));

        task.updateLags();

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(1, new TopicPartitionMetadata(0, new ProcessorMetadata()).encode()))))
        );

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(2, new TopicPartitionMetadata(0, new ProcessorMetadata()).encode()))))
        );
    }

    @Test
    public void shouldUpdateOffsetIfAllRecordsAreCorrupted() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(LogAndContinueExceptionHandler.class));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        long offset = -1L;

        final List<ConsumerRecord<byte[], byte[]>> records = asList(
            getCorruptedConsumerRecordWithOffsetAsTimestamp(++offset),
            getCorruptedConsumerRecordWithOffsetAsTimestamp(++offset)
        );
        consumer.addRecord(records.get(0));
        consumer.addRecord(records.get(1));
        task.resumePollingForPartitionsWithAvailableSpace();
        consumer.poll(Duration.ZERO);
        task.addRecords(partition1, records);
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));
        task.updateLags();

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1,
                new OffsetAndMetadata(offset + 1,
                    new TopicPartitionMetadata(RecordQueue.UNKNOWN, new ProcessorMetadata()).encode()))))
        );
    }

    @Test
    public void shouldUpdateOffsetIfValidRecordFollowsCorrupted() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(LogAndContinueExceptionHandler.class));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        long offset = -1L;

        final List<ConsumerRecord<byte[], byte[]>> records = asList(
            getCorruptedConsumerRecordWithOffsetAsTimestamp(++offset),
            getConsumerRecordWithOffsetAsTimestamp(partition1, ++offset)
        );
        consumer.addRecord(records.get(0));
        consumer.addRecord(records.get(1));
        task.resumePollingForPartitionsWithAvailableSpace();
        consumer.poll(Duration.ZERO);
        task.addRecords(partition1, records);
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));
        task.updateLags();

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(offset + 1, new TopicPartitionMetadata(offset, new ProcessorMetadata()).encode()))))
        );
    }

    @Test
    public void shouldUpdateOffsetIfCorruptedRecordFollowsValid() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(LogAndContinueExceptionHandler.class));
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });

        long offset = -1L;

        final List<ConsumerRecord<byte[], byte[]>> records = asList(
            getConsumerRecordWithOffsetAsTimestamp(partition1, ++offset),
            getCorruptedConsumerRecordWithOffsetAsTimestamp(++offset)
        );
        consumer.addRecord(records.get(0));
        consumer.addRecord(records.get(1));
        task.resumePollingForPartitionsWithAvailableSpace();
        consumer.poll(Duration.ZERO);
        task.addRecords(partition1, records);
        task.updateNextOffsets(partition1, new OffsetAndMetadata(offset + 1, Optional.empty(), ""));

        task.updateLags();

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(1, new TopicPartitionMetadata(0, new ProcessorMetadata()).encode()))))
        );

        assertTrue(task.process(offset));
        assertTrue(task.commitNeeded());
        assertThat(
            task.prepareCommit(),
            equalTo(mkMap(mkEntry(partition1, new OffsetAndMetadata(2, new TopicPartitionMetadata(0, new ProcessorMetadata()).encode()))))
        );
    }

    @Test
    public void shouldCheckpointAfterRestorationWhenAtLeastOnceEnabled() {
        final ProcessorStateManager processorStateManager = mockStateManager();
        recordCollector = mock(RecordCollectorImpl.class);

        task = createStatefulTask(createConfig(AT_LEAST_ONCE, "100"), true, processorStateManager);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        verify(processorStateManager).checkpoint();
    }

    @Test
    public void shouldNotCheckpointAfterRestorationWhenExactlyOnceEnabled() {
        final ProcessorStateManager processorStateManager = mockStateManager();
        recordCollector = mock(RecordCollectorImpl.class);

        task = createStatefulTask(createConfig(EXACTLY_ONCE_V2, "100"), true, processorStateManager);
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        verify(processorStateManager, never()).checkpoint();
        verify(processorStateManager, never()).changelogOffsets();
        verify(recordCollector, never()).offsets();
    }

    @Test
    public void punctuateShouldNotHandleFailProcessingExceptionAndThrowStreamsException() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            "100",
            LogAndContinueProcessingExceptionHandler.class
        ));

        final StreamsException streamsException = assertThrows(
            StreamsException.class,
            () -> task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw new FailedProcessingException("name", new RuntimeException("KABOOM!"));
            })
        );

        assertInstanceOf(RuntimeException.class, streamsException.getCause());
        assertEquals("KABOOM!", streamsException.getCause().getMessage());
    }

    @Test
    public void punctuateShouldNotHandleTaskCorruptedExceptionAndThrowItAsIs() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            "100",
            LogAndContinueProcessingExceptionHandler.class
        ));

        final Set<TaskId> tasksIds = new HashSet<>();
        tasksIds.add(new TaskId(0, 0));
        final TaskCorruptedException expectedException = new TaskCorruptedException(tasksIds, new InvalidOffsetException("Invalid offset") {
            @Override
            public Set<TopicPartition> partitions() {
                return new HashSet<>(Collections.singletonList(new TopicPartition("topic", 0)));
            }
        });

        final TaskCorruptedException taskCorruptedException = assertThrows(
            TaskCorruptedException.class,
            () -> task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw expectedException;
            })
        );

        assertEquals(expectedException, taskCorruptedException);
    }

    @Test
    public void punctuateShouldNotHandleTaskMigratedExceptionAndThrowItAsIs() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            "100",
            LogAndContinueProcessingExceptionHandler.class
        ));

        final TaskMigratedException expectedException = new TaskMigratedException("TaskMigratedException", new RuntimeException("Task migrated cause"));

        final TaskMigratedException taskCorruptedException = assertThrows(
            TaskMigratedException.class,
            () -> task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw expectedException;
            })
        );

        assertEquals(expectedException, taskCorruptedException);
    }

    @Test
    public void punctuateShouldNotThrowStreamsExceptionWhenProcessingExceptionHandlerRepliesWithContinue() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            "100",
            LogAndContinueProcessingExceptionHandler.class
        ));

        task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
            throw new KafkaException("KABOOM!");
        });
    }

    @Test
    public void punctuateShouldThrowStreamsExceptionWhenProcessingExceptionHandlerRepliesWithFail() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            "100",
            LogAndFailProcessingExceptionHandler.class
        ));

        final StreamsException streamsException = assertThrows(
            StreamsException.class,
            () -> task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            })
        );

        assertInstanceOf(KafkaException.class, streamsException.getCause());
        assertEquals("KABOOM!", streamsException.getCause().getMessage());
    }

    @Test
    public void punctuateShouldThrowStreamsExceptionWhenProcessingExceptionHandlerReturnsNull() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            "100",
            NullProcessingExceptionHandler.class
        ));

        final StreamsException streamsException = assertThrows(
            StreamsException.class,
            () -> task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            })
        );

        assertEquals("Fatal user code error in processing error callback", streamsException.getMessage());
        assertInstanceOf(NullPointerException.class, streamsException.getCause());
        assertEquals("Invalid ProcessingExceptionHandler response.", streamsException.getCause().getMessage());
    }

    @Test
    public void punctuateShouldThrowFailedProcessingExceptionWhenProcessingExceptionHandlerThrowsAnException() {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        task = createStatelessTask(createConfig(
            "100",
            CrashingProcessingExceptionHandler.class
        ));

        final FailedProcessingException streamsException = assertThrows(
            FailedProcessingException.class,
            () -> task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, timestamp -> {
                throw new KafkaException("KABOOM!");
            })
        );

        assertEquals("Fatal user code error in processing error callback", streamsException.getMessage());
        assertEquals("KABOOM from ProcessingExceptionHandlerMock!", streamsException.getCause().getMessage());
    }

    public static class CrashingProcessingExceptionHandler implements ProcessingExceptionHandler {
        @Override
        public ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            throw new RuntimeException("KABOOM from ProcessingExceptionHandlerMock!");
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    public static class NullProcessingExceptionHandler implements ProcessingExceptionHandler {
        @Override
        public ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            return null;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    private ProcessorStateManager mockStateManager() {
        final ProcessorStateManager manager = mock(ProcessorStateManager.class);
        doReturn(TaskType.ACTIVE).when(manager).taskType();
        doReturn(taskId).when(manager).taskId();
        return manager;
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

        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            new HashSet<>(List.of(partition1)),
            topology,
            consumer,
            new TopologyConfig(null,  config, new Properties()).getTaskConfig(),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private StreamTask createDisconnectedTask(final StreamsConfig config) {
        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, false);

        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
            singletonList(stateStore),
            emptyMap());

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw new TimeoutException("KABOOM!");
            }
        };

        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
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
            new TopologyConfig(null,  config, new Properties()).getTaskConfig(),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private StreamTask createFaultyStatefulTask(final StreamsConfig config) {
        final ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source3),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3)),
            singletonList(stateStore),
            emptyMap()
        );

        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
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
            new TopologyConfig(null,  config, new Properties()).getTaskConfig(),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
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

        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
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
            new TopologyConfig(null,  config, new Properties()).getTaskConfig(),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private StreamTask createSingleSourceStateless(final StreamsConfig config) {
        final ProcessorTopology topology = withSources(
            asList(source1, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1))
        );

        source1.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);

        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        return new StreamTask(
            taskId,
            new HashSet<>(List.of(partition1)),
            topology,
            consumer,
            new TopologyConfig(null,  config, new Properties()).getTaskConfig(),
            new StreamsMetricsImpl(metrics, "test", "processId", time),
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private StreamTask createStatelessTask(final StreamsConfig config) {
        final ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
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
            new TopologyConfig(null,  config, new Properties()).getTaskConfig(),
            new StreamsMetricsImpl(metrics, "test", "processId", time),
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private StreamTask createStatelessTaskWithForwardingTopology(final SourceNode<Integer, Integer> sourceNode) {
        final ProcessorTopology topology = withSources(
            asList(sourceNode, processorStreamTime),
            singletonMap(topic1, sourceNode)
        );

        sourceNode.addChild(processorStreamTime);

        final StreamsConfig config = createConfig();

        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
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
            new TopologyConfig(null,  config, new Properties()).getTaskConfig(),
            new StreamsMetricsImpl(metrics, "test", "processId", time),
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private void createTimeoutTask(final String eosConfig) {
        final ProcessorTopology topology = withSources(
            singletonList(timeoutSource),
            mkMap(mkEntry(topic1, timeoutSource))
        );

        final StreamsConfig config = createConfig(eosConfig, "0");
        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            null
        );

        task = new StreamTask(
            taskId,
            new HashSet<>(List.of(partition1)),
            topology,
            consumer,
            new TopologyConfig(null, config, new Properties()).getTaskConfig(),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecordWithOffsetAsTimestamp(final TopicPartition topicPartition,
                                                                                  final long offset,
                                                                                  final int value) {
        return new ConsumerRecord<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            0,
            0,
            recordKey,
            intSerializer.serialize(null, value),
            new RecordHeaders(),
            Optional.empty()
        );
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecordWithOffsetAsTimestamp(final TopicPartition topicPartition,
                                                                                  final long offset) {
        return new ConsumerRecord<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            0,
            0,
            recordKey,
            recordValue,
            new RecordHeaders(),
            Optional.empty()
        );
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecordWithOffsetAsTimestamp(final Integer key, final long offset) {
        return new ConsumerRecord<>(
            topic1,
            0,
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            0,
            0,
            new IntegerSerializer().serialize(topic1, key),
            recordValue,
            new RecordHeaders(),
            Optional.empty()
        );
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecordWithInvalidTimestamp(final long offset) {
        return new ConsumerRecord<>(
            topic1,
            0,
            offset,
            -1L, // invalid (negative) timestamp
            TimestampType.CREATE_TIME,
            0,
            0,
            recordKey,
            recordValue,
            new RecordHeaders(),
            Optional.empty()
        );
    }

    private ConsumerRecord<byte[], byte[]> getConsumerRecordWithOffsetAsTimestampWithLeaderEpoch(final TopicPartition topicPartition,
                                                                                                 final long offset,
                                                                                                 final int leaderEpoch) {
        return new ConsumerRecord<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            0,
            0,
            recordKey,
            recordValue,
            new RecordHeaders(),
            Optional.of(leaderEpoch)
        );
    }

    private ConsumerRecord<byte[], byte[]> getCorruptedConsumerRecordWithOffsetAsTimestamp(final long offset) {
        return new ConsumerRecord<>(
            topic1,
            0,
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CREATE_TIME,
            -1,
            -1,
            new byte[0],
            "I am not an integer.".getBytes(),
            new RecordHeaders(),
            Optional.empty()
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
