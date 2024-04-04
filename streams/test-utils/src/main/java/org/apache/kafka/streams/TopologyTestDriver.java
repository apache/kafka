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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ChangelogRegister;
import org.apache.kafka.streams.processor.internals.ClientUtils;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateManager;
import org.apache.kafka.streams.processor.internals.GlobalStateManagerImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateUpdateTask;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamsProducer;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.TopologyConfig.TaskConfig;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.ReadOnlyKeyValueStoreFacade;
import org.apache.kafka.streams.state.internals.ReadOnlyWindowStoreFacade;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.test.TestRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_ALPHA;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

/**
 * This class makes it easier to write tests to verify the behavior of topologies created with {@link Topology} or
 * {@link StreamsBuilder}.
 * You can test simple topologies that have a single processor, or very complex topologies that have multiple sources,
 * processors, sinks, or sub-topologies.
 * Best of all, the class works without a real Kafka broker, so the tests execute very quickly with very little overhead.
 * <p>
 * Using the {@code TopologyTestDriver} in tests is easy: simply instantiate the driver and provide a {@link Topology}
 * (cf. {@link StreamsBuilder#build()}) and {@link Properties config}, {@link #createInputTopic(String, Serializer, Serializer) create}
 * and use a {@link TestInputTopic} to supply an input records to the topology,
 * and then {@link #createOutputTopic(String, Deserializer, Deserializer) create} and use a {@link TestOutputTopic} to read and
 * verify any output records by the topology.
 * <p>
 * Although the driver doesn't use a real Kafka broker, it does simulate Kafka {@link Consumer consumers} and
 * {@link Producer producers} that read and write raw {@code byte[]} messages.
 * You can let {@link TestInputTopic} and {@link TestOutputTopic} to handle conversion
 * form regular Java objects to raw bytes.
 *
 * <h2>Driver setup</h2>
 * In order to create a {@code TopologyTestDriver} instance, you need a {@link Topology} and a {@link Properties config}.
 * The configuration needs to be representative of what you'd supply to the real topology, so that means including
 * several key properties (cf. {@link StreamsConfig}).
 * For example, the following code fragment creates a configuration that specifies a timestamp extractor,
 * default serializers and deserializers for string keys and values:
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
 * props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 * props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 * Topology topology = ...
 * TopologyTestDriver driver = new TopologyTestDriver(topology, props);
 * }</pre>
 *
 * <p> Note that the {@code TopologyTestDriver} processes input records synchronously.
 * This implies that {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit.interval.ms} and
 * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache.max.bytes.buffering} configuration have no effect.
 * The driver behaves as if both configs would be set to zero, i.e., as if a "commit" (and thus "flush") would happen
 * after each input record.
 *
 * <h2>Processing messages</h2>
 * <p>
 * Your test can supply new input records on any of the topics that the topology's sources consume.
 * This test driver simulates single-partitioned input topics.
 * Here's an example of an input message on the topic named {@code input-topic}:
 *
 * <pre>{@code
 * TestInputTopic<String, String> inputTopic = driver.createInputTopic("input-topic", stringSerdeSerializer, stringSerializer);
 * inputTopic.pipeInput("key1", "value1");
 * }</pre>
 *
 * When {@link TestInputTopic#pipeInput(Object, Object)} is called, the driver passes the input message through to the appropriate source that
 * consumes the named topic, and will invoke the processor(s) downstream of the source.
 * If your topology's processors forward messages to sinks, your test can then consume these output messages to verify
 * they match the expected outcome.
 * For example, if our topology should have generated 2 messages on {@code output-topic-1} and 1 message on
 * {@code output-topic-2}, then our test can obtain these messages using the
 * {@link TestOutputTopic#readKeyValue()}  method:
 *
 * <pre>{@code
 * TestOutputTopic<String, String> outputTopic1 = driver.createOutputTopic("output-topic-1", stringDeserializer, stringDeserializer);
 * TestOutputTopic<String, String> outputTopic2 = driver.createOutputTopic("output-topic-2", stringDeserializer, stringDeserializer);
 *
 * KeyValue<String, String> record1 = outputTopic1.readKeyValue();
 * KeyValue<String, String> record2 = outputTopic2.readKeyValue();
 * KeyValue<String, String> record3 = outputTopic1.readKeyValue();
 * }</pre>
 *
 * Again, our example topology generates messages with string keys and values, so we supply our string deserializer
 * instance for use on both the keys and values. Your test logic can then verify whether these output records are
 * correct.
 * <p>
 * Note, that calling {@code pipeInput()} will also trigger {@link PunctuationType#STREAM_TIME event-time} base
 * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) punctuation} callbacks.
 * However, you won't trigger {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type punctuations that you must
 * trigger manually via {@link #advanceWallClockTime(Duration)}.
 * <p>
 * Finally, when completed, make sure your tests {@link #close()} the driver to release all resources and
 * {@link org.apache.kafka.streams.processor.api.Processor processors}.
 *
 * <h2>Processor state</h2>
 * <p>
 * Some processors use Kafka {@link StateStore state storage}, so this driver class provides the generic
 * {@link #getStateStore(String)} as well as store-type specific methods so that your tests can check the underlying
 * state store(s) used by your topology's processors.
 * In our previous example, after we supplied a single input message and checked the three output messages, our test
 * could also check the key value store to verify the processor correctly added, removed, or updated internal state.
 * Or, our test might have pre-populated some state <em>before</em> submitting the input message, and verified afterward
 * that the processor(s) correctly updated the state.
 *
 * @see TestInputTopic
 * @see TestOutputTopic
 */
public class TopologyTestDriver implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TopologyTestDriver.class);

    private final LogContext logContext;
    private final Time mockWallClockTime;
    private InternalTopologyBuilder internalTopologyBuilder;

    private final static int PARTITION_ID = 0;
    private final static TaskId TASK_ID = new TaskId(0, PARTITION_ID);
    StreamTask task;
    private GlobalStateUpdateTask globalStateTask;
    private GlobalStateManager globalStateManager;

    private StateDirectory stateDirectory;
    private Metrics metrics;
    ProcessorTopology processorTopology;
    ProcessorTopology globalTopology;

    private final MockConsumer<byte[], byte[]> consumer;
    private final MockProducer<byte[], byte[]> producer;
    private final TestDriverProducer testDriverProducer;

    private final Map<String, TopicPartition> partitionsByInputTopic = new HashMap<>();
    private final Map<String, TopicPartition> globalPartitionsByInputTopic = new HashMap<>();
    private final Map<TopicPartition, AtomicLong> offsetsByTopicOrPatternPartition = new HashMap<>();

    private final Map<String, Queue<ProducerRecord<byte[], byte[]>>> outputRecordsByTopic = new HashMap<>();
    private final StreamsConfigUtils.ProcessingMode processingMode;

    private final StateRestoreListener stateRestoreListener = new StateRestoreListener() {
        @Override
        public void onRestoreStart(final TopicPartition topicPartition, final String storeName, final long startingOffset, final long endingOffset) {}

        @Override
        public void onBatchRestored(final TopicPartition topicPartition, final String storeName, final long batchEndOffset, final long numRestored) {}

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {}
    };

    /**
     * Create a new test diver instance.
     * Default test properties are used to initialize the driver instance
     *
     * @param topology the topology to be tested
     */
    public TopologyTestDriver(final Topology topology) {
        this(topology, new Properties());
    }

    /**
     * Create a new test diver instance.
     * Initialized the internally mocked wall-clock time with {@link System#currentTimeMillis() current system time}.
     *
     * @param topology the topology to be tested
     * @param config   the configuration for the topology
     */
    public TopologyTestDriver(final Topology topology,
                              final Properties config) {
        this(topology, config, null);
    }

    /**
     * Create a new test diver instance.
     *
     * @param topology the topology to be tested
     * @param initialWallClockTimeMs the initial value of internally mocked wall-clock time
     */
    public TopologyTestDriver(final Topology topology,
                              final Instant initialWallClockTimeMs) {
        this(topology, new Properties(), initialWallClockTimeMs);
    }

    /**
     * Create a new test diver instance.
     *
     * @param topology               the topology to be tested
     * @param config                 the configuration for the topology
     * @param initialWallClockTime   the initial value of internally mocked wall-clock time
     */
    public TopologyTestDriver(final Topology topology,
                              final Properties config,
                              final Instant initialWallClockTime) {
        this(
            topology.internalTopologyBuilder,
            config,
            initialWallClockTime == null ? System.currentTimeMillis() : initialWallClockTime.toEpochMilli());
    }

    /**
     * Create a new test diver instance.
     *
     * @param builder builder for the topology to be tested
     * @param config the configuration for the topology
     * @param initialWallClockTimeMs the initial value of internally mocked wall-clock time
     */
    private TopologyTestDriver(final InternalTopologyBuilder builder,
                               final Properties config,
                               final long initialWallClockTimeMs) {
        final Properties configCopy = new Properties();
        configCopy.putAll(config);
        configCopy.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-bootstrap-host:0");
        // provide randomized dummy app-id if it's not specified
        configCopy.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-topology-test-driver-app-id-" + ThreadLocalRandom.current().nextInt());
        // disable stream-stream left/outer join emit result throttling
        configCopy.putIfAbsent(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX, 0L);
        // disable windowed aggregation emit result throttling
        configCopy.putIfAbsent(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0L);
        final StreamsConfig streamsConfig = new ClientUtils.QuietStreamsConfig(configCopy);
        logIfTaskIdleEnabled(streamsConfig);

        logContext = new LogContext("topology-test-driver ");
        mockWallClockTime = new MockTime(initialWallClockTimeMs);
        processingMode = StreamsConfigUtils.processingMode(streamsConfig);

        final StreamsMetricsImpl streamsMetrics = setupMetrics(streamsConfig);
        setupTopology(builder, streamsConfig);

        final ThreadCache cache = new ThreadCache(
            logContext,
            Math.max(0, streamsConfig.getLong(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG)),
            streamsMetrics
        );

        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();
        producer = new MockProducer<byte[], byte[]>(true, bytesSerializer, bytesSerializer) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.singletonList(new PartitionInfo(topic, PARTITION_ID, null, null, null));
            }
        };
        testDriverProducer = new TestDriverProducer(
            streamsConfig,
            new KafkaClientSupplier() {
                @Override
                public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                    return producer;
                }

                @Override
                public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
                    throw new IllegalStateException();
                }

                @Override
                public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
                    throw new IllegalStateException();
                }

                @Override
                public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
                    throw new IllegalStateException();
                }
            },
            logContext,
            mockWallClockTime
        );

        setupGlobalTask(mockWallClockTime, streamsConfig, streamsMetrics, cache);
        setupTask(streamsConfig, streamsMetrics, cache, internalTopologyBuilder.topologyConfigs().getTaskConfig());
    }

    private static void logIfTaskIdleEnabled(final StreamsConfig streamsConfig) {
        final Long taskIdleTime = streamsConfig.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        if (taskIdleTime > 0) {
            log.info("Detected {} config in use with TopologyTestDriver (set to {}ms)." +
                         " This means you might need to use TopologyTestDriver#advanceWallClockTime()" +
                         " or enqueue records on all partitions to allow Steams to make progress." +
                         " TopologyTestDriver will log a message each time it cannot process enqueued" +
                         " records due to {}.",
                     StreamsConfig.MAX_TASK_IDLE_MS_CONFIG,
                     taskIdleTime,
                     StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        }
    }

    private StreamsMetricsImpl setupMetrics(final StreamsConfig streamsConfig) {
        final String threadId = Thread.currentThread().getName();

        final MetricConfig metricConfig = new MetricConfig()
            .samples(streamsConfig.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
            .recordLevel(Sensor.RecordingLevel.forName(streamsConfig.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
            .timeWindow(streamsConfig.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS);
        metrics = new Metrics(metricConfig, mockWallClockTime);

        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
            metrics,
            "test-client",
            streamsConfig.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG),
            mockWallClockTime
        );
        TaskMetrics.droppedRecordsSensor(threadId, TASK_ID.toString(), streamsMetrics);

        return streamsMetrics;
    }

    private void setupTopology(final InternalTopologyBuilder builder,
                               final StreamsConfig streamsConfig) {
        internalTopologyBuilder = builder;
        internalTopologyBuilder.rewriteTopology(streamsConfig);

        processorTopology = internalTopologyBuilder.buildTopology();
        globalTopology = internalTopologyBuilder.buildGlobalStateTopology();

        for (final String topic : processorTopology.sourceTopics()) {
            final TopicPartition tp = new TopicPartition(topic, PARTITION_ID);
            partitionsByInputTopic.put(topic, tp);
            offsetsByTopicOrPatternPartition.put(tp, new AtomicLong());
        }

        stateDirectory = new StateDirectory(streamsConfig, mockWallClockTime, internalTopologyBuilder.hasPersistentStores(), false);
    }

    private void setupGlobalTask(final Time mockWallClockTime,
                                 final StreamsConfig streamsConfig,
                                 final StreamsMetricsImpl streamsMetrics,
                                 final ThreadCache cache) {
        if (globalTopology != null) {
            final MockConsumer<byte[], byte[]> globalConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
            for (final String topicName : globalTopology.sourceTopics()) {
                final TopicPartition partition = new TopicPartition(topicName, 0);
                globalPartitionsByInputTopic.put(topicName, partition);
                offsetsByTopicOrPatternPartition.put(partition, new AtomicLong());
                globalConsumer.updatePartitions(topicName, Collections.singletonList(
                    new PartitionInfo(topicName, 0, null, null, null)));
                globalConsumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
                globalConsumer.updateEndOffsets(Collections.singletonMap(partition, 0L));
            }

            globalStateManager = new GlobalStateManagerImpl(
                logContext,
                mockWallClockTime,
                globalTopology,
                globalConsumer,
                stateDirectory,
                stateRestoreListener,
                streamsConfig
            );

            final GlobalProcessorContextImpl globalProcessorContext =
                new GlobalProcessorContextImpl(streamsConfig, globalStateManager, streamsMetrics, cache, mockWallClockTime);
            globalStateManager.setGlobalProcessorContext(globalProcessorContext);

            globalStateTask = new GlobalStateUpdateTask(
                logContext,
                globalTopology,
                globalProcessorContext,
                globalStateManager,
                new LogAndContinueExceptionHandler(),
                mockWallClockTime,
                streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)
            );
            globalStateTask.initialize();
            globalProcessorContext.setRecordContext(null);
        } else {
            globalStateManager = null;
            globalStateTask = null;
        }
    }

    @SuppressWarnings("deprecation")
    private void setupTask(final StreamsConfig streamsConfig,
                           final StreamsMetricsImpl streamsMetrics,
                           final ThreadCache cache,
                           final TaskConfig taskConfig) {
        if (!partitionsByInputTopic.isEmpty()) {
            consumer.assign(partitionsByInputTopic.values());
            final Map<TopicPartition, Long> startOffsets = new HashMap<>();
            for (final TopicPartition topicPartition : partitionsByInputTopic.values()) {
                startOffsets.put(topicPartition, 0L);
            }
            consumer.updateBeginningOffsets(startOffsets);

            final ProcessorStateManager stateManager = new ProcessorStateManager(
                TASK_ID,
                Task.TaskType.ACTIVE,
                StreamsConfig.EXACTLY_ONCE.equals(streamsConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)),
                logContext,
                stateDirectory,
                new MockChangelogRegister(),
                processorTopology.storeToChangelogTopic(),
                new HashSet<>(partitionsByInputTopic.values()),
                false);
            final RecordCollector recordCollector = new RecordCollectorImpl(
                logContext,
                TASK_ID,
                testDriverProducer,
                streamsConfig.defaultProductionExceptionHandler(),
                streamsMetrics,
                processorTopology
            );

            final InternalProcessorContext context = new ProcessorContextImpl(
                TASK_ID,
                streamsConfig,
                stateManager,
                streamsMetrics,
                cache
            );

            task = new StreamTask(
                TASK_ID,
                new HashSet<>(partitionsByInputTopic.values()),
                processorTopology,
                consumer,
                taskConfig,
                streamsMetrics,
                stateDirectory,
                cache,
                mockWallClockTime,
                stateManager,
                recordCollector,
                context,
                logContext,
                false
                );
            task.initializeIfNeeded();
            task.completeRestoration(noOpResetter -> { });
            task.processorContext().setRecordContext(null);

        } else {
            task = null;
        }
    }

    /**
     * Get read-only handle on global metrics registry.
     *
     * @return Map of all metrics.
     */
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(metrics.metrics());
    }

    private void pipeRecord(final String topicName,
                            final long timestamp,
                            final byte[] key,
                            final byte[] value,
                            final Headers headers) {
        final TopicPartition inputTopicOrPatternPartition = getInputTopicOrPatternPartition(topicName);
        final TopicPartition globalInputTopicPartition = globalPartitionsByInputTopic.get(topicName);

        if (inputTopicOrPatternPartition == null && globalInputTopicPartition == null) {
            throw new IllegalArgumentException("Unknown topic: " + topicName);
        }

        if (inputTopicOrPatternPartition != null) {
            enqueueTaskRecord(topicName, inputTopicOrPatternPartition, timestamp, key, value, headers);
            completeAllProcessableWork();
        }

        if (globalInputTopicPartition != null) {
            processGlobalRecord(globalInputTopicPartition, timestamp, key, value, headers);
        }
    }

    private void enqueueTaskRecord(final String inputTopic,
                                   final TopicPartition topicOrPatternPartition,
                                   final long timestamp,
                                   final byte[] key,
                                   final byte[] value,
                                   final Headers headers) {
        final long offset = offsetsByTopicOrPatternPartition.get(topicOrPatternPartition).incrementAndGet() - 1;
        task.addRecords(topicOrPatternPartition, Collections.singleton(new ConsumerRecord<>(
            inputTopic,
            topicOrPatternPartition.partition(),
            offset,
            timestamp,
            TimestampType.CREATE_TIME,
            key == null ? ConsumerRecord.NULL_SIZE : key.length,
            value == null ? ConsumerRecord.NULL_SIZE : value.length,
            key,
            value,
            headers,
            Optional.empty()))
        );
    }

    private void completeAllProcessableWork() {
        // for internally triggered processing (like wall-clock punctuations),
        // we might have buffered some records to internal topics that need to
        // be piped back in to kick-start the processing loop. This is idempotent
        // and therefore harmless in the case where all we've done is enqueued an
        // input record from the user.
        captureOutputsAndReEnqueueInternalResults();

        // If the topology only has global tasks, then `task` would be null.
        // For this method, it just means there's nothing to do.
        if (task != null) {
            task.resumePollingForPartitionsWithAvailableSpace();
            task.updateLags();
            while (task.hasRecordsQueued() && task.isProcessable(mockWallClockTime.milliseconds())) {
                // Process the record ...
                task.process(mockWallClockTime.milliseconds());
                task.maybePunctuateStreamTime();
                commit(task.prepareCommit());
                task.postCommit(true);
                captureOutputsAndReEnqueueInternalResults();
            }
            if (task.hasRecordsQueued()) {
                log.info("Due to the {} configuration, there are currently some records" +
                             " that cannot be processed. Advancing wall-clock time or" +
                             " enqueuing records on the empty topics will allow" +
                             " Streams to process more.",
                         StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
            }
        }
    }

    private void commit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (processingMode == EXACTLY_ONCE_ALPHA || processingMode == EXACTLY_ONCE_V2) {
            testDriverProducer.commitTransaction(offsets, new ConsumerGroupMetadata("dummy-app-id"));
        } else {
            consumer.commitSync(offsets);
        }
    }

    private void processGlobalRecord(final TopicPartition globalInputTopicPartition,
                                     final long timestamp,
                                     final byte[] key,
                                     final byte[] value,
                                     final Headers headers) {
        globalStateTask.update(new ConsumerRecord<>(
            globalInputTopicPartition.topic(),
            globalInputTopicPartition.partition(),
            offsetsByTopicOrPatternPartition.get(globalInputTopicPartition).incrementAndGet() - 1,
            timestamp,
            TimestampType.CREATE_TIME,
            key == null ? ConsumerRecord.NULL_SIZE : key.length,
            value == null ? ConsumerRecord.NULL_SIZE : value.length,
            key,
            value,
            headers,
            Optional.empty())
        );
        globalStateTask.flushState();
    }

    private void validateSourceTopicNameRegexPattern(final String inputRecordTopic) {
        for (final String sourceTopicName : internalTopologyBuilder.fullSourceTopicNames()) {
            if (!sourceTopicName.equals(inputRecordTopic) && Pattern.compile(sourceTopicName).matcher(inputRecordTopic).matches()) {
                throw new TopologyException("Topology add source of type String for topic: " + sourceTopicName +
                                                " cannot contain regex pattern for input record topic: " + inputRecordTopic +
                                                " and hence cannot process the message.");
            }
        }
    }

    private TopicPartition getInputTopicOrPatternPartition(final String topicName) {
        if (!internalTopologyBuilder.fullSourceTopicNames().isEmpty()) {
            validateSourceTopicNameRegexPattern(topicName);
        }

        final TopicPartition topicPartition = partitionsByInputTopic.get(topicName);
        if (topicPartition == null) {
            for (final Map.Entry<String, TopicPartition> entry : partitionsByInputTopic.entrySet()) {
                if (Pattern.compile(entry.getKey()).matcher(topicName).matches()) {
                    return entry.getValue();
                }
            }
        }
        return topicPartition;
    }

    private void captureOutputsAndReEnqueueInternalResults() {
        // Capture all the records sent to the producer ...
        final List<ProducerRecord<byte[], byte[]>> output = producer.history();
        producer.clear();

        for (final ProducerRecord<byte[], byte[]> record : output) {
            outputRecordsByTopic.computeIfAbsent(record.topic(), k -> new LinkedList<>()).add(record);

            // Forward back into the topology if the produced record is to an internal or a source topic ...
            final String outputTopicName = record.topic();

            final TopicPartition inputTopicOrPatternPartition = getInputTopicOrPatternPartition(outputTopicName);
            final TopicPartition globalInputTopicPartition = globalPartitionsByInputTopic.get(outputTopicName);

            if (inputTopicOrPatternPartition != null) {
                enqueueTaskRecord(
                    outputTopicName,
                    inputTopicOrPatternPartition,
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    record.headers()
                );
            }

            if (globalInputTopicPartition != null) {
                processGlobalRecord(
                    globalInputTopicPartition,
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    record.headers()
                );
            }
        }
    }

    /**
     * Advances the internally mocked wall-clock time.
     * This might trigger a {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type
     * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) punctuations}.
     *
     * @param advance the amount of time to advance wall-clock time
     */
    public void advanceWallClockTime(final Duration advance) {
        Objects.requireNonNull(advance, "advance cannot be null");
        mockWallClockTime.sleep(advance.toMillis());
        if (task != null) {
            task.maybePunctuateSystemTime();
            commit(task.prepareCommit());
            task.postCommit(true);
        }
        completeAllProcessableWork();
    }

    private Queue<ProducerRecord<byte[], byte[]>> getRecordsQueue(final String topicName) {
        final Queue<ProducerRecord<byte[], byte[]>> outputRecords = outputRecordsByTopic.get(topicName);
        if (outputRecords == null && !processorTopology.sinkTopics().contains(topicName)) {
            log.warn("Unrecognized topic: {}, this can occur if dynamic routing is used and no output has been "
                         + "sent to this topic yet. If not using a TopicNameExtractor, check that the output topic "
                         + "is correct.", topicName);
        }
        return outputRecords;
    }

    /**
     * Create {@link TestInputTopic} to be used for piping records to topic
     * Uses current system time as start timestamp for records.
     * Auto-advance is disabled.
     *
     * @param topicName             the name of the topic
     * @param keySerializer   the Serializer for the key type
     * @param valueSerializer the Serializer for the value type
     * @param <K> the key type
     * @param <V> the value type
     * @return {@link TestInputTopic} object
     */
    public final <K, V> TestInputTopic<K, V> createInputTopic(final String topicName,
                                                              final Serializer<K> keySerializer,
                                                              final Serializer<V> valueSerializer) {
        return new TestInputTopic<>(this, topicName, keySerializer, valueSerializer, Instant.now(), Duration.ZERO);
    }

    /**
     * Create {@link TestInputTopic} to be used for piping records to topic
     * Uses provided start timestamp and autoAdvance parameter for records
     *
     * @param topicName             the name of the topic
     * @param keySerializer   the Serializer for the key type
     * @param valueSerializer the Serializer for the value type
     * @param startTimestamp Start timestamp for auto-generated record time
     * @param autoAdvance autoAdvance duration for auto-generated record time
     * @param <K> the key type
     * @param <V> the value type
     * @return {@link TestInputTopic} object
     */
    public final <K, V> TestInputTopic<K, V> createInputTopic(final String topicName,
                                                              final Serializer<K> keySerializer,
                                                              final Serializer<V> valueSerializer,
                                                              final Instant startTimestamp,
                                                              final Duration autoAdvance) {
        return new TestInputTopic<>(this, topicName, keySerializer, valueSerializer, startTimestamp, autoAdvance);
    }

    /**
     * Create {@link TestOutputTopic} to be used for reading records from topic
     *
     * @param topicName             the name of the topic
     * @param keyDeserializer   the Deserializer for the key type
     * @param valueDeserializer the Deserializer for the value type
     * @param <K> the key type
     * @param <V> the value type
     * @return {@link TestOutputTopic} object
     */
    public final <K, V> TestOutputTopic<K, V> createOutputTopic(final String topicName,
                                                                final Deserializer<K> keyDeserializer,
                                                                final Deserializer<V> valueDeserializer) {
        return new TestOutputTopic<>(this, topicName, keyDeserializer, valueDeserializer);
    }

    /**
     * Get all the names of all the topics to which records have been produced during the test run.
     * <p>
     * Call this method after piping the input into the test driver to retrieve the full set of topic names the topology
     * produced records to.
     * <p>
     * The returned set of topic names may include user (e.g., output) and internal (e.g., changelog, repartition) topic
     * names.
     *
     * @return the set of topic names the topology has produced to
     */
    public final Set<String> producedTopicNames() {
        return Collections.unmodifiableSet(outputRecordsByTopic.keySet());
    }

    ProducerRecord<byte[], byte[]> readRecord(final String topic) {
        final Queue<? extends ProducerRecord<byte[], byte[]>> outputRecords = getRecordsQueue(topic);
        if (outputRecords == null) {
            return null;
        }
        return outputRecords.poll();
    }

    <K, V> TestRecord<K, V> readRecord(final String topic,
                                       final Deserializer<K> keyDeserializer,
                                       final Deserializer<V> valueDeserializer) {
        final Queue<? extends ProducerRecord<byte[], byte[]>> outputRecords = getRecordsQueue(topic);
        if (outputRecords == null) {
            throw new NoSuchElementException("Uninitialized topic: " + topic);
        }
        final ProducerRecord<byte[], byte[]> record = outputRecords.poll();
        if (record == null) {
            throw new NoSuchElementException("Empty topic: " + topic);
        }
        final K key = keyDeserializer.deserialize(record.topic(), record.headers(), record.key());
        final V value = valueDeserializer.deserialize(record.topic(), record.headers(), record.value());
        return new TestRecord<>(key, value, record.headers(), record.timestamp());
    }

    <K, V> void pipeRecord(final String topic,
                           final TestRecord<K, V> record,
                           final Serializer<K> keySerializer,
                           final Serializer<V> valueSerializer,
                           final Instant time) {
        final byte[] serializedKey = keySerializer.serialize(topic, record.headers(), record.key());
        final byte[] serializedValue = valueSerializer.serialize(topic, record.headers(), record.value());
        final long timestamp;
        if (time != null) {
            timestamp = time.toEpochMilli();
        } else if (record.timestamp() != null) {
            timestamp = record.timestamp();
        } else {
            throw new IllegalStateException("Provided `TestRecord` does not have a timestamp and no timestamp overwrite was provided via `time` parameter.");
        }

        pipeRecord(topic, timestamp, serializedKey, serializedValue, record.headers());
    }

    final long getQueueSize(final String topic) {
        final Queue<ProducerRecord<byte[], byte[]>> queue = getRecordsQueue(topic);
        if (queue == null) {
            //Return 0 if not initialized, getRecordsQueue throw exception if non existing topic
            return 0;
        }
        return queue.size();
    }

    final boolean isEmpty(final String topic) {
        return getQueueSize(topic) == 0;
    }

    /**
     * Get all {@link StateStore StateStores} from the topology.
     * The stores can be a "regular" or global stores.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord)}  process an input message}, and/or to check the store afterward.
     * <p>
     * Note, that {@code StateStore} might be {@code null} if a store is added but not connected to any processor.
     * <p>
     * <strong>Caution:</strong> Using this method to access stores that are added by the DSL is unsafe as the store
     * types may change. Stores added by the DSL should only be accessed via the corresponding typed methods
     * like {@link #getKeyValueStore(String)} etc.
     *
     * @return all stores my name
     * @see #getStateStore(String)
     * @see #getKeyValueStore(String)
     * @see #getTimestampedKeyValueStore(String)
     * @see #getVersionedKeyValueStore(String)
     * @see #getWindowStore(String)
     * @see #getTimestampedWindowStore(String)
     * @see #getSessionStore(String)
     */
    public Map<String, StateStore> getAllStateStores() {
        final Map<String, StateStore> allStores = new HashMap<>();
        for (final String storeName : internalTopologyBuilder.allStateStoreNames()) {
            allStores.put(storeName, getStateStore(storeName, false));
        }
        return allStores;
    }

    /**
     * Get the {@link StateStore} with the given name.
     * The store can be a "regular" or global store.
     * <p>
     * Should be used for custom stores only.
     * For built-in stores, the corresponding typed methods like {@link #getKeyValueStore(String)} should be used.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the state store, or {@code null} if no store has been registered with the given name
     * @throws IllegalArgumentException if the store is a built-in store like {@link KeyValueStore},
     * {@link WindowStore}, or {@link SessionStore}
     *
     * @see #getAllStateStores()
     * @see #getKeyValueStore(String)
     * @see #getTimestampedKeyValueStore(String)
     * @see #getVersionedKeyValueStore(String)
     * @see #getWindowStore(String)
     * @see #getTimestampedWindowStore(String)
     * @see #getSessionStore(String)
     */
    public StateStore getStateStore(final String name) throws IllegalArgumentException {
        return getStateStore(name, true);
    }

    private StateStore getStateStore(final String name,
                                     final boolean throwForBuiltInStores) {
        if (task != null) {
            final StateStore stateStore = ((ProcessorContextImpl) task.processorContext()).stateManager().getStore(name);
            if (stateStore != null) {
                if (throwForBuiltInStores) {
                    throwIfBuiltInStore(stateStore);
                }
                return stateStore;
            }
        }

        if (globalStateManager != null) {
            final StateStore stateStore = globalStateManager.getStore(name);
            if (stateStore != null) {
                if (throwForBuiltInStores) {
                    throwIfBuiltInStore(stateStore);
                }
                return stateStore;
            }

        }

        return null;
    }

    private void throwIfBuiltInStore(final StateStore stateStore) {
        if (stateStore instanceof VersionedKeyValueStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a versioned key-value store and should be accessed via `getVersionedKeyValueStore()`");
        }
        if (stateStore instanceof TimestampedKeyValueStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a timestamped key-value store and should be accessed via `getTimestampedKeyValueStore()`");
        }
        if (stateStore instanceof ReadOnlyKeyValueStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a key-value store and should be accessed via `getKeyValueStore()`");
        }
        if (stateStore instanceof TimestampedWindowStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a timestamped window store and should be accessed via `getTimestampedWindowStore()`");
        }
        if (stateStore instanceof ReadOnlyWindowStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a window store and should be accessed via `getWindowStore()`");
        }
        if (stateStore instanceof ReadOnlySessionStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a session store and should be accessed via `getSessionStore()`");
        }
    }

    /**
     * Get the {@link KeyValueStore} or {@link TimestampedKeyValueStore} with the given name.
     * The store can be a "regular" or global store.
     * <p>
     * If the registered store is a {@link TimestampedKeyValueStore} this method will return a value-only query
     * interface. <strong>It is highly recommended to update the code for this case to avoid bugs and to use
     * {@link #getTimestampedKeyValueStore(String)} for full store access instead.</strong>
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the key value store, or {@code null} if no {@link KeyValueStore} or {@link TimestampedKeyValueStore}
     * has been registered with the given name
     * @see #getAllStateStores()
     * @see #getStateStore(String)
     * @see #getTimestampedKeyValueStore(String)
     * @see #getVersionedKeyValueStore(String)
     * @see #getWindowStore(String)
     * @see #getTimestampedWindowStore(String)
     * @see #getSessionStore(String)
     */
    @SuppressWarnings("unchecked")
    public <K, V> KeyValueStore<K, V> getKeyValueStore(final String name) {
        final StateStore store = getStateStore(name, false);
        if (store instanceof TimestampedKeyValueStore) {
            log.info("Method #getTimestampedKeyValueStore() should be used to access a TimestampedKeyValueStore.");
            return new KeyValueStoreFacade<>((TimestampedKeyValueStore<K, V>) store);
        }
        return store instanceof KeyValueStore ? (KeyValueStore<K, V>) store : null;
    }

    /**
     * Get the {@link TimestampedKeyValueStore} with the given name.
     * The store can be a "regular" or global store.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the key value store, or {@code null} if no {@link TimestampedKeyValueStore} has been registered with the given name
     * @see #getAllStateStores()
     * @see #getStateStore(String)
     * @see #getKeyValueStore(String)
     * @see #getVersionedKeyValueStore(String)
     * @see #getWindowStore(String)
     * @see #getTimestampedWindowStore(String)
     * @see #getSessionStore(String)
     */
    @SuppressWarnings("unchecked")
    public <K, V> KeyValueStore<K, ValueAndTimestamp<V>> getTimestampedKeyValueStore(final String name) {
        final StateStore store = getStateStore(name, false);
        return store instanceof TimestampedKeyValueStore ? (TimestampedKeyValueStore<K, V>) store : null;
    }

    /**
     * Get the {@link VersionedKeyValueStore} with the given name.
     * The store can be a "regular" or global store.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the key value store, or {@code null} if no {@link VersionedKeyValueStore} has been registered with the given name
     * @see #getAllStateStores()
     * @see #getStateStore(String)
     * @see #getKeyValueStore(String)
     * @see #getTimestampedKeyValueStore(String)
     * @see #getWindowStore(String)
     * @see #getTimestampedWindowStore(String)
     * @see #getSessionStore(String)
     */
    @SuppressWarnings("unchecked")
    public <K, V> VersionedKeyValueStore<K, V> getVersionedKeyValueStore(final String name) {
        final StateStore store = getStateStore(name, false);
        return store instanceof VersionedKeyValueStore ? (VersionedKeyValueStore<K, V>) store : null;
    }

    /**
     * Get the {@link WindowStore} or {@link TimestampedWindowStore} with the given name.
     * The store can be a "regular" or global store.
     * <p>
     * If the registered store is a {@link TimestampedWindowStore} this method will return a value-only query
     * interface. <strong>It is highly recommended to update the code for this case to avoid bugs and to use
     * {@link #getTimestampedWindowStore(String)} for full store access instead.</strong>
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the key value store, or {@code null} if no {@link WindowStore} or {@link TimestampedWindowStore}
     * has been registered with the given name
     * @see #getAllStateStores()
     * @see #getStateStore(String)
     * @see #getKeyValueStore(String)
     * @see #getTimestampedKeyValueStore(String)
     * @see #getVersionedKeyValueStore(String)
     * @see #getTimestampedWindowStore(String)
     * @see #getSessionStore(String)
     */
    @SuppressWarnings("unchecked")
    public <K, V> WindowStore<K, V> getWindowStore(final String name) {
        final StateStore store = getStateStore(name, false);
        if (store instanceof TimestampedWindowStore) {
            log.info("Method #getTimestampedWindowStore() should be used to access a TimestampedWindowStore.");
            return new WindowStoreFacade<>((TimestampedWindowStore<K, V>) store);
        }
        return store instanceof WindowStore ? (WindowStore<K, V>) store : null;
    }

    /**
     * Get the {@link TimestampedWindowStore} with the given name.
     * The store can be a "regular" or global store.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the key value store, or {@code null} if no {@link TimestampedWindowStore} has been registered with the given name
     * @see #getAllStateStores()
     * @see #getStateStore(String)
     * @see #getKeyValueStore(String)
     * @see #getTimestampedKeyValueStore(String)
     * @see #getVersionedKeyValueStore(String)
     * @see #getWindowStore(String)
     * @see #getSessionStore(String)
     */
    @SuppressWarnings("unchecked")
    public <K, V> WindowStore<K, ValueAndTimestamp<V>> getTimestampedWindowStore(final String name) {
        final StateStore store = getStateStore(name, false);
        return store instanceof TimestampedWindowStore ? (TimestampedWindowStore<K, V>) store : null;
    }

    /**
     * Get the {@link SessionStore} with the given name.
     * The store can be a "regular" or global store.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link TestInputTopic#pipeInput(TestRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the key value store, or {@code null} if no {@link SessionStore} has been registered with the given name
     * @see #getAllStateStores()
     * @see #getStateStore(String)
     * @see #getKeyValueStore(String)
     * @see #getTimestampedKeyValueStore(String)
     * @see #getVersionedKeyValueStore(String)
     * @see #getWindowStore(String)
     * @see #getTimestampedWindowStore(String)
     */
    @SuppressWarnings("unchecked")
    public <K, V> SessionStore<K, V> getSessionStore(final String name) {
        final StateStore store = getStateStore(name, false);
        return store instanceof SessionStore ? (SessionStore<K, V>) store : null;
    }

    /**
     * Close the driver, its topology, and all processors.
     */
    public void close() {
        if (task != null) {
            task.suspend();
            task.prepareCommit();
            task.postCommit(true);
            task.closeClean();
        }
        if (globalStateTask != null) {
            try {
                globalStateTask.close(false);
            } catch (final IOException e) {
                // ignore
            }
        }
        completeAllProcessableWork();
        if (task != null && task.hasRecordsQueued()) {
            log.warn("Found some records that cannot be processed due to the" +
                         " {} configuration during TopologyTestDriver#close().",
                     StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        }
        if (processingMode == AT_LEAST_ONCE) {
            producer.close();
        }
        stateDirectory.clean();
    }

    static class MockChangelogRegister implements ChangelogRegister {
        private final Set<TopicPartition> restoringPartitions = new HashSet<>();

        @Override
        public void register(final TopicPartition partition, final ProcessorStateManager stateManager) {
            restoringPartitions.add(partition);
        }

        @Override
        public void register(final Set<TopicPartition> changelogPartitions, final ProcessorStateManager stateManager) {
            for (final TopicPartition changelogPartition : changelogPartitions) {
                register(changelogPartition, stateManager);
            }
        }

        @Override
        public void unregister(final Collection<TopicPartition> partitions) {
            restoringPartitions.removeAll(partitions);
        }
    }

    static class MockTime implements Time {
        private final AtomicLong timeMs;
        private final AtomicLong highResTimeNs;

        MockTime(final long startTimestampMs) {
            this.timeMs = new AtomicLong(startTimestampMs);
            this.highResTimeNs = new AtomicLong(startTimestampMs * 1000L * 1000L);
        }

        @Override
        public long milliseconds() {
            return timeMs.get();
        }

        @Override
        public long nanoseconds() {
            return highResTimeNs.get();
        }

        @Override
        public long hiResClockMs() {
            return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
        }

        @Override
        public void sleep(final long ms) {
            if (ms < 0) {
                throw new IllegalArgumentException("Sleep ms cannot be negative.");
            }
            timeMs.addAndGet(ms);
            highResTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
        }

        @Override
        public void waitObject(final Object obj, final Supplier<Boolean> condition, final long timeoutMs) {
            throw new UnsupportedOperationException();
        }
    }

    static class KeyValueStoreFacade<K, V> extends ReadOnlyKeyValueStoreFacade<K, V> implements KeyValueStore<K, V> {

        public KeyValueStoreFacade(final TimestampedKeyValueStore<K, V> inner) {
            super(inner);
        }

        @Deprecated
        @Override
        public void init(final ProcessorContext context,
                         final StateStore root) {
            inner.init(context, root);
        }

        @Override
        public void init(final StateStoreContext context, final StateStore root) {
            inner.init(context, root);
        }

        @Override
        public void put(final K key,
                        final V value) {
            inner.put(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP));
        }

        @Override
        public V putIfAbsent(final K key,
                             final V value) {
            return getValueOrNull(inner.putIfAbsent(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP)));
        }

        @Override
        public void putAll(final List<KeyValue<K, V>> entries) {
            for (final KeyValue<K, V> entry : entries) {
                inner.put(entry.key, ValueAndTimestamp.make(entry.value, ConsumerRecord.NO_TIMESTAMP));
            }
        }

        @Override
        public V delete(final K key) {
            return getValueOrNull(inner.delete(key));
        }

        @Override
        public void flush() {
            inner.flush();
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public String name() {
            return inner.name();
        }

        @Override
        public boolean persistent() {
            return inner.persistent();
        }

        @Override
        public boolean isOpen() {
            return inner.isOpen();
        }

        @Override
        public Position getPosition() {
            return inner.getPosition();
        }
    }

    static class WindowStoreFacade<K, V> extends ReadOnlyWindowStoreFacade<K, V> implements WindowStore<K, V> {

        public WindowStoreFacade(final TimestampedWindowStore<K, V> store) {
            super(store);
        }

        @Deprecated
        @Override
        public void init(final ProcessorContext context,
                         final StateStore root) {
            inner.init(context, root);
        }

        @Override
        public void init(final StateStoreContext context, final StateStore root) {
            inner.init(context, root);
        }

        @Override
        public void put(final K key,
                        final V value,
                        final long windowStartTimestamp) {
            inner.put(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP), windowStartTimestamp);
        }

        @Override
        public WindowStoreIterator<V> fetch(final K key,
                                            final long timeFrom,
                                            final long timeTo) {
            return fetch(key, Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
        }

        @Override
        public WindowStoreIterator<V> backwardFetch(final K key,
                                                    final long timeFrom,
                                                    final long timeTo) {
            return backwardFetch(key, Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                      final K keyTo,
                                                      final long timeFrom,
                                                      final long timeTo) {
            return fetch(keyFrom, keyTo, Instant.ofEpochMilli(timeFrom),
                Instant.ofEpochMilli(timeTo));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom,
                                                              final K keyTo,
                                                              final long timeFrom,
                                                              final long timeTo) {
            return backwardFetch(keyFrom, keyTo, Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                         final long timeTo) {
            return fetchAll(Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final long timeFrom,
                                                                 final long timeTo) {
            return backwardFetchAll(Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
        }

        @Override
        public void flush() {
            inner.flush();
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public String name() {
            return inner.name();
        }

        @Override
        public boolean persistent() {
            return inner.persistent();
        }

        @Override
        public boolean isOpen() {
            return inner.isOpen();
        }

        @Override
        public Position getPosition() {
            return inner.getPosition();
        }
    }

    private static class TestDriverProducer extends StreamsProducer {

        public TestDriverProducer(final StreamsConfig config,
                                  final KafkaClientSupplier clientSupplier,
                                  final LogContext logContext,
                                  final Time time) {
            super(config, "TopologyTestDriver-StreamThread-1", clientSupplier, new TaskId(0, 0), UUID.randomUUID(), logContext, time);
        }

        @Override
        public void commitTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                      final ConsumerGroupMetadata consumerGroupMetadata) throws ProducerFencedException {
            super.commitTransaction(offsets, consumerGroupMetadata);
        }
    }
}
