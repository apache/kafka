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
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.InternalTopologyBuilderAccessor;
import org.apache.kafka.streams.MockTime;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateManagerImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateUpdateTask;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StoreChangelogReader;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class makes it easier to write tests to verify the behavior of topologies created with a {@link Topology} or
 * {@link StreamsBuilder}.
 * You can test simple topologies that have a single processor, or very complex topologies that have multiple sources,
 * processors, and sinks.
 * Best of all, the class works without a real Kafka broker, so the tests execute very quickly with very little overhead.
 * <p>
 * Using the {@code TopologyTestDriver} in tests is easy: simply instantiate the driver and provide
 * {@link Properties configs} and a {@link Topology} (cf. {@link StreamsBuilder#build()}), use the driver to supply an
 * input message to the topology, and then use the driver to read and verify any messages output by the topology.
 * <p>
 * Although the driver doesn't use a real Kafka broker, it does simulate Kafka {@link Consumer consumers} and
 * {@link Producer producers} that read and write raw {@code byte[]} messages.
 * TODO update next paragraph
 * You can either deal with messages that have {@code byte[]} keys and values, or you can supply the
 * {@link Serializer serializers} and {@link Deserializer deserializer} that the driver can use to convert the keys and
 * values into objects.
 *
 * <h2>Driver setup</h2>
 * In order to create a {@code TopologyTestDriver} instance, you need a {@link Topology} and a {@link Properties config}.
 * The configuration needs to be representative of what you'd supply to the real topology, so that means including
 * several key properties (cf. {@link StreamsConfig}).
 * For example, the following code fragment creates a configuration that specifies a local Kafka broker list (which is
 * needed but not used), a timestamp extractor, and default serializers and deserializers for string keys and values:
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
 * props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
 * props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 * props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 * Topology topology = ...
 * TopologyTestDriver driver = new TopologyTestDriver(topology, props);
 * }</pre>
 *
 * <h2>Processing messages</h2>
 * <p>
 * Your test can supply new input records on any of the topics that the topology's sources consume.
 * This test driver simulates single partition input topics.
 * Here's an example of an input message on the topic named {@code input-topic}:
 *
 * <pre>
 * driver.process("input-topic", "key1", "value1", strSerializer, strSerializer);
 * </pre>
 *
 * {@link #process(ConsumerRecord) process(...)}
 * It's recommended to check them out.
 * In addition, you can use {@link ConsumerRecordFactory} to generate {@link ConsumerRecord ConsumerRecords} that can be
 * ingested into the driver via {@link #process(ConsumerRecord)}.
 * <p>
 * When {@code #process()} is called, the driver passes the input message through to the appropriate source that
 * consumes the named topic, and will invoke the processor(s) downstream of the source.
 * If your topology's processors forward messages to sinks, your test can then consume these output messages to verify
 * they match the expected outcome.
 * For example, if our topology should have generated 2 messages on {@code output-topic-1} and 1 message on
 * {@code output-topic-2}, then our test can obtain these messages using the
 * {@link #readOutput(String, Deserializer, Deserializer)} method:
 *
 * <pre>{@code
 * ProducerRecord<String, String> record1 = driver.readOutput("output-topic-1", strDeserializer, strDeserializer);
 * ProducerRecord<String, String> record2 = driver.readOutput("output-topic-1", strDeserializer, strDeserializer);
 * ProducerRecord<String, String> record3 = driver.readOutput("output-topic-2", strDeserializer, strDeserializer);
 * }</pre>
 *
 * Again, our example topology generates messages with string keys and values, so we supply our string deserializer
 * instance for use on both the keys and values. Your test logic can then verify whether these output records are
 * correct.
 * <p>
 * Note, that calling {@code process()} will only trigger {@link PunctuationType#STREAM_TIME event-time} base
 * {@link ProcessorContext#schedule(long, PunctuationType, Punctuator) punctuation} call backs.
 * However, you can test {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type punctuations manually via
 * {@link #advanceWallClockTime(long)}.
 * <p>
 * Finally, when completed, make sure your tests {@link #close()} the driver to release all resources and
 * {@link org.apache.kafka.streams.processor.Processor processors}.
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
 */
@InterfaceStability.Evolving
public class TopologyTestDriver {

    private final Time mockTime;

    private final static int PARTITION_ID = 0;
    private final static TaskId TASK_ID = new TaskId(0, PARTITION_ID);
    private StreamTask task;
    private GlobalStateUpdateTask globalStateTask;

    private final ProcessorTopology processorTopology;
    private final MockProducer<byte[], byte[]> producer;

    private final Set<String> internalTopics = new HashSet<>();
    private final Map<String, TopicPartition> partitionsByTopic = new HashMap<>();
    private final Map<String, TopicPartition> globalPartitionsByTopic = new HashMap<>();
    private final Map<TopicPartition, AtomicLong> offsetsByTopicPartition = new HashMap<>();

    private final Map<String, Queue<ProducerRecord<byte[], byte[]>>> outputRecordsByTopic = new HashMap<>();

    /**
     * Create a new test diver instance.
     * Initialized the internally mocked wall-clock time with {@link System#currentTimeMillis() current system time}.
     *
     * @param topology the topology to be tested
     * @param config the configuration for the topology
     */
    public TopologyTestDriver(final Topology topology,
                              final Properties config) {
        this(topology, config, System.currentTimeMillis());
    }
    /**
     * Create a new test diver instance.
     *
     * @param topology the topology to be tested
     * @param config the configuration for the topology
     * @param initialWallClockTimeMs the initial value of internally mocked wall-clock time
     */
    public TopologyTestDriver(final Topology topology,
                              final Properties config,
                              final long initialWallClockTimeMs) {
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        mockTime = new MockTime(initialWallClockTimeMs);

        InternalTopologyBuilder builder = InternalTopologyBuilderAccessor.getInternalTopologyBuilder(topology);
        builder.setApplicationId(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG));

        processorTopology = builder.build(null);
        final ProcessorTopology globalTopology  = builder.buildGlobalStateTopology();

        final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();
        producer = new MockProducer<byte[], byte[]>(true, bytesSerializer, bytesSerializer) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.singletonList(new PartitionInfo(topic, PARTITION_ID, null, null, null));
            }
        };

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final StateDirectory stateDirectory = new StateDirectory(streamsConfig, mockTime);
        final StreamsMetrics streamsMetrics = new StreamsMetricsImpl(
            new Metrics(),
            "topology-test-driver-stream-metrics",
            Collections.<String, String>emptyMap());
        final ThreadCache cache = new ThreadCache(
            new LogContext("topology-test-driver "),
            Math.max(0, streamsConfig.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG)),
            streamsMetrics);
        final StateRestoreListener stateRestoreListener = new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {}

            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {}

            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {}
        };

        for (final InternalTopologyBuilder.TopicsInfo topicsInfo : builder.topicGroups().values()) {
            internalTopics.addAll(topicsInfo.repartitionSourceTopics.keySet());
        }

        for (final String topic : processorTopology.sourceTopics()) {
            final TopicPartition tp = new TopicPartition(topic, PARTITION_ID);
            partitionsByTopic.put(topic, tp);
            offsetsByTopicPartition.put(tp, new AtomicLong());
        }
        consumer.assign(partitionsByTopic.values());

        if (globalTopology != null) {
            for (final String topicName : globalTopology.sourceTopics()) {
                final TopicPartition partition = new TopicPartition(topicName, 0);
                globalPartitionsByTopic.put(topicName, partition);
                offsetsByTopicPartition.put(partition, new AtomicLong());
                consumer.updatePartitions(topicName, Collections.singletonList(
                    new PartitionInfo(topicName, 0, null, null, null)));
                consumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
                consumer.updateEndOffsets(Collections.singletonMap(partition, 0L));
            }

            final GlobalStateManagerImpl stateManager = new GlobalStateManagerImpl(
                new LogContext("mock "),
                globalTopology,
                consumer,
                stateDirectory,
                stateRestoreListener,
                streamsConfig);

            final GlobalProcessorContextImpl globalProcessorContext
                = new GlobalProcessorContextImpl(streamsConfig, stateManager, streamsMetrics, cache);
            stateManager.setGlobalProcessorContext(globalProcessorContext);

            globalStateTask = new GlobalStateUpdateTask(
                globalTopology,
                globalProcessorContext,
                stateManager,
                new LogAndContinueExceptionHandler(),
                new LogContext());
            globalStateTask.initialize();
        }

        if (!partitionsByTopic.isEmpty()) {
            task = new StreamTask(
                TASK_ID,
                partitionsByTopic.values(),
                processorTopology,
                consumer,
                new StoreChangelogReader(
                    createRestoreConsumer(processorTopology.storeToChangelogTopic()),
                    stateRestoreListener,
                    new LogContext("topology-test-driver ")),
                streamsConfig,
                streamsMetrics,
                stateDirectory,
                cache,
                mockTime,
                producer);
            task.initialize();
        }
    }

    /**
     * Send an input message with the given key, value, and timestamp on the specified topic to the topology and then
     * commit the messages.
     *
     * @param consumerRecord the record to be processed
     */
    public void process(final ConsumerRecord<byte[], byte[]> consumerRecord) {
        final String topicName = consumerRecord.topic();

        final TopicPartition topicPartition = partitionsByTopic.get(topicName);
        if (topicPartition != null) {
            final long offset = offsetsByTopicPartition.get(topicPartition).incrementAndGet() - 1;
            task.addRecords(topicPartition, Collections.singleton(new ConsumerRecord<>(
                topicName,
                topicPartition.partition(),
                offset,
                consumerRecord.timestamp(),
                consumerRecord.timestampType(),
                consumerRecord.checksum(),
                consumerRecord.serializedKeySize(),
                consumerRecord.serializedValueSize(),
                consumerRecord.key(),
                consumerRecord.value())));
            producer.clear();

            // Process the record ...
            ((InternalProcessorContext) task.context()).setRecordContext(new ProcessorRecordContext(consumerRecord.timestamp(), offset, topicPartition.partition(), topicName));
            task.process();
            task.maybePunctuateStreamTime();
            task.commit();

            // Capture all the records sent to the producer ...
            for (final ProducerRecord<byte[], byte[]> record : producer.history()) {
                Queue<ProducerRecord<byte[], byte[]>> outputRecords = outputRecordsByTopic.get(record.topic());
                if (outputRecords == null) {
                    outputRecords = new LinkedList<>();
                    outputRecordsByTopic.put(record.topic(), outputRecords);
                }
                outputRecords.add(record);

                // Forward back into the topology if the produced record is to an internal or a source topic ...
                final String outputTopicName = record.topic();
                if (internalTopics.contains(outputTopicName) || processorTopology.sourceTopics().contains(outputTopicName)) {
                    final byte[] serializedKey = record.key();
                    final byte[] serializedValue = record.value();

                    process(new ConsumerRecord<>(
                        outputTopicName,
                        -1,
                        -1L,
                        record.timestamp(),
                        TimestampType.CREATE_TIME,
                        0L,
                        serializedKey == null ? 0 : serializedKey.length,
                        serializedValue == null ? 0 : serializedValue.length,
                        serializedKey,
                        serializedValue));
                }
            }
        } else {
            final TopicPartition globalTopicPartition = globalPartitionsByTopic.get(topicName);
            if (globalTopicPartition == null) {
                throw new IllegalArgumentException("Unknown topic: " + topicName);
            }
            final long offset = offsetsByTopicPartition.get(globalTopicPartition).incrementAndGet() - 1;
            globalStateTask.update(new ConsumerRecord<>(
                globalTopicPartition.topic(),
                globalTopicPartition.partition(),
                offset,
                consumerRecord.timestamp(),
                consumerRecord.timestampType(),
                consumerRecord.checksum(),
                consumerRecord.serializedKeySize(),
                consumerRecord.serializedValueSize(),
                consumerRecord.key(),
                consumerRecord.value()));
            globalStateTask.flushState();
        }
    }

    /**
     * Send input messages to the topology and then commit each message individually.
     *
     * @param records a lit of record to be processed
     */
    public void process(final List<ConsumerRecord<byte[], byte[]>> records) {
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            process(record);
        }
    }

    /**
     * Advances the internally mocked wall-clock time.
     * This might trigger a {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type
     * {@link ProcessorContext#schedule(long, PunctuationType, Punctuator) punctuation}.
     *
     * @param advanceMs the amount of time to advance wall-clock time in milliseconds
     */
    public void advanceWallClockTime(final long advanceMs) {
        mockTime.sleep(advanceMs);
        task.maybePunctuateSystemTime();
        task.commit();
    }

    /**
     * Read the next record from the given topic. These records were output by the topology during the previous calls to
     * {@link #process(ConsumerRecord)}.
     *
     * @param topic the name of the topic
     * @return the next record on that topic, or null if there is no record available
     */
    public ProducerRecord<byte[], byte[]> readOutput(final String topic) {
        final Queue<ProducerRecord<byte[], byte[]>> outputRecords = outputRecordsByTopic.get(topic);
        if (outputRecords == null) {
            return null;
        }
        return outputRecords.poll();
    }

    /**
     * Read the next record from the given topic. These records were output by the topology during the previous calls to
     * {@link #process(ConsumerRecord)}.
     *
     * @param topic the name of the topic
     * @param keyDeserializer the deserializer for the key type
     * @param valueDeserializer the deserializer for the value type
     * @return the next record on that topic, or null if there is no record available
     */
    public <K, V> ProducerRecord<K, V> readOutput(final String topic,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer) {
        final ProducerRecord<byte[], byte[]> record = readOutput(topic);
        if (record == null) {
            return null;
        }
        final K key = keyDeserializer.deserialize(record.topic(), record.key());
        final V value = valueDeserializer.deserialize(record.topic(), record.value());
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), key, value);
    }

    /**
     * Get the {@link StateStore} with the given name. The name should have been supplied via
     * {@link #TopologyTestDriver(Topology, Properties) this object's constructor}, and is
     * presumed to be used by a Processor within the topology.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link #process(ConsumerRecord) process an input message}, and/or to check the store afterward.
     *
     * @param name the name of the store
     * @return the state store, or null if no store has been registered with the given name
     * @see #getKeyValueStore(String)
     */
    public StateStore getStateStore(final String name) {
        return ((ProcessorContextImpl) task.context()).getStateMgr().getStore(name);
    }

    /**
     * Get the {@link KeyValueStore} with the given name. The name should have been supplied via
     * {@link #TopologyTestDriver(Topology, Properties) this object's constructor}, and is
     * presumed to be used by a Processor within the topology.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link #process(ConsumerRecord) process an input message}, and/or to check the store afterward.
     * <p>
     *
     * @param name the name of the store
     * @return the key value store, or null if no {@link KeyValueStore} has been registered with the given name
     * @see #getStateStore(String)
     */
    @SuppressWarnings("unchecked")
    public <K, V> KeyValueStore<K, V> getKeyValueStore(final String name) {
        final StateStore store = getStateStore(name);
        return store instanceof KeyValueStore ? (KeyValueStore<K, V>) getStateStore(name) : null;
    }

    /**
     * Close the driver, its topology, and all processors.
     */
    public void close() {
        if (task != null) {
            task.close(true, false);
        }
        if (globalStateTask != null) {
            try {
                globalStateTask.close();
            } catch (final IOException e) {
                // ignore
            }
        }
    }

    private MockConsumer<byte[], byte[]> createRestoreConsumer(final Map<String, String> storeToChangelogTopic) {
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.LATEST) {
            @Override
            public synchronized void seekToEnd(final Collection<TopicPartition> partitions) {}

            @Override
            public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {}

            @Override
            public synchronized long position(final TopicPartition partition) {
                return 0L;
            }
        };

        // for each store
        for (final Map.Entry<String, String> storeAndTopic: storeToChangelogTopic.entrySet()) {
            final String topicName = storeAndTopic.getValue();
            // Set up the restore-state topic ...
            // consumer.subscribe(new TopicPartition(topicName, 1));
            // Set up the partition that matches the ID (which is what ProcessorStateManager expects) ...
            final List<PartitionInfo> partitionInfos = new ArrayList<>();
            partitionInfos.add(new PartitionInfo(topicName, PARTITION_ID, null, null, null));
            consumer.updatePartitions(topicName, partitionInfos);
            consumer.updateEndOffsets(Collections.singletonMap(new TopicPartition(topicName, PARTITION_ID), 0L));
        }
        return consumer;
    }

}
