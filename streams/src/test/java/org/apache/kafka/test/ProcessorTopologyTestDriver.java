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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateManagerImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateUpdateTask;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StoreChangelogReader;
import org.apache.kafka.streams.processor.internals.StreamTask;
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class makes it easier to write tests to verify the behavior of topologies created with a {@link Topology}.
 * You can test simple topologies that have a single processor, or very complex topologies that have multiple sources, processors,
 * and sinks. And because it starts with a {@link Topology}, you can create topologies specific to your tests or you
 * can use and test code you already have that uses a builder to create topologies. Best of all, the class works without a real
 * Kafka broker, so the tests execute very quickly with very little overhead.
 * <p>
 * Using the ProcessorTopologyTestDriver in tests is easy: simply instantiate the driver with a {@link StreamsConfig} and a
 * TopologyBuilder, use the driver to supply an input message to the topology, and then use the driver to read and verify any
 * messages output by the topology.
 * <p>
 * Although the driver doesn't use a real Kafka broker, it does simulate Kafka {@link org.apache.kafka.clients.consumer.Consumer}s
 * and {@link org.apache.kafka.clients.producer.Producer}s that read and write raw {@code byte[]} messages. You can either deal
 * with messages that have {@code byte[]} keys and values, or you can supply the {@link Serializer}s and {@link Deserializer}s
 * that the driver can use to convert the keys and values into objects.
 *
 * <h2>Driver setup</h2>
 * <p>
 * In order to create a ProcessorTopologyTestDriver instance, you need a TopologyBuilder and a {@link StreamsConfig}. The
 * configuration needs to be representative of what you'd supply to the real topology, so that means including several key
 * properties. For example, the following code fragment creates a configuration that specifies a local Kafka broker list
 * (which is needed but not used), a timestamp extractor, and default serializers and deserializers for string keys and values:
 *
 * <pre>
 * StringSerializer strSerializer = new StringSerializer();
 * StringDeserializer strDeserializer = new StringDeserializer();
 * Properties props = new Properties();
 * props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
 * props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
 * props.setProperty(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, strSerializer.getClass().getName());
 * props.setProperty(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, strDeserializer.getClass().getName());
 * props.setProperty(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, strSerializer.getClass().getName());
 * props.setProperty(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, strDeserializer.getClass().getName());
 * StreamsConfig config = new StreamsConfig(props);
 * TopologyBuilder builder = ...
 * ProcessorTopologyTestDriver driver = new ProcessorTopologyTestDriver(config, builder);
 * </pre>
 *
 * <h2>Processing messages</h2>
 * <p>
 * Your test can supply new input records on any of the topics that the topology's sources consume. Here's an example of an
 * input message on the topic named {@code input-topic}:
 *
 * <pre>
 * driver.process("input-topic", "key1", "value1", strSerializer, strSerializer);
 * </pre>
 *
 * Immediately, the driver will pass the input message through to the appropriate source that consumes the named topic,
 * and will invoke the processor(s) downstream of the source. If your topology's processors forward messages to sinks,
 * your test can then consume these output messages to verify they match the expected outcome. For example, if our topology
 * should have generated 2 messages on {@code output-topic-1} and 1 message on {@code output-topic-2}, then our test can
 * obtain these messages using the {@link #readOutput(String, Deserializer, Deserializer)} method:
 *
 * <pre>
 * ProducerRecord<String, String> record1 = driver.readOutput("output-topic-1", strDeserializer, strDeserializer);
 * ProducerRecord<String, String> record2 = driver.readOutput("output-topic-1", strDeserializer, strDeserializer);
 * ProducerRecord<String, String> record3 = driver.readOutput("output-topic-2", strDeserializer, strDeserializer);
 * </pre>
 *
 * Again, our example topology generates messages with string keys and values, so we supply our string deserializer instance
 * for use on both the keys and values. Your test logic can then verify whether these output records are correct.
 * <p>
 * Finally, when completed, make sure your tests {@link #close()} the driver to release all resources and
 * {@link org.apache.kafka.streams.processor.Processor}s.
 *
 * <h2>Processor state</h2>
 * <p>
 * Some processors use Kafka {@link StateStore state storage}, so this driver class provides the {@link #getStateStore(String)}
 * and {@link #getKeyValueStore(String)} methods so that your tests can check the underlying state store(s) used by your
 * topology's processors. In our previous example, after we supplied a single input message and checked the three output messages,
 * our test could also check the key value store to verify the processor correctly added, removed, or updated internal state.
 * Or, our test might have pre-populated some state <em>before</em> submitting the input message, and verified afterward that the
 * processor(s) correctly updated the state.
 */
public class ProcessorTopologyTestDriver {

    private final static String APPLICATION_ID = "test-driver-application";
    private final static int PARTITION_ID = 0;
    private final static TaskId TASK_ID = new TaskId(0, PARTITION_ID);

    private final ProcessorTopology topology;
    private final MockProducer<byte[], byte[]> producer;
    private final Map<String, TopicPartition> partitionsByTopic = new HashMap<>();
    private final Map<TopicPartition, AtomicLong> offsetsByTopicPartition = new HashMap<>();
    private final Map<String, Queue<ProducerRecord<byte[], byte[]>>> outputRecordsByTopic = new HashMap<>();
    private final Set<String> internalTopics = new HashSet<>();
    private final Map<String, TopicPartition> globalPartitionsByTopic = new HashMap<>();
    private StreamTask task;
    private GlobalStateUpdateTask globalStateTask;

    /**
     * Create a new test driver instance.
     *
     * @param config  the stream configuration for the topology
     * @param builder the topology builder that will be used to create the topology instance
     */
    public ProcessorTopologyTestDriver(final StreamsConfig config,
                                       final InternalTopologyBuilder builder) {
        topology = builder.setApplicationId(APPLICATION_ID).build(null);
        final ProcessorTopology globalTopology = builder.buildGlobalStateTopology();

        // Set up the consumer and producer ...
        final Consumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();
        producer = new MockProducer<byte[], byte[]>(true, bytesSerializer, bytesSerializer) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.singletonList(new PartitionInfo(topic, PARTITION_ID, null, null, null));
            }
        };

        // Identify internal topics for forwarding in process ...
        for (final InternalTopologyBuilder.TopicsInfo topicsInfo : builder.topicGroups().values()) {
            internalTopics.addAll(topicsInfo.repartitionSourceTopics.keySet());
        }

        // Set up all of the topic+partition information and subscribe the consumer to each ...
        for (final String topic : topology.sourceTopics()) {
            final TopicPartition tp = new TopicPartition(topic, PARTITION_ID);
            partitionsByTopic.put(topic, tp);
            offsetsByTopicPartition.put(tp, new AtomicLong());
        }

        consumer.assign(offsetsByTopicPartition.keySet());

        final StateDirectory stateDirectory = new StateDirectory(config, Time.SYSTEM);
        final StreamsMetrics streamsMetrics = new MockStreamsMetrics(new Metrics());
        final ThreadCache cache = new ThreadCache(new LogContext("mock "), 1024 * 1024, streamsMetrics);

        if (globalTopology != null) {
            final MockConsumer<byte[], byte[]> globalConsumer = createGlobalConsumer();
            final MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
            for (final String topicName : globalTopology.sourceTopics()) {
                final List<PartitionInfo> partitionInfos = new ArrayList<>();
                partitionInfos.add(new PartitionInfo(topicName, 1, null, null, null));
                globalConsumer.updatePartitions(topicName, partitionInfos);
                final TopicPartition partition = new TopicPartition(topicName, 1);
                globalConsumer.updateEndOffsets(Collections.singletonMap(partition, 0L));
                globalPartitionsByTopic.put(topicName, partition);
                offsetsByTopicPartition.put(partition, new AtomicLong());
            }
            final GlobalStateManagerImpl stateManager = new GlobalStateManagerImpl(
                new LogContext("mock "),
                globalTopology,
                globalConsumer,
                stateDirectory,
                stateRestoreListener,
                config);
            final GlobalProcessorContextImpl globalProcessorContext = new GlobalProcessorContextImpl(config, stateManager, streamsMetrics, cache);
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
                topology,
                consumer,
                new StoreChangelogReader(
                    createRestoreConsumer(topology.storeToChangelogTopic()),
                    new MockStateRestoreListener(),
                    new LogContext("topology-test-driver ")
                ),
                config,
                streamsMetrics,
                stateDirectory,
                cache,
                new MockTime(),
                producer
            );
            task.initializeStateStores();
            task.initializeTopology();
        }
    }

    /**
     * Send an input message with the given key, value and timestamp on the specified topic to the topology, and then commit the messages.
     *
     * @param topicName the name of the topic on which the message is to be sent
     * @param key       the raw message key
     * @param value     the raw message value
     * @param timestamp the raw message timestamp
     */
    public void process(final String topicName,
                        final byte[] key,
                        final byte[] value,
                        final long timestamp) {

        final TopicPartition tp = partitionsByTopic.get(topicName);
        if (tp != null) {
            // Add the record ...
            final long offset = offsetsByTopicPartition.get(tp).incrementAndGet();
            task.addRecords(tp, records(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, timestamp, TimestampType.CREATE_TIME, 0L, 0, 0, key, value)));
            producer.clear();

            // Process the record ...
            task.process();
            ((InternalProcessorContext) task.context()).setRecordContext(new ProcessorRecordContext(timestamp, offset, tp.partition(), topicName));
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
                if (internalTopics.contains(record.topic()) || topology.sourceTopics().contains(record.topic())) {
                    process(record.topic(), record.key(), record.value(), record.timestamp());
                }
            }
        } else {
            final TopicPartition global = globalPartitionsByTopic.get(topicName);
            if (global == null) {
                throw new IllegalArgumentException("Unexpected topic: " + topicName);
            }
            final long offset = offsetsByTopicPartition.get(global).incrementAndGet();
            globalStateTask.update(new ConsumerRecord<>(global.topic(), global.partition(), offset, timestamp, TimestampType.CREATE_TIME, 0L, 0, 0, key, value));
            globalStateTask.flushState();
        }
    }

    /**
     * Send an input message with the given key and value on the specified topic to the topology.
     *
     * @param topicName the name of the topic on which the message is to be sent
     * @param key       the raw message key
     * @param value     the raw message value
     */
    public void process(final String topicName,
                        final byte[] key,
                        final byte[] value) {
        process(topicName, key, value, 0L);
    }

    /**
     * Send an input message with the given key and value on the specified topic to the topology.
     *
     * @param topicName       the name of the topic on which the message is to be sent
     * @param key             the raw message key
     * @param value           the raw message value
     * @param keySerializer   the serializer for the key
     * @param valueSerializer the serializer for the value
     */
    public <K, V> void process(final String topicName,
                               final K key,
                               final V value,
                               final Serializer<K> keySerializer,
                               final Serializer<V> valueSerializer) {
        process(topicName, key, value, keySerializer, valueSerializer, 0L);
    }

    /**
     * Send an input message with the given key and value and timestamp on the specified topic to the topology.
     *
     * @param topicName       the name of the topic on which the message is to be sent
     * @param key             the raw message key
     * @param value           the raw message value
     * @param keySerializer   the serializer for the key
     * @param valueSerializer the serializer for the value
     * @param timestamp       the raw message timestamp
     */
    public <K, V> void process(final String topicName,
                               final K key,
                               final V value,
                               final Serializer<K> keySerializer,
                               final Serializer<V> valueSerializer,
                               final long timestamp) {
        process(topicName, keySerializer.serialize(topicName, key), valueSerializer.serialize(topicName, value), timestamp);
    }

    /**
     * Read the next record from the given topic. These records were output by the topology during the previous calls to
     * {@link #process(String, byte[], byte[])}.
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
     * {@link #process(String, byte[], byte[])}.
     *
     * @param topic             the name of the topic
     * @param keyDeserializer   the deserializer for the key type
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

    private Iterable<ConsumerRecord<byte[], byte[]>> records(final ConsumerRecord<byte[], byte[]> record) {
        return Collections.singleton(record);
    }

    /**
     * Get the {@link StateStore} with the given name. The name should have been supplied via
     * {@link #ProcessorTopologyTestDriver(StreamsConfig, InternalTopologyBuilder) this object's constructor}, and is
     * presumed to be used by a Processor within the topology.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link #process(String, byte[], byte[]) process an input message}, and/or to check the store afterward.
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
     * {@link #ProcessorTopologyTestDriver(StreamsConfig, InternalTopologyBuilder) this object's constructor}, and is
     * presumed to be used by a Processor within the topology.
     * <p>
     * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
     * {@link #process(String, byte[], byte[]) process an input message}, and/or to check the store afterward.
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

    /**
     * Utility method that creates the {@link MockConsumer} used for restoring state, which should not be done by this
     * driver object unless this method is overwritten with a functional consumer.
     *
     * @param storeToChangelogTopic the map of the names of the stores to the changelog topics
     * @return the mock consumer; never null
     */
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
        // For each store ...
        for (final Map.Entry<String, String> storeAndTopic : storeToChangelogTopic.entrySet()) {
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

    private MockConsumer<byte[], byte[]> createGlobalConsumer() {
        return new MockConsumer<byte[], byte[]>(OffsetResetStrategy.LATEST) {
            @Override
            public synchronized void seekToEnd(final Collection<TopicPartition> partitions) {}

            @Override
            public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {}

            @Override
            public synchronized long position(final TopicPartition partition) {
                return 0L;
            }
        };
    }

}
