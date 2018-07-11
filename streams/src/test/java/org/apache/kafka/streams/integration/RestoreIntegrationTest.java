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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

@Category({IntegrationTest.class})
public class RestoreIntegrationTest {
    private static final int NUM_BROKERS = 1;

    private static final String APPID = "restore-test";

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
            new EmbeddedKafkaCluster(NUM_BROKERS);
    private static final String INPUT_STREAM = "input-stream";
    private static final String INPUT_STREAM_2 = "input-stream-2";
    private final int numberOfKeys = 10000;
    private KafkaStreams kafkaStreams;

    @BeforeClass
    public static void createTopics() throws InterruptedException {
        CLUSTER.createTopic(INPUT_STREAM, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(APPID + "-store-changelog", 2, 1);
    }

    private Properties props(final String applicationId) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(applicationId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void shouldRestoreStateFromSourceTopic() throws Exception {
        final AtomicInteger numReceived = new AtomicInteger(0);
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties props = props(APPID);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        // restoring from 1000 to 4000 (committed), and then process from 4000 to 5000 on each of the two partitions
        final int offsetLimitDelta = 1000;
        final int offsetCheckpointed = 1000;
        createStateForRestoration(INPUT_STREAM);
        setCommittedOffset(INPUT_STREAM, offsetLimitDelta);

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime());
        new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 0)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(INPUT_STREAM, 0), (long) offsetCheckpointed));
        new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 1)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(INPUT_STREAM, 1), (long) offsetCheckpointed));

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        builder.table(INPUT_STREAM, Consumed.with(Serdes.Integer(), Serdes.Integer()))
                .toStream()
                .foreach(new ForeachAction<Integer, Integer>() {
                    @Override
                    public void apply(final Integer key, final Integer value) {
                        if (numReceived.incrementAndGet() == 2 * offsetLimitDelta)
                            shutdownLatch.countDown();
                    }
                });

        kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    startupLatch.countDown();
                }
            }
        });

        final AtomicLong restored = new AtomicLong(0);
        kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition topicPartition, final String storeName, final long startingOffset, final long endingOffset) {

            }

            @Override
            public void onBatchRestored(final TopicPartition topicPartition, final String storeName, final long batchEndOffset, final long numRestored) {

            }

            @Override
            public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
                restored.addAndGet(totalRestored);
            }
        });
        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
        assertThat(restored.get(), equalTo((long) numberOfKeys - offsetLimitDelta * 2 - offsetCheckpointed * 2));

        assertTrue(shutdownLatch.await(30, TimeUnit.SECONDS));
        assertThat(numReceived.get(), equalTo(offsetLimitDelta * 2));
    }

    @Test
    public void shouldRestoreStateFromChangelogTopic() throws Exception {
        final AtomicInteger numReceived = new AtomicInteger(0);
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties props = props(APPID);

        // restoring from 1000 to 5000, and then process from 5000 to 10000 on each of the two partitions
        final int offsetCheckpointed = 1000;
        createStateForRestoration(APPID + "-store-changelog");
        createStateForRestoration(INPUT_STREAM);

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime());
        new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 0)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(APPID + "-store-changelog", 0), (long) offsetCheckpointed));
        new OffsetCheckpoint(new File(stateDirectory.directoryForTask(new TaskId(0, 1)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(APPID + "-store-changelog", 1), (long) offsetCheckpointed));

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        builder.table(INPUT_STREAM, Consumed.with(Serdes.Integer(), Serdes.Integer()), Materialized.as("store"))
                .toStream()
                .foreach(new ForeachAction<Integer, Integer>() {
                    @Override
                    public void apply(final Integer key, final Integer value) {
                        if (numReceived.incrementAndGet() == numberOfKeys)
                            shutdownLatch.countDown();
                    }
                });

        kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    startupLatch.countDown();
                }
            }
        });

        final AtomicLong restored = new AtomicLong(0);
        kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition topicPartition, final String storeName, final long startingOffset, final long endingOffset) {

            }

            @Override
            public void onBatchRestored(final TopicPartition topicPartition, final String storeName, final long batchEndOffset, final long numRestored) {

            }

            @Override
            public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
                restored.addAndGet(totalRestored);
            }
        });
        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
        assertThat(restored.get(), equalTo((long) numberOfKeys - 2 * offsetCheckpointed));

        assertTrue(shutdownLatch.await(30, TimeUnit.SECONDS));
        assertThat(numReceived.get(), equalTo(numberOfKeys));
    }


    @Test
    public void shouldSuccessfullyStartWhenLoggingDisabled() throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, Integer> stream = builder.stream(INPUT_STREAM);
        stream.groupByKey()
                .reduce(new Reducer<Integer>() {
                    @Override
                    public Integer apply(final Integer value1, final Integer value2) {
                        return value1 + value2;
                    }
                }, Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as("reduce-store").withLoggingDisabled());

        final CountDownLatch startupLatch = new CountDownLatch(1);
        kafkaStreams = new KafkaStreams(builder.build(), props(APPID));
        kafkaStreams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    startupLatch.countDown();
                }
            }
        });

        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void shouldProcessDataFromStoresWithLoggingDisabled() throws InterruptedException, ExecutionException {

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_STREAM_2,
                                                           Arrays.asList(KeyValue.pair(1, 1),
                                                                         KeyValue.pair(2, 2),
                                                                         KeyValue.pair(3, 3)),
                                                           TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                                                                                    IntegerSerializer.class,
                                                                                    IntegerSerializer.class),
                                                           CLUSTER.time);

        final KeyValueBytesStoreSupplier lruMapSupplier = Stores.lruMap(INPUT_STREAM_2, 10);

        final StoreBuilder<KeyValueStore<Integer, Integer>> storeBuilder = new KeyValueStoreBuilder<>(lruMapSupplier,
                                                                                                      Serdes.Integer(),
                                                                                                      Serdes.Integer(),
                                                                                                      CLUSTER.time)
                .withLoggingDisabled();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(storeBuilder);

        final KStream<Integer, Integer> stream = streamsBuilder.stream(INPUT_STREAM_2);
        final CountDownLatch processorLatch = new CountDownLatch(3);
        stream.process(new ProcessorSupplier<Integer, Integer>() {
            @Override
            public Processor<Integer, Integer> get() {
                return new KeyValueStoreProcessor(INPUT_STREAM_2, processorLatch);
            }
        }, INPUT_STREAM_2);

        final Topology topology = streamsBuilder.build();

        kafkaStreams = new KafkaStreams(topology, props(APPID + "-logging-disabled"));

        final CountDownLatch latch = new CountDownLatch(1);
        kafkaStreams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    latch.countDown();
                }
            }
        });
        kafkaStreams.start();

        latch.await(30, TimeUnit.SECONDS);

        assertTrue(processorLatch.await(30, TimeUnit.SECONDS));

    }


    public static class KeyValueStoreProcessor implements Processor<Integer, Integer> {

        private String topic;
        private final CountDownLatch processorLatch;

        private KeyValueStore<Integer, Integer> store;

        public KeyValueStoreProcessor(final String topic, final CountDownLatch processorLatch) {
            this.topic = topic;
            this.processorLatch = processorLatch;
        }

        @Override
        public void init(final ProcessorContext context) {
            this.store = (KeyValueStore<Integer, Integer>) context.getStateStore(topic);
        }

        @Override
        public void process(final Integer key, final Integer value) {
            if (key != null) {
                store.put(key, value);
                processorLatch.countDown();
            }
        }

        @Override
        public void close() {

        }
    }

    private void createStateForRestoration(final String changelogTopic) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        try (final KafkaProducer<Integer, Integer> producer =
                     new KafkaProducer<>(producerConfig, new IntegerSerializer(), new IntegerSerializer())) {

            for (int i = 0; i < numberOfKeys; i++) {
                producer.send(new ProducerRecord<>(changelogTopic, i, i));
            }
        }
    }

    private void setCommittedOffset(final String topic, final int limitDelta) {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, APPID);
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "commit-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);

        final Consumer consumer = new KafkaConsumer(consumerConfig);
        final List<TopicPartition> partitions = Arrays.asList(
            new TopicPartition(topic, 0),
            new TopicPartition(topic, 1));

        consumer.assign(partitions);
        consumer.seekToEnd(partitions);

        for (TopicPartition partition : partitions) {
            final long position = consumer.position(partition);
            consumer.seek(partition, position - limitDelta);
        }

        consumer.commitSync();
        consumer.close();
    }

}
