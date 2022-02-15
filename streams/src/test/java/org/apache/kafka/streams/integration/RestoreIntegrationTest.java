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
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.TrackingStateRestoreListener;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.rules.TestName;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForCompletion;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForStandbyCompletion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

@Category({IntegrationTest.class})
public class RestoreIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public final TestName testName = new TestName();
    private String appId;
    private String inputStream;

    private final int numberOfKeys = 10000;
    private KafkaStreams kafkaStreams;

    @Before
    public void createTopics() throws InterruptedException {
        appId = safeUniqueTestName(RestoreIntegrationTest.class, testName);
        inputStream = appId + "-input-stream";
        CLUSTER.createTopic(inputStream, 2, 1);
    }

    private Properties props() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30));
        }
    }

    @Test
    public void shouldRestoreStateFromSourceTopic() throws Exception {
        final AtomicInteger numReceived = new AtomicInteger(0);
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties props = props();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        // restoring from 1000 to 4000 (committed), and then process from 4000 to 5000 on each of the two partitions
        final int offsetLimitDelta = 1000;
        final int offsetCheckpointed = 1000;
        createStateForRestoration(inputStream, 0);
        setCommittedOffset(inputStream, offsetLimitDelta);

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true, false);
        // note here the checkpointed offset is the last processed record's offset, so without control message we should write this offset - 1
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 0)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(inputStream, 0), (long) offsetCheckpointed - 1));
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 1)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(inputStream, 1), (long) offsetCheckpointed - 1));

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        builder.table(inputStream, Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as("store").withKeySerde(Serdes.Integer()).withValueSerde(Serdes.Integer()))
                .toStream()
                .foreach((key, value) -> {
                    if (numReceived.incrementAndGet() == offsetLimitDelta * 2) {
                        shutdownLatch.countDown();
                    }
                });

        kafkaStreams = new KafkaStreams(builder.build(props), props);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                startupLatch.countDown();
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
        final String changelog = appId + "-store-changelog";
        CLUSTER.createTopic(changelog, 2, 1);

        final AtomicInteger numReceived = new AtomicInteger(0);
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties props = props();

        // restoring from 1000 to 5000, and then process from 5000 to 10000 on each of the two partitions
        final int offsetCheckpointed = 1000;
        createStateForRestoration(changelog, 0);
        createStateForRestoration(inputStream, 10000);

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true, false);
        // note here the checkpointed offset is the last processed record's offset, so without control message we should write this offset - 1
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 0)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(changelog, 0), (long) offsetCheckpointed - 1));
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 1)), ".checkpoint"))
                .write(Collections.singletonMap(new TopicPartition(changelog, 1), (long) offsetCheckpointed - 1));

        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        builder.table(inputStream, Consumed.with(Serdes.Integer(), Serdes.Integer()), Materialized.as("store"))
                .toStream()
                .foreach((key, value) -> {
                    if (numReceived.incrementAndGet() == numberOfKeys) {
                        shutdownLatch.countDown();
                    }
                });

        kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                startupLatch.countDown();
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

        final KStream<Integer, Integer> stream = builder.stream(inputStream);
        stream.groupByKey()
                .reduce(
                    (value1, value2) -> value1 + value2,
                    Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as("reduce-store").withLoggingDisabled());

        final CountDownLatch startupLatch = new CountDownLatch(1);
        kafkaStreams = new KafkaStreams(builder.build(), props());
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                startupLatch.countDown();
            }
        });

        kafkaStreams.start();

        assertTrue(startupLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void shouldProcessDataFromStoresWithLoggingDisabled() throws InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(inputStream,
                                                           asList(KeyValue.pair(1, 1),
                                                                         KeyValue.pair(2, 2),
                                                                         KeyValue.pair(3, 3)),
                                                           TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                                                                                    IntegerSerializer.class,
                                                                                    IntegerSerializer.class),
                                                           CLUSTER.time);

        final KeyValueBytesStoreSupplier lruMapSupplier = Stores.lruMap(inputStream, 10);

        final StoreBuilder<KeyValueStore<Integer, Integer>> storeBuilder = new KeyValueStoreBuilder<>(lruMapSupplier,
                                                                                                      Serdes.Integer(),
                                                                                                      Serdes.Integer(),
                                                                                                      CLUSTER.time)
                .withLoggingDisabled();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(storeBuilder);

        final KStream<Integer, Integer> stream = streamsBuilder.stream(inputStream);
        final CountDownLatch processorLatch = new CountDownLatch(3);
        stream.process(() -> new KeyValueStoreProcessor(inputStream, processorLatch), inputStream);

        final Topology topology = streamsBuilder.build();

        kafkaStreams = new KafkaStreams(topology, props());

        final CountDownLatch latch = new CountDownLatch(1);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                latch.countDown();
            }
        });
        kafkaStreams.start();

        latch.await(30, TimeUnit.SECONDS);

        assertTrue(processorLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void shouldRecycleStateFromStandbyTaskPromotedToActiveTaskAndNotRestore() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
                inputStream,
            Consumed.with(Serdes.Integer(), Serdes.Integer()), Materialized.as(getCloseCountingStore("store"))
        );
        createStateForRestoration(inputStream, 0);

        final Properties props1 = props();
        props1.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props1.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId + "-1").getPath());
        purgeLocalStreamsState(props1);
        final KafkaStreams client1 = new KafkaStreams(builder.build(), props1);

        final Properties props2 = props();
        props2.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props2.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId + "-2").getPath());
        purgeLocalStreamsState(props2);
        final KafkaStreams client2 = new KafkaStreams(builder.build(), props2);

        final TrackingStateRestoreListener restoreListener = new TrackingStateRestoreListener();
        client1.setGlobalStateRestoreListener(restoreListener);

        startApplicationAndWaitUntilRunning(asList(client1, client2), Duration.ofSeconds(60));

        waitForCompletion(client1, 1, 30 * 1000L);
        waitForCompletion(client2, 1, 30 * 1000L);
        waitForStandbyCompletion(client1, 1, 30 * 1000L);
        waitForStandbyCompletion(client2, 1, 30 * 1000L);

        // Sometimes the store happens to have already been closed sometime during startup, so just keep track
        // of where it started and make sure it doesn't happen more times from there
        final int initialStoreCloseCount = CloseCountingInMemoryStore.numStoresClosed();
        final long initialNunRestoredCount = restoreListener.totalNumRestored();

        client2.close();
        waitForApplicationState(singletonList(client2), State.NOT_RUNNING, Duration.ofSeconds(60));
        waitForApplicationState(singletonList(client1), State.REBALANCING, Duration.ofSeconds(60));
        waitForApplicationState(singletonList(client1), State.RUNNING, Duration.ofSeconds(60));

        waitForCompletion(client1, 1, 30 * 1000L);
        waitForStandbyCompletion(client1, 1, 30 * 1000L);

        assertThat(restoreListener.totalNumRestored(), CoreMatchers.equalTo(initialNunRestoredCount));

        // After stopping instance 2 and letting instance 1 take over its tasks, we should have closed just two stores
        // total: the active and standby tasks on instance 2
        assertThat(CloseCountingInMemoryStore.numStoresClosed(), equalTo(initialStoreCloseCount + 2));

        client1.close();
        waitForApplicationState(singletonList(client2), State.NOT_RUNNING, Duration.ofSeconds(60));

        assertThat(CloseCountingInMemoryStore.numStoresClosed(), CoreMatchers.equalTo(initialStoreCloseCount + 4));
    }

    private static KeyValueBytesStoreSupplier getCloseCountingStore(final String name) {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CloseCountingInMemoryStore(name);
            }

            @Override
            public String metricsScope() {
                return "close-counting";
            }
        };
    }

    static class CloseCountingInMemoryStore extends InMemoryKeyValueStore {
        static AtomicInteger numStoresClosed = new AtomicInteger(0);

        CloseCountingInMemoryStore(final String name) {
            super(name);
        }

        @Override
        public void close() {
            numStoresClosed.incrementAndGet();
            super.close();
        }

        static int numStoresClosed() {
            return numStoresClosed.get();
        }
    }

    public static class KeyValueStoreProcessor implements Processor<Integer, Integer, Void, Void> {
        private final String topic;
        private final CountDownLatch processorLatch;

        private KeyValueStore<Integer, Integer> store;

        KeyValueStoreProcessor(final String topic, final CountDownLatch processorLatch) {
            this.topic = topic;
            this.processorLatch = processorLatch;
        }

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore(topic);
        }

        @Override
        public void process(final Record<Integer, Integer> record) {
            if (record.key() != null) {
                store.put(record.key(), record.value());
                processorLatch.countDown();
            }
        }
    }

    private void createStateForRestoration(final String changelogTopic, final int startingOffset) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        try (final KafkaProducer<Integer, Integer> producer =
                     new KafkaProducer<>(producerConfig, new IntegerSerializer(), new IntegerSerializer())) {

            for (int i = 0; i < numberOfKeys; i++) {
                final int offset = startingOffset + i;
                producer.send(new ProducerRecord<>(changelogTopic, offset, offset));
            }
        }
    }

    private void setCommittedOffset(final String topic, final int limitDelta) {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "commit-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);

        final Consumer<Integer, Integer> consumer = new KafkaConsumer<>(consumerConfig);
        final List<TopicPartition> partitions = asList(
            new TopicPartition(topic, 0),
            new TopicPartition(topic, 1));

        consumer.assign(partitions);
        consumer.seekToEnd(partitions);

        for (final TopicPartition partition : partitions) {
            final long position = consumer.position(partition);
            consumer.seek(partition, position - limitDelta);
        }

        consumer.commitSync();
        consumer.close();
    }
}
