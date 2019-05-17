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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Category({IntegrationTest.class})
public class StoreUpgradeIntegrationTest {
    private static String inputStream;
    private static final String STORE_NAME = "store";

    private KafkaStreams kafkaStreams;
    private static int testCounter = 0;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @Before
    public void createTopics() throws Exception {
        inputStream = "input-stream-" + testCounter;
        CLUSTER.createTopic(inputStream);
    }

    private Properties props() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "addId-" + testCounter++);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }

    @Test
    public void shouldMigrateInMemoryKeyValueStoreToTimestampedKeyValueStoreUsingPapi() throws Exception {
        shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(false);
    }

    @Test
    public void shouldMigratePersistentKeyValueStoreToTimestampedKeyValueStoreUsingPapi() throws Exception {
        shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(true);
    }

    private void shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(final boolean persistentStore) throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(
            Stores.keyValueStoreBuilder(
                persistentStore ? Stores.persistentKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(KeyValueProcessor::new, STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValue.pair(1, 1L)));

        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValue.pair(1, 2L)));
        final long lastUpdateKeyOne = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processKeyValueAndVerifyPlainCount(2, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L)));
        final long lastUpdateKeyTwo = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processKeyValueAndVerifyPlainCount(3, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L)));
        final long lastUpdateKeyThree = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L),
            KeyValue.pair(4, 1L)));

        processKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L),
            KeyValue.pair(4, 2L)));

        processKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L),
            KeyValue.pair(4, 3L)));
        final long lastUpdateKeyFour = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        kafkaStreams.close();
        kafkaStreams = null;



        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

        streamsBuilderForNewStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
        kafkaStreams.start();

        verifyCountWithTimestamp(1, 2L, lastUpdateKeyOne);
        verifyCountWithTimestamp(2, 1L, lastUpdateKeyTwo);
        verifyCountWithTimestamp(3, 1L, lastUpdateKeyThree);
        verifyCountWithTimestamp(4, 3L, lastUpdateKeyFour);

        final long currentTime = CLUSTER.time.milliseconds();
        processKeyValueAndVerifyCountWithTimestamp(1, currentTime + 42L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(2, ValueAndTimestamp.make(1L, lastUpdateKeyTwo)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(4, ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

        processKeyValueAndVerifyCountWithTimestamp(2, currentTime + 45L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(4, ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

        // can process "out of order" record for different key
        processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 21L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(4, ValueAndTimestamp.make(4L, currentTime + 21L))));

        processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 42L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(4, ValueAndTimestamp.make(5L, currentTime + 42L))));

        // out of order (same key) record should not reduce result timestamp
        processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 10L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(4, ValueAndTimestamp.make(6L, currentTime + 42L))));

        kafkaStreams.close();
    }

    @Test
    public void shouldProxyKeyValueStoreToTimestampedKeyValueStoreUsingPapi() throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(KeyValueProcessor::new, STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValue.pair(1, 1L)));

        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValue.pair(1, 2L)));

        processKeyValueAndVerifyPlainCount(2, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L)));

        processKeyValueAndVerifyPlainCount(3, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L)));

        processKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L),
            KeyValue.pair(4, 1L)));

        processKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L),
            KeyValue.pair(4, 2L)));

        processKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(1, 2L),
            KeyValue.pair(2, 1L),
            KeyValue.pair(3, 1L),
            KeyValue.pair(4, 3L)));

        kafkaStreams.close();
        kafkaStreams = null;



        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

        streamsBuilderForNewStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
        kafkaStreams.start();

        verifyCountWithSurrogateTimestamp(1, 2L);
        verifyCountWithSurrogateTimestamp(2, 1L);
        verifyCountWithSurrogateTimestamp(3, 1L);
        verifyCountWithSurrogateTimestamp(4, 3L);

        processKeyValueAndVerifyCount(1, 42L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(2, ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(4, ValueAndTimestamp.make(3L, -1L))));

        processKeyValueAndVerifyCount(2, 45L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(4, ValueAndTimestamp.make(3L, -1L))));

        // can process "out of order" record for different key
        processKeyValueAndVerifyCount(4, 21L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(4, ValueAndTimestamp.make(4L, -1L))));

        processKeyValueAndVerifyCount(4, 42L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(4, ValueAndTimestamp.make(5L, -1L))));

        // out of order (same key) record should not reduce result timestamp
        processKeyValueAndVerifyCount(4, 10L, asList(
            KeyValue.pair(1, ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(2, ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(3, ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(4, ValueAndTimestamp.make(6L, -1L))));

        kafkaStreams.close();
    }

    private <K, V> void processKeyValueAndVerifyPlainCount(final K key,
                                                           final List<KeyValue<Integer, Object>> expectedStoreContent)
            throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputStream,
            singletonList(KeyValue.pair(key, 0)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            CLUSTER.time);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, V> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.keyValueStore());
                try (final KeyValueIterator<K, V> all = store.all()) {
                    final List<KeyValue<K, V>> storeContent = new LinkedList<>();
                    while (all.hasNext()) {
                        storeContent.add(all.next());
                    }
                    return storeContent.equals(expectedStoreContent);
                }
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private <K> void verifyCountWithTimestamp(final K key,
                                              final long value,
                                              final long timestamp) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, ValueAndTimestamp<Long>> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
                final ValueAndTimestamp<Long> count = store.get(key);
                return count.value() == value && count.timestamp() == timestamp;
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private <K> void verifyCountWithSurrogateTimestamp(final K key,
                                                       final long value) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, ValueAndTimestamp<Long>> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
                final ValueAndTimestamp<Long> count = store.get(key);
                return count.value() == value && count.timestamp() == -1L;
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private <K, V> void processKeyValueAndVerifyCount(final K key,
                                                      final long timestamp,
                                                      final List<KeyValue<Integer, Object>> expectedStoreContent)
            throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, 0)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            timestamp);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
                try (final KeyValueIterator<K, ValueAndTimestamp<V>> all = store.all()) {
                    final List<KeyValue<K, ValueAndTimestamp<V>>> storeContent = new LinkedList<>();
                    while (all.hasNext()) {
                        storeContent.add(all.next());
                    }
                    return storeContent.equals(expectedStoreContent);
                }
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private <K, V> void processKeyValueAndVerifyCountWithTimestamp(final K key,
                                                                   final long timestamp,
                                                                   final List<KeyValue<Integer, Object>> expectedStoreContent)
        throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, 0)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            timestamp);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
                try (final KeyValueIterator<K, ValueAndTimestamp<V>> all = store.all()) {
                    final List<KeyValue<K, ValueAndTimestamp<V>>> storeContent = new LinkedList<>();
                    while (all.hasNext()) {
                        storeContent.add(all.next());
                    }
                    return storeContent.equals(expectedStoreContent);
                }
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    @Test
    public void shouldMigrateInMemoryWindowStoreToTimestampedWindowStoreUsingPapi() throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();
        streamsBuilderForOldStore
            .addStateStore(
                Stores.windowStoreBuilder(
                    Stores.inMemoryWindowStore(
                        STORE_NAME,
                        Duration.ofMillis(1000L),
                        Duration.ofMillis(1000L),
                        false),
                Serdes.Integer(),
                Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(WindowedProcessor::new, STORE_NAME);

        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();
        streamsBuilderForNewStore
            .addStateStore(
                Stores.timestampedWindowStoreBuilder(
                    Stores.inMemoryWindowStore(
                        STORE_NAME,
                        Duration.ofMillis(1000L),
                        Duration.ofMillis(1000L),
                        false),
            Serdes.Integer(),
            Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(TimestampedWindowedProcessor::new, STORE_NAME);


        shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(
            new KafkaStreams(streamsBuilderForOldStore.build(), props()),
            new KafkaStreams(streamsBuilderForNewStore.build(), props()),
            false);
    }

    @Test
    public void shouldMigratePersistentWindowStoreToTimestampedWindowStoreUsingPapi() throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore
            .addStateStore(
                Stores.windowStoreBuilder(
                    Stores.persistentWindowStore(
                        STORE_NAME,
                        Duration.ofMillis(1000L),
                        Duration.ofMillis(1000L),
                        false),
                    Serdes.Integer(),
                    Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(WindowedProcessor::new, STORE_NAME);

        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();
        streamsBuilderForNewStore
            .addStateStore(
                Stores.timestampedWindowStoreBuilder(
                    Stores.persistentTimestampedWindowStore(
                        STORE_NAME,
                        Duration.ofMillis(1000L),
                        Duration.ofMillis(1000L),
                        false),
                    Serdes.Integer(),
                    Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(TimestampedWindowedProcessor::new, STORE_NAME);

        final Properties props = props();
        shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(
            new KafkaStreams(streamsBuilderForOldStore.build(), props),
            new KafkaStreams(streamsBuilderForNewStore.build(), props),
            true);
    }

    private void shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(final KafkaStreams kafkaStreamsOld,
                                                                           final KafkaStreams kafkaStreamsNew,
                                                                           final boolean persistentStore) throws Exception {
        kafkaStreams = kafkaStreamsOld;
        kafkaStreams.start();

        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 1L)));

        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L)));
        final long lastUpdateKeyOne = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processWindowedKeyValueAndVerifyPlainCount(2, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L)));
        final long lastUpdateKeyTwo = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processWindowedKeyValueAndVerifyPlainCount(3, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L)));
        final long lastUpdateKeyThree = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processWindowedKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), 1L)));

        processWindowedKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), 2L)));

        processWindowedKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L)));
        final long lastUpdateKeyFour = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        kafkaStreams.close();
        kafkaStreams = null;


        kafkaStreams = kafkaStreamsNew;
        kafkaStreams.start();

        verifyWindowedCountWithTimestamp(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L, lastUpdateKeyOne);
        verifyWindowedCountWithTimestamp(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L, lastUpdateKeyTwo);
        verifyWindowedCountWithTimestamp(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L, lastUpdateKeyThree);
        verifyWindowedCountWithTimestamp(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L, lastUpdateKeyFour);

        final long currentTime = CLUSTER.time.milliseconds();
        processKeyValueAndVerifyWindowedCountWithTimestamp(1, currentTime + 42L, asList(
            KeyValue.pair(
                new Windowed<>(1, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(
                new Windowed<>(2, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(1L, lastUpdateKeyTwo)),
            KeyValue.pair(
                new Windowed<>(3, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(
                new Windowed<>(4, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

        processKeyValueAndVerifyWindowedCountWithTimestamp(2, currentTime + 45L, asList(
            KeyValue.pair(
                new Windowed<>(1, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(
                new Windowed<>(2, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(
                new Windowed<>(3, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(
                new Windowed<>(4, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

        // can process "out of order" record for different key
        processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 21L, asList(
            KeyValue.pair(
                new Windowed<>(1, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(
                new Windowed<>(2, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(
                new Windowed<>(3, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(
                new Windowed<>(4, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(4L, currentTime + 21L))));

        processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 42L, asList(
            KeyValue.pair(
                new Windowed<>(1, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(
                new Windowed<>(2, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(
                new Windowed<>(3, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(
                new Windowed<>(4, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(5L, currentTime + 42L))));

        // out of order (same key) record should not reduce result timestamp
        processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 10L, asList(
            KeyValue.pair(
                new Windowed<>(1, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(3L, currentTime + 42L)),
            KeyValue.pair(
                new Windowed<>(2, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(2L, currentTime + 45L)),
            KeyValue.pair(
                new Windowed<>(3, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
            KeyValue.pair(
                new Windowed<>(4, new TimeWindow(0L, 1000L)),
                ValueAndTimestamp.make(6L, currentTime + 42L))));

        // test new segment
        processKeyValueAndVerifyWindowedCountWithTimestamp(10, currentTime + 100001L, singletonList(
            KeyValue.pair(
                new Windowed<>(10, new TimeWindow(100000L, 101000L)), ValueAndTimestamp.make(1L, currentTime + 100001L))));


        kafkaStreams.close();
    }

    @Test
    public void shouldProxyWindowStoreToTimestampedWindowStoreUsingPapi() throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                    STORE_NAME,
                    Duration.ofMillis(1000L),
                    Duration.ofMillis(1000L),
                    false),
                Serdes.Integer(),
                Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(WindowedProcessor::new, STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 1L)));

        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L)));

        processWindowedKeyValueAndVerifyPlainCount(2, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L)));

        processWindowedKeyValueAndVerifyPlainCount(3, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L)));

        processWindowedKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), 1L)));

        processWindowedKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), 2L)));

        processWindowedKeyValueAndVerifyPlainCount(4, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L)));

        kafkaStreams.close();
        kafkaStreams = null;



        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

        streamsBuilderForNewStore.addStateStore(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentWindowStore(
                    STORE_NAME,
                    Duration.ofMillis(1000L),
                    Duration.ofMillis(1000L),
                    false),
                Serdes.Integer(),
                Serdes.Long()))
            .<Integer, Integer>stream(inputStream)
            .process(TimestampedWindowedProcessor::new, STORE_NAME);

        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
        kafkaStreams.start();

        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L);
        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L);
        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L);
        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L);

        processKeyValueAndVerifyWindowedCountWithTimestamp(1, 42L, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L))));

        processKeyValueAndVerifyWindowedCountWithTimestamp(2, 45L, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L))));

        // can process "out of order" record for different key
        processKeyValueAndVerifyWindowedCountWithTimestamp(4, 21L, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(4L, -1L))));

        processKeyValueAndVerifyWindowedCountWithTimestamp(4, 42L, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(5L, -1L))));

        // out of order (same key) record should not reduce result timestamp
        processKeyValueAndVerifyWindowedCountWithTimestamp(4, 10L, asList(
            KeyValue.pair(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
            KeyValue.pair(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
            KeyValue.pair(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
            KeyValue.pair(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(6L, -1L))));

        // test new segment
        processKeyValueAndVerifyWindowedCountWithTimestamp(10, 100001L, singletonList(
            KeyValue.pair(new Windowed<>(10, new TimeWindow(100000L, 101000L)), ValueAndTimestamp.make(1L, -1L))));


        kafkaStreams.close();
    }

    private <K, V> void processWindowedKeyValueAndVerifyPlainCount(final K key,
                                                                   final List<KeyValue<Windowed<Integer>, Object>> expectedStoreContent)
            throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputStream,
            singletonList(KeyValue.pair(key, 0)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            CLUSTER.time);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyWindowStore<K, V> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.windowStore());
                try (final KeyValueIterator<Windowed<K>, V> all = store.all()) {
                    final List<KeyValue<Windowed<K>, V>> storeContent = new LinkedList<>();
                    while (all.hasNext()) {
                        storeContent.add(all.next());
                    }
                    return storeContent.equals(expectedStoreContent);
                }
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private <K> void verifyWindowedCountWithSurrogateTimestamp(final Windowed<K> key,
                                                               final long value) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyWindowStore<K, ValueAndTimestamp<Long>> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
                final ValueAndTimestamp<Long> count = store.fetch(key.key(), key.window().start());
                return count.value() == value && count.timestamp() == -1L;
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private <K> void verifyWindowedCountWithTimestamp(final Windowed<K> key,
                                                      final long value,
                                                      final long timestamp) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyWindowStore<K, ValueAndTimestamp<Long>> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
                final ValueAndTimestamp<Long> count = store.fetch(key.key(), key.window().start());
                return count.value() == value && count.timestamp() == timestamp;
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private <K, V> void processKeyValueAndVerifyWindowedCountWithTimestamp(final K key,
                                                                           final long timestamp,
                                                                           final List<KeyValue<Windowed<Integer>, Object>> expectedStoreContent)
            throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, 0)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            timestamp);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyWindowStore<K, ValueAndTimestamp<V>> store =
                    kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
                try (final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> all = store.all()) {
                    final List<KeyValue<Windowed<K>, ValueAndTimestamp<V>>> storeContent = new LinkedList<>();
                    while (all.hasNext()) {
                        storeContent.add(all.next());
                    }
                    return storeContent.equals(expectedStoreContent);
                }
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");
    }

    private static class KeyValueProcessor implements Processor<Integer, Integer> {
        private KeyValueStore<Integer, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            store = (KeyValueStore<Integer, Long>) context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Integer key, final Integer value) {
            final long newCount;

            final Long oldCount = store.get(key);
            if (oldCount != null) {
                newCount = oldCount + 1L;
            } else {
                newCount = 1L;
            }

            store.put(key, newCount);
        }

        @Override
        public void close() {}
    }

    private static class TimestampedKeyValueProcessor implements Processor<Integer, Integer> {
        private ProcessorContext context;
        private TimestampedKeyValueStore<Integer, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
            store = (TimestampedKeyValueStore<Integer, Long>) context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Integer key, final Integer value) {
            final long newCount;

            final ValueAndTimestamp<Long> oldCountWithTimestamp = store.get(key);
            final long newTimestamp;

            if (oldCountWithTimestamp == null) {
                newCount = 1L;
                newTimestamp = context.timestamp();
            } else {
                newCount = oldCountWithTimestamp.value() + 1L;
                newTimestamp = Math.max(oldCountWithTimestamp.timestamp(), context.timestamp());
            }

            store.put(key, ValueAndTimestamp.make(newCount, newTimestamp));
        }

        @Override
        public void close() {}
    }

    private static class WindowedProcessor implements Processor<Integer, Integer> {
        private WindowStore<Integer, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            store = (WindowStore<Integer, Long>) context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Integer key, final Integer value) {
            final long newCount;

            final Long oldCount = store.fetch(key, key < 10 ? 0L : 100000L);
            if (oldCount != null) {
                newCount = oldCount + 1L;
            } else {
                newCount = 1L;
            }

            store.put(key, newCount, key < 10 ? 0L : 100000L);
        }

        @Override
        public void close() {}
    }

    private static class TimestampedWindowedProcessor implements Processor<Integer, Integer> {
        private ProcessorContext context;
        private TimestampedWindowStore<Integer, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
            store = (TimestampedWindowStore<Integer, Long>) context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Integer key, final Integer value) {
            final long newCount;

            final ValueAndTimestamp<Long> oldCountWithTimestamp = store.fetch(key, key < 10 ? 0L : 100000L);
            final long newTimestamp;

            if (oldCountWithTimestamp == null) {
                newCount = 1L;
                newTimestamp = context.timestamp();
            } else {
                newCount = oldCountWithTimestamp.value() + 1L;
                newTimestamp = Math.max(oldCountWithTimestamp.timestamp(), context.timestamp());
            }

            store.put(key, ValueAndTimestamp.make(newCount, newTimestamp), key < 10 ? 0L : 100000L);
        }

        @Override
        public void close() {}
    }
}