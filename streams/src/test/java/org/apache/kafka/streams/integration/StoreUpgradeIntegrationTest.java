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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampImpl;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

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
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        return streamsConfiguration;
    }

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30));
            kafkaStreams.cleanUp();
        }
    }

    @Test
    public void shouldMigrateRecordFromPlainValueToValueWithTimestampStore() throws Exception {
        runUpgradeTest(false);
    }

    @Test
    public void shouldMigrateRecordFromPlainValueToValueWithTimestampStoreWithOldSupplier() throws Exception {
        runUpgradeTest(true);
    }

    private void runUpgradeTest(final boolean usePlainKeyValueByteStoreSupplier) throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STORE_NAME),
            Serdes.Integer(),
            Serdes.Long()
        ));
        streamsBuilderForOldStore
            .<Integer, Integer>stream(inputStream)
            .process(
                () -> new Processor<Integer, Integer>() {
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
                            newCount = oldCount + 1;
                        } else {
                            newCount = 1L;
                        }

                        store.put(key, newCount);
                    }

                    @Override
                    public void close() {}
                },
                STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        final LinkedList<KeyValue<Integer, Object>> expectedResult = new LinkedList<>();

        expectedResult.add(KeyValue.pair(1, 1L));
        processKeyValueAndVerifyPlainCount(1, expectedResult);

        expectedResult.remove(0);
        expectedResult.add(KeyValue.pair(1, 2L));
        processKeyValueAndVerifyPlainCount(1, expectedResult);

        expectedResult.add(KeyValue.pair(2, 1L));
        processKeyValueAndVerifyPlainCount(2, expectedResult);

        expectedResult.add(KeyValue.pair(3, 1L));
        processKeyValueAndVerifyPlainCount(3, expectedResult);

        expectedResult.add(KeyValue.pair(4, 1L));
        processKeyValueAndVerifyPlainCount(4, expectedResult);

        expectedResult.remove(3);
        expectedResult.add(KeyValue.pair(4, 2L));
        processKeyValueAndVerifyPlainCount(4, expectedResult);

        expectedResult.remove(3);
        expectedResult.add(KeyValue.pair(4, 3L));
        processKeyValueAndVerifyPlainCount(4, expectedResult);

        kafkaStreams.close();
        kafkaStreams = null;



        final StreamsBuilder newStoreBuilder = new StreamsBuilder();

        final Materialized<Integer, Long, KeyValueStore<Bytes, byte[]>> materialized;
        if (usePlainKeyValueByteStoreSupplier) {
            materialized = Materialized.as(Stores.persistentKeyValueStore(STORE_NAME));
        } else {
            materialized = Materialized.as(STORE_NAME);
        }
        newStoreBuilder
            .<Integer, Integer>stream(inputStream)
            .groupByKey()
            .count(materialized);

        kafkaStreams = new KafkaStreams(newStoreBuilder.build(), props);
        kafkaStreams.start();

        verifyCountWithSurrogateTimestamp(1, 2L);
        verifyCountWithSurrogateTimestamp(2, 1L);
        verifyCountWithSurrogateTimestamp(3, 1L);
        verifyCountWithSurrogateTimestamp(4, 3L);

        expectedResult.clear();
        expectedResult.add(KeyValue.pair(1, new ValueAndTimestampImpl<>(3L, usePlainKeyValueByteStoreSupplier ? -1 : 42L)));
        expectedResult.add(KeyValue.pair(2, new ValueAndTimestampImpl<>(1L, -1)));
        expectedResult.add(KeyValue.pair(3, new ValueAndTimestampImpl<>(1L, -1)));
        expectedResult.add(KeyValue.pair(4, new ValueAndTimestampImpl<>(3L, -1)));
        processKeyValueAndVerifyCountWithTimestamp(1, 42L, expectedResult);

        expectedResult.remove(1);
        expectedResult.add(1, KeyValue.pair(2, new ValueAndTimestampImpl<>(2L, usePlainKeyValueByteStoreSupplier ? -1 : 45)));
        processKeyValueAndVerifyCountWithTimestamp(2, 45L, expectedResult);

        // can process "out of order" record for different key
        expectedResult.remove(3);
        expectedResult.add(3, KeyValue.pair(4, new ValueAndTimestampImpl<>(4L, usePlainKeyValueByteStoreSupplier ? -1 : 21)));
        processKeyValueAndVerifyCountWithTimestamp(4, 21L, expectedResult);

        expectedResult.remove(3);
        expectedResult.add(3, KeyValue.pair(4, new ValueAndTimestampImpl<>(5L, usePlainKeyValueByteStoreSupplier ? -1 : 42)));
        processKeyValueAndVerifyCountWithTimestamp(4, 42L, expectedResult);

        // out of order (same key) record should not reduce result timestamp
        expectedResult.remove(3);
        expectedResult.add(3, KeyValue.pair(4, new ValueAndTimestampImpl<>(6L, usePlainKeyValueByteStoreSupplier ? -1 : 42)));
        processKeyValueAndVerifyCountWithTimestamp(4, 10L, expectedResult);

        kafkaStreams.close();
    }

    private <K, V> void processKeyValueAndVerifyPlainCount(final K key,
                                                           final List<KeyValue<Integer, Object>> expectedStoreContent) throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronously(inputStream,
            Collections.singletonList(KeyValue.pair(key, 0)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            CLUSTER.time);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, V> store = kafkaStreams.store(STORE_NAME, QueryableStoreTypes.keyValueStore());
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

    private <K> void verifyCountWithSurrogateTimestamp(final K key,
                                                       final long value) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, ValueAndTimestamp<Long>> store = kafkaStreams.store(STORE_NAME, QueryableStoreTypes.keyValueWithTimestampStore());
                final ValueAndTimestamp<Long> count = store.get(key);
                return count.value() == value && count.timestamp() == -1L;
            } catch (final Exception swallow) {
                swallow.printStackTrace();
                System.err.println(swallow.getMessage());
                return false;
            }
        }, "Could not get expected result in time.");

    }

    private <K, V> void processKeyValueAndVerifyCountWithTimestamp(final K key,
                                                                   final long timestamp,
                                                                   final List<KeyValue<Integer, Object>> expectedStoreContent) throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(inputStream,
            Collections.singletonList(KeyValue.pair(key, 0)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            timestamp);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> store = kafkaStreams.store(STORE_NAME, QueryableStoreTypes.keyValueWithTimestampStore());
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
}