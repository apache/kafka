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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
public class HeadersStoreUpgradeIntegrationTest {
    private static final String STORE_NAME = "store";
    private static final String WINDOW_STORE_NAME = "window-store";
    private static final long WINDOW_SIZE_MS = 1000L;
    private static final long RETENTION_MS = Duration.ofDays(1).toMillis();

    private String inputStream;

    private KafkaStreams kafkaStreams;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    public String safeTestName;

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws Exception {
        safeTestName = safeUniqueTestName(testInfo);
        inputStream = "input-stream-" + safeTestName;
        CLUSTER.createTopic(inputStream);
    }

    private Properties props() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @AfterEach
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }

    @Test
    public void shouldMigrateInMemoryTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        shouldMigrateTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(false);
    }

    @Test
    public void shouldMigratePersistentTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        shouldMigrateTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(true);
    }

    private void shouldMigrateTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(final boolean persistentStore) throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        kafkaStreams.close();
        kafkaStreams = null;

        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

        streamsBuilderForNewStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                persistentStore ? Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME);

        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
        kafkaStreams.start();

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", 11L);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", 22L);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", 33L);

        // Process new records with headers
        final Headers headers = new RecordHeaders();
        headers.add("source", "test".getBytes());

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, headers);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, headers);

        kafkaStreams.close();
    }

    @Test
    public void shouldProxyTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        kafkaStreams.close();
        kafkaStreams = null;



        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

        streamsBuilderForNewStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME);

        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
        kafkaStreams.start();

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", 11L);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", 22L);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", 33L);

        // Process new records with headers
        final RecordHeaders headers = new RecordHeaders();
        headers.add("source", "proxy-test".getBytes());
        final Headers expectedHeaders = new RecordHeaders();

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, expectedHeaders);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, expectedHeaders);

        kafkaStreams.close();
    }

    private <K, V> void processKeyValueAndVerifyTimestampedValue(final K key,
                                                                 final V value,
                                                                 final long timestamp)
        throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            timestamp,
            false);

        TestUtils.waitForCondition(
            () -> {
                try {
                    final ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> store =
                        IntegrationTestUtils.getStore(STORE_NAME, kafkaStreams, QueryableStoreTypes.timestampedKeyValueStore());

                    if (store == null) {
                        return false;
                    }

                    final ValueAndTimestamp<V> result = store.get(key);
                    return result != null && result.value().equals(value) && result.timestamp() == timestamp;
                } catch (final Exception swallow) {
                    swallow.printStackTrace();
                    System.err.println(swallow.getMessage());
                    return false;
                }
            },
            60_000L,
            "Could not get expected result in time.");
    }

    private <K, V> void processKeyValueWithTimestampAndHeadersAndVerify(final K key,
                                                                        final V value,
                                                                        final long timestamp,
                                                                        final Headers headers,
                                                                        final Headers expectedHeaders)
        throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            headers,
            timestamp,
            false);

        TestUtils.waitForCondition(
            () -> {
                try {
                    final ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> store = IntegrationTestUtils
                        .getStore(STORE_NAME, kafkaStreams, QueryableStoreTypes.keyValueStore());

                    if (store == null)
                        return false;

                    final ValueTimestampHeaders<V> result = store.get(key);
                    return result != null
                        && result.value().equals(value)
                        && result.timestamp() == timestamp
                        && result.headers().equals(expectedHeaders);
                } catch (final Exception swallow) {
                    swallow.printStackTrace();
                    System.err.println(swallow.getMessage());
                    return false;
                }
            },
            60_000L,
            "Could not get expected result in time.");
    }

    private <K, V> void verifyLegacyValuesWithEmptyHeaders(final K key,
                                                           final V value,
                                                           final long timestamp) throws Exception {
        TestUtils.waitForCondition(
            () -> {
                try {
                    final ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> store = IntegrationTestUtils
                        .getStore(STORE_NAME, kafkaStreams, QueryableStoreTypes.keyValueStore());

                    if (store == null)
                        return false;

                    final ValueTimestampHeaders<V> result = store.get(key);
                    return result != null
                        && result.value().equals(value)
                        && result.timestamp() == timestamp
                        && result.headers().toArray().length == 0;
                } catch (final Exception swallow) {
                    swallow.printStackTrace();
                    System.err.println(swallow.getMessage());
                    return false;
                }
            },
            60_000L,
            "Could not get expected result in time.");
    }

    private static class TimestampedKeyValueProcessor implements Processor<String, String, Void, Void> {
        private TimestampedKeyValueStore<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp()));
        }
    }

    private static class TimestampedKeyValueWithHeadersProcessor implements Processor<String, String, Void, Void> {
        private TimestampedKeyValueStoreWithHeaders<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
        }
    }

    @Test
    public void shouldMigrateInMemoryTimestampedWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        shouldMigrateTimestampedWindowStoreToTimestampedWindowStoreWithHeaders(false);
    }

    @Test
    public void shouldMigratePersistentTimestampedWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        shouldMigrateTimestampedWindowStoreToTimestampedWindowStoreWithHeaders(true);
    }

    /**
     * Tests migration from TimestampedWindowStore to TimestampedWindowStoreWithHeaders.
     * This is a true migration where both supplier and builder are upgraded.
     */
    private void shouldMigrateTimestampedWindowStoreToTimestampedWindowStoreWithHeaders(final boolean persistentStore) throws Exception {
        // Phase 1: Run with old TimestampedWindowStore
        final StreamsBuilder oldBuilder = new StreamsBuilder();
        oldBuilder.addStateStore(
                Stores.timestampedWindowStoreBuilder(
                    persistentStore
                        ? Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false)
                        : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedWindowedProcessor::new, WINDOW_STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(oldBuilder.build(), props);
        kafkaStreams.start();

        final long baseTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key1", "value1", baseTime + 100);
        processWindowedKeyValueAndVerifyTimestamped("key2", "value2", baseTime + 200);
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", baseTime + 300);

        kafkaStreams.close();
        kafkaStreams = null;

        final StreamsBuilder newBuilder = new StreamsBuilder();
        newBuilder.addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    persistentStore
                        ? Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false)
                        : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME);

        kafkaStreams = new KafkaStreams(newBuilder.build(), props);
        kafkaStreams.start();

        verifyWindowValueWithEmptyHeaders("key1", "value1", baseTime + 100);
        verifyWindowValueWithEmptyHeaders("key2", "value2", baseTime + 200);
        verifyWindowValueWithEmptyHeaders("key3", "value3", baseTime + 300);

        final Headers headers = new RecordHeaders();
        headers.add("source", "migration-test".getBytes());
        headers.add("version", "1.0".getBytes());

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, headers);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, headers);

        kafkaStreams.close();
    }

    @Test
    public void shouldProxyTimestampedWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        final StreamsBuilder oldBuilder = new StreamsBuilder();
        oldBuilder.addStateStore(
                Stores.timestampedWindowStoreBuilder(
                    Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedWindowedProcessor::new, WINDOW_STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(oldBuilder.build(), props);
        kafkaStreams.start();

        final long baseTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key1", "value1", baseTime + 100);
        processWindowedKeyValueAndVerifyTimestamped("key2", "value2", baseTime + 200);
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", baseTime + 300);

        kafkaStreams.close();
        kafkaStreams = null;

        // Restart with headers-aware builder but non-headers supplier (proxy/adapter mode)
        final StreamsBuilder newBuilder = new StreamsBuilder();
        newBuilder.addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),  // non-headers supplier!
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME);

        kafkaStreams = new KafkaStreams(newBuilder.build(), props);
        kafkaStreams.start();

        verifyWindowValueWithEmptyHeaders("key1", "value1", baseTime + 100);
        verifyWindowValueWithEmptyHeaders("key2", "value2", baseTime + 200);
        verifyWindowValueWithEmptyHeaders("key3", "value3", baseTime + 300);

        final RecordHeaders headers = new RecordHeaders();
        headers.add("source", "proxy-test".getBytes());

        // In proxy mode, headers are stripped when writing to non-headers store
        // So we expect empty headers when reading back
        final Headers expectedHeaders = new RecordHeaders();

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, expectedHeaders);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, expectedHeaders);

        kafkaStreams.close();
    }

    private void processWindowedKeyValueAndVerifyTimestamped(final String key,
                                                             final String value,
                                                             final long timestamp) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            timestamp,
            false);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyWindowStore<String, ValueAndTimestamp<String>> store =
                    IntegrationTestUtils.getStore(WINDOW_STORE_NAME, kafkaStreams, QueryableStoreTypes.timestampedWindowStore());

                if (store == null) {
                    return false;
                }

                final long windowStart = timestamp - (timestamp % WINDOW_SIZE_MS);
                final ValueAndTimestamp<String> result = store.fetch(key, windowStart);

                return result != null
                    && result.value().equals(value)
                    && result.timestamp() == timestamp;
            } catch (final Exception e) {
                return false;
            }
        }, 60_000L, "Could not verify timestamped value in time.");
    }

    private void processWindowedKeyValueWithHeadersAndVerify(final String key,
                                                              final String value,
                                                              final long timestamp,
                                                              final Headers headers,
                                                              final Headers expectedHeaders) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            headers,
            timestamp,
            false);

        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> store =
                    IntegrationTestUtils.getStore(WINDOW_STORE_NAME, kafkaStreams, QueryableStoreTypes.windowStore());

                if (store == null) {
                    return false;
                }

                final long windowStart = timestamp - (timestamp % WINDOW_SIZE_MS);

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> results = new LinkedList<>();
                try (final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> kv = iterator.next();
                        if (kv.key.key().equals(key) && kv.key.window().start() == windowStart) {
                            results.add(kv);
                        }
                    }
                }

                if (results.isEmpty()) {
                    return false;
                }

                final ValueTimestampHeaders<String> result = results.get(0).value;
                return result != null
                    && result.value().equals(value)
                    && result.timestamp() == timestamp
                    && result.headers().equals(expectedHeaders);
            } catch (final Exception e) {
                e.printStackTrace();
                return false;
            }
        }, 60_000L, "Could not verify windowed value with headers in time.");
    }

    private void verifyWindowValueWithEmptyHeaders(final String key,
                                                    final String value,
                                                    final long timestamp) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> store =
                    IntegrationTestUtils.getStore(WINDOW_STORE_NAME, kafkaStreams, QueryableStoreTypes.windowStore());

                if (store == null) {
                    return false;
                }

                final long windowStart = timestamp - (timestamp % WINDOW_SIZE_MS);

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> results = new LinkedList<>();
                try (final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> kv = iterator.next();
                        if (kv.key.key().equals(key) && kv.key.window().start() == windowStart) {
                            results.add(kv);
                        }
                    }
                }

                if (results.isEmpty()) {
                    return false;
                }

                final ValueTimestampHeaders<String> result = results.get(0).value;
                assertNotNull(result, "Result should not be null");
                assertEquals(value, result.value(), "Value should match");
                assertEquals(timestamp, result.timestamp(), "Timestamp should match");

                // Verify headers exist but are empty (migrated from timestamped store without headers)
                assertNotNull(result.headers(), "Headers should not be null for migrated data");
                assertEquals(0, result.headers().toArray().length, "Headers should be empty for migrated data");

                return true;
            } catch (final Exception e) {
                e.printStackTrace();
                return false;
            }
        }, 60_000L, "Could not verify legacy value with empty headers in time.");
    }

    /**
     * Processor for TimestampedWindowStore (without headers).
     */
    private static class TimestampedWindowedProcessor implements Processor<String, String, Void, Void> {
        private TimestampedWindowStore<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(WINDOW_STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            final long windowStart = record.timestamp() - (record.timestamp() % WINDOW_SIZE_MS);
            store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp()), windowStart);
        }
    }

    /**
     * Processor for TimestampedWindowStoreWithHeaders (with headers).
     */
    private static class TimestampedWindowedWithHeadersProcessor implements Processor<String, String, Void, Void> {
        private TimestampedWindowStoreWithHeaders<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(WINDOW_STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            final long windowStart = record.timestamp() - (record.timestamp() % WINDOW_SIZE_MS);
            store.put(record.key(),
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()),
                windowStart);
        }
    }

    @Test
    public void shouldFailDowngradeFromTimestampedWindowStoreWithHeadersToTimestampedWindowStore() throws Exception {
        final Properties props = props();
        setupAndPopulateWindowStoreWithHeaders(props, singletonList(KeyValue.pair("key1", 100L)));
        kafkaStreams = null;

        // Attempt to downgrade to non-headers window store
        final StreamsBuilder downgradedBuilder = new StreamsBuilder();
        downgradedBuilder.addStateStore(
                Stores.timestampedWindowStoreBuilder(
                    Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME,
                        Duration.ofMillis(RETENTION_MS),
                        Duration.ofMillis(WINDOW_SIZE_MS),
                        false),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedWindowedProcessor::new, WINDOW_STORE_NAME);

        kafkaStreams = new KafkaStreams(downgradedBuilder.build(), props);

        boolean exceptionThrown = false;
        try {
            kafkaStreams.start();
        } catch (final Exception e) {
            Throwable cause = e;
            while (cause != null) {
                if (cause instanceof ProcessorStateException &&
                    cause.getMessage() != null &&
                    cause.getMessage().contains("headers-aware") &&
                    cause.getMessage().contains("Downgrade")) {
                    exceptionThrown = true;
                    break;
                }
                cause = cause.getCause();
            }

            if (!exceptionThrown) {
                throw new AssertionError("Expected ProcessorStateException about downgrade not being supported, but got: " + e.getMessage(), e);
            }
        } finally {
            kafkaStreams.close(Duration.ofSeconds(30L));
        }

        if (!exceptionThrown) {
            throw new AssertionError("Expected ProcessorStateException to be thrown when attempting to downgrade from headers-aware to non-headers window store");
        }
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedWindowStoreWithHeadersAfterCleanup() throws Exception {
        final Properties props = props();
        setupAndPopulateWindowStoreWithHeaders(props, asList(KeyValue.pair("key1", 100L), KeyValue.pair("key2", 200L)));

        kafkaStreams.cleanUp(); // Delete local state
        kafkaStreams = null;

        final StreamsBuilder downgradedBuilder = new StreamsBuilder();
        downgradedBuilder.addStateStore(
                Stores.timestampedWindowStoreBuilder(
                    Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME,
                        Duration.ofMillis(RETENTION_MS),
                        Duration.ofMillis(WINDOW_SIZE_MS),
                        false),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedWindowedProcessor::new, WINDOW_STORE_NAME);

        kafkaStreams = new KafkaStreams(downgradedBuilder.build(), props);
        kafkaStreams.start();

        final long newTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", newTime + 300);
        processWindowedKeyValueAndVerifyTimestamped("key4", "value4", newTime + 400);

        kafkaStreams.close();
    }

    private boolean windowStoreContainsKey(final String key, final long timestamp) {
        try {
            final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> store =
                IntegrationTestUtils.getStore(WINDOW_STORE_NAME, kafkaStreams, QueryableStoreTypes.windowStore());

            if (store == null) {
                return false;
            }

            final long expectedWindowStart = timestamp - (timestamp % WINDOW_SIZE_MS);
            try (final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.all()) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> kv = iterator.next();
                    if (kv.key.key().equals(key) && kv.key.window().start() == expectedWindowStart) {
                        return true;
                    }
                }
            }
            return false;
        } catch (final Exception e) {
            return false;
        }
    }

    /**
     * Setup and populate a window store with headers.
     * @param props Streams properties
     * @param records List of (key, timestampOffset) tuples. Values will be generated as "value{N}"
     * @return base time used for record timestamps
     */
    private long setupAndPopulateWindowStoreWithHeaders(final Properties props,
                                                        final List<KeyValue<String, Long>> records) throws Exception {
        final long baseTime = setupWindowStoreWithHeaders(props);

        for (int i = 0; i < records.size(); i++) {
            final KeyValue<String, Long> record = records.get(i);
            final String value = "value" + (i + 1);
            produceRecordWithHeaders(record.key, value, baseTime + record.value);
        }

        // Wait for all records to be processed
        TestUtils.waitForCondition(
            () -> {
                for (final KeyValue<String, Long> record : records) {
                    if (!windowStoreContainsKey(record.key, baseTime + record.value)) {
                        return false;
                    }
                }
                return true;
            },
            30_000L,
            "Store was not populated with expected data"
        );

        kafkaStreams.close();
        return baseTime;
    }

    private long setupWindowStoreWithHeaders(final Properties props) {
        final StreamsBuilder headersBuilder = new StreamsBuilder();
        headersBuilder.addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME,
                        Duration.ofMillis(RETENTION_MS),
                        Duration.ofMillis(WINDOW_SIZE_MS),
                        false),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME);

        kafkaStreams = new KafkaStreams(headersBuilder.build(), props);
        kafkaStreams.start();

        return CLUSTER.time.milliseconds();
    }

    private void produceRecordWithHeaders(final String key, final String value, final long timestamp) throws Exception {
        final Headers headers = new RecordHeaders();
        headers.add("source", "test".getBytes());

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            headers,
            timestamp,
            false);
    }
}