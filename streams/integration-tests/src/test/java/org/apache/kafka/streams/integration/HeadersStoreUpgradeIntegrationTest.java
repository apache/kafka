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
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
public class HeadersStoreUpgradeIntegrationTest {
    private static final String STORE_NAME = "store";
    private static final String WINDOW_STORE_NAME = "window-store";
    private static final String SESSION_STORE_NAME = "session-store";
    private static final long WINDOW_SIZE_MS = 1000L;
    private static final long RETENTION_MS = Duration.ofDays(1).toMillis();
    private static final long DEFAULT_STORE_TIMEOUT_MS = 60_000L;
    private static final Logger LOG = LoggerFactory.getLogger(HeadersStoreUpgradeIntegrationTest.class);
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
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    private void buildAndStart(final StoreBuilder<?> storeBuilder,
                               final ProcessorSupplier<String, String, Void, Void> processorSupplier,
                               final String storeName,
                               final Properties props) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(storeBuilder)
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(processorSupplier, storeName);
        kafkaStreams = new KafkaStreams(builder.build(), props);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
    }

    /**
     * Produces a single record (no explicit timestamp) into the input stream. Keys and values are
     * always {@link String}, matching the {@link StringSerializer} used below.
     */
    private void produce(final String key, final String value) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            CLUSTER.time,
            false);
    }

    /**
     * Produces a single record with an explicit timestamp into the input stream.
     */
    private void produce(final String key, final String value, final long timestamp) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            timestamp,
            false);
    }

    /**
     * Produces a single record with an explicit timestamp and headers into the input stream.
     */
    private void produce(final String key, final String value, final long timestamp, final Headers headers) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            headers,
            timestamp,
            false);
    }

    /**
     * Shared skeleton for the process-and-verify / verify helpers: waits until the named store is
     * queryable and the supplied condition holds. The store lookup is retried until the condition
     * passes or the timeout elapses; transient query {@link Exception}s (e.g. store not yet ready)
     * are swallowed and treated as "not ready".
     *
     * <p>A condition that uses {@code assertX(...)} does not fail fast: {@link TestUtils#waitForCondition}
     * retries on {@link AssertionError} until the timeout, then rethrows the last assertion failure
     * (with its specific message). Returning {@code false} and throwing an assertion therefore differ
     * only in the message reported once the timeout is hit.
     */
    private <S> void awaitStore(final String storeName,
                                final QueryableStoreType<S> storeType,
                                final Predicate<S> condition,
                                final String message) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                return condition.test(IntegrationTestUtils.getStore(storeName, kafkaStreams, storeType));
            } catch (final Exception swallow) {
                LOG.error("Error while checking result for store {}", storeName, swallow);
                return false;
            }
        }, DEFAULT_STORE_TIMEOUT_MS, message);
    }

    /**
     * Computes the start of the window that {@code timestamp} falls into for the fixed
     * {@link #WINDOW_SIZE_MS} window size.
     */
    private static long windowStart(final long timestamp) {
        return timestamp - (timestamp % WINDOW_SIZE_MS);
    }

    /**
     * Finds the entry stored for {@code key} in the window that {@code timestamp} falls into, by
     * scanning {@link ReadOnlyWindowStore#all()} and matching on key and window start. Returns the
     * matched {@link KeyValue} so an empty result means "no such entry" and is not conflated with a
     * matched entry that happens to have a {@code null} value — callers can assert on the value.
     */
    private static Optional<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> findWindowedValue(
        final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> store,
        final String key,
        final long timestamp) {
        final long start = windowStart(timestamp);
        try (final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> iterator = store.all()) {
            while (iterator.hasNext()) {
                final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> kv = iterator.next();
                if (kv.key.key().equals(key) && kv.key.window().start() == start) {
                    return Optional.of(kv);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Finds the entry stored for {@code key} in the session bounded by {@code timestamp}, by scanning
     * {@link ReadOnlySessionStore#fetch(Object)} and matching on key and an exact session window
     * ({@code SessionWindow(timestamp, timestamp)}). Both window start and end must equal
     * {@code timestamp}, so a migration bug that mangles either boundary fails the match. Returns the
     * matched {@link KeyValue} so an empty result means "no such entry" and is not conflated with a
     * matched entry that happens to have a {@code null} value.
     */
    private static <V> Optional<KeyValue<Windowed<String>, V>> findSessionValue(
        final ReadOnlySessionStore<String, V> store,
        final String key,
        final long timestamp) {
        try (final KeyValueIterator<Windowed<String>, V> iterator = store.fetch(key)) {
            while (iterator.hasNext()) {
                final KeyValue<Windowed<String>, V> kv = iterator.next();
                if (kv.key.key().equals(key)
                    && kv.key.window().start() == timestamp
                    && kv.key.window().end() == timestamp) {
                    return Optional.of(kv);
                }
            }
        }
        return Optional.empty();
    }

    private void assertDowngradeThrowsProcessorStateException(
            final String downgradeTarget,
            final StoreBuilder<?> storeBuilder,
            final ProcessorSupplier<String, String, Void, Void> processorSupplier,
            final String storeName,
            final Properties props) {
        boolean exceptionThrown = false;
        try {
            buildAndStart(storeBuilder, processorSupplier, storeName, props);
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
                throw new AssertionError(
                    "Expected ProcessorStateException about downgrade " + downgradeTarget
                        + " not being supported, but got: " + e.getMessage(), e);
            }
        } finally {
            if (kafkaStreams != null) {
                kafkaStreams.close(Duration.ofSeconds(30L));
            }
        }
        if (!exceptionThrown) {
            throw new AssertionError(
                "Expected ProcessorStateException to be thrown when attempting to downgrade "
                    + downgradeTarget + " from headers-aware store");
        }
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
        final Properties props = props();

        buildAndStart(
            Stores.timestampedKeyValueStoreBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueProcessor::new, STORE_NAME, props);

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        kafkaStreams.close();
        kafkaStreams = null;

        buildAndStart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME, props);

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
        final Properties props = props();

        buildAndStart(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueProcessor::new, STORE_NAME, props);

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        kafkaStreams.close();
        kafkaStreams = null;

        buildAndStart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME, props);

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

    @Test
    public void shouldMigrateInMemoryPlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        shouldMigratePlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(false);
    }

    @Test
    public void shouldMigratePersistentPlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        shouldMigratePlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(true);
    }

    private void shouldMigratePlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(final boolean persistentStore) throws Exception {
        final Properties props = props();

        buildAndStart(
            Stores.keyValueStoreBuilder(
                persistentStore ? Stores.persistentKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KeyValueProcessor::new, STORE_NAME, props);

        processKeyValueAndVerifyValue("key1", "value1");
        final long lastUpdateKeyOne = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processKeyValueAndVerifyValue("key2", "value2");
        final long lastUpdateKeyTwo = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        processKeyValueAndVerifyValue("key3", "value3");
        final long lastUpdateKeyThree = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

        kafkaStreams.close();
        kafkaStreams = null;

        buildAndStart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME, props);

        // Verify legacy data can be read with empty headers and timestamp
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", lastUpdateKeyOne);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", lastUpdateKeyTwo);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", lastUpdateKeyThree);

        // Process new records with headers
        final Headers headers = new RecordHeaders();
        headers.add("source", "test".getBytes());

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, headers);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, headers);

        kafkaStreams.close();
    }

    @Test
    public void shouldProxyPlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        final Properties props = props();

        buildAndStart(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KeyValueProcessor::new, STORE_NAME, props);

        processKeyValueAndVerifyValue("key1", "value1");
        processKeyValueAndVerifyValue("key2", "value2");
        processKeyValueAndVerifyValue("key3", "value3");

        kafkaStreams.close();
        kafkaStreams = null;

        buildAndStart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME, props);

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", -1L);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", -1L);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", -1L);

        // Process new records with headers
        final RecordHeaders headers = new RecordHeaders();
        headers.add("source", "proxy-test".getBytes());
        final Headers expectedHeaders = new RecordHeaders();

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, -1, headers, expectedHeaders);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, -1, headers, expectedHeaders);

        kafkaStreams.close();
    }

    private void processKeyValueAndVerifyTimestampedValue(final String key,
                                                          final String value,
                                                          final long timestamp) throws Exception {
        produce(key, value, timestamp);
        verifyLegacyTimestampedValue(key, value, timestamp);
    }

    private void processKeyValueAndVerifyValue(final String key,
                                               final String value) throws Exception {
        produce(key, value);

        awaitStore(STORE_NAME, QueryableStoreTypes.<String, String>keyValueStore(),
            store -> {
                final String result = store.get(key);
                return result != null && result.equals(value);
            },
            "Could not get expected result in time.");
    }

    private void verifyLegacyTimestampedValue(final String key,
                                              final String value,
                                              final long timestamp) throws Exception {
        awaitStore(STORE_NAME, QueryableStoreTypes.<String, String>timestampedKeyValueStore(),
            store -> {
                final ValueAndTimestamp<String> result = store.get(key);
                return result != null && result.value().equals(value) && result.timestamp() == timestamp;
            },
            "Could not get expected result in time.");
    }

    private void processKeyValueWithTimestampAndHeadersAndVerify(final String key,
                                                                 final String value,
                                                                 final long timestamp,
                                                                 final Headers headers,
                                                                 final Headers expectedHeaders) throws Exception {
        processKeyValueWithTimestampAndHeadersAndVerify(key, value, timestamp, timestamp, headers, expectedHeaders);
    }

    private void processKeyValueWithTimestampAndHeadersAndVerify(final String key,
                                                                 final String value,
                                                                 final long timestamp,
                                                                 final long expectedTimestamp,
                                                                 final Headers headers,
                                                                 final Headers expectedHeaders) throws Exception {
        produce(key, value, timestamp, headers);
        verifyKeyValueWithHeaders(key, value, expectedTimestamp, expectedHeaders);
    }

    private void verifyLegacyValuesWithEmptyHeaders(final String key,
                                                    final String value,
                                                    final long timestamp) throws Exception {
        verifyKeyValueWithHeaders(key, value, timestamp, new RecordHeaders());
    }

    /**
     * Verifies the value stored for {@code key} in the timestamped-with-headers key-value store,
     * expecting {@code expectedTimestamp} and {@code expectedHeaders}. Pass an empty
     * {@link RecordHeaders} for stores migrated without headers.
     */
    private void verifyKeyValueWithHeaders(final String key,
                                           final String value,
                                           final long expectedTimestamp,
                                           final Headers expectedHeaders) throws Exception {
        awaitStore(STORE_NAME, QueryableStoreTypes.<String, String>timestampedKeyValueStoreWithHeaders(),
            store -> {
                final ValueTimestampHeaders<String> result = store.get(key);
                return result != null
                    && result.value().equals(value)
                    && result.timestamp() == expectedTimestamp
                    && result.headers().equals(expectedHeaders);
            },
            "Could not get expected result in time.");
    }

    private static class KeyValueProcessor implements Processor<String, String, Void, Void> {
        private KeyValueStore<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), record.value());
        }
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
    public void shouldMigrateInMemoryPlainWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        shouldMigratePlainWindowStoreToTimestampedWindowStoreWithHeaders(false);
    }

    @Test
    public void shouldMigratePersistentPlainWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        shouldMigratePlainWindowStoreToTimestampedWindowStoreWithHeaders(true);
    }

    private void shouldMigratePlainWindowStoreToTimestampedWindowStoreWithHeaders(final boolean persistentStore) throws Exception {
        final Properties props = props();

        // Run with old plain WindowStore
        buildAndStart(
            Stores.windowStoreBuilder(
                persistentStore
                    ? Stores.persistentWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            PlainWindowedProcessor::new, WINDOW_STORE_NAME, props);

        final long baseTime = CLUSTER.time.milliseconds();
        processPlainWindowedKeyValueAndVerify("key1", "value1", baseTime + 100);
        processPlainWindowedKeyValueAndVerify("key2", "value2", baseTime + 200);
        processPlainWindowedKeyValueAndVerify("key3", "value3", baseTime + 300);

        kafkaStreams.close();
        kafkaStreams = null;

        // Restart with TimestampedWindowStoreWithHeaders
        buildAndStart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                persistentStore
                    ? Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME, props);

        verifyWindowValue("key1", "value1", baseTime + 100, persistentStore ? -1L : baseTime + 100);
        verifyWindowValue("key2", "value2", baseTime + 200, persistentStore ? -1L : baseTime + 200);
        verifyWindowValue("key3", "value3", baseTime + 300, persistentStore ? -1L : baseTime + 300);

        final Headers headers = new RecordHeaders();
        headers.add("source", "migration-test".getBytes());
        headers.add("version", "1.0".getBytes());

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, headers);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, headers);

        kafkaStreams.close();
    }

    @Test
    public void shouldProxyPlainWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        final Properties props = props();

        buildAndStart(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            PlainWindowedProcessor::new, WINDOW_STORE_NAME, props);

        final long baseTime = CLUSTER.time.milliseconds();
        processPlainWindowedKeyValueAndVerify("key1", "value1", baseTime + 100);
        processPlainWindowedKeyValueAndVerify("key2", "value2", baseTime + 200);
        processPlainWindowedKeyValueAndVerify("key3", "value3", baseTime + 300);

        kafkaStreams.close();
        kafkaStreams = null;

        // Restart with headers-aware builder but non-headers supplier (proxy/adapter mode)
        buildAndStart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME, props);

        verifyWindowValue("key1", "value1", baseTime + 100, -1L);
        verifyWindowValue("key2", "value2", baseTime + 200, -1L);
        verifyWindowValue("key3", "value3", baseTime + 300, -1L);

        final RecordHeaders headers = new RecordHeaders();
        headers.add("source", "proxy-test".getBytes());
        headers.add("version", "2.0".getBytes());

        // In proxy mode with plain store, headers and timestamps are not preserved
        final RecordHeaders expectedHeaders = new RecordHeaders();

        // Plain window store does not preserve timestamps, so the stored timestamp is -1.
        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, -1L, headers, expectedHeaders);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, -1L, headers, expectedHeaders);

        kafkaStreams.close();
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
        final Properties props = props();

        // Phase 1: Run with old TimestampedWindowStore
        buildAndStart(
            Stores.timestampedWindowStoreBuilder(
                persistentStore
                    ? Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedProcessor::new, WINDOW_STORE_NAME, props);

        final long baseTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key1", "value1", baseTime + 100);
        processWindowedKeyValueAndVerifyTimestamped("key2", "value2", baseTime + 200);
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", baseTime + 300);

        kafkaStreams.close();
        kafkaStreams = null;

        buildAndStart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                persistentStore
                    ? Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME, props);

        verifyWindowValue("key1", "value1", baseTime + 100, baseTime + 100);
        verifyWindowValue("key2", "value2", baseTime + 200, baseTime + 200);
        verifyWindowValue("key3", "value3", baseTime + 300, baseTime + 300);

        final Headers headers = new RecordHeaders();
        headers.add("source", "migration-test".getBytes());
        headers.add("version", "1.0".getBytes());

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, headers);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, headers);

        kafkaStreams.close();
    }

    @Test
    public void shouldProxyTimestampedWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        final Properties props = props();

        buildAndStart(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedProcessor::new, WINDOW_STORE_NAME, props);

        final long baseTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key1", "value1", baseTime + 100);
        processWindowedKeyValueAndVerifyTimestamped("key2", "value2", baseTime + 200);
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", baseTime + 300);

        kafkaStreams.close();
        kafkaStreams = null;

        // Restart with headers-aware builder but non-headers supplier (proxy/adapter mode)
        buildAndStart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME, props);

        verifyWindowValue("key1", "value1", baseTime + 100, baseTime + 100);
        verifyWindowValue("key2", "value2", baseTime + 200, baseTime + 200);
        verifyWindowValue("key3", "value3", baseTime + 300, baseTime + 300);

        final RecordHeaders headers = new RecordHeaders();
        headers.add("source", "proxy-test".getBytes());

        // In proxy mode, headers are stripped when writing to non-headers store
        // So we expect empty headers when reading back
        final Headers expectedHeaders = new RecordHeaders();

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, expectedHeaders);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, expectedHeaders);

        kafkaStreams.close();
    }

    private void processPlainWindowedKeyValueAndVerify(final String key,
                                                       final String value,
                                                       final long timestamp) throws Exception {
        produce(key, value, timestamp);

        awaitStore(WINDOW_STORE_NAME, QueryableStoreTypes.<String, String>windowStore(),
            store -> {
                final String result = store.fetch(key, windowStart(timestamp));
                return result != null && result.equals(value);
            },
            "Could not verify plain window value in time.");
    }

    /**
     * Verifies the value stored for {@code key} in the window that {@code windowTimestamp} falls into,
     * expecting {@code expectedTimestamp} and empty headers (i.e. a store migrated without headers).
     */
    private void verifyWindowValue(final String key,
                                   final String value,
                                   final long windowTimestamp,
                                   final long expectedTimestamp) throws Exception {
        verifyWindowValue(key, value, windowTimestamp, expectedTimestamp, new RecordHeaders());
    }

    /**
     * Verifies the value stored for {@code key} in the window that {@code windowTimestamp} falls into,
     * expecting {@code expectedTimestamp} and {@code expectedHeaders} in the store. Pass an empty
     * {@link RecordHeaders} for stores migrated without headers.
     */
    private void verifyWindowValue(final String key,
                                   final String value,
                                   final long windowTimestamp,
                                   final long expectedTimestamp,
                                   final Headers expectedHeaders) throws Exception {
        awaitStore(WINDOW_STORE_NAME, QueryableStoreTypes.<String, String>timestampedWindowStoreWithHeaders(),
            store -> {
                final Optional<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> result =
                    findWindowedValue(store, key, windowTimestamp);
                if (result.isEmpty()) {
                    return false;
                }

                final ValueTimestampHeaders<String> actual = result.get().value;
                assertNotNull(actual, "Stored value should not be null");
                assertEquals(value, actual.value(), "Value should match");
                assertEquals(expectedTimestamp, actual.timestamp(), "Timestamp should be " + expectedTimestamp);
                assertEquals(expectedHeaders, actual.headers(), "Headers should match");
                return true;
            },
            "Could not verify window value in time.");
    }

    private void processWindowedKeyValueAndVerifyTimestamped(final String key,
                                                             final String value,
                                                             final long timestamp) throws Exception {
        produce(key, value, timestamp);

        awaitStore(WINDOW_STORE_NAME, QueryableStoreTypes.<String, String>timestampedWindowStore(),
            store -> {
                final ValueAndTimestamp<String> result = store.fetch(key, windowStart(timestamp));
                return result != null
                    && result.value().equals(value)
                    && result.timestamp() == timestamp;
            },
            "Could not verify timestamped value in time.");
    }

    private void processWindowedKeyValueWithHeadersAndVerify(final String key,
                                                             final String value,
                                                             final long timestamp,
                                                             final Headers headers,
                                                             final Headers expectedHeaders) throws Exception {
        processWindowedKeyValueWithHeadersAndVerify(key, value, timestamp, timestamp, headers, expectedHeaders);
    }

    /**
     * Produces a windowed record with headers and verifies the stored value/headers, expecting
     * {@code expectedTimestamp} in the store. For a plain window store (no timestamp preserved)
     * pass {@code expectedTimestamp == -1L}; otherwise pass the produced {@code timestamp}.
     */
    private void processWindowedKeyValueWithHeadersAndVerify(final String key,
                                                             final String value,
                                                             final long timestamp,
                                                             final long expectedTimestamp,
                                                             final Headers headers,
                                                             final Headers expectedHeaders) throws Exception {
        produce(key, value, timestamp, headers);
        verifyWindowValue(key, value, timestamp, expectedTimestamp, expectedHeaders);
    }

    /**
     * Processor for plain WindowStore (without timestamps or headers).
     */
    private static class PlainWindowedProcessor implements Processor<String, String, Void, Void> {
        private WindowStore<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(WINDOW_STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            final long windowStart = record.timestamp() - (record.timestamp() % WINDOW_SIZE_MS);
            store.put(record.key(), record.value(), windowStart);
        }
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
    public void shouldFailDowngradeFromTimestampedKeyValueStoreWithHeadersToPlainKeyValueStore() throws Exception {
        final Properties props = props();
        setupAndPopulateKeyValueStoreWithHeaders(props);
        kafkaStreams = null;

        assertDowngradeThrowsProcessorStateException(
            "to plain key-value store",
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KeyValueProcessor::new, STORE_NAME, props);
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedKeyValueStoreWithHeadersToPlainKeyValueStoreAfterCleanup() throws Exception {
        final Properties props = props();
        setupAndPopulateKeyValueStoreWithHeaders(props);

        kafkaStreams.cleanUp(); // Delete local state
        kafkaStreams = null;

        buildAndStart(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KeyValueProcessor::new, STORE_NAME, props);

        processKeyValueAndVerifyValue("key3", "value3");
        processKeyValueAndVerifyValue("key4", "value4");

        kafkaStreams.close();
    }

    @Test
    public void shouldFailDowngradeFromTimestampedKeyValueStoreWithHeadersToTimestampedKeyValueStore() throws Exception {
        final Properties props = props();
        setupAndPopulateKeyValueStoreWithHeaders(props);
        kafkaStreams = null;

        assertDowngradeThrowsProcessorStateException(
            "to timestamped key-value store",
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueProcessor::new, STORE_NAME, props);
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedKeyValueStoreWithHeadersToTimestampedKeyValueStoreAfterCleanup() throws Exception {
        final Properties props = props();
        setupAndPopulateKeyValueStoreWithHeaders(props);

        kafkaStreams.cleanUp(); // Delete local state
        kafkaStreams = null;

        buildAndStart(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueProcessor::new, STORE_NAME, props);

        // verify legacy key, values
        verifyLegacyTimestampedValue("key1", "value1", 11L);
        verifyLegacyTimestampedValue("key2", "value2", 22L);

        processKeyValueAndVerifyTimestampedValue("key3", "value3", 333L);
        processKeyValueAndVerifyTimestampedValue("key4", "value4", 444L);

        kafkaStreams.close();
    }


    @Test
    public void shouldFailDowngradeFromTimestampedWindowStoreWithHeadersToPlainWindowStore() throws Exception {
        final Properties props = props();
        setupAndPopulateWindowStoreWithHeaders(props, List.of(KeyValue.pair("key1", 100L)));
        kafkaStreams = null;

        assertDowngradeThrowsProcessorStateException(
            "to plain window store",
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME,
                    Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE_MS),
                    false),
                Serdes.String(),
                Serdes.String()),
            PlainWindowedProcessor::new, WINDOW_STORE_NAME, props);
    }

    @Test
    public void shouldFailDowngradeFromTimestampedWindowStoreWithHeadersToTimestampedWindowStore() throws Exception {
        final Properties props = props();
        setupAndPopulateWindowStoreWithHeaders(props, singletonList(KeyValue.pair("key1", 100L)));
        kafkaStreams = null;

        assertDowngradeThrowsProcessorStateException(
            "to timestamped window store",
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME,
                    Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE_MS),
                    false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedProcessor::new, WINDOW_STORE_NAME, props);
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedWindowStoreWithHeadersToPlainWindowStoreAfterCleanup() throws Exception {
        final Properties props = props();
        setupAndPopulateWindowStoreWithHeaders(props, asList(KeyValue.pair("key1", 100L), KeyValue.pair("key2", 200L)));

        kafkaStreams.cleanUp();
        kafkaStreams = null;

        buildAndStart(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME,
                    Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE_MS),
                    false),
                Serdes.String(),
                Serdes.String()),
            PlainWindowedProcessor::new, WINDOW_STORE_NAME, props);

        final long newTime = CLUSTER.time.milliseconds();
        processPlainWindowedKeyValueAndVerify("key3", "value3", newTime + 300);
        processPlainWindowedKeyValueAndVerify("key4", "value4", newTime + 400);

        kafkaStreams.close();
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedWindowStoreWithHeadersAfterCleanup() throws Exception {
        final Properties props = props();
        setupAndPopulateWindowStoreWithHeaders(props, asList(KeyValue.pair("key1", 100L), KeyValue.pair("key2", 200L)));

        kafkaStreams.cleanUp(); // Delete local state
        kafkaStreams = null;

        buildAndStart(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME,
                    Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE_MS),
                    false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedProcessor::new, WINDOW_STORE_NAME, props);

        final long newTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", newTime + 300);
        processWindowedKeyValueAndVerifyTimestamped("key4", "value4", newTime + 400);

        kafkaStreams.close();
    }

    /**
     * Setup and populate a window store with headers.
     * @param props Streams properties
     * @param records List of (key, timestampOffset) tuples. Values will be generated as "value{N}"
     */
    private void setupAndPopulateWindowStoreWithHeaders(final Properties props,
                                                        final List<KeyValue<String, Long>> records) throws Exception {
        final long baseTime = setupWindowStoreWithHeaders(props);

        for (int i = 0; i < records.size(); i++) {
            final KeyValue<String, Long> record = records.get(i);
            final String value = "value" + (i + 1);
            produceRecordWithHeaders(record.key, value, baseTime + record.value);
        }

        // Wait for all records to be processed
        awaitStore(WINDOW_STORE_NAME, QueryableStoreTypes.<String, String>timestampedWindowStoreWithHeaders(),
            store -> records.stream()
                .allMatch(record -> findWindowedValue(store, record.key, baseTime + record.value).isPresent()),
            "Store was not populated with expected data");

        kafkaStreams.close();
    }

    private long setupWindowStoreWithHeaders(final Properties props) throws Exception {
        buildAndStart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME,
                    Duration.ofMillis(RETENTION_MS),
                    Duration.ofMillis(WINDOW_SIZE_MS),
                    false),
                Serdes.String(),
                Serdes.String()),
            TimestampedWindowedWithHeadersProcessor::new, WINDOW_STORE_NAME, props);

        return CLUSTER.time.milliseconds();
    }

    private void produceRecordWithHeaders(final String key, final String value, final long timestamp) {
        final Headers headers = new RecordHeaders();
        headers.add("source", "test".getBytes());

        produce(key, value, timestamp, headers);
    }

    private void setupAndPopulateKeyValueStoreWithHeaders(final Properties props) throws Exception {
        buildAndStart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME, props);

        final Headers headers = new RecordHeaders();
        headers.add("source", "test".getBytes());

        processKeyValueWithTimestampAndHeadersAndVerify("key1", "value1", 11L, headers, headers);
        processKeyValueWithTimestampAndHeadersAndVerify("key2", "value2", 22L, headers, headers);

        kafkaStreams.close();
    }

    // ==================== Session Store Tests ====================

    @Test
    public void shouldMigratePersistentSessionStoreToSessionStoreWithHeadersUsingPapi() throws Exception {
        shouldMigrateSessionStoreToSessionStoreWithHeaders(true);
    }

    @Test
    public void shouldMigrateInMemorySessionStoreToSessionStoreWithHeadersUsingPapi() throws Exception {
        shouldMigrateSessionStoreToSessionStoreWithHeaders(false);
    }

    private void shouldMigrateSessionStoreToSessionStoreWithHeaders(final boolean isPersistent) throws Exception {
        // Phase 1: Run with plain SessionStore
        final StreamsBuilder oldBuilder = new StreamsBuilder();
        oldBuilder.addStateStore(
                Stores.sessionStoreBuilder(
                    isPersistent ? Stores.persistentSessionStore(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)) :
                        Stores.inMemorySessionStore(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(SessionProcessor::new, SESSION_STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(oldBuilder.build(), props);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        final long baseTime = CLUSTER.time.milliseconds();
        processSessionKeyValueAndVerify("key1", "value1", baseTime + 100);
        processSessionKeyValueAndVerify("key2", "value2", baseTime + 200);
        processSessionKeyValueAndVerify("key3", "value3", baseTime + 300);

        kafkaStreams.close(Duration.ofSeconds(5L));
        kafkaStreams = null;

        // Phase 2: Restart with SessionStoreWithHeaders (headers-aware supplier)
        final StreamsBuilder newBuilder = new StreamsBuilder();
        newBuilder.addStateStore(
                Stores.sessionStoreWithHeadersBuilder(
                    isPersistent ? Stores.persistentSessionStoreWithHeaders(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)) :
                        Stores.inMemorySessionStore(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(SessionWithHeadersProcessor::new, SESSION_STORE_NAME);

        kafkaStreams = new KafkaStreams(newBuilder.build(), props);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        // Verify legacy data can be read with empty headers
        verifySessionValueWithEmptyHeaders("key1", "value1", baseTime + 100);
        verifySessionValueWithEmptyHeaders("key2", "value2", baseTime + 200);
        verifySessionValueWithEmptyHeaders("key3", "value3", baseTime + 300);

        // Process new records with headers
        final Headers headers = new RecordHeaders();
        headers.add("source", "migration-test".getBytes());

        processSessionKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, headers);
        processSessionKeyValueWithHeadersAndVerify("key5", "value5", baseTime + 500, headers, headers);

        kafkaStreams.close();
    }

    @Test
    public void shouldProxySessionStoreToSessionStoreWithHeaders() throws Exception {
        // Phase 1: Run with plain SessionStore
        final StreamsBuilder oldBuilder = new StreamsBuilder();
        oldBuilder.addStateStore(
                Stores.sessionStoreBuilder(
                    Stores.persistentSessionStore(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(SessionProcessor::new, SESSION_STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(oldBuilder.build(), props);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        final long baseTime = CLUSTER.time.milliseconds();
        processSessionKeyValueAndVerify("key1", "value1", baseTime + 100);
        processSessionKeyValueAndVerify("key2", "value2", baseTime + 200);
        processSessionKeyValueAndVerify("key3", "value3", baseTime + 300);

        kafkaStreams.close();
        kafkaStreams = null;

        // Phase 2: Restart with headers-aware builder but non-headers supplier (proxy/adapter mode)
        final StreamsBuilder newBuilder = new StreamsBuilder();
        newBuilder.addStateStore(
                Stores.sessionStoreWithHeadersBuilder(
                    Stores.persistentSessionStore(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)),  // non-headers supplier!
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(SessionWithHeadersProcessor::new, SESSION_STORE_NAME);

        kafkaStreams = new KafkaStreams(newBuilder.build(), props);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        // Verify legacy data can be read with empty headers
        verifySessionValueWithEmptyHeaders("key1", "value1", baseTime + 100);
        verifySessionValueWithEmptyHeaders("key2", "value2", baseTime + 200);
        verifySessionValueWithEmptyHeaders("key3", "value3", baseTime + 300);

        // In proxy mode, headers are stripped when writing to non-headers store
        // So we expect empty headers when reading back
        final RecordHeaders headers = new RecordHeaders();
        headers.add("source", "proxy-test".getBytes());
        final Headers expectedHeaders = new RecordHeaders();

        processSessionKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, expectedHeaders);
        processSessionKeyValueWithHeadersAndVerify("key5", "value5", baseTime + 500, headers, expectedHeaders);

        kafkaStreams.close();
    }

    @Test
    public void shouldFailDowngradeFromSessionStoreWithHeadersToSessionStore() throws Exception {
        final Properties props = props();
        setupAndPopulateSessionStoreWithHeaders(props);
        kafkaStreams = null;

        // Attempt to downgrade to plain session store
        final StreamsBuilder downgradedBuilder = new StreamsBuilder();
        downgradedBuilder.addStateStore(
                Stores.sessionStoreBuilder(
                    Stores.persistentSessionStore(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(SessionProcessor::new, SESSION_STORE_NAME);

        kafkaStreams = new KafkaStreams(downgradedBuilder.build(), props);

        boolean exceptionThrown = false;
        try {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
        } catch (final Exception e) {
            Throwable cause = e;
            while (cause != null) {
                if (cause instanceof ProcessorStateException &&
                    cause.getMessage() != null &&
                    cause.getMessage().contains("incompatible settings")) {
                    exceptionThrown = true;
                    break;
                }
                cause = cause.getCause();
            }

            if (!exceptionThrown) {
                throw new AssertionError("Expected ProcessorStateException about incompatible settings, but got: " + e.getMessage(), e);
            }
        } finally {
            kafkaStreams.close(Duration.ofSeconds(30L));
        }

        if (!exceptionThrown) {
            throw new AssertionError("Expected ProcessorStateException to be thrown when attempting to downgrade from headers-aware to plain session store");
        }
    }

    @Test
    public void shouldSuccessfullyDowngradeFromSessionStoreWithHeadersToSessionStoreAfterCleanup() throws Exception {
        final Properties props = props();
        setupAndPopulateSessionStoreWithHeaders(props);

        kafkaStreams.cleanUp(); // Delete local state
        kafkaStreams = null;

        final StreamsBuilder downgradedBuilder = new StreamsBuilder();
        downgradedBuilder.addStateStore(
                Stores.sessionStoreBuilder(
                    Stores.persistentSessionStore(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(SessionProcessor::new, SESSION_STORE_NAME);

        kafkaStreams = new KafkaStreams(downgradedBuilder.build(), props);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        final long newTime = CLUSTER.time.milliseconds();
        processSessionKeyValueAndVerify("key3", "value3", newTime + 300);
        processSessionKeyValueAndVerify("key4", "value4", newTime + 400);

        kafkaStreams.close();
    }

    // ==================== Session Store Helper Methods ====================

    private void processSessionKeyValueAndVerify(final String key,
                                                  final String value,
                                                  final long timestamp) throws Exception {
        produce(key, value, timestamp);

        awaitStore(SESSION_STORE_NAME, QueryableStoreTypes.<String, String>sessionStore(),
            store -> {
                final Optional<KeyValue<Windowed<String>, String>> result = findSessionValue(store, key, timestamp);
                return result.isPresent() && value.equals(result.get().value);
            },
            "Could not verify session value in time.");
    }

    private void verifySessionValueWithEmptyHeaders(final String key,
                                                    final String value,
                                                    final long timestamp) throws Exception {
        awaitStore(SESSION_STORE_NAME, QueryableStoreTypes.<String, String>sessionStoreWithHeaders(),
            store -> {
                final Optional<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> result =
                    findSessionValue(store, key, timestamp);
                if (result.isEmpty()) {
                    return false;
                }

                final AggregationWithHeaders<String> actual = result.get().value;
                assertNotNull(actual, "Stored value should not be null");
                assertEquals(value, actual.aggregation(), "Value should match");

                // Verify headers exist but are empty (migrated from plain session store)
                assertNotNull(actual.headers(), "Headers should not be null for migrated data");
                assertEquals(0, actual.headers().toArray().length, "Headers should be empty for migrated data");

                return true;
            },
            "Could not verify legacy session value with empty headers in time.");
    }

    private void processSessionKeyValueWithHeadersAndVerify(final String key,
                                                            final String value,
                                                            final long timestamp,
                                                            final Headers headers,
                                                            final Headers expectedHeaders) throws Exception {
        produce(key, value, timestamp, headers);

        awaitStore(SESSION_STORE_NAME, QueryableStoreTypes.<String, String>sessionStoreWithHeaders(),
            store -> {
                final Optional<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> result =
                    findSessionValue(store, key, timestamp);
                return result.isPresent()
                    && result.get().value != null
                    && result.get().value.aggregation().equals(value)
                    && result.get().value.headers().equals(expectedHeaders);
            },
            "Could not verify session value with headers in time.");
    }

    private void setupAndPopulateSessionStoreWithHeaders(final Properties props) throws Exception {
        final StreamsBuilder headersBuilder = new StreamsBuilder();
        headersBuilder.addStateStore(
                Stores.sessionStoreWithHeadersBuilder(
                    Stores.persistentSessionStoreWithHeaders(SESSION_STORE_NAME, Duration.ofMillis(RETENTION_MS)),
                    Serdes.String(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(SessionWithHeadersProcessor::new, SESSION_STORE_NAME);

        kafkaStreams = new KafkaStreams(headersBuilder.build(), props);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        final long baseTime = CLUSTER.time.milliseconds();
        final Headers headers = new RecordHeaders();
        headers.add("source", "test".getBytes());

        produce("key1", "value1", baseTime + 100, headers);

        awaitStore(SESSION_STORE_NAME, QueryableStoreTypes.<String, String>sessionStoreWithHeaders(),
            store -> findSessionValue(store, "key1", baseTime + 100).isPresent(),
            "Store was not populated with expected data");

        kafkaStreams.close();
    }

    // ==================== Session Store Processors ====================

    private static class SessionProcessor implements Processor<String, String, Void, Void> {
        private SessionStore<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(SESSION_STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            final Windowed<String> sessionKey = new Windowed<>(record.key(),
                new SessionWindow(record.timestamp(), record.timestamp()));
            store.put(sessionKey, record.value());
        }
    }

    private static class SessionWithHeadersProcessor implements Processor<String, String, Void, Void> {
        private SessionStoreWithHeaders<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(SESSION_STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            final Windowed<String> sessionKey = new Windowed<>(record.key(),
                new SessionWindow(record.timestamp(), record.timestamp()));
            store.put(sessionKey, AggregationWithHeaders.make(record.value(), record.headers()));
        }
    }

}