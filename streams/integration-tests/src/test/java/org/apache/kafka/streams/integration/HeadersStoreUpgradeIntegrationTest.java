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
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.CloseOptions.GroupMembershipOperation;
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
import org.apache.kafka.streams.processor.StateStore;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("integration")
public class HeadersStoreUpgradeIntegrationTest {
    private static final String STORE_NAME = "store";
    private static final String WINDOW_STORE_NAME = "window-store";
    private static final String SESSION_STORE_NAME = "session-store";
    private static final long WINDOW_SIZE_MS = 1000L;
    private static final Duration WINDOW_SIZE = Duration.ofMillis(WINDOW_SIZE_MS);
    private static final Duration RETENTION = Duration.ofDays(1);
    private static final long DEFAULT_STORE_TIMEOUT_MS = 60_000L;
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30L);
    /**
     * Timestamp reported through the headers-aware view for records written to a store that does
     * not preserve timestamps (a persistent plain key-value or window store, migrated or proxied).
     * In-memory plain stores still report a real timestamp after migration.
     */
    private static final long NO_TIMESTAMP = -1L;
    private static final Logger LOG = LoggerFactory.getLogger(HeadersStoreUpgradeIntegrationTest.class);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private String safeTestName;
    private String inputStream;
    private Properties streamsConfig;
    private KafkaStreams kafkaStreams;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws Exception {
        safeTestName = safeUniqueTestName(testInfo);
        inputStream = "input-stream-" + safeTestName;
        CLUSTER.createTopic(inputStream);
        // Created once per test so that every restart within a test reuses the same state directory.
        streamsConfig = props();
    }

    @AfterEach
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(CLOSE_TIMEOUT);
            kafkaStreams.cleanUp();
        }
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
                               final String storeName) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(storeBuilder)
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(processorSupplier, storeName);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
    }

    /**
     * Stops the running instance (leaving the group) and starts a new one over the same state
     * directory with the given store and processor — the upgrade step every migration/proxy test
     * performs between writing legacy data and reading it back through the new store type.
     */
    private void restart(final StoreBuilder<?> storeBuilder,
                         final ProcessorSupplier<String, String, Void, Void> processorSupplier,
                         final String storeName) throws Exception {
        closeAndLeaveGroupBeforeRestart();
        kafkaStreams = null;
        buildAndStart(storeBuilder, processorSupplier, storeName);
    }

    /**
     * Deletes the local state of the (already closed) instance, so the next start has to rebuild
     * its stores from the changelog — the supported way to downgrade from a headers-aware store.
     */
    private void wipeLocalState() {
        kafkaStreams.cleanUp();
        kafkaStreams = null;
    }

    private void closeAndLeaveGroupBeforeRestart() {
        // Leave the group so the immediate restart with the same application id
        // does not wait for the previous member's session timeout.
        kafkaStreams.close(
            CloseOptions.groupMembershipOperation(GroupMembershipOperation.LEAVE_GROUP)
                .withTimeout(CLOSE_TIMEOUT));
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
     * Builds a fresh {@link Headers} from alternating key/value strings, e.g.
     * {@code headers("source", "test")}. With no arguments it returns empty headers, which is what
     * a store migrated or proxied without header support reports. A new instance is returned on
     * every call because {@link Headers} is mutable.
     */
    private static Headers headers(final String... keyValuePairs) {
        final Headers headers = new RecordHeaders();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            headers.add(keyValuePairs[i], keyValuePairs[i + 1].getBytes());
        }
        return headers;
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

    /**
     * Starts a new instance with the given (downgraded) store over the state written by a
     * headers-aware store, and asserts that startup fails with a {@link ProcessorStateException}
     * whose message contains all of {@code expectedMessageFragments}. The exception is thrown
     * synchronously from {@link KafkaStreams#start()}, which opens existing local stores, usually
     * wrapped in another exception, so the whole cause chain is searched.
     *
     * <p>Callers pass the fragments because the message depends on the store kind and downgrade
     * target: key-value and window stores report an explicit unsupported "Downgrade" naming the
     * target ("to regular store" or "to timestamped store"), so each test only passes on its own
     * error, while a session store only sees an unexpected column family ("incompatible settings").
     */
    private void assertDowngradeThrowsProcessorStateException(
            final String downgradeTarget,
            final StoreBuilder<?> storeBuilder,
            final ProcessorSupplier<String, String, Void, Void> processorSupplier,
            final String storeName,
            final String... expectedMessageFragments) {
        // The populate phase already closed the headers-aware instance; drop it so only the
        // downgraded instance is closed below and in shutdown().
        kafkaStreams = null;
        final Exception exception;
        try {
            exception = assertThrows(Exception.class,
                () -> buildAndStart(storeBuilder, processorSupplier, storeName),
                "Expected ProcessorStateException to be thrown when attempting to downgrade "
                    + downgradeTarget + " from headers-aware store");
        } finally {
            if (kafkaStreams != null) {
                kafkaStreams.close(CLOSE_TIMEOUT);
            }
        }
        if (!hasProcessorStateExceptionCause(exception, expectedMessageFragments)) {
            fail("Expected ProcessorStateException about downgrade " + downgradeTarget
                + " not being supported, but got: " + exception.getMessage(), exception);
        }
    }

    private static boolean hasProcessorStateExceptionCause(final Throwable throwable,
                                                           final String... expectedMessageFragments) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            final String message = cause.getMessage();
            if (cause instanceof ProcessorStateException
                && message != null
                && Arrays.stream(expectedMessageFragments).allMatch(message::contains)) {
                return true;
            }
        }
        return false;
    }

    // ==================== Key-Value Store Tests ====================

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMigrateTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(final boolean persistentStore) throws Exception {
        buildAndStart(
            Stores.timestampedKeyValueStoreBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_PROCESSOR, STORE_NAME);

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        restart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_WITH_HEADERS_PROCESSOR, STORE_NAME);

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", 11L);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", 22L);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", 33L);

        // Process new records with headers
        final Headers headers = headers("source", "test");

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, headers);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, headers);
    }

    @Test
    public void shouldProxyTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        buildAndStart(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_PROCESSOR, STORE_NAME);

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        restart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_WITH_HEADERS_PROCESSOR, STORE_NAME);

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", 11L);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", 22L);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", 33L);

        // Process new records with headers
        final Headers headers = headers("source", "proxy-test");
        final Headers expectedHeaders = headers();

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, expectedHeaders);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, expectedHeaders);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMigratePlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(final boolean persistentStore) throws Exception {
        buildAndStart(
            Stores.keyValueStoreBuilder(
                persistentStore ? Stores.persistentKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KEY_VALUE_PROCESSOR, STORE_NAME);

        processKeyValueAndVerifyValue("key1", "value1");
        final long lastUpdateKeyOne = persistentStore ? NO_TIMESTAMP : CLUSTER.time.milliseconds() - 1L;

        processKeyValueAndVerifyValue("key2", "value2");
        final long lastUpdateKeyTwo = persistentStore ? NO_TIMESTAMP : CLUSTER.time.milliseconds() - 1L;

        processKeyValueAndVerifyValue("key3", "value3");
        final long lastUpdateKeyThree = persistentStore ? NO_TIMESTAMP : CLUSTER.time.milliseconds() - 1L;

        restart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_WITH_HEADERS_PROCESSOR, STORE_NAME);

        // Verify legacy data can be read with empty headers and timestamp
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", lastUpdateKeyOne);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", lastUpdateKeyTwo);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", lastUpdateKeyThree);

        // Process new records with headers
        final Headers headers = headers("source", "test");

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, headers);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, headers);
    }

    @Test
    public void shouldProxyPlainKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        buildAndStart(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KEY_VALUE_PROCESSOR, STORE_NAME);

        processKeyValueAndVerifyValue("key1", "value1");
        processKeyValueAndVerifyValue("key2", "value2");
        processKeyValueAndVerifyValue("key3", "value3");

        restart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_WITH_HEADERS_PROCESSOR, STORE_NAME);

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", NO_TIMESTAMP);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", NO_TIMESTAMP);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", NO_TIMESTAMP);

        // Process new records with headers
        final Headers headers = headers("source", "proxy-test");
        final Headers expectedHeaders = headers();

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, NO_TIMESTAMP, headers, expectedHeaders);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, NO_TIMESTAMP, headers, expectedHeaders);
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
        verifyKeyValueWithHeaders(key, value, timestamp, headers());
    }

    /**
     * Verifies the value stored for {@code key} in the timestamped-with-headers key-value store,
     * expecting {@code expectedTimestamp} and {@code expectedHeaders}. Pass empty {@link #headers()}
     * for stores migrated without headers.
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

    // ==================== Window Store Tests ====================

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMigratePlainWindowStoreToTimestampedWindowStoreWithHeaders(final boolean persistentStore) throws Exception {
        // Run with old plain WindowStore
        buildAndStart(
            Stores.windowStoreBuilder(
                persistentStore
                    ? Stores.persistentWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            PLAIN_WINDOWED_PROCESSOR, WINDOW_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        processPlainWindowedKeyValueAndVerify("key1", "value1", baseTime + 100);
        processPlainWindowedKeyValueAndVerify("key2", "value2", baseTime + 200);
        processPlainWindowedKeyValueAndVerify("key3", "value3", baseTime + 300);

        // Restart with TimestampedWindowStoreWithHeaders
        restart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                persistentStore
                    ? Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_WITH_HEADERS_PROCESSOR, WINDOW_STORE_NAME);

        verifyWindowValue("key1", "value1", baseTime + 100, persistentStore ? NO_TIMESTAMP : baseTime + 100);
        verifyWindowValue("key2", "value2", baseTime + 200, persistentStore ? NO_TIMESTAMP : baseTime + 200);
        verifyWindowValue("key3", "value3", baseTime + 300, persistentStore ? NO_TIMESTAMP : baseTime + 300);

        final Headers headers = headers("source", "migration-test", "version", "1.0");

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, headers);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, headers);
    }

    @Test
    public void shouldProxyPlainWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        buildAndStart(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            PLAIN_WINDOWED_PROCESSOR, WINDOW_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        processPlainWindowedKeyValueAndVerify("key1", "value1", baseTime + 100);
        processPlainWindowedKeyValueAndVerify("key2", "value2", baseTime + 200);
        processPlainWindowedKeyValueAndVerify("key3", "value3", baseTime + 300);

        // Restart with headers-aware builder but non-headers supplier (proxy/adapter mode)
        restart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_WITH_HEADERS_PROCESSOR, WINDOW_STORE_NAME);

        verifyWindowValue("key1", "value1", baseTime + 100, NO_TIMESTAMP);
        verifyWindowValue("key2", "value2", baseTime + 200, NO_TIMESTAMP);
        verifyWindowValue("key3", "value3", baseTime + 300, NO_TIMESTAMP);

        final Headers headers = headers("source", "proxy-test", "version", "2.0");

        // In proxy mode with plain store, headers and timestamps are not preserved
        final Headers expectedHeaders = headers();

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, NO_TIMESTAMP, headers, expectedHeaders);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, NO_TIMESTAMP, headers, expectedHeaders);
    }

    /**
     * Tests migration from TimestampedWindowStore to TimestampedWindowStoreWithHeaders.
     * This is a true migration where both supplier and builder are upgraded.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMigrateTimestampedWindowStoreToTimestampedWindowStoreWithHeaders(final boolean persistentStore) throws Exception {
        // Phase 1: Run with old TimestampedWindowStore
        buildAndStart(
            Stores.timestampedWindowStoreBuilder(
                persistentStore
                    ? Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_PROCESSOR, WINDOW_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key1", "value1", baseTime + 100);
        processWindowedKeyValueAndVerifyTimestamped("key2", "value2", baseTime + 200);
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", baseTime + 300);

        restart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                persistentStore
                    ? Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false)
                    : Stores.inMemoryWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_WITH_HEADERS_PROCESSOR, WINDOW_STORE_NAME);

        verifyWindowValue("key1", "value1", baseTime + 100, baseTime + 100);
        verifyWindowValue("key2", "value2", baseTime + 200, baseTime + 200);
        verifyWindowValue("key3", "value3", baseTime + 300, baseTime + 300);

        final Headers headers = headers("source", "migration-test", "version", "1.0");

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, headers);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, headers);
    }

    @Test
    public void shouldProxyTimestampedWindowStoreToTimestampedWindowStoreWithHeaders() throws Exception {
        buildAndStart(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_PROCESSOR, WINDOW_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key1", "value1", baseTime + 100);
        processWindowedKeyValueAndVerifyTimestamped("key2", "value2", baseTime + 200);
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", baseTime + 300);

        // Restart with headers-aware builder but non-headers supplier (proxy/adapter mode)
        restart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_WITH_HEADERS_PROCESSOR, WINDOW_STORE_NAME);

        verifyWindowValue("key1", "value1", baseTime + 100, baseTime + 100);
        verifyWindowValue("key2", "value2", baseTime + 200, baseTime + 200);
        verifyWindowValue("key3", "value3", baseTime + 300, baseTime + 300);

        final Headers headers = headers("source", "proxy-test");

        // In proxy mode, headers are stripped when writing to non-headers store
        // So we expect empty headers when reading back
        final Headers expectedHeaders = headers();

        processWindowedKeyValueWithHeadersAndVerify("key3", "value3-updated", baseTime + 350, headers, expectedHeaders);
        processWindowedKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, expectedHeaders);
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
        verifyWindowValue(key, value, windowTimestamp, expectedTimestamp, headers());
    }

    /**
     * Verifies the value stored for {@code key} in the window that {@code windowTimestamp} falls into,
     * expecting {@code expectedTimestamp} and {@code expectedHeaders} in the store. Pass empty
     * {@link #headers()} for stores migrated without headers.
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
     * pass {@link #NO_TIMESTAMP}; otherwise pass the produced {@code timestamp}.
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

    // ==================== Downgrade Tests ====================

    @Test
    public void shouldFailDowngradeFromTimestampedKeyValueStoreWithHeadersToPlainKeyValueStore() throws Exception {
        setupAndPopulateKeyValueStoreWithHeaders();

        assertDowngradeThrowsProcessorStateException(
            "to plain key-value store",
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KEY_VALUE_PROCESSOR, STORE_NAME,
            "headers-aware", "Downgrade", "to regular store");
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedKeyValueStoreWithHeadersToPlainKeyValueStoreAfterCleanup() throws Exception {
        setupAndPopulateKeyValueStoreWithHeaders();
        wipeLocalState();

        buildAndStart(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            KEY_VALUE_PROCESSOR, STORE_NAME);

        processKeyValueAndVerifyValue("key3", "value3");
        processKeyValueAndVerifyValue("key4", "value4");
    }

    @Test
    public void shouldFailDowngradeFromTimestampedKeyValueStoreWithHeadersToTimestampedKeyValueStore() throws Exception {
        setupAndPopulateKeyValueStoreWithHeaders();

        assertDowngradeThrowsProcessorStateException(
            "to timestamped key-value store",
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_PROCESSOR, STORE_NAME,
            "headers-aware", "Downgrade", "to timestamped store");
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedKeyValueStoreWithHeadersToTimestampedKeyValueStoreAfterCleanup() throws Exception {
        setupAndPopulateKeyValueStoreWithHeaders();
        wipeLocalState();

        buildAndStart(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_PROCESSOR, STORE_NAME);

        // verify legacy key, values
        verifyLegacyTimestampedValue("key1", "value1", 11L);
        verifyLegacyTimestampedValue("key2", "value2", 22L);

        processKeyValueAndVerifyTimestampedValue("key3", "value3", 333L);
        processKeyValueAndVerifyTimestampedValue("key4", "value4", 444L);
    }

    @Test
    public void shouldFailDowngradeFromTimestampedWindowStoreWithHeadersToPlainWindowStore() throws Exception {
        setupAndPopulateWindowStoreWithHeaders(List.of(KeyValue.pair("key1", 100L)));

        assertDowngradeThrowsProcessorStateException(
            "to plain window store",
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            PLAIN_WINDOWED_PROCESSOR, WINDOW_STORE_NAME,
            "headers-aware", "Downgrade", "to regular store");
    }

    @Test
    public void shouldFailDowngradeFromTimestampedWindowStoreWithHeadersToTimestampedWindowStore() throws Exception {
        setupAndPopulateWindowStoreWithHeaders(List.of(KeyValue.pair("key1", 100L)));

        assertDowngradeThrowsProcessorStateException(
            "to timestamped window store",
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_PROCESSOR, WINDOW_STORE_NAME,
            "headers-aware", "Downgrade", "to timestamped store");
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedWindowStoreWithHeadersToPlainWindowStoreAfterCleanup() throws Exception {
        setupAndPopulateWindowStoreWithHeaders(List.of(KeyValue.pair("key1", 100L), KeyValue.pair("key2", 200L)));
        wipeLocalState();

        buildAndStart(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            PLAIN_WINDOWED_PROCESSOR, WINDOW_STORE_NAME);

        final long newTime = CLUSTER.time.milliseconds();
        processPlainWindowedKeyValueAndVerify("key3", "value3", newTime + 300);
        processPlainWindowedKeyValueAndVerify("key4", "value4", newTime + 400);
    }

    @Test
    public void shouldSuccessfullyDowngradeFromTimestampedWindowStoreWithHeadersAfterCleanup() throws Exception {
        setupAndPopulateWindowStoreWithHeaders(List.of(KeyValue.pair("key1", 100L), KeyValue.pair("key2", 200L)));
        wipeLocalState();

        buildAndStart(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_PROCESSOR, WINDOW_STORE_NAME);

        final long newTime = CLUSTER.time.milliseconds();
        processWindowedKeyValueAndVerifyTimestamped("key3", "value3", newTime + 300);
        processWindowedKeyValueAndVerifyTimestamped("key4", "value4", newTime + 400);
    }

    /**
     * Setup and populate a window store with headers, then close the instance (leaving the group).
     * @param records List of (key, timestampOffset) tuples. Values will be generated as "value{N}"
     */
    private void setupAndPopulateWindowStoreWithHeaders(final List<KeyValue<String, Long>> records) throws Exception {
        buildAndStart(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(WINDOW_STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_WINDOWED_WITH_HEADERS_PROCESSOR, WINDOW_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        for (int i = 0; i < records.size(); i++) {
            final KeyValue<String, Long> record = records.get(i);
            final String value = "value" + (i + 1);
            produce(record.key, value, baseTime + record.value, headers("source", "test"));
        }

        // Wait for all records to be processed
        awaitStore(WINDOW_STORE_NAME, QueryableStoreTypes.<String, String>timestampedWindowStoreWithHeaders(),
            store -> records.stream()
                .allMatch(record -> findWindowedValue(store, record.key, baseTime + record.value).isPresent()),
            "Store was not populated with expected data");

        closeAndLeaveGroupBeforeRestart();
    }

    /**
     * Setup and populate a key-value store with headers, then close the instance (leaving the group).
     */
    private void setupAndPopulateKeyValueStoreWithHeaders() throws Exception {
        buildAndStart(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                Serdes.String(),
                Serdes.String()),
            TIMESTAMPED_KEY_VALUE_WITH_HEADERS_PROCESSOR, STORE_NAME);

        final Headers headers = headers("source", "test");

        processKeyValueWithTimestampAndHeadersAndVerify("key1", "value1", 11L, headers, headers);
        processKeyValueWithTimestampAndHeadersAndVerify("key2", "value2", 22L, headers, headers);

        closeAndLeaveGroupBeforeRestart();
    }

    // ==================== Session Store Tests ====================

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMigrateSessionStoreToSessionStoreWithHeadersUsingPapi(final boolean persistentStore) throws Exception {
        // Phase 1: Run with plain SessionStore
        buildAndStart(
            Stores.sessionStoreBuilder(
                persistentStore
                    ? Stores.persistentSessionStore(SESSION_STORE_NAME, RETENTION)
                    : Stores.inMemorySessionStore(SESSION_STORE_NAME, RETENTION),
                Serdes.String(),
                Serdes.String()),
            SESSION_PROCESSOR, SESSION_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        processSessionKeyValueAndVerify("key1", "value1", baseTime + 100);
        processSessionKeyValueAndVerify("key2", "value2", baseTime + 200);
        processSessionKeyValueAndVerify("key3", "value3", baseTime + 300);

        // Phase 2: Restart with SessionStoreWithHeaders (headers-aware supplier)
        restart(
            Stores.sessionStoreWithHeadersBuilder(
                persistentStore
                    ? Stores.persistentSessionStoreWithHeaders(SESSION_STORE_NAME, RETENTION)
                    : Stores.inMemorySessionStore(SESSION_STORE_NAME, RETENTION),
                Serdes.String(),
                Serdes.String()),
            SESSION_WITH_HEADERS_PROCESSOR, SESSION_STORE_NAME);

        // Verify legacy data can be read with empty headers
        verifySessionValueWithEmptyHeaders("key1", "value1", baseTime + 100);
        verifySessionValueWithEmptyHeaders("key2", "value2", baseTime + 200);
        verifySessionValueWithEmptyHeaders("key3", "value3", baseTime + 300);

        // Process new records with headers
        final Headers headers = headers("source", "migration-test");

        processSessionKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, headers);
        processSessionKeyValueWithHeadersAndVerify("key5", "value5", baseTime + 500, headers, headers);
    }

    @Test
    public void shouldProxySessionStoreToSessionStoreWithHeaders() throws Exception {
        // Phase 1: Run with plain SessionStore
        buildAndStart(
            Stores.sessionStoreBuilder(
                Stores.persistentSessionStore(SESSION_STORE_NAME, RETENTION),
                Serdes.String(),
                Serdes.String()),
            SESSION_PROCESSOR, SESSION_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        processSessionKeyValueAndVerify("key1", "value1", baseTime + 100);
        processSessionKeyValueAndVerify("key2", "value2", baseTime + 200);
        processSessionKeyValueAndVerify("key3", "value3", baseTime + 300);

        // Phase 2: Restart with headers-aware builder but non-headers supplier (proxy/adapter mode)
        restart(
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStore(SESSION_STORE_NAME, RETENTION),  // non-headers supplier!
                Serdes.String(),
                Serdes.String()),
            SESSION_WITH_HEADERS_PROCESSOR, SESSION_STORE_NAME);

        // Verify legacy data can be read with empty headers
        verifySessionValueWithEmptyHeaders("key1", "value1", baseTime + 100);
        verifySessionValueWithEmptyHeaders("key2", "value2", baseTime + 200);
        verifySessionValueWithEmptyHeaders("key3", "value3", baseTime + 300);

        // In proxy mode, headers are stripped when writing to non-headers store
        // So we expect empty headers when reading back
        final Headers headers = headers("source", "proxy-test");
        final Headers expectedHeaders = headers();

        processSessionKeyValueWithHeadersAndVerify("key4", "value4", baseTime + 400, headers, expectedHeaders);
        processSessionKeyValueWithHeadersAndVerify("key5", "value5", baseTime + 500, headers, expectedHeaders);
    }

    @Test
    public void shouldFailDowngradeFromSessionStoreWithHeadersToSessionStore() throws Exception {
        setupAndPopulateSessionStoreWithHeaders();

        assertDowngradeThrowsProcessorStateException(
            "to plain session store",
            Stores.sessionStoreBuilder(
                Stores.persistentSessionStore(SESSION_STORE_NAME, RETENTION),
                Serdes.String(),
                Serdes.String()),
            SESSION_PROCESSOR, SESSION_STORE_NAME,
            "incompatible settings");
    }

    @Test
    public void shouldSuccessfullyDowngradeFromSessionStoreWithHeadersToSessionStoreAfterCleanup() throws Exception {
        setupAndPopulateSessionStoreWithHeaders();
        wipeLocalState();

        buildAndStart(
            Stores.sessionStoreBuilder(
                Stores.persistentSessionStore(SESSION_STORE_NAME, RETENTION),
                Serdes.String(),
                Serdes.String()),
            SESSION_PROCESSOR, SESSION_STORE_NAME);

        final long newTime = CLUSTER.time.milliseconds();
        processSessionKeyValueAndVerify("key3", "value3", newTime + 300);
        processSessionKeyValueAndVerify("key4", "value4", newTime + 400);
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
        verifySessionValue(key, value, timestamp, headers());
    }

    private void processSessionKeyValueWithHeadersAndVerify(final String key,
                                                            final String value,
                                                            final long timestamp,
                                                            final Headers headers,
                                                            final Headers expectedHeaders) throws Exception {
        produce(key, value, timestamp, headers);
        verifySessionValue(key, value, timestamp, expectedHeaders);
    }

    /**
     * Verifies the aggregation stored for {@code key} in the session bounded by {@code timestamp},
     * expecting {@code value} and {@code expectedHeaders}. Pass empty {@link #headers()} for
     * sessions migrated without headers.
     */
    private void verifySessionValue(final String key,
                                    final String value,
                                    final long timestamp,
                                    final Headers expectedHeaders) throws Exception {
        awaitStore(SESSION_STORE_NAME, QueryableStoreTypes.<String, String>sessionStoreWithHeaders(),
            store -> {
                final Optional<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> result =
                    findSessionValue(store, key, timestamp);
                if (result.isEmpty()) {
                    return false;
                }

                final AggregationWithHeaders<String> actual = result.get().value;
                assertEquals(value, actual.aggregation(), "Value should match");
                assertEquals(expectedHeaders, actual.headers(), "Headers should match");
                return true;
            },
            "Could not verify session value in time.");
    }

    /**
     * Setup and populate a session store with headers, then close the instance (leaving the group).
     */
    private void setupAndPopulateSessionStoreWithHeaders() throws Exception {
        buildAndStart(
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStoreWithHeaders(SESSION_STORE_NAME, RETENTION),
                Serdes.String(),
                Serdes.String()),
            SESSION_WITH_HEADERS_PROCESSOR, SESSION_STORE_NAME);

        final long baseTime = CLUSTER.time.milliseconds();
        produce("key1", "value1", baseTime + 100, headers("source", "test"));

        awaitStore(SESSION_STORE_NAME, QueryableStoreTypes.<String, String>sessionStoreWithHeaders(),
            store -> findSessionValue(store, "key1", baseTime + 100).isPresent(),
            "Store was not populated with expected data");

        closeAndLeaveGroupBeforeRestart();
    }

    // ==================== Processors ====================

    /**
     * Builds a processor supplier that looks up the store named {@code storeName} on init and hands
     * every record to {@code writer}. All processors in this test differ only in the store type and
     * how a record is written into it, so they share this skeleton instead of one class each.
     */
    private static <S extends StateStore> ProcessorSupplier<String, String, Void, Void> storeWriter(
            final String storeName,
            final BiConsumer<S, Record<String, String>> writer) {
        return () -> new Processor<>() {
            private S store;

            @Override
            public void init(final ProcessorContext<Void, Void> context) {
                store = context.getStateStore(storeName);
            }

            @Override
            public void process(final Record<String, String> record) {
                writer.accept(store, record);
            }
        };
    }

    private static Windowed<String> sessionKey(final Record<String, String> record) {
        return new Windowed<>(record.key(), new SessionWindow(record.timestamp(), record.timestamp()));
    }

    private static final ProcessorSupplier<String, String, Void, Void> KEY_VALUE_PROCESSOR =
        storeWriter(STORE_NAME, (final KeyValueStore<String, String> store, final Record<String, String> record) ->
            store.put(record.key(), record.value()));

    private static final ProcessorSupplier<String, String, Void, Void> TIMESTAMPED_KEY_VALUE_PROCESSOR =
        storeWriter(STORE_NAME, (final TimestampedKeyValueStore<String, String> store, final Record<String, String> record) ->
            store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp())));

    private static final ProcessorSupplier<String, String, Void, Void> TIMESTAMPED_KEY_VALUE_WITH_HEADERS_PROCESSOR =
        storeWriter(STORE_NAME, (final TimestampedKeyValueStoreWithHeaders<String, String> store, final Record<String, String> record) ->
            store.put(record.key(), ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers())));

    /**
     * Processor for plain WindowStore (without timestamps or headers).
     */
    private static final ProcessorSupplier<String, String, Void, Void> PLAIN_WINDOWED_PROCESSOR =
        storeWriter(WINDOW_STORE_NAME, (final WindowStore<String, String> store, final Record<String, String> record) ->
            store.put(record.key(), record.value(), windowStart(record.timestamp())));

    /**
     * Processor for TimestampedWindowStore (without headers).
     */
    private static final ProcessorSupplier<String, String, Void, Void> TIMESTAMPED_WINDOWED_PROCESSOR =
        storeWriter(WINDOW_STORE_NAME, (final TimestampedWindowStore<String, String> store, final Record<String, String> record) ->
            store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp()), windowStart(record.timestamp())));

    /**
     * Processor for TimestampedWindowStoreWithHeaders (with headers).
     */
    private static final ProcessorSupplier<String, String, Void, Void> TIMESTAMPED_WINDOWED_WITH_HEADERS_PROCESSOR =
        storeWriter(WINDOW_STORE_NAME, (final TimestampedWindowStoreWithHeaders<String, String> store, final Record<String, String> record) ->
            store.put(record.key(),
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()),
                windowStart(record.timestamp())));

    private static final ProcessorSupplier<String, String, Void, Void> SESSION_PROCESSOR =
        storeWriter(SESSION_STORE_NAME, (final SessionStore<String, String> store, final Record<String, String> record) ->
            store.put(sessionKey(record), record.value()));

    private static final ProcessorSupplier<String, String, Void, Void> SESSION_WITH_HEADERS_PROCESSOR =
        storeWriter(SESSION_STORE_NAME, (final SessionStoreWithHeaders<String, String> store, final Record<String, String> record) ->
            store.put(sessionKey(record), AggregationWithHeaders.make(record.value(), record.headers())));
}
