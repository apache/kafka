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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.DslStoreFormat;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.state.DslSessionParams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.common.utils.Utils.delete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests upgrade path from time-ordered session store (without headers) to
 * time-ordered session store with headers using direct supplier creation.
 * <p>
 * This test validates lazy migration from DEFAULT CF to headers CF and ensures
 * the store can read old data after upgrade.
 */
public class TimeOrderedSessionStoreUpgradeTest {

    private static final String STORE_NAME = "time-ordered-session-store";
    private static final long RETENTION_MS = Duration.ofMinutes(5).toMillis();

    private InternalMockProcessorContext<Bytes, byte[]> context;
    private File baseDir;

    @BeforeEach
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        baseDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            baseDir,
            Serdes.Bytes(),
            Serdes.ByteArray(),
            new StreamsConfig(props)
        );
    }

    @AfterEach
    public void tearDown() {
        if (baseDir != null) {
            try {
                delete(baseDir);
            } catch (final Exception e) {
                // Ignore
            }
        }
    }

    private static byte[] serializeValueWithHeaders(final byte[] value, final Headers headers) {
        if (value == null) {
            return null;
        }
        final HeadersSerializer.PreSerializedHeaders preSerializedHeaders = HeadersSerializer.prepareSerialization(headers);
        final byte[] rawHeaders = HeadersSerializer
            .serialize(preSerializedHeaders, ByteBuffer.allocate(preSerializedHeaders.requiredBufferSizeForHeaders))
            .array();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            ByteUtils.writeVarint(rawHeaders.length, out);
            out.write(rawHeaders);
            out.write(value);
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize value with headers", e);
        }
    }

    @Test
    public void shouldMigrateFromWithoutHeadersToWithHeaders() {
        final RocksDbTimeOrderedSessionBytesStoreSupplier oldSupplier =
            new RocksDbTimeOrderedSessionBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, true, false);

        final SessionStore<Bytes, byte[]> oldStore = oldSupplier.get();
        oldStore.init(context, oldStore);

        final Bytes key1 = Bytes.wrap("key1".getBytes());
        final Bytes key2 = Bytes.wrap("key2".getBytes());
        final Bytes key3 = Bytes.wrap("key3".getBytes());

        oldStore.put(new Windowed<>(key1, new SessionWindow(100, 200)), "value1".getBytes());
        oldStore.put(new Windowed<>(key2, new SessionWindow(150, 250)), "value2".getBytes());
        oldStore.put(new Windowed<>(key3, new SessionWindow(200, 300)), "value3".getBytes());

        // Verify old data
        assertEquals("value1", new String(oldStore.fetchSession(key1, 100, 200)));
        assertEquals("value2", new String(oldStore.fetchSession(key2, 150, 250)));
        assertEquals("value3", new String(oldStore.fetchSession(key3, 200, 300)));

        oldStore.close();

        // Reopen with headers
        final RocksDbTimeOrderedSessionBytesStoreSupplier newSupplier =
            new RocksDbTimeOrderedSessionBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, true, true);

        final SessionStore<Bytes, byte[]> newStore = newSupplier.get();
        newStore.init(context, newStore);

        // Verify old data readable with empty headers via lazy migration
        byte[] fetch = newStore.fetchSession(key1, 100, 200);
        assertNotNull(fetch);
        assertEquals("value1", new String(AggregationWithHeadersDeserializer.rawAggregation(fetch)));
        assertEquals(0, AggregationWithHeadersDeserializer.headers(fetch).toArray().length, "Old data should have empty headers after migration");

        fetch = newStore.fetchSession(key2, 150, 250);
        assertNotNull(fetch);
        assertEquals("value2", new String(AggregationWithHeadersDeserializer.rawAggregation(fetch)));
        assertEquals(0, AggregationWithHeadersDeserializer.headers(fetch).toArray().length, "Old data should have empty headers after migration");

        fetch = newStore.fetchSession(key3, 200, 300);
        assertNotNull(fetch);
        assertEquals("value3", new String(AggregationWithHeadersDeserializer.rawAggregation(fetch)));
        assertEquals(0, AggregationWithHeadersDeserializer.headers(fetch).toArray().length, "Old data should have empty headers after migration");

        newStore.close();
    }

    @Test
    public void shouldMigrateFromWithIndexToWithIndexAndHeaders() {
        final RocksDbTimeOrderedSessionBytesStoreSupplier oldSupplier =
            new RocksDbTimeOrderedSessionBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, true, false);

        final SessionStore<Bytes, byte[]> oldStore = oldSupplier.get();
        oldStore.init(context, oldStore);

        final Bytes key1 = Bytes.wrap("key1".getBytes());
        oldStore.put(new Windowed<>(key1, new SessionWindow(100, 200)), "value1".getBytes());
        oldStore.close();

        // Upgrade to headers
        final RocksDbTimeOrderedSessionBytesStoreSupplier newSupplier =
            new RocksDbTimeOrderedSessionBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, true, true);

        final SessionStore<Bytes, byte[]> newStore = newSupplier.get();
        newStore.init(context, newStore);

        // Verify old data still accessible
        final byte[] fetch = newStore.fetchSession(key1, 100, 200);
        assertNotNull(fetch);
        assertEquals("value1", new String(AggregationWithHeadersDeserializer.rawAggregation(fetch)));

        newStore.close();
    }

    @Test
    public void shouldMigrateFromWithoutIndexToWithIndexAndHeaders() {
        final RocksDbTimeOrderedSessionBytesStoreSupplier oldSupplier =
            new RocksDbTimeOrderedSessionBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, false, false);

        final SessionStore<Bytes, byte[]> oldStore = oldSupplier.get();
        oldStore.init(context, oldStore);

        final Bytes key1 = Bytes.wrap("key1".getBytes());
        oldStore.put(new Windowed<>(key1, new SessionWindow(100, 200)), "value1".getBytes());
        oldStore.close();

        // Upgrade to both index and headers
        final RocksDbTimeOrderedSessionBytesStoreSupplier newSupplier =
            new RocksDbTimeOrderedSessionBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, true, true);

        final SessionStore<Bytes, byte[]> newStore = newSupplier.get();
        newStore.init(context, newStore);

        // Verify old data still accessible
        final byte[] fetch = newStore.fetchSession(key1, 100, 200);
        assertNotNull(fetch);
        assertEquals("value1", new String(AggregationWithHeadersDeserializer.rawAggregation(fetch)));

        newStore.close();
    }

    @Test
    public void shouldWriteAndReadWithHeaders() {
        // Start fresh with headers
        final RocksDbTimeOrderedSessionBytesStoreSupplier supplier =
            new RocksDbTimeOrderedSessionBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, true, true);

        final SessionStore<Bytes, byte[]> store = supplier.get();
        store.init(context, store);

        final Bytes key1 = Bytes.wrap("key1".getBytes());
        final Bytes key2 = Bytes.wrap("key2".getBytes());

        // Write with empty headers
        store.put(new Windowed<>(key1, new SessionWindow(100, 200)),
            serializeValueWithHeaders("value1".getBytes(), new RecordHeaders()));

        // Write with actual headers
        final RecordHeaders headersWithData = new RecordHeaders();
        headersWithData.add("header-key-1", "header-value-1".getBytes());
        headersWithData.add("header-key-2", "header-value-2".getBytes());
        store.put(new Windowed<>(key2, new SessionWindow(150, 250)),
            serializeValueWithHeaders("value2".getBytes(), headersWithData));

        // Verify values
        assertEquals("value1", new String(AggregationWithHeadersDeserializer.rawAggregation(store.fetchSession(key1, 100, 200))));
        assertEquals("value2", new String(AggregationWithHeadersDeserializer.rawAggregation(store.fetchSession(key2, 150, 250))));

        // Verify headers for key1 (empty)
        final Headers key1Headers = AggregationWithHeadersDeserializer.headers(store.fetchSession(key1, 100, 200));
        assertEquals(0, key1Headers.toArray().length);

        // Verify headers for key2 (with data)
        final Headers key2Headers = AggregationWithHeadersDeserializer.headers(store.fetchSession(key2, 150, 250));
        assertEquals(2, key2Headers.toArray().length);
        assertEquals("header-value-1", new String(key2Headers.lastHeader("header-key-1").value()));
        assertEquals("header-value-2", new String(key2Headers.lastHeader("header-key-2").value()));

        // Verify findSessions works
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iter = store.findSessions(100, 250)) {
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            assertEquals(2, count);
        }

        store.close();
    }

    // --- DSL-level supplier resolution tests ---

    /**
     * Uses {@link BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers#sessionStore(DslSessionParams)}
     * to create stores — the same code path the DSL materializer uses when
     * {@code ON_WINDOW_CLOSE} is set. Validates the upgrade from PLAIN to HEADERS format.
     */
    @Test
    public void shouldMigrateViaDslSupplierPath() {
        final BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers dslSuppliers =
            new BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers();

        // Phase 1: create store via DSL supplier with PLAIN format
        final SessionBytesStoreSupplier oldSupplier = dslSuppliers.sessionStore(
            new DslSessionParams(STORE_NAME, Duration.ofMillis(RETENTION_MS),
                EmitStrategy.onWindowClose(), DslStoreFormat.PLAIN));

        final SessionStore<Bytes, byte[]> oldStore = oldSupplier.get();
        oldStore.init(context, oldStore);

        final Bytes key1 = Bytes.wrap("key1".getBytes());
        oldStore.put(new Windowed<>(key1, new SessionWindow(100, 200)), "value1".getBytes());

        assertEquals("value1", new String(oldStore.fetchSession(key1, 100, 200)));
        oldStore.close();

        // Phase 2: create store via DSL supplier with HEADERS format
        final SessionBytesStoreSupplier newSupplier = dslSuppliers.sessionStore(
            new DslSessionParams(STORE_NAME, Duration.ofMillis(RETENTION_MS),
                EmitStrategy.onWindowClose(), DslStoreFormat.HEADERS));

        final SessionStore<Bytes, byte[]> newStore = newSupplier.get();
        newStore.init(context, newStore);

        // Verify old data readable with empty headers via lazy migration
        final byte[] fetch = newStore.fetchSession(key1, 100, 200);
        assertNotNull(fetch, "Old data should be readable after upgrade via DSL supplier path");
        assertEquals("value1", new String(AggregationWithHeadersDeserializer.rawAggregation(fetch)));
        assertEquals(0, AggregationWithHeadersDeserializer.headers(fetch).toArray().length,
            "Old data should have empty headers after migration");

        newStore.close();
    }

    // --- DSL end-to-end tests via TopologyTestDriver ---

    private static final String INPUT_TOPIC = "input-topic";
    private static final String MATERIALIZED_STORE = "session-count-store";

    static Stream<Arguments> dslStoreFormats() {
        return Stream.of(
            Arguments.of(false),
            Arguments.of(true)
        );
    }

    /**
     * Builds a real DSL session aggregation topology with {@code ON_WINDOW_CLOSE} and verifies
     * the full chain from DSL → materializer → supplier → store works correctly with both
     * PLAIN and HEADERS formats.
     */
    @ParameterizedTest
    @MethodSource("dslStoreFormats")
    public void shouldAggregateSessionsViaDslWithOnWindowClose(final boolean withHeaders) {
        final Properties dslProps = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        if (withHeaders) {
            dslProps.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        }

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(500)))
            .emitStrategy(EmitStrategy.onWindowClose())
            .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(MATERIALIZED_STORE)
                .withRetention(Duration.ofMinutes(5)));

        try (final TopologyTestDriver driver = new TopologyTestDriver(streamsBuilder.build(), dslProps)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            // Send records within the same session window (gap = 500ms)
            inputTopic.pipeInput("A", "v1", 100);
            inputTopic.pipeInput("A", "v2", 200);
            inputTopic.pipeInput("B", "v3", 150);

            // Advance time past the session gap to close windows
            inputTopic.pipeInput("A", "v4", 2000);

            // Query the materialized store
            final SessionStore<String, Long> store = driver.getSessionStore(MATERIALIZED_STORE);
            assertNotNull(store, "Session store should be materialized");

            final List<KeyValue<Windowed<String>, Long>> results = toList(store.fetch("A", "B"));
            // Expect: A's session [100,200] with count=2, B's session [150,150] with count=1
            // A's new session [2000,2000] may or may not be closed yet
            boolean foundASession = false;
            boolean foundBSession = false;
            for (final KeyValue<Windowed<String>, Long> kv : results) {
                if (kv.key.key().equals("A") && kv.key.window().start() == 100 && kv.key.window().end() == 200) {
                    assertEquals(2L, kv.value);
                    foundASession = true;
                }
                if (kv.key.key().equals("B") && kv.key.window().start() == 150 && kv.key.window().end() == 150) {
                    assertEquals(1L, kv.value);
                    foundBSession = true;
                }
            }
            assertTrue(foundASession, "Should find A's session [100,200]");
            assertTrue(foundBSession, "Should find B's session [150,150]");
        }
    }

    private static <K, V> List<KeyValue<K, V>> toList(final KeyValueIterator<K, V> iterator) {
        final List<KeyValue<K, V>> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        iterator.close();
        return result;
    }
}
