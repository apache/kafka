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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.toStoreKeyBinary;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class InMemoryWindowStoreTest extends AbstractWindowBytesStoreTest {

    private static final String STORE_NAME = "InMemoryWindowStore";

    @Override
    <K, V> WindowStore<K, V> buildWindowStore(final long retentionPeriod,
                                              final long windowSize,
                                              final boolean retainDuplicates,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {
        return Stores.windowStoreBuilder(
            Stores.inMemoryWindowStore(
                STORE_NAME,
                ofMillis(retentionPeriod),
                ofMillis(windowSize),
                retainDuplicates),
            keySerde,
            valueSerde)
            .build();
    }

    @Test
    public void shouldCountNumEntries() {
        final InMemoryWindowStore store = new InMemoryWindowStore("test", RETENTION_PERIOD, WINDOW_SIZE, false, "scope");
        store.init(context, store);

        assertEquals(0L, store.numEntries());

        store.put(
                Bytes.wrap("a".getBytes()),
                "1".getBytes(),
                0L
        );
        assertEquals(1L, store.numEntries());

        store.put(
                Bytes.wrap("b".getBytes()),
                "2".getBytes(),
                0L
        );
        assertEquals(2L, store.numEntries());

        store.put(
                Bytes.wrap("a".getBytes()),
                "3".getBytes(),
                10L
        );
        assertEquals(3L, store.numEntries());

        // overwrite existing entry (same key, same timestamp)
        store.put(
                Bytes.wrap("a".getBytes()),
                "4".getBytes(),
                0L
        );
        assertEquals(3L, store.numEntries());

        // delete entry by putting null
        store.put(
                Bytes.wrap("b".getBytes()),
                null,
                0L
        );
        assertEquals(2L, store.numEntries());

        store.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldRestore() {
        // should be empty initially
        assertFalse(windowStore.all().hasNext());

        final StateSerdes<Integer, String> serdes = new StateSerdes<>("", Serdes.Integer(),
            Serdes.String());

        final List<KeyValue<byte[], byte[]>> restorableEntries = new LinkedList<>();

        restorableEntries
            .add(new KeyValue<>(toStoreKeyBinary(1, 0L, 0, new RecordHeaders(), serdes).get(), serdes.rawValue("one")));
        restorableEntries.add(new KeyValue<>(toStoreKeyBinary(2, WINDOW_SIZE, 0, new RecordHeaders(), serdes).get(),
            serdes.rawValue("two")));
        restorableEntries.add(new KeyValue<>(toStoreKeyBinary(3, 2 * WINDOW_SIZE, 0, new RecordHeaders(), serdes).get(),
            serdes.rawValue("three")));

        context.restore(STORE_NAME, restorableEntries);
        try (final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore
            .fetchAll(0L, 2 * WINDOW_SIZE)) {

            assertEquals(windowedPair(1, "one", 0L), iterator.next());
            assertEquals(windowedPair(2, "two", WINDOW_SIZE), iterator.next());
            assertEquals(windowedPair(3, "three", 2 * WINDOW_SIZE), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldNotExpireFromOpenIterator() {
        windowStore.put(1, "one", 0L);
        windowStore.put(1, "two", 10L);

        windowStore.put(2, "one", 5L);
        windowStore.put(2, "two", 15L);

        final WindowStoreIterator<String> iterator1 = windowStore.fetch(1, 0L, 50L);
        final WindowStoreIterator<String> iterator2 = windowStore.fetch(2, 0L, 50L);

        // This put expires all four previous records, but they should still be returned from already open iterators
        windowStore.put(1, "four", 2 * RETENTION_PERIOD);

        assertEquals(new KeyValue<>(0L, "one"), iterator1.next());
        assertEquals(new KeyValue<>(5L, "one"), iterator2.next());

        assertEquals(new KeyValue<>(15L, "two"), iterator2.next());
        assertEquals(new KeyValue<>(10L, "two"), iterator1.next());

        assertFalse(iterator1.hasNext());
        assertFalse(iterator2.hasNext());

        iterator1.close();
        iterator2.close();

        // Make sure expired records are removed now that open iterators are closed
        assertFalse(windowStore.fetch(1, 0L, 50L).hasNext());
    }

    @Test
    public void testExpiration() {
        long currentTime = 0;
        windowStore.put(1, "one", currentTime);

        currentTime += RETENTION_PERIOD / 4;
        windowStore.put(1, "two", currentTime);

        currentTime += RETENTION_PERIOD / 4;
        windowStore.put(1, "three", currentTime);

        currentTime += RETENTION_PERIOD / 4;
        windowStore.put(1, "four", currentTime);

        // increase current time to the full RETENTION_PERIOD to expire first record
        currentTime = currentTime + RETENTION_PERIOD / 4;
        windowStore.put(1, "five", currentTime);

        KeyValueIterator<Windowed<Integer>, String> iterator = windowStore
            .fetchAll(0L, currentTime);

        // effect of this put (expires next oldest record, adds new one) should not be reflected in the already fetched results
        currentTime = currentTime + RETENTION_PERIOD / 4;
        windowStore.put(1, "six", currentTime);

        // should only have middle 4 values, as (only) the first record was expired at the time of the fetch
        // and the last was inserted after the fetch
        assertEquals(windowedPair(1, "two", RETENTION_PERIOD / 4), iterator.next());
        assertEquals(windowedPair(1, "three", RETENTION_PERIOD / 2), iterator.next());
        assertEquals(windowedPair(1, "four", 3 * (RETENTION_PERIOD / 4)), iterator.next());
        assertEquals(windowedPair(1, "five", RETENTION_PERIOD), iterator.next());
        assertFalse(iterator.hasNext());

        iterator = windowStore.fetchAll(0L, currentTime);

        // If we fetch again after the last put, the second oldest record should have expired and newest should appear in results
        assertEquals(windowedPair(1, "three", RETENTION_PERIOD / 2), iterator.next());
        assertEquals(windowedPair(1, "four", 3 * (RETENTION_PERIOD / 4)), iterator.next());
        assertEquals(windowedPair(1, "five", RETENTION_PERIOD), iterator.next());
        assertEquals(windowedPair(1, "six", 5 * (RETENTION_PERIOD / 4)), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldMatchPositionAfterPut() {
        final MeteredWindowStore<Integer, String> meteredSessionStore = (MeteredWindowStore<Integer, String>) windowStore;
        final ChangeLoggingWindowBytesStore changeLoggingSessionBytesStore = (ChangeLoggingWindowBytesStore) meteredSessionStore.wrapped();
        final InMemoryWindowStore inMemoryWindowStore = (InMemoryWindowStore) changeLoggingSessionBytesStore.wrapped();

        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        windowStore.put(0, "0", SEGMENT_INTERVAL);
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        windowStore.put(1, "1", SEGMENT_INTERVAL);
        context.setRecordContext(new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders()));
        windowStore.put(2, "2", SEGMENT_INTERVAL);
        context.setRecordContext(new ProcessorRecordContext(0, 4, 0, "", new RecordHeaders()));
        windowStore.put(3, "3", SEGMENT_INTERVAL);

        final Position expected = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 4L)))));
        final Position actual = inMemoryWindowStore.getPosition();
        assertEquals(expected, actual);
    }

    @Test
    public void readOnlyCommittedShouldHideStagedFetchValue() {
        final InMemoryWindowStore txnStore = openTransactionalWindowStore();
        try {
            final Bytes key = Bytes.wrap("k".getBytes());
            txnStore.put(key, "v1".getBytes(), 0L);
            txnStore.commit(java.util.Map.of());

            txnStore.put(key, "v2".getBytes(), WINDOW_SIZE);

            final ReadOnlyWindowStore<Bytes, byte[]> uncommitted = txnStore.readOnly(IsolationLevel.READ_UNCOMMITTED);
            final ReadOnlyWindowStore<Bytes, byte[]> committed = txnStore.readOnly(IsolationLevel.READ_COMMITTED);

            assertArrayEquals("v1".getBytes(), uncommitted.fetch(key, 0L));
            assertArrayEquals("v2".getBytes(), uncommitted.fetch(key, WINDOW_SIZE));
            assertArrayEquals("v1".getBytes(), committed.fetch(key, 0L));
            assertNull(committed.fetch(key, WINDOW_SIZE));
        } finally {
            txnStore.close();
        }
    }

    @Test
    public void readOnlyFetchRangeShouldRespectIsolationLevel() {
        final InMemoryWindowStore txnStore = openTransactionalWindowStore();
        try {
            final Bytes key = Bytes.wrap("k".getBytes());
            txnStore.put(key, "v1".getBytes(), 0L);
            txnStore.commit(java.util.Map.of());
            txnStore.put(key, "v2".getBytes(), WINDOW_SIZE);

            final ReadOnlyWindowStore<Bytes, byte[]> uncommitted = txnStore.readOnly(IsolationLevel.READ_UNCOMMITTED);
            final ReadOnlyWindowStore<Bytes, byte[]> committed = txnStore.readOnly(IsolationLevel.READ_COMMITTED);

            try (WindowStoreIterator<byte[]> it = uncommitted.fetch(key, Instant.ofEpochMilli(0), Instant.ofEpochMilli(WINDOW_SIZE))) {
                final List<String> values = new LinkedList<>();
                while (it.hasNext()) {
                    values.add(new String(it.next().value));
                }
                assertEquals(List.of("v1", "v2"), values);
            }
            try (WindowStoreIterator<byte[]> it = committed.fetch(key, Instant.ofEpochMilli(0), Instant.ofEpochMilli(WINDOW_SIZE))) {
                final List<String> values = new LinkedList<>();
                while (it.hasNext()) {
                    values.add(new String(it.next().value));
                }
                assertEquals(List.of("v1"), values);
            }
        } finally {
            txnStore.close();
        }
    }

    @Test
    public void readOnlyAllShouldRespectIsolationLevel() {
        final InMemoryWindowStore txnStore = openTransactionalWindowStore();
        try {
            txnStore.put(Bytes.wrap("k1".getBytes()), "v1".getBytes(), 0L);
            txnStore.commit(java.util.Map.of());
            txnStore.put(Bytes.wrap("k2".getBytes()), "v2".getBytes(), WINDOW_SIZE);

            try (KeyValueIterator<Windowed<Bytes>, byte[]> it =
                     txnStore.readOnly(IsolationLevel.READ_UNCOMMITTED).all()) {
                int count = 0;
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                assertEquals(2, count);
            }
            try (KeyValueIterator<Windowed<Bytes>, byte[]> it =
                     txnStore.readOnly(IsolationLevel.READ_COMMITTED).all()) {
                int count = 0;
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                assertEquals(1, count);
            }
        } finally {
            txnStore.close();
        }
    }

    @Test
    public void readOnlyCommittedFetchAllShouldNotRaceConcurrentCommit() throws InterruptedException {
        // A READ_COMMITTED scan from a non-owner (IQ) thread must observe a point-in-time snapshot of
        // the committed segments, taken under the buffer's snapshot read-lock. Without it the view
        // iterated the live segment map while commit()'s write-lock-guarded flushToBase() mutated it,
        // exposing a half-applied commit. Each commit flips every key to a single generation, so a
        // correct snapshot scan must see one uniform generation across the full key set.
        final InMemoryWindowStore txnStore = openTransactionalWindowStore();
        final int numKeys = 300;
        final int rounds = 300;
        final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);

        try {
            for (int i = 0; i < numKeys; i++) {
                txnStore.put(Bytes.wrap(("k" + i).getBytes()), "gen0".getBytes(), 0L);
            }
            txnStore.commit(java.util.Map.of());

            final Thread reader = new Thread(() -> {
                final ReadOnlyWindowStore<Bytes, byte[]> committed = txnStore.readOnly(IsolationLevel.READ_COMMITTED);
                try {
                    while (!stop.get()) {
                        try (KeyValueIterator<Windowed<Bytes>, byte[]> it =
                                 committed.fetchAll(Instant.ofEpochMilli(0), Instant.ofEpochMilli(RETENTION_PERIOD))) {
                            String generation = null;
                            int count = 0;
                            while (it.hasNext()) {
                                final String value = new String(it.next().value);
                                if (generation == null) {
                                    generation = value;
                                } else {
                                    assertEquals(generation, value, "scan observed a partial commit");
                                }
                                count++;
                            }
                            assertEquals(numKeys, count, "scan observed a partial commit");
                        }
                    }
                } catch (final Throwable t) {
                    readerFailure.set(t);
                }
            }, "iq-reader");
            reader.start();

            for (int round = 1; round <= rounds && readerFailure.get() == null; round++) {
                final String generation = "gen" + round;
                for (int i = 0; i < numKeys; i++) {
                    txnStore.put(Bytes.wrap(("k" + i).getBytes()), generation.getBytes(), 0L);
                }
                txnStore.commit(java.util.Map.of());
            }
            stop.set(true);
            reader.join(TimeUnit.SECONDS.toMillis(30));

            if (readerFailure.get() != null) {
                throw new AssertionError("READ_COMMITTED fetchAll raced a concurrent commit", readerFailure.get());
            }
        } finally {
            stop.set(true);
            txnStore.close();
        }
    }

    @Test
    public void shouldReportUncommittedPositionForTransactionalStore() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.setProperty(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, "true");
        final InternalMockProcessorContext<Bytes, byte[]> ctx = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            new Serdes.BytesSerde(),
            new Serdes.ByteArraySerde(),
            new StreamsConfig(props)
        );
        final InMemoryWindowStore store = new InMemoryWindowStore(
            "txn-pos-window-store", RETENTION_PERIOD, WINDOW_SIZE, false, "scope");
        store.init(ctx, store);
        try {
            ctx.setRecordContext(new ProcessorRecordContext(0, 1, 0, "topic", new RecordHeaders()));
            store.put(Bytes.wrap("k".getBytes()), "v".getBytes(), 0L);

            final Position expected = Position.fromMap(mkMap(mkEntry("topic", mkMap(mkEntry(0, 1L)))));

            // READ_UNCOMMITTED query should expose the staged position before commit
            final QueryResult<?> uncommitted = store.query(
                WindowKeyQuery.withKeyAndWindowStartRange(
                    Bytes.wrap("k".getBytes()), Instant.ofEpochMilli(0), Instant.ofEpochMilli(WINDOW_SIZE)),
                PositionBound.unbounded(),
                new QueryConfig(false));
            assertEquals(expected, uncommitted.getPosition(), "READ_UNCOMMITTED query position");

            // getPosition() reports the uncommitted (committed + staged) position, mirroring
            // RocksDBStore, so the changelog consistency vector reflects the staged write.
            assertEquals(expected, store.getPosition(), "getPosition before commit (uncommitted)");

            // after commit, committed position populated
            store.commit(java.util.Map.of());
            assertEquals(expected, store.getPosition(), "getPosition after commit");
        } finally {
            store.close();
        }
    }

    private InMemoryWindowStore openTransactionalWindowStore() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.setProperty(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, "true");
        final InternalMockProcessorContext<Bytes, byte[]> ctx = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            new Serdes.BytesSerde(),
            new Serdes.ByteArraySerde(),
            new StreamsConfig(props)
        );
        final InMemoryWindowStore store = new InMemoryWindowStore(
            "txn-in-memory-window-store", RETENTION_PERIOD, WINDOW_SIZE, false, "scope");
        store.init(ctx, store);
        return store;
    }

    @Nested
    class InMemoryStoreIQv2Tests {
        private static final long WINDOW_SIZE = 10_000L;
        private static final long RETENTION_PERIOD = 60_000L;
        private static final long SEGMENT_INTERVAL = 30_000L;

        private InMemoryWindowStore inMemoryStore;
        private InternalMockProcessorContext<String, String> context;
        private File baseDir;

        @BeforeEach
        public void setUp() {
            final Properties props = StreamsTestUtils.getStreamsConfig();
            baseDir = TestUtils.tempDirectory();
            context = new InternalMockProcessorContext<>(
                baseDir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
            );

            inMemoryStore = new InMemoryWindowStore(
                "iqv2-inmemory-test-store",
                RETENTION_PERIOD,
                WINDOW_SIZE,
                false,  // retainDuplicates
                "test-metrics-scope"
            );

            inMemoryStore.init(context, inMemoryStore);
        }

        @AfterEach
        public void tearDown() {
            if (inMemoryStore != null) {
                inMemoryStore.close();
            }
        }

        @Test
        public void shouldHandleWindowKeyQuerySuccessfullyOnInMemoryStore() {
            // Build a typed window store using timestamped window store with headers
            final TimestampedWindowStoreWithHeaders<String, String> typedStore = Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.inMemoryWindowStore(
                    "typed-window-store",
                    ofMillis(RETENTION_PERIOD),
                    ofMillis(WINDOW_SIZE),
                    false),
                Serdes.String(),
                Serdes.String())
                .withLoggingDisabled()
                .build();

            typedStore.init(context, typedStore);

            try {
                // Put some data into the store
                context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
                typedStore.put("test-key", ValueTimestampHeaders.make("value1", 1000L, new RecordHeaders()), 1000L);
                context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
                typedStore.put("test-key", ValueTimestampHeaders.make("value2", 5000L, new RecordHeaders()), 5000L);

                // Query at typed level - WindowKeyQuery should return windowed values with timestamps
                final WindowKeyQuery<String, ValueAndTimestamp<String>> query = WindowKeyQuery.withKeyAndWindowStartRange(
                    "test-key",
                    Instant.ofEpochMilli(0),
                    Instant.ofEpochMilli(10000L)
                );
                final QueryResult<WindowStoreIterator<ValueAndTimestamp<String>>> result =
                    typedStore.query(query, PositionBound.unbounded(), new QueryConfig(false));

                // Verify IQv2 query result
                assertTrue(result.isSuccess(), "Expected query to succeed on in-memory store");
                assertNotNull(result.getResult(), "Expected result iterator to be present");
                assertNotNull(result.getPosition(), "Expected position to be set");

                // Verify the actual query results
                final WindowStoreIterator<ValueAndTimestamp<String>> iterator = result.getResult();
                assertTrue(iterator.hasNext(), "Expected at least one result");

                KeyValue<Long, ValueAndTimestamp<String>> kv = iterator.next();
                assertEquals(1000L, kv.key, "Expected first window timestamp");
                assertEquals("value1", kv.value.value(), "WindowKeyQuery should return the value");
                assertEquals(1000L, kv.value.timestamp(), "WindowKeyQuery should return the timestamp");

                assertTrue(iterator.hasNext(), "Expected second result");
                kv = iterator.next();
                assertEquals(5000L, kv.key, "Expected second window timestamp");
                assertEquals("value2", kv.value.value(), "WindowKeyQuery should return the value");
                assertEquals(5000L, kv.value.timestamp(), "WindowKeyQuery should return the timestamp");

                assertFalse(iterator.hasNext(), "Expected no more results");
                iterator.close();
            } finally {
                typedStore.close();
            }
        }

        @Test
        public void shouldHandleWindowRangeQuerySuccessfullyOnInMemoryStore() {
            // Build a typed window store using timestamped window store with headers
            final TimestampedWindowStoreWithHeaders<String, String> typedStore = Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.inMemoryWindowStore(
                    "typed-window-range-store",
                    ofMillis(RETENTION_PERIOD),
                    ofMillis(WINDOW_SIZE),
                    false),
                Serdes.String(),
                Serdes.String())
                .withLoggingDisabled()
                .build();

            typedStore.init(context, typedStore);

            try {
                // Put some data into the store
                context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
                typedStore.put("key1", ValueTimestampHeaders.make("value1", 1000L, new RecordHeaders()), 1000L);
                context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
                typedStore.put("key2", ValueTimestampHeaders.make("value2", 5000L, new RecordHeaders()), 5000L);
                context.setRecordContext(new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders()));
                typedStore.put("key3", ValueTimestampHeaders.make("value3", 3000L, new RecordHeaders()), 3000L);

                // Query at typed level - WindowRangeQuery should return all windowed key-values with timestamps
                final WindowRangeQuery<String, ValueAndTimestamp<String>> query = WindowRangeQuery.withWindowStartRange(
                    Instant.ofEpochMilli(0),
                    Instant.ofEpochMilli(10000L)
                );
                final QueryResult<KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>>> result =
                    typedStore.query(query, PositionBound.unbounded(), new QueryConfig(false));

                // Verify IQv2 query result
                assertTrue(result.isSuccess(), "Expected query to succeed on in-memory store");
                assertNotNull(result.getResult(), "Expected result iterator to be present");
                assertNotNull(result.getPosition(), "Expected position to be set");

                // Verify the actual query results (should be sorted by window timestamp)
                final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> iterator = result.getResult();

                assertTrue(iterator.hasNext(), "Expected at least one result");
                KeyValue<Windowed<String>, ValueAndTimestamp<String>> kv = iterator.next();
                assertEquals("key1", kv.key.key(), "Expected first key");
                assertEquals(1000L, kv.key.window().start(), "Expected first window start");
                assertEquals("value1", kv.value.value(), "WindowRangeQuery should return the value");
                assertEquals(1000L, kv.value.timestamp(), "WindowRangeQuery should return the timestamp");

                assertTrue(iterator.hasNext(), "Expected second result");
                kv = iterator.next();
                assertEquals("key3", kv.key.key(), "Expected second key");
                assertEquals(3000L, kv.key.window().start(), "Expected second window start");
                assertEquals("value3", kv.value.value(), "WindowRangeQuery should return the value");

                assertTrue(iterator.hasNext(), "Expected third result");
                kv = iterator.next();
                assertEquals("key2", kv.key.key(), "Expected third key");
                assertEquals(5000L, kv.key.window().start(), "Expected third window start");
                assertEquals("value2", kv.value.value(), "WindowRangeQuery should return the value");

                assertFalse(iterator.hasNext(), "Expected no more results");
                iterator.close();
            } finally {
                typedStore.close();
            }
        }

        @Test
        public void shouldCollectExecutionInfoForInMemoryStoreWhenRequested() {
            final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
            );
            final PositionBound positionBound = PositionBound.unbounded();
            final QueryConfig config = new QueryConfig(true); // Enable execution info

            final QueryResult<WindowStoreIterator<byte[]>> result = inMemoryStore.query(query, positionBound, config);

            // Verify: Execution info was collected
            assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
            boolean foundInMemoryStoreInfo = false;
            for (final String info : result.getExecutionInfo()) {
                if (info.contains("Handled in") && info.contains(InMemoryWindowStore.class.getName())) {
                    foundInMemoryStoreInfo = true;
                    break;
                }
            }
            assertTrue(foundInMemoryStoreInfo, "Expected execution info to mention the in-memory store class");
        }

        @Test
        public void shouldNotCollectExecutionInfoForInMemoryStoreWhenNotRequested() {
            final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
            );
            final PositionBound positionBound = PositionBound.unbounded();
            final QueryConfig config = new QueryConfig(false); // Disable execution info

            final QueryResult<WindowStoreIterator<byte[]>> result = inMemoryStore.query(query, positionBound, config);

            // Verify: No execution info was collected
            assertTrue(result.getExecutionInfo().isEmpty(), "Expected no execution info to be collected");
        }
    }
}
