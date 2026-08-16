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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InMemoryKeyValueStoreTest extends AbstractKeyValueStoreTest {

    private KeyValueStore<Bytes, byte[]> byteStore;
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final KeyValueStoreTestDriver<Bytes, byte[]> byteStoreDriver = KeyValueStoreTestDriver.create(Bytes.class, byte[].class);
    private InMemoryKeyValueStore inMemoryKeyValueStore;

    @BeforeEach
    public void createStringKeyValueStore() {
        super.before();
        final StateStoreContext byteStoreContext = byteStoreDriver.context();
        final StoreBuilder<KeyValueStore<Bytes, byte[]>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("in-memory-byte-store"),
            new Serdes.BytesSerde(),
            new Serdes.ByteArraySerde());
        byteStore = storeBuilder.build();
        byteStore.init(byteStoreContext, byteStore);
        this.inMemoryKeyValueStore = getInMemoryStore();
    }

    @AfterEach
    public void after() {
        super.after();
        byteStore.close();
        byteStoreDriver.clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final StateStoreContext context) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("my-store"),
            (Serde<K>) context.keySerde(),
            (Serde<V>) context.valueSerde());

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);
        return store;
    }

    InMemoryKeyValueStore getInMemoryStore() {
        return new InMemoryKeyValueStore("in-memory-store-test");
    }

    @Test
    public void shouldRemoveKeysWithNullValues() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");
        driver.addEntryToRestoreLog(0, null);

        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());

        assertEquals(3, driver.sizeOf(store));

        assertThat(store.get(0), nullValue());
    }


    @Test
    public void shouldReturnKeysWithGivenPrefix() {

        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "k1")),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "prefix_3")),
            stringSerializer.serialize(null, "b")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "k2")),
            stringSerializer.serialize(null, "c")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "prefix_2")),
            stringSerializer.serialize(null, "d")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "k3")),
            stringSerializer.serialize(null, "e")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "prefix_1")),
            stringSerializer.serialize(null, "f")));

        byteStore.putAll(entries);
        byteStore.commit(Map.of());

        final List<String> valuesWithPrefix = new ArrayList<>();
        int numberOfKeysReturned = 0;

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = byteStore.prefixScan("prefix", stringSerializer)) {
            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                valuesWithPrefix.add(new String(next.value));
                numberOfKeysReturned++;
            }
        }

        assertThat(numberOfKeysReturned, is(3));
        assertThat(valuesWithPrefix.get(0), is("f"));
        assertThat(valuesWithPrefix.get(1), is("d"));
        assertThat(valuesWithPrefix.get(2), is("b"));
    }

    @Test
    public void shouldReturnKeysWithGivenPrefixExcludingNextKeyLargestKey() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "abc")),
            stringSerializer.serialize(null, "f")));

        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "abcd")),
            stringSerializer.serialize(null, "f")));

        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "abce")),
            stringSerializer.serialize(null, "f")));

        byteStore.putAll(entries);
        byteStore.commit(Map.of());

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefixAsabcd = byteStore.prefixScan("abcd", stringSerializer)) {
            int numberOfKeysReturned = 0;

            while (keysWithPrefixAsabcd.hasNext()) {
                keysWithPrefixAsabcd.next().key.get();
                numberOfKeysReturned++;
            }

            assertThat(numberOfKeysReturned, is(1));
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldReturnUUIDsWithStringPrefix() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        final UUIDSerializer uuidSerializer = new UUIDSerializer();
        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        final String prefix = uuid1.toString().substring(0, 4);
        entries.add(new KeyValue<>(
            new Bytes(uuidSerializer.serialize(null, uuid1)),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(uuidSerializer.serialize(null, uuid2)),
            stringSerializer.serialize(null, "b")));

        byteStore.putAll(entries);
        byteStore.commit(Map.of());

        final List<String> valuesWithPrefix = new ArrayList<>();
        int numberOfKeysReturned = 0;

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = byteStore.prefixScan(prefix, stringSerializer)) {
            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                valuesWithPrefix.add(new String(next.value));
                numberOfKeysReturned++;
            }
        }

        assertThat(numberOfKeysReturned, is(1));
        assertThat(valuesWithPrefix.get(0), is("a"));
    }

    @Test
    public void shouldReturnNoKeys() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "a")),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "b")),
            stringSerializer.serialize(null, "c")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "c")),
            stringSerializer.serialize(null, "e")));
        byteStore.putAll(entries);
        byteStore.commit(Map.of());

        int numberOfKeysReturned = 0;

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = byteStore.prefixScan("bb", stringSerializer)) {
            while (keysWithPrefix.hasNext()) {
                keysWithPrefix.next();
                numberOfKeysReturned++;
            }
        }

        assertThat(numberOfKeysReturned, is(0));
    }

    @SuppressWarnings("resource")
    @Test
    public void shouldThrowNullPointerIfPrefixKeySerializerIsNull() {
        assertThrows(NullPointerException.class, () -> byteStore.prefixScan("bb", null));
    }

    @Test
    public void shouldMatchPositionAfterPut() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);

        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        context.setRecordContext(new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders()));
        inMemoryKeyValueStore.put(bytesKey("key3"), bytesValue("value3"));

        final Position expected = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 3L)))));
        final Position actual = inMemoryKeyValueStore.getPosition();
        assertEquals(expected, actual);
    }

    @Test
    public void iteratorHasNextOnEmptyStoreShouldReturnFalse() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        try (final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all()) {
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void iteratorHasNextOnDeletedEntryShouldReturnFalse() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertTrue(iter.hasNext());
        inMemoryKeyValueStore.delete(bytesKey("key"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void iteratorHasNextShouldNotAdvanceIterator() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext()); // should still point to the first element
    }

    @Test
    public void iteratorHasNextShouldReturnTrueIfElementsRemaining() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        inMemoryKeyValueStore.delete(bytesKey("key1"));
        assertTrue(iter.hasNext());
    }

    @Test
    public void iteratorNextShouldReturnNextElement() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(bytesKey("key"), next.key);
        assertArrayEquals(bytesValue("value"), next.value);
    }

    @Test
    public void iteratorNextAfterHasNextShouldReturnNextElement() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertTrue(iter.hasNext());
        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(bytesKey("key"), next.key);
        assertArrayEquals(bytesValue("value"), next.value);
    }

    @Test
    public void iteratorNextOnEmptyStoreShouldThrowException() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();
        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    public void iteratorNextShouldThrowExceptionIfRemainingElementsDeleted() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(bytesKey("key1"), next.key);
        assertArrayEquals(bytesValue("value1"), next.value);

        inMemoryKeyValueStore.delete(bytesKey("key2"));
        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    public void iteratorNextShouldSkipDeletedElements() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        inMemoryKeyValueStore.delete(bytesKey("key1"));
        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(bytesKey("key2"), next.key);
        assertArrayEquals(bytesValue("value2"), next.value);
    }

    @Test
    public void iteratorNextShouldIterateOverAllElements() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        final KeyValue<Bytes, byte[]> next1 = iter.next();
        assertEquals(bytesKey("key1"), next1.key);
        assertArrayEquals(bytesValue("value1"), next1.value);

        final KeyValue<Bytes, byte[]> next2 = iter.next();
        assertEquals(bytesKey("key2"), next2.key);
        assertArrayEquals(bytesValue("value2"), next2.value);

        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    public void iteratorPeekNextKeyOnEmptyStoreShouldThrowException() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();
        assertThrows(NoSuchElementException.class, iter::peekNextKey);
    }

    @Test
    public void iteratorPeekNextKeyOnDeletedEntryShouldThrowException() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertEquals(bytesKey("key"), iter.peekNextKey());
        inMemoryKeyValueStore.delete(bytesKey("key"));
        assertThrows(NoSuchElementException.class, iter::peekNextKey);
    }

    @Test
    public void iteratorPeekNextKeyShouldNotAdvanceIterator() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertEquals(bytesKey("key"), iter.peekNextKey());
        assertEquals(bytesKey("key"), iter.peekNextKey());
    }

    @Test
    public void iteratorPeekNextKeyShouldSkipDeletedElements() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        inMemoryKeyValueStore.delete(bytesKey("key1"));
        assertEquals(bytesKey("key2"), iter.peekNextKey());
    }

    @Test
    public void iteratorShouldThrowIllegalStateExceptionIfAlreadyClosed() {
        inMemoryKeyValueStore.init(context, inMemoryKeyValueStore);
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        iter.close();
        assertThrows(IllegalStateException.class, iter::hasNext);
        assertThrows(IllegalStateException.class, iter::next);
        assertThrows(IllegalStateException.class, iter::peekNextKey);
    }

    @Test
    public void readOnlyCommittedShouldHideStagedWritesFromTransactionBuffer() {
        final InMemoryKeyValueStore txnStore = openTransactionalStore();
        try {
            final Bytes k1 = bytesKey("k1");
            final Bytes k2 = bytesKey("k2");
            txnStore.put(k1, bytesValue("v1"));
            txnStore.commit(Map.of());

            txnStore.put(k1, bytesValue("v1-staged"));
            txnStore.put(k2, bytesValue("v2-staged"));

            final ReadOnlyKeyValueStore<Bytes, byte[]> uncommitted = txnStore.readOnly(IsolationLevel.READ_UNCOMMITTED);
            final ReadOnlyKeyValueStore<Bytes, byte[]> committed = txnStore.readOnly(IsolationLevel.READ_COMMITTED);

            assertArrayEquals(bytesValue("v1-staged"), uncommitted.get(k1));
            assertArrayEquals(bytesValue("v2-staged"), uncommitted.get(k2));
            assertArrayEquals(bytesValue("v1"), committed.get(k1));
            assertThat(committed.get(k2), nullValue());

            try (KeyValueIterator<Bytes, byte[]> it = committed.all()) {
                final List<String> keys = new ArrayList<>();
                while (it.hasNext()) {
                    keys.add(new String(it.next().key.get()));
                }
                assertEquals(List.of("k1"), keys);
            }
            try (KeyValueIterator<Bytes, byte[]> it = uncommitted.all()) {
                final List<String> keys = new ArrayList<>();
                while (it.hasNext()) {
                    keys.add(new String(it.next().key.get()));
                }
                assertEquals(List.of("k1", "k2"), keys);
            }
        } finally {
            txnStore.close();
        }
    }

    @Test
    public void readOnlyCommittedShouldNotSeeStagedDelete() {
        final InMemoryKeyValueStore txnStore = openTransactionalStore();
        try {
            final Bytes k = bytesKey("k");
            txnStore.put(k, bytesValue("v"));
            txnStore.commit(Map.of());

            txnStore.delete(k);

            assertThat(txnStore.readOnly(IsolationLevel.READ_UNCOMMITTED).get(k), nullValue());
            assertArrayEquals(bytesValue("v"), txnStore.readOnly(IsolationLevel.READ_COMMITTED).get(k));
        } finally {
            txnStore.close();
        }
    }

    @Test
    public void readOnlyPrefixScanShouldRespectIsolationLevel() {
        final InMemoryKeyValueStore txnStore = openTransactionalStore();
        try {
            txnStore.put(bytesKey("p-1"), bytesValue("a"));
            txnStore.commit(Map.of());
            txnStore.put(bytesKey("p-2"), bytesValue("b"));
            txnStore.put(bytesKey("q-1"), bytesValue("z"));

            final List<String> uncommittedKeys = new ArrayList<>();
            try (KeyValueIterator<Bytes, byte[]> it = txnStore.readOnly(IsolationLevel.READ_UNCOMMITTED)
                    .prefixScan("p-", stringSerializer)) {
                while (it.hasNext()) {
                    uncommittedKeys.add(new String(it.next().key.get()));
                }
            }
            final List<String> committedKeys = new ArrayList<>();
            try (KeyValueIterator<Bytes, byte[]> it = txnStore.readOnly(IsolationLevel.READ_COMMITTED)
                    .prefixScan("p-", stringSerializer)) {
                while (it.hasNext()) {
                    committedKeys.add(new String(it.next().key.get()));
                }
            }
            assertEquals(List.of("p-1", "p-2"), uncommittedKeys);
            assertEquals(List.of("p-1"), committedKeys);
        } finally {
            txnStore.close();
        }
    }

    @Test
    public void readOnlyCommittedScanShouldNotRaceConcurrentCommit() throws InterruptedException {
        // A READ_COMMITTED scan from a non-owner (IQ) thread must observe a point-in-time snapshot
        // of the committed base map, taken under the buffer's snapshot read-lock. Before the fix the
        // view copied the live TreeMap under the store monitor, which does not exclude commit()'s
        // write-lock-guarded flushToBase(), so a concurrent commit threw ConcurrentModificationException
        // and/or exposed a half-applied commit. Each commit flips every key to a single generation, so
        // a correct snapshot scan must see one uniform generation.
        final InMemoryKeyValueStore txnStore = openTransactionalStore();
        final int numKeys = 500;
        final int rounds = 300;
        final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);

        try {
            for (int i = 0; i < numKeys; i++) {
                txnStore.put(bytesKey("k" + i), bytesValue("gen0"));
            }
            txnStore.commit(Map.of()); // owner thread owns the buffer

            final Thread reader = new Thread(() -> {
                final ReadOnlyKeyValueStore<Bytes, byte[]> committed = txnStore.readOnly(IsolationLevel.READ_COMMITTED);
                try {
                    while (!stop.get()) {
                        try (KeyValueIterator<Bytes, byte[]> it = committed.all()) {
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
                            // a committed snapshot is always the full, uniform key set
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
                    txnStore.put(bytesKey("k" + i), bytesValue(generation));
                }
                txnStore.commit(Map.of());
            }
            stop.set(true);
            reader.join(TimeUnit.SECONDS.toMillis(30));

            if (readerFailure.get() != null) {
                throw new AssertionError("READ_COMMITTED scan raced a concurrent commit", readerFailure.get());
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
        final InMemoryKeyValueStore store = new InMemoryKeyValueStore("txn-pos-store");
        store.init(ctx, store);
        try {
            ctx.setRecordContext(new ProcessorRecordContext(0, 1, 0, "topic", new RecordHeaders()));
            store.put(bytesKey("key1"), bytesValue("value1"));

            final Position expected = Position.fromMap(mkMap(mkEntry("topic", mkMap(mkEntry(0, 1L)))));

            // READ_UNCOMMITTED query should expose the staged position before commit
            final org.apache.kafka.streams.query.QueryResult<?> uncommitted = store.query(
                org.apache.kafka.streams.query.RangeQuery.withNoBounds(),
                org.apache.kafka.streams.query.PositionBound.unbounded(),
                new org.apache.kafka.streams.query.QueryConfig(false));
            assertEquals(expected, uncommitted.getPosition(), "READ_UNCOMMITTED query position");

            // getPosition() reports the uncommitted (committed + staged) position, mirroring
            // RocksDBStore, so the changelog consistency vector reflects the staged write.
            assertEquals(expected, store.getPosition(), "getPosition before commit (uncommitted)");

            // after commit, committed position populated
            store.commit(Map.of());
            assertEquals(expected, store.getPosition(), "getPosition after commit");
        } finally {
            store.close();
        }
    }

    private InMemoryKeyValueStore openTransactionalStore() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.setProperty(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, "true");
        final InternalMockProcessorContext<Bytes, byte[]> ctx = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            new Serdes.BytesSerde(),
            new Serdes.ByteArraySerde(),
            new StreamsConfig(props)
        );
        final InMemoryKeyValueStore store = new InMemoryKeyValueStore("txn-in-memory-store");
        store.init(ctx, store);
        return store;
    }

    private byte[] bytesValue(final String value) {
        return value.getBytes();
    }

    private Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }

}
