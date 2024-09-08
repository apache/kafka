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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

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

    @SuppressWarnings("unchecked")
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
        byteStore.flush();

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
        byteStore.flush();

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefixAsabcd = byteStore.prefixScan("abcd", stringSerializer)) {
            int numberOfKeysReturned = 0;

            while (keysWithPrefixAsabcd.hasNext()) {
                keysWithPrefixAsabcd.next().key.get();
                numberOfKeysReturned++;
            }

            assertThat(numberOfKeysReturned, is(1));
        }
    }

    @Test
    public void shouldReturnUUIDsWithStringPrefix() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        final Serializer<UUID> uuidSerializer = Serdes.UUID().serializer();
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
        byteStore.flush();

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
        byteStore.flush();

        int numberOfKeysReturned = 0;

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = byteStore.prefixScan("bb", stringSerializer)) {
            while (keysWithPrefix.hasNext()) {
                keysWithPrefix.next();
                numberOfKeysReturned++;
            }
        }

        assertThat(numberOfKeysReturned, is(0));
    }

    @Test
    public void shouldThrowNullPointerIfPrefixKeySerializerIsNull() {
        assertThrows(NullPointerException.class, () -> byteStore.prefixScan("bb", null));
    }

    @Test
    public void shouldMatchPositionAfterPut() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);

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
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();
        assertFalse(iter.hasNext());
    }

    @Test
    public void iteratorHasNextOnDeletedEntryShouldReturnFalse() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertTrue(iter.hasNext());
        inMemoryKeyValueStore.delete(bytesKey("key"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void iteratorHasNextShouldNotAdvanceIterator() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext()); // should still point to the first element
    }

    @Test
    public void iteratorHasNextShouldReturnTrueIfElementsRemaining() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        inMemoryKeyValueStore.delete(bytesKey("key1"));
        assertTrue(iter.hasNext());
    }

    @Test
    public void iteratorNextShouldReturnNextElement() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(bytesKey("key"), next.key);
        assertArrayEquals(bytesValue("value"), next.value);
    }

    @Test
    public void iteratorNextAfterHasNextShouldReturnNextElement() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertTrue(iter.hasNext());
        final KeyValue<Bytes, byte[]> next = iter.next();
        assertEquals(bytesKey("key"), next.key);
        assertArrayEquals(bytesValue("value"), next.value);
    }

    @Test
    public void iteratorNextOnEmptyStoreShouldThrowException() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();
        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    public void iteratorNextShouldThrowExceptionIfRemainingElementsDeleted() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
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
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
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
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
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
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();
        assertThrows(NoSuchElementException.class, iter::peekNextKey);
    }

    @Test
    public void iteratorPeekNextKeyOnDeletedEntryShouldThrowException() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertEquals(bytesKey("key"), iter.peekNextKey());
        inMemoryKeyValueStore.delete(bytesKey("key"));
        assertThrows(NoSuchElementException.class, iter::peekNextKey);
    }

    @Test
    public void iteratorPeekNextKeyShouldNotAdvanceIterator() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key"), bytesValue("value"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        assertEquals(bytesKey("key"), iter.peekNextKey());
        assertEquals(bytesKey("key"), iter.peekNextKey());
    }

    @Test
    public void iteratorPeekNextKeyShouldSkipDeletedElements() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        inMemoryKeyValueStore.put(bytesKey("key1"), bytesValue("value1"));
        inMemoryKeyValueStore.put(bytesKey("key2"), bytesValue("value2"));
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        inMemoryKeyValueStore.delete(bytesKey("key1"));
        assertEquals(bytesKey("key2"), iter.peekNextKey());
    }

    @Test
    public void iteratorShouldThrowIllegalStateExceptionIfAlreadyClosed() {
        inMemoryKeyValueStore.init((StateStoreContext) context, inMemoryKeyValueStore);
        final KeyValueIterator<Bytes, byte[]> iter = inMemoryKeyValueStore.all();

        iter.close();
        assertThrows(IllegalStateException.class, iter::hasNext);
        assertThrows(IllegalStateException.class, iter::next);
        assertThrows(IllegalStateException.class, iter::peekNextKey);
    }

    private byte[] bytesValue(final String value) {
        return value.getBytes();
    }

    private Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }

}
