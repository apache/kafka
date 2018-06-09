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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class AbstractKeyValueStoreTest {

    protected abstract <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext context);

    protected InternalMockProcessorContext context;
    protected KeyValueStore<Integer, String> store;
    protected KeyValueStoreTestDriver<Integer, String> driver;

    @Before
    public void before() {
        driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        context = (InternalMockProcessorContext) driver.context();
        context.setTime(10);
        store = createKeyValueStore(context);
    }

    @After
    public void after() {
        store.close();
        driver.clear();
    }

    private static Map<Integer, String> getContents(final KeyValueIterator<Integer, String> iter) {
        final HashMap<Integer, String> result = new HashMap<>();
        while (iter.hasNext()) {
            KeyValue<Integer, String> entry = iter.next();
            result.put(entry.key, entry.value);
        }
        return result;
    }

    @Test
    public void shouldNotIncludeDeletedFromRangeResult() {
        store.close();

        final Serializer<String> serializer = new StringSerializer() {
            private int numCalls = 0;

            @Override
            public byte[] serialize(final String topic, final String data) {
                if (++numCalls > 3) {
                    fail("Value serializer is called; it should never happen");
                }

                return super.serialize(topic, data);
            }
        };

        context.setValueSerde(Serdes.serdeFrom(serializer, new StringDeserializer()));
        store = createKeyValueStore(driver.context());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.delete(0);
        store.delete(1);

        // should not include deleted records in iterator
        final Map<Integer, String> expectedContents = Collections.singletonMap(2, "two");
        assertEquals(expectedContents, getContents(store.all()));
    }

    @Test
    public void shouldDeleteIfSerializedValueIsNull() {
        store.close();

        final Serializer<String> serializer = new StringSerializer() {
            @Override
            public byte[] serialize(final String topic, final String data) {
                if (data.equals("null")) {
                    // will be serialized to null bytes, indicating deletes
                    return null;
                }
                return super.serialize(topic, data);
            }
        };

        context.setValueSerde(Serdes.serdeFrom(serializer, new StringDeserializer()));
        store = createKeyValueStore(driver.context());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(0, "null");
        store.put(1, "null");

        // should not include deleted records in iterator
        final Map<Integer, String> expectedContents = Collections.singletonMap(2, "two");
        assertEquals(expectedContents, getContents(store.all()));
    }

    @Test
    public void testPutGetRange() {
        // Verify that the store reads and writes correctly ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        assertEquals(5, driver.sizeOf(store));
        assertEquals("zero", store.get(0));
        assertEquals("one", store.get(1));
        assertEquals("two", store.get(2));
        assertNull(store.get(3));
        assertEquals("four", store.get(4));
        assertEquals("five", store.get(5));
        store.delete(5);
        assertEquals(4, driver.sizeOf(store));

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        assertEquals("zero", driver.flushedEntryStored(0));
        assertEquals("one", driver.flushedEntryStored(1));
        assertEquals("two", driver.flushedEntryStored(2));
        assertEquals("four", driver.flushedEntryStored(4));
        assertEquals(null, driver.flushedEntryStored(5));

        assertEquals(false, driver.flushedEntryRemoved(0));
        assertEquals(false, driver.flushedEntryRemoved(1));
        assertEquals(false, driver.flushedEntryRemoved(2));
        assertEquals(false, driver.flushedEntryRemoved(4));
        assertEquals(true, driver.flushedEntryRemoved(5));

        final HashMap<Integer, String> expectedContents = new HashMap<>();
        expectedContents.put(2, "two");
        expectedContents.put(4, "four");

        // Check range iteration ...
        assertEquals(expectedContents, getContents(store.range(2, 4)));
        assertEquals(expectedContents, getContents(store.range(2, 6)));

        // Check all iteration ...
        expectedContents.put(0, "zero");
        expectedContents.put(1, "one");
        assertEquals(expectedContents, getContents(store.all()));
    }

    @Test
    public void testPutGetRangeWithDefaultSerdes() {
        // Verify that the store reads and writes correctly ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        assertEquals(5, driver.sizeOf(store));
        assertEquals("zero", store.get(0));
        assertEquals("one", store.get(1));
        assertEquals("two", store.get(2));
        assertNull(store.get(3));
        assertEquals("four", store.get(4));
        assertEquals("five", store.get(5));
        store.delete(5);

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        assertEquals("zero", driver.flushedEntryStored(0));
        assertEquals("one", driver.flushedEntryStored(1));
        assertEquals("two", driver.flushedEntryStored(2));
        assertEquals("four", driver.flushedEntryStored(4));
        assertEquals(null, driver.flushedEntryStored(5));

        assertEquals(false, driver.flushedEntryRemoved(0));
        assertEquals(false, driver.flushedEntryRemoved(1));
        assertEquals(false, driver.flushedEntryRemoved(2));
        assertEquals(false, driver.flushedEntryRemoved(4));
        assertEquals(true, driver.flushedEntryRemoved(5));
    }

    @Test
    public void testRestore() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());

        // Verify that the store's contents were properly restored ...
        assertEquals(0, driver.checkForRestoredEntries(store));

        // and there are no other entries ...
        assertEquals(4, driver.sizeOf(store));
    }

    @Test
    public void testRestoreWithDefaultSerdes() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());
        // Verify that the store's contents were properly restored ...
        assertEquals(0, driver.checkForRestoredEntries(store));

        // and there are no other entries ...
        assertEquals(4, driver.sizeOf(store));
    }

    @Test
    public void testPutIfAbsent() {
        // Verify that the store reads and writes correctly ...
        assertNull(store.putIfAbsent(0, "zero"));
        assertNull(store.putIfAbsent(1, "one"));
        assertNull(store.putIfAbsent(2, "two"));
        assertNull(store.putIfAbsent(4, "four"));
        assertEquals("four", store.putIfAbsent(4, "unexpected value"));
        assertEquals(4, driver.sizeOf(store));
        assertEquals("zero", store.get(0));
        assertEquals("one", store.get(1));
        assertEquals("two", store.get(2));
        assertNull(store.get(3));
        assertEquals("four", store.get(4));

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        assertEquals("zero", driver.flushedEntryStored(0));
        assertEquals("one", driver.flushedEntryStored(1));
        assertEquals("two", driver.flushedEntryStored(2));
        assertEquals("four", driver.flushedEntryStored(4));

        assertEquals(false, driver.flushedEntryRemoved(0));
        assertEquals(false, driver.flushedEntryRemoved(1));
        assertEquals(false, driver.flushedEntryRemoved(2));
        assertEquals(false, driver.flushedEntryRemoved(4));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        store.put(null, "anyValue");
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        store.put(1, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutIfAbsentNullKey() {
        store.putIfAbsent(null, "anyValue");
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutIfAbsentNullValue() {
        store.putIfAbsent(1, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutAllNullKey() {
        store.putAll(Collections.singletonList(new KeyValue<Integer, String>(null, "anyValue")));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnPutAllNullKey() {
        store.putAll(Collections.singletonList(new KeyValue<Integer, String>(1, null)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnDeleteNullKey() {
        store.delete(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        store.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        store.range(null, 2);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        store.range(2, null);
    }

    @Test
    public void testSize() {
        assertEquals("A newly created store should have no entries", 0, store.approximateNumEntries());

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(4, "four");
        store.put(5, "five");
        store.flush();
        assertEquals(5, store.approximateNumEntries());
    }

    @Test
    public void shouldPutAll() {
        List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "one"));
        entries.add(new KeyValue<>(2, "two"));

        store.putAll(entries);

        final List<KeyValue<Integer, String>> allReturned = new ArrayList<>();
        final List<KeyValue<Integer, String>> expectedReturned = Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));
        final Iterator<KeyValue<Integer, String>> iterator = store.all();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }
        assertThat(allReturned, equalTo(expectedReturned));

    }

    @Test
    public void shouldDeleteFromStore() {
        store.put(1, "one");
        store.put(2, "two");
        store.delete(2);
        assertNull(store.get(2));
    }
}
