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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class InMemoryKeyValueLoggedStoreTest extends AbstractKeyValueStoreTest {

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<Integer> integerSerde = Serdes.Integer();

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext context) {
        final StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("my-store"),
            (Serde<K>) context.keySerde(),
            (Serde<V>) context.valueSerde())
            .withLoggingEnabled(Collections.singletonMap("retention.ms", "1000"));

        final StateStore store = storeBuilder.build();
        store.init(context, store);

        return (KeyValueStore<K, V>) store;
    }

    @Test
    public void shouldPutAll() {
        List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "1"));
        entries.add(new KeyValue<>(2, "2"));
        store.putAll(entries);
        assertEquals(store.get(1), "1");
        assertEquals(store.get(2), "2");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRemoveLeastUsedEntry() {

        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(2, integerSerde, stringSerde);

        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");
        inMemoryKeyValueLoggedStore.get(1);
        inMemoryKeyValueLoggedStore.put(3, "three");

        assertThat(inMemoryKeyValueLoggedStore.get(2), nullValue());
        assertThat(inMemoryKeyValueLoggedStore.get(1), is("one"));
        assertThat(inMemoryKeyValueLoggedStore.get(3), is("three"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldUseContextSerdeWhenNoneProvided() {

        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(2, null, null);

        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");

        // still able to access data in store and deserialize using context Serde
        assertThat(inMemoryKeyValueLoggedStore.get(2), is("two"));
        assertThat(inMemoryKeyValueLoggedStore.get(1), is("one"));
    }

    @Test
    public void shouldReturnCorrectApproximateNumberOfEntries() {
        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(3, integerSerde, stringSerde);

        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");
        inMemoryKeyValueLoggedStore.put(3, "three");

        assertThat(inMemoryKeyValueLoggedStore.approximateNumEntries(), is(3L));
    }

    @Test
    public void shouldReturnAll() {
        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(3, integerSerde, stringSerde);
        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");
        inMemoryKeyValueLoggedStore.put(3, "three");

        final List<KeyValue<Integer, String>> allReturned = new ArrayList<>();
        final List<KeyValue<Integer, String>> expectedReturned = Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"), KeyValue.pair(3, "three"));
        final Iterator<KeyValue<Integer, String>> iterator = inMemoryKeyValueLoggedStore.all();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }

        assertThat(allReturned, equalTo(expectedReturned));
    }

    @Test
    public void shouldPutAllInLoggedStore() {
        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(3, integerSerde, stringSerde);
        final List<KeyValue<Integer, String>> allReturned = new ArrayList<>();
        final List<KeyValue<Integer, String>> initialPutAll = Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"), KeyValue.pair(3, "three"));

        inMemoryKeyValueLoggedStore.putAll(initialPutAll);

        final Iterator<KeyValue<Integer, String>> iterator = inMemoryKeyValueLoggedStore.all();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }
        assertThat(allReturned, equalTo(initialPutAll));
    }

    @Test
    public void shouldPutIfAbsentValidUpdate() {
        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(3, integerSerde, stringSerde);
        String returnedValue = inMemoryKeyValueLoggedStore.putIfAbsent(1, "one");
        // null not previously in store
        assertNull(returnedValue);

        String attemptedUpdate = inMemoryKeyValueLoggedStore.putIfAbsent(1, "one-more");

        // already in store so sticking with original value, no update
        assertThat(attemptedUpdate, equalTo("one"));
    }


    @Test
    public void shouldReturnRange() {
        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(3, integerSerde, stringSerde);
        final List<KeyValue<Integer, String>> allReturnedRange = new ArrayList<>();
        final List<KeyValue<Integer, String>>
            initialPutAll =
            Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"), KeyValue.pair(3, "three"), KeyValue.pair(4, "four"));

        final List<KeyValue<Integer, String>> expectedRange = Arrays.asList(KeyValue.pair(3, "three"), KeyValue.pair(4, "four"));
        inMemoryKeyValueLoggedStore.putAll(initialPutAll);

        final Iterator<KeyValue<Integer, String>> iterator = inMemoryKeyValueLoggedStore.range(3, 4);

        while (iterator.hasNext()) {
            allReturnedRange.add(iterator.next());
        }
        assertThat(allReturnedRange, equalTo(expectedRange));
    }

    @Test
    public void shouldDeleteFromStore() {
        final InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = getInMemoryLoggedStore(3, integerSerde, stringSerde);

        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");
        inMemoryKeyValueLoggedStore.put(3, "three");

        String deletedValue = inMemoryKeyValueLoggedStore.delete(2);

        assertThat(deletedValue, is("two"));
        assertThat(inMemoryKeyValueLoggedStore.get(2), nullValue());
    }


    @SuppressWarnings("unchecked")
    private KeyValueStore<Integer, String> getLruCacheInnerStore(final int size,
                                                                 final Serde<Integer> integerSerde,
                                                                 final Serde<String> stringSerde) {

        return new MemoryNavigableLRUCache("test-cache", size, integerSerde, stringSerde);

    }

    private InMemoryKeyValueLoggedStore<Integer, String> getInMemoryLoggedStore(final int innerSize,
                                                                                final Serde<Integer> integerSerde,
                                                                                final Serde<String> stringSerde) {

        final KeyValueStore<Integer, String> inner = getLruCacheInnerStore(innerSize, integerSerde, stringSerde);
        InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore = new InMemoryKeyValueLoggedStore<>(inner, integerSerde, stringSerde);
        inMemoryKeyValueLoggedStore.init(context, inner);

        return inMemoryKeyValueLoggedStore;
    }
}
