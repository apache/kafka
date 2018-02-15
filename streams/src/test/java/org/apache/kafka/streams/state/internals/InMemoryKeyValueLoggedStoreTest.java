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
import org.apache.kafka.test.KeyValueIteratorStub;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class InMemoryKeyValueLoggedStoreTest extends AbstractKeyValueStoreTest {


    private KeyValueStore<Integer, String> innerKeyValueStore = EasyMock.createNiceMock(KeyValueStore.class);
    private InMemoryKeyValueLoggedStore<Integer, String> inMemoryKeyValueLoggedStore;

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

    @Before
    public void setUp() {
        innerKeyValueStore.init(EasyMock.isA(ProcessorContext.class), EasyMock.isA(StateStore.class));
        EasyMock.expect(innerKeyValueStore.name()).andReturn("inner-store").times(2);
        inMemoryKeyValueLoggedStore = new InMemoryKeyValueLoggedStore<>(innerKeyValueStore, integerSerde, stringSerde);
    }

    @Test
    public void shouldPutAll() {
        List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "1"));
        entries.add(new KeyValue<>(2, "2"));

        innerKeyValueStore.putAll(entries);
        EasyMock.replay(innerKeyValueStore);

        inMemoryKeyValueLoggedStore.init(context, innerKeyValueStore);
        inMemoryKeyValueLoggedStore.putAll(entries);
        EasyMock.verify(innerKeyValueStore);
    }

    @Test
    public void shouldReturnCorrectApproximateNumberOfEntries() {
        innerKeyValueStore.put(1, "one");
        innerKeyValueStore.put(2, "two");
        EasyMock.expect(innerKeyValueStore.approximateNumEntries()).andReturn(2L);
        EasyMock.replay(innerKeyValueStore);

        inMemoryKeyValueLoggedStore.init(context, innerKeyValueStore);
        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");

        assertThat(inMemoryKeyValueLoggedStore.approximateNumEntries(), is(2L));
        EasyMock.verify(innerKeyValueStore);
    }

    @Test
    public void shouldReturnAll() {
        final KeyValueIteratorStub<Integer, String>
            iteratorStub =
            new KeyValueIteratorStub<>(Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two")).iterator());
        innerKeyValueStore.put(1, "one");
        innerKeyValueStore.put(2, "two");
        EasyMock.expect(innerKeyValueStore.all()).andReturn(iteratorStub);
        EasyMock.replay(innerKeyValueStore);

        inMemoryKeyValueLoggedStore.init(context, innerKeyValueStore);
        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");

        final List<KeyValue<Integer, String>> allReturned = new ArrayList<>();
        final List<KeyValue<Integer, String>> expectedReturned = Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));
        final Iterator<KeyValue<Integer, String>> iterator = inMemoryKeyValueLoggedStore.all();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }

        assertThat(allReturned, equalTo(expectedReturned));
        EasyMock.verify(innerKeyValueStore);
    }

    @Test
    public void shouldPutAllInLoggedStore() {
        final KeyValueIteratorStub<Integer, String>
            iteratorStub =
            new KeyValueIteratorStub<>(Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two")).iterator());
        innerKeyValueStore.putAll(Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two")));
        EasyMock.expect(innerKeyValueStore.all()).andReturn(iteratorStub);
        EasyMock.replay(innerKeyValueStore);

        inMemoryKeyValueLoggedStore.init(context, innerKeyValueStore);

        final List<KeyValue<Integer, String>> allReturned = new ArrayList<>();
        final List<KeyValue<Integer, String>> initialPutAll = Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));

        inMemoryKeyValueLoggedStore.putAll(initialPutAll);

        final Iterator<KeyValue<Integer, String>> iterator = inMemoryKeyValueLoggedStore.all();

        while (iterator.hasNext()) {
            allReturned.add(iterator.next());
        }
        assertThat(allReturned, equalTo(initialPutAll));
        EasyMock.verify(innerKeyValueStore);
    }

    @Test
    public void shouldPutIfAbsentValidUpdate() {
        EasyMock.expect(innerKeyValueStore.putIfAbsent(1, "one")).andReturn(null);
        EasyMock.expect(innerKeyValueStore.putIfAbsent(1, "one-more")).andReturn("one");
        EasyMock.replay(innerKeyValueStore);

        inMemoryKeyValueLoggedStore.init(context, innerKeyValueStore);
        inMemoryKeyValueLoggedStore.putIfAbsent(1, "one");

        inMemoryKeyValueLoggedStore.putIfAbsent(1, "one-more");

        EasyMock.verify(innerKeyValueStore);
    }


    @Test
    public void shouldReturnRange() {
        final List<KeyValue<Integer, String>> keyValueList = Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));
        final KeyValueIteratorStub<Integer, String> iteratorStub = new KeyValueIteratorStub<>(keyValueList.iterator());
        innerKeyValueStore.putAll(keyValueList);

        EasyMock.expect(innerKeyValueStore.range(1, 2)).andReturn(iteratorStub);
        EasyMock.replay(innerKeyValueStore);

        inMemoryKeyValueLoggedStore.init(context, innerKeyValueStore);

        final List<KeyValue<Integer, String>> allReturnedRange = new ArrayList<>();
        final List<KeyValue<Integer, String>>
            initialPutAll =
            Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));

        final List<KeyValue<Integer, String>> expectedRange = Arrays.asList(KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));
        inMemoryKeyValueLoggedStore.putAll(initialPutAll);

        final Iterator<KeyValue<Integer, String>> iterator = inMemoryKeyValueLoggedStore.range(1, 2);

        while (iterator.hasNext()) {
            allReturnedRange.add(iterator.next());
        }
        assertThat(allReturnedRange, equalTo(expectedRange));
        EasyMock.verify(innerKeyValueStore);
    }

    @Test
    public void shouldDeleteFromStore() {
        innerKeyValueStore.put(1, "one");
        innerKeyValueStore.put(2, "two");
        EasyMock.expect(innerKeyValueStore.delete(2)).andReturn("two");
        EasyMock.replay(innerKeyValueStore);

        inMemoryKeyValueLoggedStore.init(context, innerKeyValueStore);

        inMemoryKeyValueLoggedStore.put(1, "one");
        inMemoryKeyValueLoggedStore.put(2, "two");

        inMemoryKeyValueLoggedStore.delete(2);
        EasyMock.verify(innerKeyValueStore);
    }

}
