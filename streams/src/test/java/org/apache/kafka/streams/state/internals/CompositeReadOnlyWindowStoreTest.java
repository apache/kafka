/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.StateStoreProviderStub;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class CompositeReadOnlyWindowStoreTest {

    private final String storeName = "window-store";
    private StateStoreProviderStub stubProviderOne;
    private StateStoreProviderStub stubProviderTwo;
    private CompositeReadOnlyWindowStore<String, String>
        windowStore;
    private ReadOnlyWindowStoreStub<String, String> underlyingWindowStore;
    private ReadOnlyWindowStoreStub<String, String>
            otherUnderlyingStore;

    @Before
    public void before() {
        stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);
        underlyingWindowStore = new ReadOnlyWindowStoreStub<>();
        stubProviderOne.addStore(storeName, underlyingWindowStore);

        otherUnderlyingStore = new ReadOnlyWindowStoreStub<>();
        stubProviderOne.addStore("other-window-store", otherUnderlyingStore);


        windowStore = new CompositeReadOnlyWindowStore<>(
            new WrappingStoreProvider(Arrays.<StateStoreProvider>asList(stubProviderOne, stubProviderTwo)),
                QueryableStoreTypes.<String, String>windowStore(),
                storeName);
    }

    @Test
    public void shouldFetchValuesFromWindowStore() throws Exception {
        underlyingWindowStore.put("my-key", "my-value", 0L);
        underlyingWindowStore.put("my-key", "my-later-value", 10L);

        final WindowStoreIterator<String> iterator = windowStore.fetch("my-key", 0L, 25L);
        final List<KeyValue<Long, String>> results = toList(iterator);

        assertEquals(asList(new KeyValue<>(0L, "my-value"),
                            new KeyValue<>(10L, "my-later-value")),
                     results);
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoData() throws Exception {
        final WindowStoreIterator<String> iterator = windowStore.fetch("my-key", 0L, 25L);
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() throws Exception {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>();
        stubProviderTwo.addStore(storeName, secondUnderlying);

        underlyingWindowStore.put("key-one", "value-one", 0L);
        secondUnderlying.put("key-two", "value-two", 10L);

        final List<KeyValue<Long, String>> keyOneResults = toList(windowStore.fetch("key-one", 0L,
                                                                                    1L));
        final List<KeyValue<Long, String>> keyTwoResults = toList(windowStore.fetch("key-two", 10L,
                                                                                    11L));

        assertEquals(Collections.singletonList(KeyValue.pair(0L, "value-one")), keyOneResults);
        assertEquals(Collections.singletonList(KeyValue.pair(10L, "value-two")), keyTwoResults);
    }

    @Test
    public void shouldNotGetValuesFromOtherStores() throws Exception {
        otherUnderlyingStore.put("some-key", "some-value", 0L);
        underlyingWindowStore.put("some-key", "my-value", 1L);

        final List<KeyValue<Long, String>> results = toList(windowStore.fetch("some-key", 0L, 2L));
        assertEquals(Collections.singletonList(new KeyValue<>(1L, "my-value")), results);
    }


    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStateStoreExceptionOnRebalance() throws Exception {
        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(new StateStoreProviderStub(true), QueryableStoreTypes.windowStore(), "foo");
        store.fetch("key", 1, 10);
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStateStoreExceptionIfFetchThrows() throws Exception {
        underlyingWindowStore.setOpen(false);
        underlyingWindowStore.fetch("key", 1, 10);
    }

    static <K, V> List<KeyValue<K, V>> toList(final Iterator<KeyValue<K, V>> iterator) {
        final List<KeyValue<K, V>> results = new ArrayList<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }
}