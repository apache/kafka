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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StateStoreProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStoreTest.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CompositeReadOnlyKeyValueStoreTest {

    private final String storeName = "my-store";
    private StateStoreProviderStub stubProviderTwo;
    private KeyValueStore<String, String> stubOneUnderlying;
    private CompositeReadOnlyKeyValueStore<String, String> theStore;
    private KeyValueStore<String, String>
        otherUnderlyingStore;

    @Before
    public void before() {
        final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub();
        stubProviderTwo = new StateStoreProviderStub();

        stubOneUnderlying = new InMemoryKeyValueStore<>(storeName);
        stubProviderOne.addStore(storeName, stubOneUnderlying);
        otherUnderlyingStore = new InMemoryKeyValueStore<>(storeName);
        stubProviderOne.addStore("other-store", otherUnderlyingStore);

        theStore = new CompositeReadOnlyKeyValueStore<>(
            new WrappingStoreProvider(Arrays.<StateStoreProvider>asList(stubProviderOne, stubProviderTwo)),
                                        QueryableStoreTypes.<String, String>keyValueStore(),
                                        storeName);
    }

    @Test
    public void shouldReturnNullIfKeyDoesntExist() throws Exception {
        assertNull(theStore.get("whatever"));
    }

    @Test
    public void shouldReturnValueIfExists() throws Exception {
        stubOneUnderlying.put("key", "value");
        assertEquals("value", theStore.get("key"));
    }

    @Test
    public void shouldNotGetValuesFromOtherStores() throws Exception {
        otherUnderlyingStore.put("otherKey", "otherValue");
        assertNull(theStore.get("otherKey"));
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() throws Exception {
        final KeyValueStore<String, String>
            cache =
            new InMemoryKeyValueStore<>(storeName);
        stubProviderTwo.addStore(storeName, cache);

        cache.put("key-two", "key-two-value");
        stubOneUnderlying.put("key-one", "key-one-value");

        assertEquals("key-two-value", theStore.get("key-two"));
        assertEquals("key-one-value", theStore.get("key-one"));
    }

    @Test
    public void shouldSupportRange() throws Exception {
        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("c", "c");

        final List<KeyValue<String, String>> results = toList(theStore.range("a", "c"));
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertEquals(2, results.size());
    }

    @Test
    public void shouldSupportRangeAcrossMultipleKVStores() throws Exception {
        final KeyValueStore<String, String>
            cache =
            new InMemoryKeyValueStore<>(storeName);
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.range("a", "e"));
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertEquals(4, results.size());
    }

    @Test
    public void shouldSupportAllAcrossMultipleStores() throws Exception {
        final KeyValueStore<String, String>
            cache =
            new InMemoryKeyValueStore<>(storeName);
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.all());
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertTrue(results.contains(new KeyValue<>("x", "x")));
        assertTrue(results.contains(new KeyValue<>("z", "z")));
        assertEquals(6, results.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStoreExceptionIfNoStoresExistOnGet() throws Exception {
        noStores().get("anything");
    }


    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStoreExceptionIfNoStoresExistOnRange() throws Exception {
        noStores().range("anything", "something");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStoreExceptionIfNoStoresExistOnAll() throws Exception {
        noStores().all();
    }

    @Test
    public void shouldGetApproximateEntriesAcrossAllStores() throws Exception {
        final KeyValueStore<String, String>
                cache =
                new InMemoryKeyValueStore<>(storeName);
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        assertEquals(6, theStore.approximateNumEntries());
    }

    @Test
    public void shouldReturnLongMaxValueOnOverflow() throws Exception {
        stubProviderTwo.addStore(storeName, new InMemoryKeyValueStore<String, String>(storeName) {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });

        stubOneUnderlying.put("overflow", "me");
        assertEquals(Long.MAX_VALUE, theStore.approximateNumEntries());
    }

    private CompositeReadOnlyKeyValueStore<Object, Object> noStores() {
        return new CompositeReadOnlyKeyValueStore<>(new WrappingStoreProvider(Collections.<StateStoreProvider>emptyList()),
                QueryableStoreTypes.keyValueStore(), storeName);
    }
}