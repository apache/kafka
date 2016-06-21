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

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CompositeReadOnlyStoreTest {

    private final String storeName = "my-store";
    private ReadOnlyStoreProviderStub stubProviderTwo;
    private ReadOnlyStoreProviderStub stubProviderOne;
    private MemoryLRUCache<String, String> stubOneUnderlying;
    private CompositeReadOnlyStore<String, String> theStore;
    private MemoryLRUCache<Object, Object>
        otherUnderlyingStore;

    @Before
    public void before() {
        stubProviderOne = new ReadOnlyStoreProviderStub();
        stubProviderTwo = new ReadOnlyStoreProviderStub();

        stubOneUnderlying = new MemoryLRUCache<>(storeName, 10);
        stubProviderOne.addKeyValueStore(storeName, stubOneUnderlying);
        otherUnderlyingStore = new MemoryLRUCache<>("other-store", 10);
        stubProviderOne.addKeyValueStore("other-store", otherUnderlyingStore);

        theStore = new CompositeReadOnlyStore<>(
            Arrays.<ReadOnlyStoreProvider>asList(stubProviderOne,
                                                 stubProviderTwo),
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
        final MemoryLRUCache<String, String>
            cache =
            new MemoryLRUCache<>(storeName, 10);
        stubProviderTwo.addKeyValueStore(storeName, cache);

        cache.put("key-two", "key-two-value");
        stubOneUnderlying.put("key-one", "key-one-value");

        assertEquals("key-two-value", theStore.get("key-two"));
        assertEquals("key-one-value", theStore.get("key-one"));
    }

}