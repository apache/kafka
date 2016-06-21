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
package org.apache.kafka.streams;


import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.ReadOnlyStoreProvider;
import org.apache.kafka.streams.state.internals.ReadOnlyStoreProviderStub;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CompositeReadOnlyStoreProviderTest {

    private final String keyValueStore = "key-value";
    private final String windowStore = "window-store";
    private CompositeReadOnlyStoreProvider storeProvider;

    @Before
    public void before() {
        final ReadOnlyStoreProviderStub theStoreProvider = new ReadOnlyStoreProviderStub();
        theStoreProvider.addKeyValueStore(keyValueStore, new NoOpReadOnlyStore<>());
        theStoreProvider.addWindowStore(windowStore, new NoOpReadOnlyStore<>());
        storeProvider =
            new CompositeReadOnlyStoreProvider(
                Collections.<ReadOnlyStoreProvider>singletonList(theStoreProvider));
    }

    @Test
    public void shouldReturnNullIfKVStoreDoesntExist() throws Exception {
        assertNull(storeProvider.getStore("not-a-store"));
    }

    @Test
    public void shouldReturnNullIfWindowStoreDoesntExist() throws Exception {
        assertNull(storeProvider.getWindowedStore("not-a-store"));
    }

    @Test
    public void shouldReturnKVStoreWhenItExists() throws Exception {
        assertNotNull(storeProvider.getStore(keyValueStore));
    }

    @Test
    public void shouldReturnWindowStoreWhenItExists() throws Exception {
        assertNotNull(storeProvider.getWindowedStore(windowStore));
    }

    @Test
    public void shouldNotReturnKVStoreWhenIsWindowStore() throws Exception {
        assertNull(storeProvider.getStore(windowStore));
    }

    @Test
    public void shouldNotReturnWindowStoreWhenIsKVStore() throws Exception {
        assertNull(storeProvider.getWindowedStore(keyValueStore));
    }


    class NoOpReadOnlyStore<K, V>
        implements ReadOnlyKeyValueStore<K, V>, ReadOnlyWindowStore<K, V> {

        @Override
        public V get(final K key) {
            return null;
        }

        @Override
        public WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
            return null;
        }
    }

}