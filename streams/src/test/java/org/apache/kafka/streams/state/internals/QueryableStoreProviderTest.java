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


import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.NoOpWindowStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StateStoreProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class QueryableStoreProviderTest {

    private final String keyValueStore = "key-value";
    private final String windowStore = "window-store";
    private QueryableStoreProvider storeProvider;

    @Before
    public void before() {
        final StateStoreProviderStub theStoreProvider = new StateStoreProviderStub();
        theStoreProvider.addStore(keyValueStore, new NoOpReadOnlyStore<>());
        theStoreProvider.addStore(windowStore, new NoOpWindowStore());
        storeProvider =
            new QueryableStoreProvider(
                Collections.<StateStoreProvider>singletonList(theStoreProvider));
    }

    @Test
    public void shouldReturnNullIfKVStoreDoesntExist() throws Exception {
        assertNull(storeProvider.getStore("not-a-store", QueryableStoreTypes.keyValueStore()));
    }

    @Test
    public void shouldReturnNullIfWindowStoreDoesntExist() throws Exception {
        assertNull(storeProvider.getStore("not-a-store", QueryableStoreTypes.windowStore()));
    }

    @Test
    public void shouldReturnKVStoreWhenItExists() throws Exception {
        assertNotNull(storeProvider.getStore(keyValueStore, QueryableStoreTypes.keyValueStore()));
    }

    @Test
    public void shouldReturnWindowStoreWhenItExists() throws Exception {
        assertNotNull(storeProvider.getStore(windowStore, QueryableStoreTypes.windowStore()));
    }

    @Test
    public void shouldNotReturnKVStoreWhenIsWindowStore() throws Exception {
        assertNull(storeProvider.getStore(windowStore, QueryableStoreTypes.keyValueStore()));
    }

    @Test
    public void shouldNotReturnWindowStoreWhenIsKVStore() throws Exception {
        assertNull(storeProvider.getStore(keyValueStore, QueryableStoreTypes.windowStore()));
    }


    class NoOpReadOnlyStore<K, V>
        implements ReadOnlyKeyValueStore<K, V>, StateStore {

        @Override
        public V get(final K key) {
            return null;
        }

        @Override
        public KeyValueIterator<K, V> range(final K from, final K to) {
            return null;
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return null;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public void init(final ProcessorContext context, final StateStore root) {

        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public boolean isOpen() {
            return false;
        }
    }

}