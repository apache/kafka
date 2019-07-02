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
package org.apache.kafka.test;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.internals.StateStoreClosedException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.DelegatingPeekingKeyValueIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ReadOnlyKeyValueStoreStub<K, V> implements ReadOnlyKeyValueStore<K, V>, StateStore {
    private final String name;
    private Map<K, V> map = new HashMap<>();
    private boolean open = true;

    public ReadOnlyKeyValueStoreStub(final String name) {
        this.name = name;
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new StateStoreClosedException("not open");
        }
    }
    @Override
    public V get(K key) {
        validateStoreOpen();
        return map.get(key);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        validateStoreOpen();
        return null;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        validateStoreOpen();
        final KeyValueIterator<K, V> underlying = new KeyValueIteratorStub(name, map.entrySet().iterator());
        return new DelegatingPeekingKeyValueIterator<>(name, underlying);
    }

    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        return map.size();
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }



    private class KeyValueIteratorStub implements KeyValueIterator<K, V> {
        private final Iterator<Map.Entry<K, V>> iter;
        private final String storeName;
        private volatile boolean open = true;

        KeyValueIteratorStub(final String storeName, final Iterator<Map.Entry<K, V>> iter) {
            this.storeName = storeName;
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            if (!open) {
                throw new StateStoreClosedException(String.format("RocksDB iterator for store %s has closed", storeName));
            }
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final Map.Entry<K, V> entry = iter.next();
            return new KeyValue<>(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public K peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }

}
