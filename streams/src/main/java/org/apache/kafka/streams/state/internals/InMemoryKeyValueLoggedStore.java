/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;

public class InMemoryKeyValueLoggedStore<K, V> implements KeyValueStore<K, V> {

    private final KeyValueStore<K, V> inner;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String storeName;

    private StateSerdes<K, V> serdes;
    private StoreChangeLogger<K, V> changeLogger;
    private StoreChangeLogger.ValueGetter<K, V> getter;

    public InMemoryKeyValueLoggedStore(final String storeName, final KeyValueStore<K, V> inner, Serde<K> keySerde, Serde<V> valueSerde) {
        this.storeName = storeName;
        this.inner = inner;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public String name() {
        return this.storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        // construct the serde
        this.serdes = new StateSerdes<>(storeName,
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.changeLogger = new StoreChangeLogger<>(storeName, context, serdes);

        context.register(root, true, new StateRestoreCallback() {
            @Override
            public void restore(byte[] key, byte[] value) {

                // directly call inner functions so that the operation is not logged
                inner.put(serdes.keyFrom(key), serdes.valueFrom(value));
            }
        });

        inner.init(context, root);

        this.getter = new StoreChangeLogger.ValueGetter<K, V>() {
            @Override
            public V get(K key) {
                return inner.get(key);
            }
        };
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public V get(K key) {
        return this.inner.get(key);
    }

    @Override
    public void put(K key, V value) {
        this.inner.put(key, value);

        changeLogger.add(key);
        changeLogger.maybeLogChange(this.getter);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V originalValue = this.inner.putIfAbsent(key, value);
        if (originalValue == null) {
            changeLogger.add(key);
            changeLogger.maybeLogChange(this.getter);
        }
        return originalValue;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        this.inner.putAll(entries);

        for (KeyValue<K, V> entry : entries) {
            K key = entry.key;
            changeLogger.add(key);
        }
        changeLogger.maybeLogChange(this.getter);
    }

    @Override
    public V delete(K key) {
        V value = this.inner.delete(key);

        removed(key);

        return value;
    }

    /**
     * Called when the underlying {@link #inner} {@link KeyValueStore} removes an entry in response to a call from this
     * store.
     *
     * @param key the key for the entry that the inner store removed
     */
    protected void removed(K key) {
        changeLogger.delete(key);
        changeLogger.maybeLogChange(this.getter);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return this.inner.range(from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return this.inner.all();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public void flush() {
        this.inner.flush();

        changeLogger.logChange(getter);
    }
}
