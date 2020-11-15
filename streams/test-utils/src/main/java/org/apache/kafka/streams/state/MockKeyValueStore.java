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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockKeyValueStore<K, V>
        extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, K, V>
        implements KeyValueStore<K, V>  {
    // keep a global counter of flushes and a local reference to which store had which
    // flush, so we can reason about the order in which stores get flushed.
    private static final AtomicInteger GLOBAL_FLUSH_COUNTER = new AtomicInteger(0);
    private final AtomicInteger instanceLastFlushCount = new AtomicInteger(-1);


    public boolean initialized = false;
    public boolean flushed = false;
    public boolean closed = true;

    public String name;
    public boolean persistent;

    protected final Time time;


    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<K, V> serdes;

    public final List<KeyValue<K, V>> capturedPutCalls = new LinkedList<>();
    public final List<KeyValue<K, V>> capturedGetCalls = new LinkedList<>();
    public final List<KeyValue<K, V>> capturedDeleteCalls = new LinkedList<>();


    public MockKeyValueStore(final KeyValueBytesStoreSupplier keyValueBytesStoreSupplier,
                             final Serde<K> keySerde,
                             final Serde<V> valueSerde,
                             final boolean persistent,
                             final Time time) {
        super(keyValueBytesStoreSupplier.get());
        this.name = keyValueBytesStoreSupplier.name();
        this.time = time != null ? time : Time.SYSTEM;
        this.persistent = persistent;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @SuppressWarnings("unchecked")
    void initStoreSerde(final ProcessorContext context) {
        serdes = new StateSerdes<>(
                ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        context.register(root, stateRestoreCallback);
        initialized = true;
        closed = false;
    }

    @Override
    public void flush() {
        instanceLastFlushCount.set(GLOBAL_FLUSH_COUNTER.getAndIncrement());
        wrapped().flush();
        flushed = true;
    }

    public int getLastFlushCount() {
        return instanceLastFlushCount.get();
    }

    @Override
    public void close() {
        wrapped().close();
        closed = true;
    }

    @Override
    public boolean persistent() {
        return persistent;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    public final StateRestoreCallback stateRestoreCallback = new StateRestoreCallback() {

        @Override
        public void restore(final byte[] key,
                            final byte[] value) {
        }
    };

    @Override
    public void put(final K key, final V value) {
        capturedPutCalls.add(new KeyValue<>(key, value));
        wrapped().put(keyBytes(key), serdes.rawValue(value));
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        final V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
            capturedPutCalls.add(new KeyValue<>(key, value));
        }
        return originalValue;
    }

    @Override
    public V delete(final K key) {
        V value = outerValue(wrapped().delete(keyBytes(key)));
        capturedDeleteCalls.add(new KeyValue<>(key, value));
        return value;
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        for (final KeyValue<K, V> entry : entries) {
            put(entry.key, entry.value);
            capturedPutCalls.add(entry);
        }
    }

    @Override
    public V get(final K key) {
        V value = outerValue(wrapped().get(keyBytes(key)));
        capturedGetCalls.add(new KeyValue<>(key, value));
        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyValueIterator<K,V> range(final K from, final K to) {
        return new MockKeyValueStore.MockKeyValueIterator(
                wrapped().range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))));
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyValueIterator<K,V>  all() {
        return new MockKeyValueStore.MockKeyValueIterator(wrapped().all());
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    private V outerValue(final byte[] value) {
        return value != null ? serdes.valueFrom(value) : null;
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    private class MockKeyValueIterator implements KeyValueIterator<K, V> {

        private final KeyValueIterator<Bytes, byte[]> iter;

        private MockKeyValueIterator(final KeyValueIterator<Bytes, byte[]> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(
                    serdes.keyFrom(keyValue.key.get()),
                    outerValue(keyValue.value));
        }

        @Override
        public void close() {
            iter.close();
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}
