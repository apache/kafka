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

import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

/* A generic wrapper around the key-value bytes stores for use in simple tests where only a basic store is required */
public class KeyValueBytesStoreWrapper<K, V> implements KeyValueStore<K, V> {

    private final KeyValueStore<Bytes, byte[]> wrapped;
    private final StateSerdes<K, V> serdes;

    public KeyValueBytesStoreWrapper(final KeyValueStore<Bytes, byte[]> wrapped,
        final Serde<K> keySerde,
        final Serde<V> valueSerde) {
        this.wrapped = wrapped;
        serdes = new StateSerdes<>("serdes-topic-name", keySerde, valueSerde);
    }

    public void put(final K key, final V value) {
        wrapped.put(Bytes.wrap(serdes.rawKey(key)), serdes.rawValue(value));
    }

    public V putIfAbsent(final K key, final V value) {
        return serdes.valueFrom(wrapped.putIfAbsent(Bytes.wrap(serdes.rawKey(key)), serdes.rawValue(value)));
    }

    public void putAll(final List<KeyValue<K, V>> entries) {
        for (final KeyValue<K, V> entry : entries) {
            wrapped.put(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value));
        }
    }

    public V delete(final K key) {
        return serdes.valueFrom(wrapped.delete(Bytes.wrap(serdes.rawKey(key))));
    }

    public V get(final K key) {
        return serdes.valueFrom(wrapped.get(Bytes.wrap(serdes.rawKey(key))));
    }

    public KeyValueIterator<K, V> range(final K from, final K to) {
        return new BytesIteratorWrapper(wrapped.range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))));
    }

    public KeyValueIterator<K, V> all() {
        return new BytesIteratorWrapper(wrapped.all());
    }

    public void init(final ProcessorContext context, final StateStore root) {
        wrapped.init(context, root);
    }

    public String name() {
        return wrapped.name();
    }

    public void flush() {
        wrapped.flush();
    }

    public void close() {
        wrapped.close();
    }

    public boolean isOpen() {
        return wrapped.isOpen();
    }

    public boolean persistent() {
        return wrapped.persistent();
    }

    public long approximateNumEntries() {
        return wrapped.approximateNumEntries();
    }

    private class BytesIteratorWrapper implements KeyValueIterator<K, V> {
        private final KeyValueIterator<Bytes, byte[]> underlying;

        BytesIteratorWrapper(final KeyValueIterator<Bytes, byte[]> underlying) {
            this.underlying = underlying;
        }

        public boolean hasNext() {
            return underlying.hasNext();
        }

        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> next = underlying.next();
            return new KeyValue<>(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value));
        }

        public void close() {
            underlying.close();
        }

        public K peekNextKey() {
            return serdes.keyFrom(underlying.peekNextKey().get());
        }
    }
}
