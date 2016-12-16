/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.NoSuchElementException;

/**
 * Provides a read-only view over the join between two KTables
 */
class KeyValueStoreJoinView<K, V, K1, V1, R> implements ReadOnlyKeyValueStore<K, R>, StateStore {
    private final String name;
    private final ReadOnlyKeyValueStore<K, V> storeOne;
    private final KTableValueGetter<K1, V1> valueGetter;
    private final KeyValueMapper<K, V, K1> keyMapper;
    private final ValueJoiner<V, V1, R> joiner;
    private final boolean leftJoin;

    private volatile boolean open = false;


    KeyValueStoreJoinView(final String name,
                          final ReadOnlyKeyValueStore<K, V> storeOne,
                          final KTableValueGetter<K1, V1> valueGetter,
                          final KeyValueMapper<K, V, K1> keyMapper,
                          final ValueJoiner<V, V1, R> joiner,
                          final boolean leftJoin) {
        this.name = name;
        this.storeOne = storeOne;
        this.valueGetter = valueGetter;
        this.keyMapper = keyMapper;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        valueGetter.init(context);
        context.register(root, false, null);
        open = true;
    }

    @Override
    public void flush() {
        //no-op
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

    @Override
    public R get(final K key) {
        validateStoreOpen();
        if (key == null) {
            return null;
        }
        return doJoin(key, storeOne.get(key));
    }

    private R doJoin(final K key, final V value) {
        if (value != null) {
            V1 right = valueGetter.get(keyMapper.apply(key, value));

            if (leftJoin || right != null) {
                return joiner.apply(value, right);
            }
        }
        return null;
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + this.name + " is currently closed");
        }
    }

    @Override
    public KeyValueIterator<K, R> range(final K from, final K to) {
        validateStoreOpen();
        return new KeyValueJoinIterator(storeOne.range(from, to));
    }

    @Override
    public KeyValueIterator<K, R> all() {
        validateStoreOpen();
        return new KeyValueJoinIterator(storeOne.all());
    }

    @Override
    public long approximateNumEntries() {
        return storeOne.approximateNumEntries();
    }

    private class KeyValueJoinIterator implements KeyValueIterator<K, R> {
        private final KeyValueIterator<K, V> iterator;
        private KeyValue<K, R> next;

        KeyValueJoinIterator(final KeyValueIterator<K, V> iterator) {
            this.iterator = iterator;
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            while (iterator.hasNext() && next == null) {
                final KeyValue<K, V> kv = iterator.next();
                final R result = doJoin(kv.key, kv.value);
                if (result != null) {
                    next = KeyValue.pair(kv.key, result);
                }
            }
            return next != null;
        }

        @Override
        public KeyValue<K, R> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<K, R> result = next;
            next = null;
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
