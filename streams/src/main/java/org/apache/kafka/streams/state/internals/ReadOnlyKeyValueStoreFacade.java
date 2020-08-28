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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class ReadOnlyKeyValueStoreFacade<K, V> implements ReadOnlyKeyValueStore<K, V> {
    protected final TimestampedKeyValueStore<K, V> inner;

    protected ReadOnlyKeyValueStoreFacade(final TimestampedKeyValueStore<K, V> store) {
        inner = store;
    }

    @Override
    public V get(final K key) {
        return getValueOrNull(inner.get(key));
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        return new KeyValueIteratorFacade<>(inner.range(from, to));
    }

    @Override
    public KeyValueIterator<K, V> reverseRange(final K from,
                                               final K to) {
        return new KeyValueIteratorFacade<>(inner.reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new KeyValueIteratorFacade<>(inner.all());
    }

    @Override
    public KeyValueIterator<K, V> reverseAll() {
        return new KeyValueIteratorFacade<>(inner.reverseAll());
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }
}