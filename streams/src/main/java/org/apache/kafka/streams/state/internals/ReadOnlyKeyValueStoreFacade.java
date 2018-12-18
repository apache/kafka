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
import org.apache.kafka.streams.state.KeyValueWithTimestampStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class ReadOnlyKeyValueStoreFacade<K, V> implements ReadOnlyKeyValueStore<K, V> {
    protected final KeyValueWithTimestampStore<K, V> inner;

    protected ReadOnlyKeyValueStoreFacade(final KeyValueWithTimestampStore<K, V> store) {
        inner = store;
    }

    @Override
    public V get(final K key) {
        final ValueAndTimestamp<V> valueAndTimestamp = inner.get(key);
        return valueAndTimestamp.value();
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        final KeyValueIterator<K, ValueAndTimestamp<V>> innerIterator = inner.range(from, to);
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        final KeyValueIterator<K, ValueAndTimestamp<V>> innerIterator = inner.all();
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }
}
