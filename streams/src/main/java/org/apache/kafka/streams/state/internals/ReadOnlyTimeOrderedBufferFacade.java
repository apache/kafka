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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class ReadOnlyTimeOrderedBufferFacade<K, V> implements ReadOnlyKeyValueStore<K, V> {
    protected final TimeOrderedKeyValueBuffer<K, V> inner;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;

    protected ReadOnlyTimeOrderedBufferFacade(final TimeOrderedKeyValueBuffer<K, V> store, final Serde<K> keySerde, final Serde<V> valueSerde) {
        inner = store;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public V get(final K key) {
        return valueSerde.deserializer().deserialize(null, inner.get(Bytes.wrap(keySerde.serializer().serialize(null, key))));
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        final Bytes fromBytes = Bytes.wrap(keySerde.serializer().serialize(null, from));
        final Bytes toBytes = Bytes.wrap(keySerde.serializer().serialize(null, to));
        return new TimeOrderedBufferIteratorFacade<>(inner.range(fromBytes, toBytes), keySerde, valueSerde);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new TimeOrderedBufferIteratorFacade<>(inner.all(), keySerde, valueSerde);
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }
}