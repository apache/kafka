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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TimeOrderedBufferIteratorFacade<K, V> implements KeyValueIterator<K, V> {
    private final KeyValueIterator<Bytes, byte[]> innerIterator;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;

    public TimeOrderedBufferIteratorFacade(final KeyValueIterator<Bytes, byte[]> iterator, final Serde<K> keySerde, final Serde<V> valueSerde) {
        innerIterator = iterator;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public boolean hasNext() {
        return innerIterator.hasNext();
    }

    @Override
    public K peekNextKey() {
        return keySerde.deserializer().deserialize(null, innerIterator.peekNextKey().get());
    }

    @Override
    public KeyValue<K, V> next() {
        final KeyValue<Bytes, byte[]> innerKeyValue = innerIterator.next();
        final K key = keySerde.deserializer().deserialize(null, innerKeyValue.key.get());
        final V value = valueSerde.deserializer().deserialize(null, innerKeyValue.value);
        return KeyValue.pair(key, value);
    }

    @Override
    public void close() {
        innerIterator.close();
    }
}