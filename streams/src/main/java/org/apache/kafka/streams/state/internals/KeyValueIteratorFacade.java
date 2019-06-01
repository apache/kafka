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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KeyValueIteratorFacade<K, V> implements KeyValueIterator<K, V> {
    private final KeyValueIterator<K, ValueAndTimestamp<V>> innerIterator;

    public KeyValueIteratorFacade(final KeyValueIterator<K, ValueAndTimestamp<V>> iterator) {
        innerIterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return innerIterator.hasNext();
    }

    @Override
    public K peekNextKey() {
        return innerIterator.peekNextKey();
    }

    @Override
    public KeyValue<K, V> next() {
        final KeyValue<K, ValueAndTimestamp<V>> innerKeyValue = innerIterator.next();
        return KeyValue.pair(innerKeyValue.key, getValueOrNull(innerKeyValue.value));
    }

    @Override
    public void close() {
        innerIterator.close();
    }
}