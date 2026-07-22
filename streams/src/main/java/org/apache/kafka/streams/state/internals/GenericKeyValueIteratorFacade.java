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

import java.util.function.Function;

/**
 * Generic iterator facade that wraps a {@link KeyValueIterator} and converts values
 * using a provided converter function.
 *
 * @param <K> key type
 * @param <InV> input value type (from inner iterator)
 * @param <OutV> output value type (exposed by this facade)
 */
class GenericKeyValueIteratorFacade<K, InV, OutV> implements KeyValueIterator<K, OutV> {
    private final KeyValueIterator<K, InV> innerIterator;
    private final Function<InV, OutV> valueConverter;

    GenericKeyValueIteratorFacade(final KeyValueIterator<K, InV> innerIterator,
                                  final Function<InV, OutV> valueConverter) {
        this.innerIterator = innerIterator;
        this.valueConverter = valueConverter;
    }

    @Override
    public void close() {
        innerIterator.close();
    }

    @Override
    public K peekNextKey() {
        return innerIterator.peekNextKey();
    }

    @Override
    public boolean hasNext() {
        return innerIterator.hasNext();
    }

    @Override
    public KeyValue<K, OutV> next() {
        final KeyValue<K, InV> innerKeyValue = innerIterator.next();
        return KeyValue.pair(innerKeyValue.key, valueConverter.apply(innerKeyValue.value));
    }
}