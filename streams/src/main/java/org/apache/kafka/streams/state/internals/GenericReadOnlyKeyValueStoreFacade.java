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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.function.Function;

/**
 * Generic facade that wraps a {@link ReadOnlyKeyValueStore} and converts values
 * using a provided converter function.
 *
 * @param <K> key type
 * @param <InV> input value type (from inner store)
 * @param <OutV> output value type (exposed by this facade)
 */
public class GenericReadOnlyKeyValueStoreFacade<K, InV, OutV> implements ReadOnlyKeyValueStore<K, OutV> {
    private final ReadOnlyKeyValueStore<K, InV> inner;
    private final Function<InV, OutV> valueConverter;

    public GenericReadOnlyKeyValueStoreFacade(final ReadOnlyKeyValueStore<K, InV> inner,
                                              final Function<InV, OutV> valueConverter) {
        this.inner = inner;
        this.valueConverter = valueConverter;
    }

    @Override
    public OutV get(final K key) {
        return valueConverter.apply(inner.get(key));
    }

    @Override
    public KeyValueIterator<K, OutV> range(final K from, final K to) {
        return new GenericKeyValueIteratorFacade<>(inner.range(from, to), valueConverter);
    }

    @Override
    public KeyValueIterator<K, OutV> reverseRange(final K from, final K to) {
        return new GenericKeyValueIteratorFacade<>(inner.reverseRange(from, to), valueConverter);
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, OutV> prefixScan(final P prefix,
                                                                               final PS prefixKeySerializer) {
        return new GenericKeyValueIteratorFacade<>(inner.prefixScan(prefix, prefixKeySerializer), valueConverter);
    }

    @Override
    public KeyValueIterator<K, OutV> all() {
        return new GenericKeyValueIteratorFacade<>(inner.all(), valueConverter);
    }

    @Override
    public KeyValueIterator<K, OutV> reverseAll() {
        return new GenericKeyValueIteratorFacade<>(inner.reverseAll(), valueConverter);
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }
}