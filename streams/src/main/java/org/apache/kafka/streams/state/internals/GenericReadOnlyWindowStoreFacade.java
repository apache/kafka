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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.function.Function;

/**
 * Generic facade that wraps a {@link ReadOnlyWindowStore} and converts values
 * using a provided converter function.
 *
 * @param <K> key type
 * @param <InV> input value type (from inner store)
 * @param <OutV> output value type (exposed by this facade)
 */
public class GenericReadOnlyWindowStoreFacade<K, InV, OutV> implements ReadOnlyWindowStore<K, OutV> {
    private final ReadOnlyWindowStore<K, InV> inner;
    private final Function<InV, OutV> valueConverter;

    public GenericReadOnlyWindowStoreFacade(final ReadOnlyWindowStore<K, InV> inner,
                                            final Function<InV, OutV> valueConverter) {
        this.inner = inner;
        this.valueConverter = valueConverter;
    }

    @Override
    public OutV fetch(final K key, final long time) {
        return valueConverter.apply(inner.fetch(key, time));
    }

    @Override
    public WindowStoreIterator<OutV> fetch(final K key,
                                           final Instant timeFrom,
                                           final Instant timeTo) throws IllegalArgumentException {
        return new GenericWindowStoreIteratorFacade<>(inner.fetch(key, timeFrom, timeTo), valueConverter);
    }

    @Override
    public WindowStoreIterator<OutV> backwardFetch(final K key,
                                                   final Instant timeFrom,
                                                   final Instant timeTo) throws IllegalArgumentException {
        return new GenericWindowStoreIteratorFacade<>(inner.backwardFetch(key, timeFrom, timeTo), valueConverter);
    }

    @Override
    public KeyValueIterator<Windowed<K>, OutV> fetch(final K keyFrom,
                                                     final K keyTo,
                                                     final Instant timeFrom,
                                                     final Instant timeTo) throws IllegalArgumentException {
        return new GenericKeyValueIteratorFacade<>(inner.fetch(keyFrom, keyTo, timeFrom, timeTo), valueConverter);
    }

    @Override
    public KeyValueIterator<Windowed<K>, OutV> backwardFetch(final K keyFrom,
                                                             final K keyTo,
                                                             final Instant timeFrom,
                                                             final Instant timeTo) throws IllegalArgumentException {
        return new GenericKeyValueIteratorFacade<>(inner.backwardFetch(keyFrom, keyTo, timeFrom, timeTo), valueConverter);
    }

    @Override
    public KeyValueIterator<Windowed<K>, OutV> fetchAll(final Instant timeFrom,
                                                        final Instant timeTo) throws IllegalArgumentException {
        return new GenericKeyValueIteratorFacade<>(inner.fetchAll(timeFrom, timeTo), valueConverter);
    }

    @Override
    public KeyValueIterator<Windowed<K>, OutV> backwardFetchAll(final Instant timeFrom,
                                                                final Instant timeTo) throws IllegalArgumentException {
        return new GenericKeyValueIteratorFacade<>(inner.backwardFetchAll(timeFrom, timeTo), valueConverter);
    }

    @Override
    public KeyValueIterator<Windowed<K>, OutV> all() {
        return new GenericKeyValueIteratorFacade<>(inner.all(), valueConverter);
    }

    @Override
    public KeyValueIterator<Windowed<K>, OutV> backwardAll() {
        return new GenericKeyValueIteratorFacade<>(inner.backwardAll(), valueConverter);
    }
}