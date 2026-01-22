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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

public class MeteredTimestampedWindowStoreWithHeaders<K, V>
    extends MeteredWindowStore<K, ValueTimestampHeaders<V>>
    implements TimestampedWindowStoreWithHeaders<K, V> {

    MeteredTimestampedWindowStoreWithHeaders(final WindowStore<Bytes, byte[]> inner,
                                             final long windowSizeMs,
                                             final String metricScope,
                                             final Time time,
                                             final Serde<K> keySerde,
                                             final Serde<ValueTimestampHeaders<V>> valueSerde) {
        super(inner, windowSizeMs, metricScope, time, keySerde, valueSerde);
    }

    @Override
    public void put(K key, V value, long windowStartTimestamp, long timestamp, Headers headers) {
        final ValueTimestampHeaders<V> valueWithHeaders = ValueTimestampHeaders.make(value, timestamp, headers);
        super.put(key, valueWithHeaders, windowStartTimestamp);
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> fetchWithHeaders(K key, long timeFrom, long timeTo) {
        return fetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetchWithHeaders(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        return fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetchAllWithHeaders(long timeFrom, long timeTo) {
        return fetchAll(timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> backwardFetchWithHeaders(K key, long timeFrom, long timeTo) {
        return backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetchWithHeaders(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        return backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetchAllWithHeaders(long timeFrom, long timeTo) {
        return backwardFetchAll(timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> fetchWithHeaders(K key, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return fetch(key, timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<V>> backwardFetchWithHeaders(K key, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetchWithHeaders(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetchWithHeaders(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> fetchAllWithHeaders(Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return fetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> backwardFetchAllWithHeaders(Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return backwardFetchAll(timeFrom, timeTo);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Serde<ValueTimestampHeaders<V>> prepareValueSerde(final Serde<ValueTimestampHeaders<V>> valueSerde,
                                                                final SerdeGetter getter) {
        if (valueSerde == null) {
            return new ValueTimestampHeadersSerde<>((Serde<V>) getter.valueSerde());
        } else {
            return super.prepareValueSerde(valueSerde, getter);
        }
    }
}
