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
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.function.Function;

/**
 * Delegates to an inner {@link KeyValueIterator} and maps each value byte array
 * through the given function (e.g. header-format conversion).
 */
class MappingKeyValueIteratorAdapter<K> implements KeyValueIterator<K, byte[]> {

    private final KeyValueIterator<K, byte[]> innerIterator;
    private final Function<byte[], byte[]> valueMapper;

    MappingKeyValueIteratorAdapter(
        final KeyValueIterator<K, byte[]> innerIterator,
        final Function<byte[], byte[]> valueMapper
    ) {
        this.innerIterator = innerIterator;
        this.valueMapper = valueMapper;
    }

    /**
     * Ensures backward compatibility between {@link TimestampedKeyValueStoreWithHeaders}
     * and plain {@link KeyValueStore}: values are wrapped with empty headers
     * and timestamp {@code -1}.
     *
     * @see PlainToHeadersStoreAdapter
     * @see PlainToHeadersWindowStoreAdapter
     */
    static <K> KeyValueIterator<K, byte[]> plainToHeaders(final KeyValueIterator<K, byte[]> inner) {
        return new MappingKeyValueIteratorAdapter<>(inner, HeadersBytesStore::convertFromPlainToHeaderFormat);
    }

    /**
     * Ensures backward compatibility between {@link TimestampedKeyValueStoreWithHeaders}
     * and {@link TimestampedKeyValueStore}, and between {@link SessionStoreWithHeaders}
     * and {@link SessionStore}: both read paths only need empty headers prepended.
     *
     * @see TimestampedToHeadersStoreAdapter
     * @see TimestampedToHeadersWindowStoreAdapter
     * @see SessionToHeadersStoreAdapter
     */
    static <K> KeyValueIterator<K, byte[]> timestampedToHeaders(final KeyValueIterator<K, byte[]> inner) {
        return new MappingKeyValueIteratorAdapter<>(inner, HeadersBytesStore::convertToHeaderFormat);
    }

    /**
     * Window-store variant of {@link #plainToHeaders}: converts plain values to the
     * timestamp-with-headers format while preserving the {@link WindowStoreIterator}
     * marker on the return type.
     *
     * @see PlainToHeadersWindowStoreAdapter
     */
    static WindowStoreIterator<byte[]> plainToHeadersWindow(final KeyValueIterator<Long, byte[]> inner) {
        return new WindowStoreIteratorAdapter(inner, HeadersBytesStore::convertFromPlainToHeaderFormat);
    }

    /**
     * Window-store variant of {@link #timestampedToHeaders}: adds empty headers to
     * timestamp-only values while preserving the {@link WindowStoreIterator} marker
     * on the return type.
     *
     * @see TimestampedToHeadersWindowStoreAdapter
     */
    static WindowStoreIterator<byte[]> timestampedToHeadersWindow(final KeyValueIterator<Long, byte[]> inner) {
        return new WindowStoreIteratorAdapter(inner, HeadersBytesStore::convertToHeaderFormat);
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
    public KeyValue<K, byte[]> next() {
        final KeyValue<K, byte[]> keyValue = innerIterator.next();
        if (keyValue == null) {
            return null;
        }
        return KeyValue.pair(keyValue.key, valueMapper.apply(keyValue.value));
    }

    /**
     * Carries the {@link WindowStoreIterator} marker on top of the shared mapping
     * behavior, so window-store adapters can return values in the header format.
     */
    private static final class WindowStoreIteratorAdapter
        extends MappingKeyValueIteratorAdapter<Long>
        implements WindowStoreIterator<byte[]> {

        WindowStoreIteratorAdapter(final KeyValueIterator<Long, byte[]> inner,
                                   final Function<byte[], byte[]> valueMapper) {
            super(inner, valueMapper);
        }
    }
}
