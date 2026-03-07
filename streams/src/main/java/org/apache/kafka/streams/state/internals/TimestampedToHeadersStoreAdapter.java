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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;

/**
 * This class is used to ensure backward compatibility at DSL level between
 * {@link TimestampedKeyValueStoreWithHeaders} and
 * {@link TimestampedKeyValueStore}.
 * <p>
 * If a user provides a supplier for {@code TimestampedKeyValueStore} (without headers) via
 * {@link Materialized#as(KeyValueBytesStoreSupplier)} when building
 * a {@code TimestampedKeyValueStoreWithHeaders}, this adapter is used to translate between
 * the timestamped {@code byte[]} format and the timestamped-with-headers {@code byte[]} format.
 *
 * @see TimestampedToHeadersIteratorAdapter
 */
@SuppressWarnings("unchecked")
public class TimestampedToHeadersStoreAdapter implements KeyValueStore<Bytes, byte[]> {
    final KeyValueStore<Bytes, byte[]> store;

    TimestampedToHeadersStoreAdapter(final KeyValueStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        if (!(store instanceof TimestampedBytesStore)) {
            throw new IllegalArgumentException("Provided store must be a timestamped store, but it is not.");
        }
        this.store = store;
    }

    /**
     * Extract raw timestamped value (timestamp + value) from serialized ValueTimestampHeaders.
     * This strips the headers portion but keeps timestamp and value intact.
     *
     * Format conversion:
     * Input:  [headersSize(varint)][headers][timestamp(8)][value]
     * Output: [timestamp(8)][value]
     */
    static byte[] rawTimestampedValue(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // Skip headers, keep timestamp + value
        buffer.position(buffer.position() + headersSize);

        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestampAndHeaders) {
        store.put(key, rawTimestampedValue(valueWithTimestampAndHeaders));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] valueWithTimestampAndHeaders) {
        return convertToHeaderFormat(store.putIfAbsent(
            key,
            rawTimestampedValue(valueWithTimestampAndHeaders)));
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueWithTimestampAndHeaders = entry.value;
            store.put(entry.key, rawTimestampedValue(valueWithTimestampAndHeaders));
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        return convertToHeaderFormat(store.delete(key));
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        store.init(stateStoreContext, root);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        store.commit(changelogOffsets);
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        throw new UnsupportedOperationException("Queries (IQv2) are not supported for timestamped key-value stores with headers yet.");
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }

    @Override
    public byte[] get(final Bytes key) {
        return convertToHeaderFormat(store.get(key));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        return new TimestampedToHeadersIteratorAdapter<>(store.range(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                        final Bytes to) {
        return new TimestampedToHeadersIteratorAdapter<>(store.reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return new TimestampedToHeadersIteratorAdapter<>(store.all());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return new TimestampedToHeadersIteratorAdapter<>(store.reverseAll());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        return new TimestampedToHeadersIteratorAdapter<>(store.prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }
}