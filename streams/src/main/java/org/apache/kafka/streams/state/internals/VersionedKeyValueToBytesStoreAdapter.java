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

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * Adapts from {@link VersionedKeyValueStore} (user-friendly versioned store interface) to
 * {@link KeyValueStore}. By representing a {@code VersionedKeyValueStore} as a
 * {@code KeyValueStore}, this allows reuse of existing {@link StreamsBuilder} and {@link KTable}
 * method interfaces which accept {@code Materialized<K, V, KeyValueStore<Bytes, byte[]>)}
 * for versioned key-value stores.
 */
public class VersionedKeyValueToBytesStoreAdapter implements VersionedBytesStore {
    private static final Serde<ValueAndTimestamp<byte[]>> VALUE_AND_TIMESTAMP_SERDE
        = new ValueAndTimestampSerde<>(new ByteArraySerde());
    private static final Serializer<ValueAndTimestamp<byte[]>> VALUE_AND_TIMESTAMP_SERIALIZER
        = VALUE_AND_TIMESTAMP_SERDE.serializer();

    final VersionedKeyValueStore<Bytes, byte[]> inner;

    public VersionedKeyValueToBytesStoreAdapter(final VersionedKeyValueStore<Bytes, byte[]> inner) {
        this.inner = Objects.requireNonNull(inner);
    }

    @Override
    public long put(final Bytes key, final byte[] value, final long timestamp) {
        return inner.put(key, value, timestamp);
    }

    @Override
    public byte[] get(final Bytes key) {
        final VersionedRecord<byte[]> versionedRecord = inner.get(key);
        return serializeAsBytes(versionedRecord);
    }

    @Override
    public byte[] get(final Bytes key, final long asOfTimestamp) {
        final VersionedRecord<byte[]> versionedRecord = inner.get(key, asOfTimestamp);
        return serializeAsBytes(versionedRecord);
    }

    @Override
    public byte[] delete(final Bytes key, final long timestamp) {
        final VersionedRecord<byte[]> versionedRecord = inner.delete(key, timestamp);
        return serializeAsBytes(versionedRecord);
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void flush() {
        inner.flush();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        return inner.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return inner.getPosition();
    }

    @Override
    public void put(final Bytes key, final byte[] rawValueAndTimestamp) {
        throw new UnsupportedOperationException("Versioned key-value stores should use put(key, value, timestamp) instead");
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support putIfAbsent(key, value)");
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support putAll(entries)");
    }

    @Override
    public byte[] delete(final Bytes key) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support delete(key). Use delete(key, timestamp) instead.");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support range(from, to)");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support reverseRange(from, to)");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException("Versioned key-value stores do not support all()");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        throw new UnsupportedOperationException("Versioned key-value stores do not support reverseAll()");
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support prefixScan(prefix, prefixKeySerializer)");
    }

    @Override
    public long approximateNumEntries() {
        throw new UnsupportedOperationException("Versioned key-value stores do not support approximateNumEntries()");
    }

    private static byte[] serializeAsBytes(final VersionedRecord<byte[]> versionedRecord) {
        if (versionedRecord == null) {
            return null;
        }
        return VALUE_AND_TIMESTAMP_SERIALIZER.serialize(
            null,
            ValueAndTimestamp.make(versionedRecord.value(), versionedRecord.timestamp()));
    }
}