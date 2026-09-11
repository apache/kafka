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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A persistent, versioned key-value store based on RocksDB that additionally
 * preserves record headers.
 * <p>
 * Headers are embedded into the value bytes using the format:
 * {@code [headersSize(varint)][headersBytes][rawValue]}
 * before delegating to the parent {@link RocksDBVersionedStore}. On reads,
 * headers are extracted from the stored value bytes.
 */
public class RocksDBVersionedStoreWithHeaders
        extends RocksDBVersionedStore
        implements VersionedKeyValueStoreWithHeaders<Bytes, byte[]> {

    RocksDBVersionedStoreWithHeaders(final String name,
                                     final String metricsScope,
                                     final long historyRetention,
                                     final long segmentInterval) {
        super(name, metricsScope, historyRetention, segmentInterval);
    }

    @Override
    public long put(final Bytes key, final byte[] value, final long timestamp, final Headers headers) {
        Objects.requireNonNull(headers, "headers cannot be null");
        return super.put(key, encodeValue(value, headers), timestamp);
    }

    @Override
    public long put(final Bytes key, final byte[] value, final long timestamp) {
        // non-headers put: embed empty headers
        return put(key, value, timestamp, new RecordHeaders());
    }

    @Override
    public VersionedRecord<byte[]> get(final Bytes key) {
        final VersionedRecord<byte[]> record = super.get(key);
        return decodeRecord(record);
    }

    @Override
    public VersionedRecord<byte[]> get(final Bytes key, final long asOfTimestamp) {
        final VersionedRecord<byte[]> record = super.get(key, asOfTimestamp);
        return decodeRecord(record);
    }

    @Override
    public VersionedRecord<byte[]> delete(final Bytes key, final long timestamp) {
        final VersionedRecord<byte[]> record = super.delete(key, timestamp);
        return decodeRecord(record);
    }

    @Override
    VersionedRecordIterator<byte[]> get(final Bytes key, final long fromTimestamp, final long toTimestamp, final ResultOrder order) {
        return new DecodingVersionedRecordIterator(super.get(key, fromTimestamp, toTimestamp, order, IsolationLevel.READ_UNCOMMITTED));
    }

    @Override
    VersionedRecordIterator<byte[]> get(final Bytes key,
                                        final long fromTimestamp,
                                        final long toTimestamp,
                                        final ResultOrder order,
                                        final IsolationLevel level) {
        return new DecodingVersionedRecordIterator(super.get(key, fromTimestamp, toTimestamp, order, level));
    }

    @Override
    public VersionedKeyValueStoreWithHeaders<Bytes, byte[]> readOnly(final IsolationLevel isolationLevel) {
        final VersionedKeyValueStore<Bytes, byte[]> readOnly = super.readOnly(isolationLevel);
        if (readOnly == this) {
            return this;
        }
        return new ReadOnlyView(readOnly);
    }

    @Override
    void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        final List<ConsumerRecord<byte[], byte[]>> encodedRecords = new ArrayList<>(records.size());
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            encodedRecords.add(recordWithEncodedValue(record));
        }
        super.restoreBatch(encodedRecords);
    }

    private static ConsumerRecord<byte[], byte[]> recordWithEncodedValue(final ConsumerRecord<byte[], byte[]> record) {
        final byte[] encodedValue = encodeValue(record.value(), record.headers());
        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            record.serializedKeySize(),
            encodedValue == null ? ConsumerRecord.NULL_SIZE : encodedValue.length,
            record.key(),
            encodedValue,
            record.headers(),
            record.leaderEpoch()
        );
    }

    private static byte[] encodeValue(final byte[] value, final Headers headers) {
        if (value == null) {
            return null;
        }
        if (!headers.iterator().hasNext()) {
            return HeadersBytesStore.convertToHeaderFormat(value);
        }

        final HeadersSerializer.PreSerializedHeaders prep = HeadersSerializer.prepareSerialization(headers);
        final int payloadSize = prep.requiredBufferSizeForHeaders + value.length;
        final ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(prep.requiredBufferSizeForHeaders) + payloadSize);
        ByteUtils.writeVarint(prep.requiredBufferSizeForHeaders, buffer);
        HeadersSerializer.serialize(prep, buffer);
        buffer.put(value);
        return buffer.array();
    }

    private static VersionedRecord<byte[]> decodeRecord(final VersionedRecord<byte[]> record) {
        if (record == null) {
            return null;
        }
        final byte[] encodedValue = record.value();
        final Headers headers = Utils.headers(encodedValue);
        final byte[] rawValue = extractRawValue(encodedValue);
        if (record.validTo().isPresent()) {
            return new VersionedRecord<>(rawValue, record.timestamp(), record.validTo().get(), headers);
        } else {
            return new VersionedRecord<>(rawValue, record.timestamp(), headers);
        }
    }

    /**
     * Extract raw value from encoded value bytes, stripping the headers prefix.
     * Format: [headersSize(varint)][headersBytes][rawValue]
     */
    private static byte[] extractRawValue(final byte[] encodedValue) {
        if (encodedValue == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(encodedValue);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize);
        final byte[] rawValue = new byte[buffer.remaining()];
        buffer.get(rawValue);
        return rawValue;
    }

    private static final class DecodingVersionedRecordIterator implements VersionedRecordIterator<byte[]> {
        private final VersionedRecordIterator<byte[]> inner;

        private DecodingVersionedRecordIterator(final VersionedRecordIterator<byte[]> inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public VersionedRecord<byte[]> next() {
            return decodeRecord(inner.next());
        }
    }

    private static final class ReadOnlyView implements VersionedKeyValueStoreWithHeaders<Bytes, byte[]> {
        private final VersionedKeyValueStore<Bytes, byte[]> inner;

        private ReadOnlyView(final VersionedKeyValueStore<Bytes, byte[]> inner) {
            this.inner = inner;
        }

        @Override
        public VersionedRecord<byte[]> get(final Bytes key) {
            return decodeRecord(inner.get(key));
        }

        @Override
        public VersionedRecord<byte[]> get(final Bytes key, final long asOfTimestamp) {
            return decodeRecord(inner.get(key, asOfTimestamp));
        }

        @Override
        public long put(final Bytes key, final byte[] value, final long timestamp) {
            throw new UnsupportedOperationException("put not supported on a read-only view");
        }

        @Override
        public long put(final Bytes key, final byte[] value, final long timestamp, final Headers headers) {
            throw new UnsupportedOperationException("put not supported on a read-only view");
        }

        @Override
        public VersionedRecord<byte[]> delete(final Bytes key, final long timestamp) {
            throw new UnsupportedOperationException("delete not supported on a read-only view");
        }

        @Override
        public String name() {
            return inner.name();
        }

        @Override
        public void init(final StateStoreContext stateStoreContext, final StateStore root) {
            throw new UnsupportedOperationException("init not supported on a read-only view");
        }

        @Override
        public void close() { }

        @Override
        public boolean persistent() {
            return inner.persistent();
        }

        @Override
        public boolean isOpen() {
            return inner.isOpen();
        }
    }

}
