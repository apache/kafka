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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * This a PoC for 2CF solution
 * Implementation of TimestampedKeyValueStoreWithHeaders that wraps a bytes-level store
 * and handles encoding/decoding of ValueTimestampHeaders.
 */
class TimestampedKeyValueStoreWithHeadersImpl
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, Object, Object>
    implements TimestampedKeyValueStoreWithHeaders<Bytes, byte[]> {

    TimestampedKeyValueStoreWithHeadersImpl(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        wrapped().init(context, root);
    }

    @Override
    public void put(final Bytes key, final ValueTimestampHeaders<byte[]> value) {
        if (value == null) {
            wrapped().put(key, null);
        } else {
            final byte[] encoded = encodeValueWithTimestampAndHeaders(
                value.value(),
                value.timestamp(),
                value.headers()
            );
            wrapped().put(key, encoded);
        }
    }

    @Override
    public ValueTimestampHeaders<byte[]> putIfAbsent(final Bytes key, final ValueTimestampHeaders<byte[]> value) {
        final byte[] encoded = encodeValueWithTimestampAndHeaders(
            value.value(),
            value.timestamp(),
            value.headers()
        );
        final byte[] previousEncoded = wrapped().putIfAbsent(key, encoded);
        return decodeValueWithTimestampAndHeaders(previousEncoded);
    }

    @Override
    public void putAll(final java.util.List<KeyValue<Bytes, ValueTimestampHeaders<byte[]>>> entries) {
        final java.util.List<KeyValue<Bytes, byte[]>> encodedEntries = new java.util.ArrayList<>(entries.size());
        for (final KeyValue<Bytes, ValueTimestampHeaders<byte[]>> entry : entries) {
            final byte[] encoded = encodeValueWithTimestampAndHeaders(
                entry.value.value(),
                entry.value.timestamp(),
                entry.value.headers()
            );
            encodedEntries.add(KeyValue.pair(entry.key, encoded));
        }
        wrapped().putAll(encodedEntries);
    }

    @Override
    public ValueTimestampHeaders<byte[]> delete(final Bytes key) {
        final byte[] encoded = wrapped().delete(key);
        return decodeValueWithTimestampAndHeaders(encoded);
    }

    @Override
    public ValueTimestampHeaders<byte[]> get(final Bytes key) {
        final byte[] encoded = wrapped().get(key);
        return decodeValueWithTimestampAndHeaders(encoded);
    }

    @Override
    public KeyValueIterator<Bytes, ValueTimestampHeaders<byte[]>> range(final Bytes from, final Bytes to) {
        return new KeyValueIteratorHeadersWrapper(wrapped().range(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, ValueTimestampHeaders<byte[]>> reverseRange(final Bytes from, final Bytes to) {
        return new KeyValueIteratorHeadersWrapper(wrapped().reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, ValueTimestampHeaders<byte[]>> all() {
        return new KeyValueIteratorHeadersWrapper(wrapped().all());
    }

    @Override
    public KeyValueIterator<Bytes, ValueTimestampHeaders<byte[]>> reverseAll() {
        return new KeyValueIteratorHeadersWrapper(wrapped().reverseAll());
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    /**
     * Encodes value, timestamp, and headers into a single byte array.
     * Format: [HeaderSize(2)][Headers][Timestamp(8)][Payload]
     */
    private byte[] encodeValueWithTimestampAndHeaders(final byte[] value,
                                                       final long timestamp,
                                                       final Headers headers) {
        if (value == null) {
            return null;
        }

        final byte[] serializedHeaders = serializeHeaders(headers);
        final int headerSize = serializedHeaders.length;

        if (headerSize > 65535) {
            throw new IllegalStateException(
                "Serialized headers size " + headerSize +
                " bytes exceeds maximum of 65535 bytes"
            );
        }

        final int totalSize = 2 + headerSize + 8 + value.length;
        return ByteBuffer.allocate(totalSize)
            .putShort((short) headerSize)
            .put(serializedHeaders)
            .putLong(timestamp)
            .put(value)
            .array();
    }

    /**
     * Decodes byte array back into ValueTimestampHeaders.
     * Format: [HeaderSize(2)][Headers][Timestamp(8)][Payload]
     */
    private ValueTimestampHeaders<byte[]> decodeValueWithTimestampAndHeaders(final byte[] encodedValue) {
        if (encodedValue == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(encodedValue);

        final int headerSize = buffer.getShort() & 0xFFFF;

        final byte[] headerBytes = new byte[headerSize];
        buffer.get(headerBytes);
        final Headers headers = deserializeHeaders(headerBytes);

        final long timestamp = buffer.getLong();

        final int valueSize = buffer.remaining();
        final byte[] value = new byte[valueSize];
        buffer.get(value);

        return ValueTimestampHeaders.make(value, timestamp, headers);
    }

    /**
     * Serializes headers into byte array.
     * Format: [NumHeaders(4)][Header1][Header2]...
     * Each header: [KeyLength(4)][KeyBytes][ValueLength(4)][ValueBytes]
     */
    private byte[] serializeHeaders(final Headers headers) {
        if (headers == null) {
            return ByteBuffer.allocate(4).putInt(0).array();
        }

        int totalSize = 4;
        int headerCount = 0;

        for (final Header header : headers) {
            headerCount++;
            final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = header.value();

            totalSize += 4;
            totalSize += keyBytes.length;
            totalSize += 4;
            if (valueBytes != null) {
                totalSize += valueBytes.length;
            }
        }

        final ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(headerCount);

        for (final Header header : headers) {
            final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = header.value();

            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);

            if (valueBytes == null) {
                buffer.putInt(-1);
            } else {
                buffer.putInt(valueBytes.length);
                buffer.put(valueBytes);
            }
        }

        return buffer.array();
    }

    /**
     * Deserializes headers from byte array.
     */
    private Headers deserializeHeaders(final byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length == 0) {
            return new RecordHeaders();
        }

        final ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        final int headerCount = buffer.getInt();

        final RecordHeaders headers = new RecordHeaders();

        for (int i = 0; i < headerCount; i++) {
            final int keyLength = buffer.getInt();
            final byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            final int valueLength = buffer.getInt();
            final byte[] value;
            if (valueLength == -1) {
                value = null;
            } else {
                value = new byte[valueLength];
                buffer.get(value);
            }

            headers.add(new RecordHeader(key, value));
        }

        return headers;
    }

    /**
     * Iterator wrapper that decodes bytes to ValueTimestampHeaders.
     */
    private class KeyValueIteratorHeadersWrapper implements KeyValueIterator<Bytes, ValueTimestampHeaders<byte[]>> {
        private final KeyValueIterator<Bytes, byte[]> innerIterator;

        KeyValueIteratorHeadersWrapper(final KeyValueIterator<Bytes, byte[]> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Bytes peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Bytes, ValueTimestampHeaders<byte[]>> next() {
            final KeyValue<Bytes, byte[]> next = innerIterator.next();
            return KeyValue.pair(
                next.key,
                decodeValueWithTimestampAndHeaders(next.value)
            );
        }
    }
}
