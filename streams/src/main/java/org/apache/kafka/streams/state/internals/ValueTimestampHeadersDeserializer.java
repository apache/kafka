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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

/**
 * Deserializer for ValueTimestampHeaders.
 *
 * Deserialization format (per KIP-1271):
 * [headersSize(varint)][headersBytes][timestamp(8)][value]
 *
 * Where:
 * - headersSize: Size of the headersBytes section in bytes, encoded as varint
 * - headersBytes:
 *   - For null/empty headers: headersSize = 0, headersBytes is omitted (0 bytes)
 *   - For non-empty headers: headersSize > 0, serialized headers in the format [count(varint)][header1][header2]... to be processed by HeadersDeserializer.
 * - timestamp: 8-byte long timestamp
 * - value: Serialized value to be deserialized with the provided value deserializer
 *
 * This is used by KIP-1271 to deserialize values with timestamps and headers from state stores.
 */
class ValueTimestampHeadersDeserializer<V> implements WrappingNullableDeserializer<ValueTimestampHeaders<V>, Void, V> {
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

    public final Deserializer<V> valueDeserializer;
    private final LongDeserializer timestampDeserializer;

    ValueTimestampHeadersDeserializer(final Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(valueDeserializer);
        this.valueDeserializer = valueDeserializer;
        this.timestampDeserializer = new LongDeserializer();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        valueDeserializer.configure(configs, isKey);
        timestampDeserializer.configure(configs, isKey);
    }

    @Override
    public ValueTimestampHeaders<V> deserialize(final String topic, final byte[] valueTimestampHeaders) {
        if (valueTimestampHeaders == null) {
            return null;
        }

        // DEBUG LOGGING - START
        System.out.println("=== ValueTimestampHeadersDeserializer.deserialize ===");
        System.out.println("Topic: " + topic);
        System.out.println("Raw bytes length: " + valueTimestampHeaders.length);
        System.out.println("Raw bytes (hex): " + bytesToHex(valueTimestampHeaders));
        // DEBUG LOGGING - END

        final ByteBuffer buffer = ByteBuffer.wrap(valueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);

        // DEBUG LOGGING - START
        System.out.println("Headers size: " + headersSize);
        System.out.println("Buffer position after headersSize: " + buffer.position());
        // DEBUG LOGGING - END

        final byte[] rawHeaders = readBytes(buffer, headersSize);
        final Headers headers = HeadersDeserializer.deserialize(rawHeaders);
        final byte[] rawTimestamp = readBytes(buffer, Long.BYTES);
        final long timestamp = timestampDeserializer.deserialize(topic, rawTimestamp);
        final byte[] rawValue = readBytes(buffer, buffer.remaining());

        // DEBUG LOGGING - START
        System.out.println("Raw value bytes length: " + rawValue.length);
        System.out.println("Raw value (hex): " + bytesToHex(rawValue));
        // DEBUG LOGGING - END

        final V value = valueDeserializer.deserialize(topic, headers, rawValue);

        // DEBUG LOGGING - START
        System.out.println("Deserialized value: " + value);
        System.out.println("Timestamp: " + timestamp);
        System.out.println("Headers count: " + (headers != null ? headers.toArray().length : 0));
        System.out.println("=========================================");
        // DEBUG LOGGING - END

        return ValueTimestampHeaders.make(value, timestamp, headers);
    }

    private static String bytesToHex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }

    @Override
    public void close() {
        valueDeserializer.close();
        timestampDeserializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        // ValueTimestampHeadersDeserializer never wraps a null deserializer (or configure would throw),
        // but it may wrap a deserializer that itself wraps a null deserializer.
        initNullableDeserializer(valueDeserializer, getter);
    }

    /**
     * Reads the specified number of bytes from the buffer with validation.
     *
     * @param buffer the ByteBuffer to read from
     * @param length the number of bytes to read
     * @return the byte array containing the read bytes
     * @throws SerializationException if buffer doesn't have enough bytes
     */
    private static byte[] readBytes(final ByteBuffer buffer, final int length) {
        if (buffer.remaining() < length) {
            throw new SerializationException(
                "Invalid ValueTimestampHeaders format: expected " + length +
                " bytes but only " + buffer.remaining() + " bytes remaining"
            );
        }
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Extract value from serialized ValueTimestampHeaders.
     */
    static <T> T value(final byte[] rawValueTimestampHeaders, final Deserializer<T> deserializer) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // skip headers plus timestamp
        buffer.position(buffer.position() + headersSize + Long.BYTES);
        final byte[] bytes = readBytes(buffer, buffer.remaining());

        return deserializer.deserialize("", bytes);
    }

    /**
     * Extract timestamp from serialized ValueTimestampHeaders.
     */
    static long timestamp(final byte[] rawValueTimestampHeaders) {
        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize);

        final byte[] rawTimestamp = readBytes(buffer, Long.BYTES);
        return LONG_DESERIALIZER.deserialize("", rawTimestamp);
    }

    /**
     * Extract headers from serialized ValueTimestampHeaders.
     */
    static Headers headers(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        final byte[] rawHeaders = readBytes(buffer, headersSize);
        return HeadersDeserializer.deserialize(rawHeaders);
    }
    /**
     * Extract raw value from serialized ValueTimestampHeaders.
     */
    static byte[] rawValue(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize + Long.BYTES);
        return readBytes(buffer, buffer.remaining());
    }
}
