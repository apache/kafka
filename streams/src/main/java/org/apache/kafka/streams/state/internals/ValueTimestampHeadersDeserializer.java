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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
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
 * Deserialization format:
 * [HeaderSize(2)][Headers][Timestamp(8)][Value]
 *
 * This is used by KIP-1271 to deserialize values with timestamps and headers from state stores.
 */
class ValueTimestampHeadersDeserializer<V> implements WrappingNullableDeserializer<ValueTimestampHeaders<V>, Void, V> {
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

    public final Deserializer<V> valueDeserializer;
    private final Deserializer<Long> timestampDeserializer;
    private final HeadersDeserializer headersDeserializer;

    ValueTimestampHeadersDeserializer(final Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(valueDeserializer);
        this.valueDeserializer = valueDeserializer;
        this.timestampDeserializer = new LongDeserializer();
        this.headersDeserializer = new HeadersDeserializer();
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

        final ByteBuffer buffer = ByteBuffer.wrap(valueTimestampHeaders);

        // Read header size
        final short headerSize = buffer.getShort();

        // Read headers
        final byte[] rawHeaders = new byte[headerSize];
        buffer.get(rawHeaders);
        final Headers headers = headersDeserializer.deserialize(rawHeaders);

        // Read timestamp
        final byte[] rawTimestamp = new byte[Long.BYTES];
        buffer.get(rawTimestamp);
        final long timestamp = timestampDeserializer.deserialize(topic, rawTimestamp);

        // Read value
        final byte[] rawValue = new byte[buffer.remaining()];
        buffer.get(rawValue);
        final V value = valueDeserializer.deserialize(topic, rawValue);

        return ValueTimestampHeaders.make(value, timestamp, headers);
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
     * Extract raw value bytes from serialized ValueTimestampHeaders.
     */
    static byte[] rawValue(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final short headerSize = buffer.getShort();

        // Skip headers and timestamp to get to value
        final int valueOffset = 2 + headerSize + Long.BYTES;
        final int rawValueLength = rawValueTimestampHeaders.length - valueOffset;

        return ByteBuffer
            .allocate(rawValueLength)
            .put(rawValueTimestampHeaders, valueOffset, rawValueLength)
            .array();
    }

    /**
     * Extract timestamp from serialized ValueTimestampHeaders.
     */
    static long timestamp(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            throw new IllegalArgumentException("Cannot extract timestamp from null data");
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final short headerSize = buffer.getShort();

        // Skip headers to get to timestamp
        buffer.position(2 + headerSize);
        final byte[] rawTimestamp = new byte[Long.BYTES];
        buffer.get(rawTimestamp);

        return LONG_DESERIALIZER.deserialize(null, rawTimestamp);
    }

    /**
     * Extract headers from serialized ValueTimestampHeaders.
     */
    static Headers headers(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            throw new IllegalArgumentException("Cannot extract headers from null data");
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final short headerSize = buffer.getShort();

        final byte[] rawHeaders = new byte[headerSize];
        buffer.get(rawHeaders);

        return new HeadersDeserializer().deserialize(rawHeaders);
    }
}
