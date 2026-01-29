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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

/**
 * Serializer for ValueTimestampHeaders.
 *
 * Serialization format (per KIP-1271):
 * [HeadersSize(varint)][HeadersBytes][Timestamp(8)][Value]
 *
 * Where:
 * - HeadersSize: Size of the HeadersBytes section in bytes, encoded as varint
 * - HeadersBytes: Serialized headers ([count(varint)][header1][header2]...) from HeadersSerializer
 * - Timestamp: 8-byte long timestamp
 * - Value: Serialized value using the provided value serializer
 *
 * This is used by KIP-1271 to serialize values with timestamps and headers for state stores.
 */
public class ValueTimestampHeadersSerializer<V> implements WrappingNullableSerializer<ValueTimestampHeaders<V>, Void, V> {
    public final Serializer<V> valueSerializer;
    private final Serializer<Long> timestampSerializer;
    private final HeadersSerializer headersSerializer;

    ValueTimestampHeadersSerializer(final Serializer<V> valueSerializer) {
        Objects.requireNonNull(valueSerializer);
        this.valueSerializer = valueSerializer;
        this.timestampSerializer = new LongSerializer();
        this.headersSerializer = new HeadersSerializer();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        valueSerializer.configure(configs, isKey);
        timestampSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final ValueTimestampHeaders<V> data) {
        if (data == null) {
            return null;
        }
        return serialize(topic, data.value(), data.timestamp(), data.headers());
    }

    public byte[] serialize(final String topic, final V data, final long timestamp, final Headers headers) {
        if (data == null) {
            return null;
        }

        final byte[] rawValue = valueSerializer.serialize(topic, headers, data);

        // Since we can't control the result of the internal serializer, we make sure that the result
        // is not null as well.
        // Serializing non-null values to null can be useful when working with Optional-like values
        // where the Optional.empty case is serialized to null.
        if (rawValue == null) {
            return null;
        }

        final byte[] rawHeaders = headersSerializer.serialize(headers);  // [count][header1][header2]...
        final byte[] rawTimestamp = timestampSerializer.serialize(topic, timestamp);

        // Format: [HeadersSize(varint)][HeadersBytes][Timestamp(8)][Value]
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            ByteUtils.writeVarint(rawHeaders.length, out);  // headers_size
            out.write(rawHeaders);                           // [count][header1][header2]...
            out.write(rawTimestamp);                         // [timestamp(8)]
            out.write(rawValue);                             // [value]

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ValueTimestampHeaders", e);
        }
    }

    @Override
    public void close() {
        valueSerializer.close();
        timestampSerializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        // ValueTimestampHeadersSerializer never wraps a null serializer (or configure would throw),
        // but it may wrap a serializer that itself wraps a null serializer.
        initNullableSerializer(valueSerializer, getter);
    }

    /**
     * Compares two serialized records (produced by this serializer) and returns true iff:
     * - the underlying value bytes and headers are identical, and
     * - the new timestamp is strictly greater than the old timestamp.
     * <p>
     * This method is used for optimization: if values and headers haven't changed and time
     * is not increasing, we can skip the update.
     *
     * @param oldRecord the old serialized record
     * @param newRecord the new serialized record
     * @return true if values/headers are same and time is increasing
     */
    public static boolean valuesAndHeadersAreSameAndTimeIsIncreasing(
        final byte[] oldRecord,
        final byte[] newRecord
    ) {
        if (oldRecord == newRecord) {
            // Same reference, trivially the same (might both be null)
            return true;
        } else if (oldRecord == null || newRecord == null) {
            // Only one is null, cannot be the same
            return false;
        } else if (newRecord.length != oldRecord.length) {
            // Different length, cannot be the same
            return false;
        } else if (timeIsDecreasing(oldRecord, newRecord)) {
            // Time moved backwards, need to update regardless of value changes
            return false;
        } else {
            // All other checks passed, compare binary data
            return valuesAndHeadersAreSame(oldRecord, newRecord);
        }
    }

    /**
     * Checks if timestamp in newRecord is less than or equal to timestamp in oldRecord.
     */
    private static boolean timeIsDecreasing(final byte[] oldRecord, final byte[] newRecord) {
        return extractTimestamp(newRecord) <= extractTimestamp(oldRecord);
    }

    /**
     * Extracts the timestamp from a serialized record.
     * Format: [headers_size][headers_bytes][timestamp(8)][value]
     */
    private static long extractTimestamp(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // Skip headers_size and headers_bytes
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize);

        // Read timestamp (8 bytes)
        return buffer.getLong();
    }

    /**
     * Checks if values and headers are the same in two serialized records.
     * Compares headers section and value section, skipping the timestamp.
     */
    private static boolean valuesAndHeadersAreSame(final byte[] left, final byte[] right) {
        final ByteBuffer leftBuffer = ByteBuffer.wrap(left);
        final ByteBuffer rightBuffer = ByteBuffer.wrap(right);

        // Read headers_size from both
        final int leftHeadersSize = ByteUtils.readVarint(leftBuffer);
        final int rightHeadersSize = ByteUtils.readVarint(rightBuffer);

        if (leftHeadersSize != rightHeadersSize) {
            return false;
        }

        // Compare headers_bytes
        for (int i = 0; i < leftHeadersSize; i++) {
            if (leftBuffer.get() != rightBuffer.get()) {
                return false;
            }
        }

        // Skip timestamp (8 bytes) in both
        leftBuffer.position(leftBuffer.position() + Long.BYTES);
        rightBuffer.position(rightBuffer.position() + Long.BYTES);

        // Compare remaining bytes (value)
        while (leftBuffer.hasRemaining()) {
            if (leftBuffer.get() != rightBuffer.get()) {
                return false;
            }
        }

        return true;
    }
}
