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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

/**
 * Serializer for ValueTimestampHeaders.
 *
 * Serialization format (per KIP-1271):
 * [headersSize(varint)][headersBytes][timestamp(8)][value]
 *
 * Where:
 * - headersSize: Size of the headersBytes section in bytes, encoded as varint
 * - headersBytes:
 *   - For null/empty headers: headersSize = 0, headersBytes is omitted (0 bytes)
 *   - For non-empty headers: headersSize > 0, serialized headers ([count(varint)][header1][header2]...) from HeadersSerializer
 * - timestamp: 8-byte long timestamp
 * - value: Serialized value using the provided value serializer
 *
 * This is used by KIP-1271 to serialize values with timestamps and headers for state stores.
 */
public class ValueTimestampHeadersSerializer<V> implements WrappingNullableSerializer<ValueTimestampHeaders<V>, Void, V> {
    public final Serializer<V> valueSerializer;
    private final LongSerializer timestampSerializer;
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
        headersSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final ValueTimestampHeaders<V> valueTimestampHeaders) {
        if (valueTimestampHeaders == null) {
            return null;
        }
        return serialize(topic, valueTimestampHeaders.value(), valueTimestampHeaders.timestamp(), valueTimestampHeaders.headers());
    }

    private byte[] serialize(final String topic, final V plainValue, final long timestamp, final Headers headers) {
        if (plainValue == null) {
            return null;
        }

        final byte[] rawValue = valueSerializer.serialize(topic, headers, plainValue);

        // Since we can't control the result of the internal serializer, we make sure that the result
        // is not null as well.
        // Serializing non-null values to null can be useful when working with Optional-like values
        // where the Optional.empty case is serialized to null.
        // See the discussion here: https://github.com/apache/kafka/pull/7679
        if (rawValue == null) {
            return null;
        }

        final byte[] rawTimestamp = timestampSerializer.serialize(topic, timestamp);

        // empty (byte[0]) for null/empty headers, or [count][header1][header2]... for non-empty
        final byte[] rawHeaders = headersSerializer.serialize(topic, headers);

        // Format: [headersSize(varint)][headersBytes][timestamp(8)][value]
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {

            ByteUtils.writeVarint(rawHeaders.length, out);  // headersSize (it may be 0 due to null/empty headers)
            out.write(rawHeaders);                          // empty (byte[0]) for null/empty headers, or [count][header1][header2]... for non-empty
            out.write(rawTimestamp);                        // [timestamp(8)]
            out.write(rawValue);                            // [value]

            return baos.toByteArray();
        } catch (final IOException e) {
            throw new SerializationException("Failed to serialize ValueTimestampHeaders", e);
        }
    }

    @Override
    public void close() {
        valueSerializer.close();
        timestampSerializer.close();
        headersSerializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        // ValueTimestampHeadersSerializer never wraps a null serializer (or configure would throw),
        // but it may wrap a serializer that itself wraps a null serializer.
        initNullableSerializer(valueSerializer, getter);
    }
}
