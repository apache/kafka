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
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.AggregationWithHeaders;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

/**
 * Deserializer for AggregationWithHeaders.
 * Deserialization format (per KIP-1271):
 * [headersSize(varint)][headersBytes][aggregation]
 * <p>
 * Where:
 * - headersSize: Size of the headersBytes section in bytes, encoded as varint
 * - headersBytes:
 *   - For null/empty headers: headersSize = 0, headersBytes is omitted (0 bytes)
 *   - For non-empty headers: headersSize > 0, serialized headers in the format [count(varint)][header1][header2]... to be processed by HeadersDeserializer.
 * - aggregation: Serialized aggregation to be deserialized with the provided aggregation deserializer
 * <p>
 * This is used by KIP-1271 to deserialize aggregations with headers from session state stores.
 */
class AggregationWithHeadersDeserializer<AGG> implements WrappingNullableDeserializer<AggregationWithHeaders<AGG>, Void, AGG> {

    public final Deserializer<AGG> aggregationDeserializer;

    AggregationWithHeadersDeserializer(final Deserializer<AGG> aggregationDeserializer) {
        Objects.requireNonNull(aggregationDeserializer);
        this.aggregationDeserializer = aggregationDeserializer;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        aggregationDeserializer.configure(configs, isKey);
    }

    @Override
    public AggregationWithHeaders<AGG> deserialize(final String topic, final byte[] aggregationWithHeaders) {
        if (aggregationWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(aggregationWithHeaders);
        final Headers headers = readHeaders(buffer);
        final byte[] rawAggregation = readBytes(buffer, buffer.remaining());
        final AGG aggregation = aggregationDeserializer.deserialize(topic, headers, rawAggregation);

        return AggregationWithHeaders.makeAllowNullable(aggregation, headers);
    }

    @Override
    public void close() {
        aggregationDeserializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        initNullableDeserializer(aggregationDeserializer, getter);
    }


    /**
     * Reads the specified number of bytes from the buffer with validation.
     *
     * @param buffer the ByteBuffer to read from
     * @param length the number of bytes to read
     * @return the byte array containing the read bytes
     * @throws SerializationException if buffer doesn't have enough bytes or length is negative
     */
    private static byte[] readBytes(final ByteBuffer buffer, final int length) {
        if (length < 0) {
            throw new SerializationException(
                "Invalid AggregationWithHeaders format: negative length " + length
            );
        }
        if (buffer.remaining() < length) {
            throw new SerializationException(
                "Invalid AggregationWithHeaders format: expected " + length +
                    " bytes but only " + buffer.remaining() + " bytes remaining"
            );
        }
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Extract headers from serialized AggregationWithHeaders.
     */
    static Headers headers(final byte[] rawAggregationWithHeaders) {
        if (rawAggregationWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawAggregationWithHeaders);
        return readHeaders(buffer);
    }

    /**
     * Extract the raw aggregation bytes from serialized AggregationWithHeaders,
     * stripping the headers prefix.
     */
    static byte[] rawAggregation(final byte[] aggregationWithHeaders) {
        if (aggregationWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(aggregationWithHeaders);
        readHeaders(buffer);
        return readBytes(buffer, buffer.remaining());
    }

    private static Headers readHeaders(final ByteBuffer buffer) {
        final int headersSize = ByteUtils.readVarint(buffer);
        final byte[] rawHeaders = readBytes(buffer, headersSize);
        return HeadersDeserializer.deserialize(rawHeaders);
    }
}