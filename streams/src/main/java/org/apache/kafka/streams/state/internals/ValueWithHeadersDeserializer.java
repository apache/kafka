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
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.AggregationWithHeaders;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;
import static org.apache.kafka.streams.state.internals.Utils.readBytes;

/**
 * Deserializer for values with headers.
 * Deserialization format (per KIP-1271):
 * [headersSize(varint)][headersBytes][value]
 * <p>
 * Where:
 * - headersSize: Size of the headersBytes section in bytes, encoded as varint
 * - headersBytes:
 *   - For null/empty headers: headersSize = 0, headersBytes is omitted (0 bytes)
 *   - For non-empty headers: headersSize > 0, serialized headers in the format [count(varint)][header1][header2]... to be processed by HeadersDeserializer.
 * - value: Serialized value to be deserialized with the provided value deserializer
 * <p>
 * This is used by KIP-1271 to deserialize values with headers from session and window state stores.
 */
class ValueWithHeadersDeserializer<V> implements WrappingNullableDeserializer<AggregationWithHeaders<V>, Void, V> {

    public final Deserializer<V> valueDeserializer;

    ValueWithHeadersDeserializer(final Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(valueDeserializer);
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        valueDeserializer.configure(configs, isKey);
    }

    @Override
    public AggregationWithHeaders<V> deserialize(final String topic, final byte[] valueWithHeaders) {
        if (valueWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(valueWithHeaders);
        final Headers headers = Utils.readHeaders(buffer);
        final byte[] rawAggregation = readBytes(buffer, buffer.remaining());
        final V aggregation = valueDeserializer.deserialize(topic, headers, rawAggregation);

        return AggregationWithHeaders.makeAllowNullable(value, headers);
    }

    @Override
    public void close() {
        valueDeserializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        initNullableDeserializer(valueDeserializer, getter);
    }

    /**
     * Extract headers from serialized value with headers.
     */
    static Headers headers(final byte[] rawValueWithHeaders) {
        if (rawValueWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueWithHeaders);
        return readHeaders(buffer);
    }

    /**
     * Extract the raw value bytes from serialized value with headers,
     * stripping the headers prefix.
     */
    static byte[] rawAggregation(final byte[] valueWithHeaders) {
        if (valueWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(valueWithHeaders);
        readHeaders(buffer);
        return readBytes(buffer, buffer.remaining());
    }

    private static Headers readHeaders(final ByteBuffer buffer) {
        final int headersSize = ByteUtils.readVarint(buffer);
        final byte[] rawHeaders = readBytes(buffer, headersSize);
        return HeadersDeserializer.deserialize(rawHeaders);
    }
}
