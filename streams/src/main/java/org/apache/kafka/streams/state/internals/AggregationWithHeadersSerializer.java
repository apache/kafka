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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.AggregationWithHeaders;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

/**
 * Serializer for AggregationWithHeaders.
 <p>
 * Serialization format (per KIP-1271):
 * [headersSize(varint)][headersBytes][aggregation]
 * <p>
 * Where:
 * - headersSize: Size of the headersBytes section in bytes, encoded as varint
 * - headersBytes:
 *   - For null/empty headers: headersSize = 0, headersBytes is omitted (0 bytes)
 *   - For non-empty headers: headersSize > 0, serialized headers ([count(varint)][header1][header2]...) from HeadersSerializer
 * - aggregation: Serialized aggregation using the provided aggregation serializer
 * <p>
 * This is used by KIP-1271 to serialize aggregations with headers for session state stores.
 */
class AggregationWithHeadersSerializer<AGG> implements WrappingNullableSerializer<AggregationWithHeaders<AGG>, Void, AGG> {
    public final Serializer<AGG> aggregationSerializer;

    AggregationWithHeadersSerializer(final Serializer<AGG> aggregationSerializer) {
        Objects.requireNonNull(aggregationSerializer);
        this.aggregationSerializer = aggregationSerializer;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        aggregationSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final AggregationWithHeaders<AGG> aggregationWithHeaders) {
        if (aggregationWithHeaders == null) {
            return null;
        }
        return serialize(topic, aggregationWithHeaders.aggregation(), aggregationWithHeaders.headers());
    }

    private byte[] serialize(final String topic, final AGG plainAggregation, final Headers headers) {
        if (plainAggregation == null) {
            return null;
        }

        final byte[] rawAggregation = aggregationSerializer.serialize(topic, headers, plainAggregation);

        if (rawAggregation == null) {
            return null;
        }

        final byte[] rawHeaders = HeadersSerializer.serialize(headers);

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {

            ByteUtils.writeVarint(rawHeaders.length, out);
            out.write(rawHeaders);
            out.write(rawAggregation);

            return baos.toByteArray();
        } catch (final IOException e) {
            throw new SerializationException("Failed to serialize AggregationWithHeaders on topic: " + topic, e);
        }
    }

    @Override
    public void close() {
        aggregationSerializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        initNullableSerializer(aggregationSerializer, getter);
    }
}