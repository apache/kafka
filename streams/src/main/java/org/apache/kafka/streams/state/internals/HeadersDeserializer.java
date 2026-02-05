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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Deserializer for Kafka Headers.
 *
 * Deserialization format (per KIP-1271):
 * [NumHeaders(varint)][Header1][Header2]...
 *
 * Each header:
 * [KeyLength(varint)][KeyBytes(UTF-8)][ValueLength(varint)][ValueBytes]
 *
 * Note: ValueLength is -1 for null values (encoded as varint).
 * All integers are decoded from varints (signed varint encoding).
 *
 * This deserializer expects the headersBytes portion without a size prefix.
 * The size prefix is handled by the outer deserializer (e.g., ValueTimestampHeadersDeserializer).
 *
 * This is used by KIP-1271 to deserialize headers from state stores.
 */
public class HeadersDeserializer implements Deserializer<Headers> {

    /**
     * Deserializes headers from a byte array using varint encoding per KIP-1271.
     * <p>
     * The input format is [count][header1][header2]... without a size prefix.
     *
     * @param topic topic associated with the data
     * @param data the serialized byte array (can be null)
     * @return the deserialized headers
     */
    public Headers deserialize(final String topic, final byte[] data) {
        if (data == null || data.length == 0) {
            return new RecordHeaders();
        }

        final ByteBuffer buffer = ByteBuffer.wrap(data);
        final int headersCount = ByteUtils.readVarint(buffer);

        if (headersCount == 0) {
            return new RecordHeaders();
        }

        final RecordHeaders headers = new RecordHeaders();

        for (int i = 0; i < headersCount; i++) {
            final int keyLength = ByteUtils.readVarint(buffer);
            final byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            final int valueLength = ByteUtils.readVarint(buffer);
            final byte[] value;
            if (valueLength == -1) {
                value = null;
            } else {
                value = new byte[valueLength];
                buffer.get(value);
            }

            headers.add(key, value);
        }

        return headers;
    }
}
