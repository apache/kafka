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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for Kafka Headers.
 * <p>
 * Serialization format (per KIP-1271):
 * <ul>
 * <li>For null or empty headers: empty byte array (0 bytes)</li>
 * <li>For non-empty headers: [NumHeaders(varint)][Header1][Header2]...</li>
 * </ul>
 * <p>
 * Each header:
 * [KeyLength(varint)][KeyBytes(UTF-8)][ValueLength(varint)][ValueBytes]
 * <p>
 * Note: ValueLength is -1 for null values (encoded as varint).
 * All integers are encoded as varints (signed varint encoding).
 * <p>
 * This serializer produces the headersBytes portion. The headersSize prefix
 * is added by the outer serializer (e.g., ValueTimestampHeadersSerializer).
 * <p>
 * This is used by KIP-1271 to serialize headers for storage in state stores.
 */
public class HeadersSerializer implements Serializer<Headers> {

    /**
     * Serializes headers into a byte array using varint encoding per KIP-1271.
     * <p>
     * The output format is [count][header1][header2]... without a size prefix.
     * The size prefix is added by the outer serializer that uses this.
     * <p>
     * For null or empty headers, returns an empty byte array (0 bytes)
     * instead of encoding headerCount=0 (1 byte).
     *
     * @param topic topic associated with data
     * @param headers the headers to serialize (can be null)
     * @return the serialized byte array (empty array if headers are null or empty)
     */
    @Override
    public byte[] serialize(final String topic, final Headers headers) {
        final Header[] headersArray = (headers == null) ? new Header[0] : headers.toArray();

        if (headersArray.length == 0) {
            return new byte[0];
        }

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {

            ByteUtils.writeVarint(headersArray.length, out);

            for (final Header header : headersArray) {
                final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
                final byte[] valueBytes = header.value();

                ByteUtils.writeVarint(keyBytes.length, out);
                out.write(keyBytes);

                // Write value length and value bytes (varint + raw bytes)
                // null is represented as -1, encoded as varint
                if (valueBytes == null) {
                    ByteUtils.writeVarint(-1, out);
                } else {
                    ByteUtils.writeVarint(valueBytes.length, out);
                    out.write(valueBytes);
                }
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize headers", e);
        }
    }
}
