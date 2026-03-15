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
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
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
class HeadersSerializer {

    /**
     * Serializes headers into a ByteBuffer using varint encoding per KIP-1271.
     * <p>
     * The output format is [count][header1][header2]... without a size prefix.
     * The size prefix is added by the outer serializer that uses this.
     * <p>
     * For null or empty headers, returns an empty byte array (0 bytes)
     * instead of encoding headerCount=0 (1 byte).
     * <p>
     * The returned ByteBuffer may have larger capacity than actual payload size.
     * It's position will be set to zero, and its limit marks the payload end.
     *
     * @param headers the headers to serialize (can be null)
     * @return the serialized byte array (empty array if headers are null or empty)
     */
    public static ByteBuffer serialize(final Headers headers) {
        final Header[] headersArray = (headers == null) ? new Header[0] : headers.toArray();

        if (headersArray.length == 0) {
            return ByteBuffer.allocate(0);
        }

        // we first estimate an upper bound for the buffer we need,
        // so we can allocate the whole buffer at once

        // cache to avoid translating String header-keys to byte[] twice
        final byte[][] serializedHeaderKeys = new byte[headersArray.length][];

        // start with 5 bytes for varint encoding of header count
        int estimatedBufferSize = 5;
        int i = 0;
        for (final Header header : headersArray) {
            // adding 5 bytes for varint encoding of header-key length
            serializedHeaderKeys[i] = header.key().getBytes(StandardCharsets.UTF_8);
            estimatedBufferSize += 5 + serializedHeaderKeys[i].length;
            ++i;

            final byte[] value = header.value();
            if (value == null) {
                ++estimatedBufferSize;
            } else {
                // adding 5 bytes for varint encoding of header-value length
                estimatedBufferSize += 5 + value.length;
            }
        }

        final ByteBuffer result = ByteBuffer.allocate(estimatedBufferSize);
        ByteUtils.writeVarint(headersArray.length, result);

        i = 0;
        for (final Header header : headersArray) {
            final String headerKey = header.key();
            ByteUtils.writeVarint(headerKey.length(), result);
            result.put(serializedHeaderKeys[i++]);

            final byte[] headerValue = header.value();
            if (headerValue != null) {
                ByteUtils.writeVarint(headerValue.length, result);
                result.put(headerValue);
            } else {
                result.put((byte) 0x01); // hardcoded varint encoding for `-1`
            }
        }

        result.limit(result.position());
        result.position(0);

        return result;
    }
}
