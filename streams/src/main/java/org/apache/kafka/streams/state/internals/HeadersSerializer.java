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

    final static class PreSerializedHeaders {
        final int requiredBufferSizeForHeaders;
        final byte[][] serializedHeaders;

        PreSerializedHeaders(final int requiredBufferSizeForHeaders, final byte[][] serializedHeaders) {
            this.requiredBufferSizeForHeaders = requiredBufferSizeForHeaders;
            this.serializedHeaders = serializedHeaders;
        }
    }

    public static PreSerializedHeaders prepareSerialization(final Headers headers) {
        final Header[] headersArray = (headers == null) ? new Header[0] : headers.toArray();

        if (headersArray.length == 0) {
            return new PreSerializedHeaders(0, null);
        }

        // we first compute the size for the buffer we need,
        // so we can allocate the whole buffer at once later

        // cache to avoid translating String header-keys to byte[] twice
        final byte[][] serializedHeaders = new byte[headersArray.length][];

        // start with varint encoding of header count
        int requiredBufferSizeForHeaders = ByteUtils.sizeOfVarint(headersArray.length);
        int i = 0;

        for (final Header header : headersArray) {
            final byte[] rawHeaderKey = header.key().getBytes(StandardCharsets.UTF_8);
            requiredBufferSizeForHeaders += ByteUtils.sizeOfVarint(rawHeaderKey.length) + rawHeaderKey.length;

            final byte[] rawHeaderValue = header.value();
            if (rawHeaderValue == null) {
                ++requiredBufferSizeForHeaders;
            } else {
                requiredBufferSizeForHeaders += ByteUtils.sizeOfVarint(rawHeaderValue.length) + rawHeaderValue.length;
            }

            serializedHeaders[i] = rawHeaderKey;
//            serializedHeaders[i][1] = rawHeaderValue;
            ++i;
        }

        return new PreSerializedHeaders(requiredBufferSizeForHeaders, serializedHeaders);
    }

    /**
     * Serializes headers into a ByteBuffer using varint encoding per KIP-1271.
     * <p>
     * The output format is [count][header1][header2]... without a size prefix.
     * The size prefix is added by the outer serializer that uses this.
     * <p>
     * For null or empty headers, returns an empty byte array (0 bytes)
     * instead of encoding headerCount=0 (1 byte).
     *
     * @param preSerializedHeaders the preSerializedHeaders
     * @param buffer the buffer to write the serialized header into (it's expected that the buffer position is set correctly)
     * @return the modified {@code buffer} containing the serializer headers (empty array if headers are null or empty),\
     * with corresponding advanced position
     */
    public static ByteBuffer serialize(final PreSerializedHeaders preSerializedHeaders, final ByteBuffer buffer) {
        return null;
    }
    public static ByteBuffer serialize(final PreSerializedHeaders preSerializedHeaders, final ByteBuffer buffer, final Headers headers) {
        if (preSerializedHeaders.requiredBufferSizeForHeaders == 0) {
            return buffer;
        }

        final Header[] headersArray = (headers == null) ? new Header[0] : headers.toArray();

//        ByteUtils.writeVarint(preSerializedHeaders.serializedHeaders.length, buffer);
        ByteUtils.writeVarint(headersArray.length, buffer);

//        for (final byte[][] serializedHeader : preSerializedHeaders.serializedHeaders) {
//            ByteUtils.writeVarint(serializedHeader[0].length, buffer);
//            buffer.put(serializedHeader[0]);
//
//            if (serializedHeader[1] != null) {
//                ByteUtils.writeVarint(serializedHeader[1].length, buffer);
//                buffer.put(serializedHeader[1]);
//            } else {
//                buffer.put((byte) 0x01); // hardcoded varint encoding for `-1`
//            }
//        }

        int i = 0;
        for (final Header header : headersArray) {
//            final byte[] rawHeaderKey = header.key().getBytes(StandardCharsets.UTF_8);
            final byte[] rawHeaderKey = preSerializedHeaders.serializedHeaders[i++];
            ByteUtils.writeVarint(rawHeaderKey.length, buffer);
            buffer.put(rawHeaderKey);

            final byte[] rawHeaderValue = header.value();
            if (rawHeaderValue != null) {
                ByteUtils.writeVarint(rawHeaderValue.length, buffer);
                buffer.put(rawHeaderValue);
            } else {
                buffer.put((byte) 0x01); // hardcoded varint encoding for `-1`
            }
        }
        return buffer;
    }
}
