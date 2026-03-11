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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.StateSerdes;

import java.nio.ByteBuffer;

public class Utils {
    /**
     * Extract raw plain value from serialized ValueTimestampHeaders.
     * This strips both the headers and timestamp portions.
     *
     * Format conversion:
     * Input:  [headersSize(varint)][headers][timestamp(8)][value]
     * Output: [value]
     */
    public static byte[] rawPlainValue(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        // If the header is empty, then copy the value bytes directly
        if (hasEmptyHeadersAndTimestamp(rawValueTimestampHeaders)) {
            // Strip header size (varint 1 byte), empty headers (no bytes), and timestamp
            final byte[] res = new byte[rawValueTimestampHeaders.length - 1 - StateSerdes.TIMESTAMP_SIZE]; 
            System.arraycopy(rawValueTimestampHeaders, 1 + StateSerdes.TIMESTAMP_SIZE, res, 0, res.length);
            return res;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // Skip headers and timestamp (8 bytes)
        buffer.position(buffer.position() + headersSize + 8);

        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static boolean hasEmptyHeadersAndTimestamp(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders.length > 0 && rawValueTimestampHeaders[0] == 0x00) {
            // Header size (varint 1 byte), empty headers (no bytes), and timestamp
            if (rawValueTimestampHeaders.length - 1 - StateSerdes.TIMESTAMP_SIZE < 0) {
                throw new SerializationException(
                    "Invalid format: input length " + rawValueTimestampHeaders.length +
                    " is less than min length of timestamped value with empty headers " +
                    (1 + StateSerdes.TIMESTAMP_SIZE)
                );
            }
            return true;
        }
        return false;
    }

    /**
     * Extract raw timestamped value (timestamp + value) from serialized ValueTimestampHeaders.
     * This strips the headers portion but keeps timestamp and value intact.
     *
     * Format conversion:
     * Input:  [headersSize(varint)][headers][timestamp(8)][value]
     * Output: [timestamp(8)][value]
     */
    public static byte[] rawTimestampedValue(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        // If the header is empty, then copy the value and timestamp bytes directly
        if (hasEmptyHeadersAndTimestamp(rawValueTimestampHeaders)) {
            // Strip header size (varint 1 byte), empty headers (no bytes)
            final byte[] res = new byte[rawValueTimestampHeaders.length - 1]; 
            System.arraycopy(rawValueTimestampHeaders, 1, res, 0, res.length);
            return res;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // Skip headers, keep timestamp + value
        if (headersSize < 0 || headersSize > buffer.remaining() || buffer.remaining() - headersSize < StateSerdes.TIMESTAMP_SIZE) {
            throw new SerializationException(
                "Invalid format: headers size " + headersSize + 
                ", timestamp expected size " + StateSerdes.TIMESTAMP_SIZE + 
                ", but buffer size " + buffer.remaining()
            );
        }
        buffer.position(buffer.position() + headersSize);

        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * Reads the next specified number of bytes from the buffer's current position with validation.
     *
     * @param buffer the ByteBuffer to read from
     * @param length the number of bytes to read
     * @return the byte array containing the read bytes
     * @throws SerializationException if buffer doesn't have enough bytes or length is negative
     */
    public static byte[] readBytes(final ByteBuffer buffer, final int length) {
        if (length < 0) {
            throw new SerializationException(
                "Invalid format: negative length " + length
            );
        }
        if (buffer.remaining() < length) {
            throw new SerializationException(
                "Invalid format: expected " + length +
                    " bytes but only " + buffer.remaining() + " bytes remaining"
            );
        }
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Serialize the key with headers into bytes
     * @param key the key to serialize
     * @param headers the Headers as context
     * @param serdes the StateSerdes as serializer
     * @return the Bytes of the key
     */
    public static <K> Bytes keyBytes(final K key, final Headers headers, final StateSerdes<K, ?> serdes) {
        return Bytes.wrap(serdes.rawKey(key, headers));
    }

    /**
     * Serialize the key into bytes
     * @param key the key to serialize
     * @param serdes the StateSerdes as serializer
     * @return the Bytes of the key
     */
    static <K> Bytes keyBytes(final K key, final StateSerdes<K, ?> serdes) {
        return keyBytes(key, new RecordHeaders(), serdes);
    }

    /**
     * Serialize the session key with headers into bytes
     * @param sessionKey the Windowed session key to serialize
     * @param headers the Headers as context
     * @param serdes the StateSerdes as serializer
     * @return the Bytes of the key
     */
    static <K> Bytes keyBytes(final Windowed<K> sessionKey, final Headers headers, final StateSerdes<K, ?> serdes) {
        return keyBytes(sessionKey.key(), headers, serdes);
    }
}
