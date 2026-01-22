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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for Kafka Headers.
 *
 * Serialization format:
 * [NumHeaders(4)][Header1][Header2]...
 *
 * Each header:
 * [KeyLength(4)][KeyBytes][ValueLength(4)][ValueBytes]
 *
 * Note: ValueLength is -1 for null values.
 *
 * This is used by KIP-1271 to serialize headers for storage in state stores.
 */
public class HeadersSerializer {

    /**
     * Serializes headers into a byte array.
     *
     * @param headers the headers to serialize (can be null)
     * @return the serialized byte array
     */
    public byte[] serialize(final Headers headers) {
        if (headers == null) {
            // Empty headers: just [NumHeaders(4) = 0]
            return ByteBuffer.allocate(4).putInt(0).array();
        }

        // First pass: calculate total size
        int totalSize = 4; // For number of headers
        int headerCount = 0;

        for (final Header header : headers) {
            headerCount++;
            final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = header.value();

            totalSize += 4; // Key length
            totalSize += keyBytes.length;
            totalSize += 4; // Value length
            if (valueBytes != null) {
                totalSize += valueBytes.length;
            }
        }

        // Second pass: write data
        final ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(headerCount);

        for (final Header header : headers) {
            final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = header.value();

            // Write key
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);

            // Write value (null is represented as -1 length)
            if (valueBytes == null) {
                buffer.putInt(-1);
            } else {
                buffer.putInt(valueBytes.length);
                buffer.put(valueBytes);
            }
        }

        return buffer.array();
    }
}
