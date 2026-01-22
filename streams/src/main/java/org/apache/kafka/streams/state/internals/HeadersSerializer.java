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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Serializer for Kafka Headers.
 *
 * Serialization format (per KIP-1271):
 * [NumHeaders(varint)][Header1][Header2]...
 *
 * Each header:
 * [KeyLength(varint)][KeyBytes(UTF-8)][ValueLength(varint)][ValueBytes]
 *
 * Note: ValueLength is -1 for null values (encoded as varint).
 * All integers are encoded as varints (signed varint encoding).
 *
 * This is used by KIP-1271 to serialize headers for storage in state stores.
 */
public class HeadersSerializer {

    /**
     * Serializes headers into a byte array using varint encoding per KIP-1271.
     *
     * @param headers the headers to serialize (can be null)
     * @return the serialized byte array
     */
    public byte[] serialize(final Headers headers) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            if (headers == null) {
                // Empty headers: just [NumHeaders(varint) = 0]
                ByteUtils.writeVarint(0, out);
                return baos.toByteArray();
            }

            // Count headers
            int headerCount = 0;
            final Iterator<Header> iterator = headers.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                headerCount++;
            }

            // Write header count as varint
            ByteUtils.writeVarint(headerCount, out);

            // Write each header
            for (final Header header : headers) {
                final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
                final byte[] valueBytes = header.value();

                // Write key length and key bytes (varint + UTF-8)
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
