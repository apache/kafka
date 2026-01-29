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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Marker interface to indicate that a bytes store understands the value-with-headers format
 * and can convert legacy "plain value" entries to the new format.
 * <p>
 * This is analogous to {@link TimestampedBytesStore} but for header-aware stores.
 * <p>
 * Per KIP-1271, the value format is: [headers_size(varint)][headers_bytes][payload_bytes]
 * where payload_bytes is the existing serialized value (e.g., [timestamp(8)][value] for timestamped stores).
 */
public interface HeadersBytesStore {

    /**
     * Converts a legacy value (without headers) to the header-embedded format.
     * <p>
     * For timestamped stores, the legacy format is: [timestamp(8)][value]
     * The new format is: [headers_size(varint)][headers_bytes][timestamp(8)][value]
     * <p>
     * This method adds empty headers to the existing value format.
     * <p>
     * The headers_bytes format per KIP-1271 is: [count(varint)][header1][header2]...
     * For empty headers, this is simply a single varint encoding 0.
     *
     * @param key   the key bytes (may be used for context-dependent conversion; typically unused)
     * @param value the legacy value bytes (for timestamped stores: [timestamp(8)][value])
     * @return the value in header-embedded format with empty headers
     */
    static byte[] convertToHeaderFormat(final byte[] key, final byte[] value) {
        if (value == null) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            // Serialize empty headers: [count(varint) = 0]
            final ByteArrayOutputStream headersStream = new ByteArrayOutputStream();
            final DataOutputStream headersOut = new DataOutputStream(headersStream);
            ByteUtils.writeVarint(0, headersOut);  // header count = 0
            final byte[] emptyHeadersBytes = headersStream.toByteArray();

            // Write format: [headers_size(varint)][headers_bytes][payload]
            ByteUtils.writeVarint(emptyHeadersBytes.length, out);  // headers_size
            out.write(emptyHeadersBytes);                          // headers_bytes: [count=0]
            out.write(value);                                      // payload: [timestamp(8)][value]

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert to header format", e);
        }
    }
}
