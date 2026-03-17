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


/**
 * Marker interface to indicate that a bytes store understands the value-with-headers format
 * and can convert legacy timestamped value entries (ValueAndTimestamp format) to the new format.
 * <p>
 * This is analogous to {@link TimestampedBytesStore} but for header-aware stores.
 * <p>
 * Per KIP-1271, the value format is: [headersSize(varint)][headersBytes][payloadBytes]
 * where payloadBytes is the existing serialized value (e.g., [timestamp(8)][value] for timestamped stores).
 */
public interface HeadersBytesStore {

    /**
     * Converts a legacy value (without headers) to the header-embedded format by adding empty headers prefix.
     * <p>
     * This is a general-purpose method that prepends an empty headers prefix to any byte array.
     * The header-embedded format is: [headersSize(varint)][headersBytes][payload]
     * where empty headers are represented as headersSize=0 (encoded as [0x00]).
     * <p>
     * Usage examples:
     * <ul>
     *   <li>Timestamped stores: [timestamp(8)][value] → [0x00][timestamp(8)][value]</li>
     *   <li>Window stores: [value] → [0x00][value]</li>
     * </ul>
     *
     * @param value the legacy value bytes (may include timestamp for timestamped stores, or plain value for window stores)
     * @return the value in header-embedded format with empty headers prefix [0x00][value]
     */
    static byte[] convertToHeaderFormat(final byte[] value) {
        if (value == null) {
            return null;
        }

        // Format: [headersSize(varint)][headersBytes][payload]
        // For empty headers:
        //   headersSize = varint(0) = [0x00]
        //   headersBytes = [] (empty, 0 bytes)
        // Result: [0x00][payload]
        final byte[] res = new byte[1 + value.length];
        // res[0] is initialized to 0x00 per Java Specification
        System.arraycopy(value, 0, res, 1, value.length);
        return res;
    }

    static byte[] convertFromPlainToHeaderFormat(final byte[] value) {
        if (value == null) {
            return null;
        }

        // Format: [headersSize(varint)][headersBytes][timestamp(8)][payload]
        // For empty headers and timestamp=-1:
        //   headersSize = varint(0) = [0x00]
        //   headersBytes = [] (empty, 0 bytes)
        //   timestamp = -1 (8 bytes)
        // Result: [0x00][timestamp=-1][payload]
        final byte[] result = new byte[1 + 8 + value.length];
        result[0] = 0x00; // empty headers
        // timestamp = -1 (8 bytes in big-endian)
        result[1] = (byte) 0xFF;
        result[2] = (byte) 0xFF;
        result[3] = (byte) 0xFF;
        result[4] = (byte) 0xFF;
        result[5] = (byte) 0xFF;
        result[6] = (byte) 0xFF;
        result[7] = (byte) 0xFF;
        result[8] = (byte) 0xFF;
        System.arraycopy(value, 0, result, 9, value.length);
        return result;
    }
}
