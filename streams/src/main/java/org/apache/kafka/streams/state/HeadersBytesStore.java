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
     * Converts a legacy timestamped value (ValueAndTimestamp format, without headers) to the header-embedded format.
     * <p>
     * For timestamped stores, the legacy format is: [timestamp(8)][value]
     * The new format is: [headersSize(varint)][headersBytes][timestamp(8)][value]
     * <p>
     * This method adds empty headers to the existing timestamped value format.
     * <p>
     * Empty headers are represented as 0 bytes (headersSize=0, no headersBytes),
     *
     * @param valueAndTimestamp the legacy timestamped value bytes
     * @return the value in header-embedded format with empty headers
     */
    static byte[] convertToHeaderFormat(final byte[] valueAndTimestamp) {
        if (valueAndTimestamp == null) {
            return null;
        }

        // Format: [headersSize(varint)][headersBytes][payload]
        // For empty headers:
        //   headersSize = varint(0) = [0x00]
        //   headersBytes = [] (empty, 0 bytes)
        // Result: [0x00][payload]
        final byte[] res = new byte[1 + valueAndTimestamp.length];
        // res[0] is initialized to 0x00 per Java Specification
        System.arraycopy(valueAndTimestamp, 0, res, 1, valueAndTimestamp.length);
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
