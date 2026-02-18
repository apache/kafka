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

import java.nio.ByteBuffer;

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
        return ByteBuffer
            .allocate(1 + valueAndTimestamp.length)
            .put((byte) 0x00)
            .put(valueAndTimestamp)
            .array();
    }
}
