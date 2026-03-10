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

import org.apache.kafka.common.utils.ByteUtils;

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
    static byte[] rawPlainValue(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // Skip headers and timestamp (8 bytes)
        buffer.position(buffer.position() + headersSize + 8);

        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

}