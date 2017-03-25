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

package org.apache.kafka.common.utils;

import java.nio.ByteBuffer;

/**
 *
 */
public class Crc32C {

    /**
     * Compute the CRC32C (Castagnoli) of the segment of the byte array given by the specified size and offset
     *
     * @param bytes The bytes to checksum
     * @param offset the offset at which to begin the checksum computation
     * @param size the number of bytes to checksum
     * @return The CRC32C
     */
    public static long compute(byte[] bytes, int offset, int size) {
        AbstractChecksum crc = create();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Compute the CRC32C (Castagnoli) of a byte buffer from a given offset (relative to the buffer's current position)
     *
     * @param buffer The buffer with the underlying data
     * @param offset The offset relative to the current position
     * @param size The number of bytes beginning from the offset to include
     * @return The CRC32C
     */
    public static long compute(ByteBuffer buffer, int offset, int size) {
        AbstractChecksum crc = create();
        crc.update(buffer, offset, size);
        return crc.getValue();
    }

    public static AbstractChecksum create() {
        return new PureJavaCrc32C();
    }
}
