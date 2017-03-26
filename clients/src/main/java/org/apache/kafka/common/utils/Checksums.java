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
import java.util.zip.Checksum;

public final class Checksums {

    private Checksums() {
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int length) {
        update(checksum, buffer, 0, length);
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, length);
        } else {
            int start = buffer.position() + offset;
            for (int i = start; i < start + length; i++)
                checksum.update(buffer.get(i));
        }
    }

    /**
     * Update the CRC32 given an integer
     */
    public static void updateInt(Checksum checksum, int input) {
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input /* >> 0 */);
    }

    public static void updateLong(Checksum checksum, long input) {
        checksum.update((byte) (input >> 56));
        checksum.update((byte) (input >> 48));
        checksum.update((byte) (input >> 40));
        checksum.update((byte) (input >> 32));
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input /* >> 0 */);
    }
}
