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

public abstract class AbstractChecksum implements Checksum {

    public final void update(ByteBuffer buffer, int length) {
        update(buffer, 0, length);
    }

    public final void update(ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            update(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, length);
        } else {
            int start = buffer.position() + offset;
            for (int i = start; i < start + length; i++)
                update(buffer.get(i));
        }
    }

    /**
     * Update the CRC32 given an integer
     */
    public final void updateInt(int input) {
        update((byte) (input >> 24));
        update((byte) (input >> 16));
        update((byte) (input >> 8));
        update((byte) input /* >> 0 */);
    }

    public final void updateLong(long input) {
        update((byte) (input >> 56));
        update((byte) (input >> 48));
        update((byte) (input >> 40));
        update((byte) (input >> 32));
        update((byte) (input >> 24));
        update((byte) (input >> 16));
        update((byte) (input >> 8));
        update((byte) input /* >> 0 */);
    }

}
