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

import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;

class OrderedBytes {

    private static final int MIN_KEY_LENGTH = 1;
    /**
     * Returns the upper byte range for a key with a given fixed size maximum suffix
     *
     * Assumes the minimum key length is one byte
     */
    static Bytes upperRange(Bytes key, byte[] maxSuffix) {
        final byte[] bytes = key.get();
        ByteBuffer rangeEnd = ByteBuffer.allocate(bytes.length + maxSuffix.length);

        int i = 0;
        while (i < bytes.length && (
            i < MIN_KEY_LENGTH // assumes keys are at least one byte long
            || (bytes[i] & 0xFF) >= (maxSuffix[0] & 0xFF)
            )) {
            rangeEnd.put(bytes[i++]);
        }

        rangeEnd.put(maxSuffix);
        rangeEnd.flip();

        byte[] res = new byte[rangeEnd.remaining()];
        ByteBuffer.wrap(res).put(rangeEnd);
        return Bytes.wrap(res);
    }

    static Bytes lowerRange(Bytes key, byte[] minSuffix) {
        final byte[] bytes = key.get();
        ByteBuffer rangeStart = ByteBuffer.allocate(bytes.length + minSuffix.length);
        // any key in the range would start at least with the given prefix to be
        // in the range, and have at least SUFFIX_SIZE number of trailing zero bytes.

        // unless there is a maximum key length, you can keep appending more zero bytes
        // to keyFrom to create a key that will match the range, yet that would precede
        // WindowStoreUtils.toBinaryKey(keyFrom, from, 0) in byte order
        return Bytes.wrap(
            rangeStart
                .put(bytes)
                .put(minSuffix)
                .array()
        );
    }
}
