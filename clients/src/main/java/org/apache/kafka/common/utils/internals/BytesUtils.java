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
package org.apache.kafka.common.utils.internals;

import org.apache.kafka.common.utils.Bytes;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Internal utility class for Bytes-related operations.
 * This class is for internal Kafka use only and is not part of the public API.
 */
public final class BytesUtils {

    private BytesUtils() {
        // Utility class, prevent instantiation
    }

    /**
     * Increment the underlying byte array by adding 1.
     *
     * @param input - The byte array to increment
     * @return A new copy of the incremented byte array
     * @throws IndexOutOfBoundsException if incrementing causes the underlying input byte array to overflow
     */
    public static Bytes increment(Bytes input) throws IndexOutOfBoundsException {
        byte[] inputArr = input.get();
        byte[] ret = new byte[inputArr.length];
        int carry = 1;
        for (int i = inputArr.length - 1; i >= 0; i--) {
            if (inputArr[i] == (byte) 0xFF && carry == 1) {
                ret[i] = (byte) 0x00;
            } else {
                ret[i] = (byte) (inputArr[i] + carry);
                carry = 0;
            }
        }
        if (carry == 0) {
            return Bytes.wrap(ret);
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * A byte array comparator based on lexicographic ordering.
     */
    public static final ByteArrayComparator BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

    public interface ByteArrayComparator extends Comparator<byte[]> {

        int compare(final byte[] buffer1, int offset1, int length1,
                    final byte[] buffer2, int offset2, int length2);
    }

    public static class LexicographicByteArrayComparator implements ByteArrayComparator {

        @Override
        public int compare(byte[] buffer1, byte[] buffer2) {
            return compare(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
        }

        public int compare(final byte[] buffer1, int offset1, int length1,
                           final byte[] buffer2, int offset2, int length2) {

            // short circuit equal case
            if (buffer1 == buffer2 &&
                    offset1 == offset2 &&
                    length1 == length2) {
                return 0;
            }

            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            return Arrays.compareUnsigned(buffer1, offset1, end1, buffer2, offset2, end2);
        }
    }
}
