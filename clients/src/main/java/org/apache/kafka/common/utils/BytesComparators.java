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

import java.io.Serializable;
import java.util.Comparator;

public final class BytesComparators {
    public interface ByteArrayComparator extends Comparator<byte[]>, Serializable {
        int compare(final byte[] buffer1, int offset1, int length1,
                    final byte[] buffer2, int offset2, int length2);
    }

    /**
     * A byte array comparator based on lexicograpic ordering.
     */
    public final static ByteArrayComparator BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

    /**
     * A byte array comparator used on lexicographc ordering, but only comparing prefixes.
     * i.e. 0001     == 00010003   (only first 4 bytes are compared)
     *      00010003 == 0001       (only first 4 bytes are compared)
     */
    public final static ByteArrayComparator PREFIX_BYTES_LEXICO_COMPARATOR = new LexicographicPrefixByteArrayComparator();

    private static class LexicographicByteArrayComparator implements ByteArrayComparator {

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

            // similar to Arrays.compare() but considers offset and length
            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }
    }

    private static class LexicographicPrefixByteArrayComparator extends LexicographicByteArrayComparator {
        public int compare(final byte[] buffer1, int offset1, int length1,
                           final byte[] buffer2, int offset2, int length2) {
            // short circuit equal case
            if (buffer1 == buffer2 &&
                offset1 == offset2 &&
                length1 == length2) {
                return 0;
            }

            // similar to Arrays.compare() but considers offset and length
            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;
                if (a != b) {
                    return a - b;
                }
            }

            // If above comparison passed, then both prefixes are equals
            return 0;
        }
    }
}
