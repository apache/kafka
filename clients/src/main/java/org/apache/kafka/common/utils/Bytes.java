/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Utility class that handles immutable byte arrays.
 */
public class Bytes implements Comparable<Bytes> {

    /** When we encode strings, we always specify UTF8 encoding */
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;

    private final byte[] bytes;

    // cache the hash code for the string, default to 0
    private int hashCode;

    public static Bytes wrap(byte[] bytes) {
        return new Bytes(bytes);
    }

    /**
     * Create a Bytes using the byte array.
     *
     * @param bytes This array becomes the backing storage for the object.
     */
    public Bytes(byte[] bytes) {
        this.bytes = bytes;

        // initialize hash code to 0
        hashCode = 0;
    }

    /**
     * Get the data from the Bytes.
     * @return The data is only valid between offset and offset+length.
     */
    public byte[] get() {
        return this.bytes;
    }

    /**
     * The hashcode is cached except for the case where it is computed as 0, in which
     * case we compute the hashcode on every call.
     *
     * @return the hashcode
     */
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(bytes);
        }

        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other instanceof Bytes)
            return compareTo((Bytes) other) == 0;

        return false;
    }

    @Override
    public int compareTo(Bytes that) {
        return BYTES_LEXICO_COMPARATOR.compare(this.bytes, that.bytes);
    }

    @Override
    public String toString() {
        return Bytes.toString(bytes, 0, bytes.length);
    }

    /**
     * Convert utf8 encoded bytes into a string. If
     * the given byte array is null, this method will return null.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @param len length of utf-8 sequence
     * @return String made from {@code b} or null
     */
    private static String toString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, UTF8_CHARSET);
    }

    /**
     * A byte array comparator based on lexicograpic ordering.
     */
    public final static Comparator<byte[]> BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

    private interface ByteArrayComparator extends Comparator<byte[]> {

        int compare(final byte[] buffer1, int offset1, int length1,
                    final byte[] buffer2, int offset2, int length2);
    }

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
}
