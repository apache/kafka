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
import java.util.Comparator;

/**
 * Utility class that handles immutable byte arrays.
 */
public class Bytes implements Comparable<Bytes> {

    private final byte[] bytes;

    private final int offset;

    private final int length;

    private int hashCode;

    private boolean hashCodeComputed;

    public static Bytes wrap(byte[] bytes) {
        return new Bytes(bytes);
    }

    /**
     * Create a Bytes using the byte array.
     *
     * @param bytes This array becomes the backing storage for the object.
     */
    public Bytes(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    /**
     * Create a Bytes using the byte array.
     *
     * @param bytes the byte array to set to
     * @param offset the offset in {@code bytes} to start at
     * @param length the number of bytes in {@code bytes}
     */
    public Bytes(final byte[] bytes, final int offset, final int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;

        hashCodeComputed = false;
    }

    /**
     * Get the data from the Bytes.
     * @return The data is only valid between offset and offset+length.
     */
    public byte[] get() {
        return this.bytes;
    }

    @Override
    public int hashCode() {
        if (!hashCodeComputed) {
            hashCode = Bytes.hashCode(bytes, offset, length);
            hashCodeComputed = true;
        }

        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof byte[]) {
            return compareTo((byte[]) other) == 0;
        }
        if (other instanceof Bytes) {
            return compareTo((Bytes) other) == 0;
        }
        return false;
    }

    @Override
    public int compareTo(Bytes that) {
        return ((ByteArrayComparator) BYTES_LEXICO_COMPARATOR).compare(
                this.bytes, this.offset, this.length,
                that.bytes, that.offset, that.length);
    }

    /**
     * Compares the bytes in this object to the specified byte array following lexicograpic ordering.
     *
     * @param that the comparing byte array
     * @return Positive if left is bigger than right, 0 if they are equal, and
     *         negative if left is smaller than right
     */
    public int compareTo(final byte[] that) {
        return ((ByteArrayComparator) BYTES_LEXICO_COMPARATOR).compare(
                this.bytes, this.offset, this.length,
                that, 0, that.length);
    }

    @Override
    public String toString() {
        return Bytes.toString(bytes, offset, length);
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

    /** When we encode strings, we always specify UTF8 encoding */
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;

    /**
     * Compute the hashcode of a given byte array following Arrays.hashCode(bytes)
     * while considers {@code offset} and {@code length}.
     *
     * @param bytes array to hash
     * @param offset offset to start from
     * @param length length to hash
     * */
    private static int hashCode(byte[] bytes, int offset, int length) {
        if (bytes == null)
            return 0;

        int hash = 1;
        for (int i = offset; i < offset + length; i++)
            hash = (31 * hash) + bytes[i];

        return hash;
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
                int a = (buffer1[i] & 0xff);
                int b = (buffer2[j] & 0xff);
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }
    }


}
