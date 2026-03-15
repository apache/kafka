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

import org.apache.kafka.common.utils.internals.BytesUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 * An immutable wrapper for a byte array.
 *
 * <p>This class provides a convenient way to work with byte arrays in Kafka APIs,
 * particularly in Kafka Streams state stores and serialization. It implements
 * {@link Comparable} to enable ordering of byte arrays.
 *
 * <p>The class caches the hashCode for improved performance when used as keys
 * in hash-based data structures.
 */
public class Bytes implements Comparable<Bytes> {

    public static final byte[] EMPTY = new byte[0];

    private static final char[] HEX_CHARS_UPPER = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private final byte[] bytes;

    // cache the hash code for the string, default to 0
    private int hashCode;

    /**
     * Creates a Bytes instance wrapping the given byte array.
     *
     * <p>The provided array becomes the backing storage for the object.
     *
     * @param bytes the byte array to wrap, or null
     * @return a new Bytes instance, or null if the input is null
     */
    public static Bytes wrap(byte[] bytes) {
        if (bytes == null)
            return null;
        return new Bytes(bytes);
    }

    /**
     * Create a Bytes using the byte array.
     *
     * @param bytes This array becomes the backing storage for the object.
     * @throws NullPointerException if bytes is null
     */
    public Bytes(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes cannot be null");

        // initialize hash code to 0
        hashCode = 0;
    }

    /**
     * Get the data from the Bytes.
     * @return The underlying byte array
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
        if (other == null)
            return false;

        // we intentionally use the function to compute hashcode here
        if (this.hashCode() != other.hashCode())
            return false;

        if (other instanceof Bytes)
            return Arrays.equals(this.bytes, ((Bytes) other).get());

        return false;
    }

    @Override
    public int compareTo(Bytes that) {
        return BytesUtils.BYTES_LEXICO_COMPARATOR.compare(this.bytes, that.bytes);
    }

    @Override
    public String toString() {
        return Bytes.toString(bytes, 0, bytes.length);
    }

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg:
     * \x00 \x05 etc.
     *
     * This function is brought from org.apache.hadoop.hbase.util.Bytes
     *
     * @param b array to write out
     * @param off offset to start at
     * @param len length to write
     * @return string output
     */
    private static String toString(final byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();

        if (b == null)
            return result.toString();

        // just in case we are passed a 'len' that is > buffer length...
        if (off >= b.length)
            return result.toString();

        if (off + len > b.length)
            len = b.length - off;

        for (int i = off; i < off + len; ++i) {
            int ch = b[i] & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\') {
                result.append((char) ch);
            } else {
                result.append("\\x");
                result.append(HEX_CHARS_UPPER[ch / 0x10]);
                result.append(HEX_CHARS_UPPER[ch % 0x10]);
            }
        }
        return result.toString();
    }

    /**
     * Increment the underlying byte array by adding 1.
     *
     * @param input - The byte array to increment
     * @return A new copy of the incremented byte array.
     * @throws IndexOutOfBoundsException if incrementing causes the underlying input byte array to overflow.
     * @deprecated This method is not part of the public API and will be removed in version 5.0.
     *             Internal Kafka code should use {@link org.apache.kafka.common.utils.internals.BytesUtils#increment(Bytes)} instead.
     */
    @Deprecated(since = "4.3", forRemoval = true)
    public static Bytes increment(Bytes input) throws IndexOutOfBoundsException {
        return BytesUtils.increment(input);
    }

    /**
     * A byte array comparator based on lexicographic ordering.
     * @deprecated This field is not part of the public API and will be removed in version 5.0.
     *             Internal Kafka code should use {@link org.apache.kafka.common.utils.internals.BytesUtils#BYTES_LEXICO_COMPARATOR} instead.
     */
    @Deprecated(since = "4.3", forRemoval = true)
    public static final ByteArrayComparator BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

    /**
     * A byte array comparator interface.
     *
     * @deprecated This interface is not part of the public API and will be removed in version 5.0.
     *             Internal Kafka code should use {@link org.apache.kafka.common.utils.internals.BytesUtils.ByteArrayComparator} instead.
     */
    @Deprecated(since = "4.3", forRemoval = true)
    public interface ByteArrayComparator extends Comparator<byte[]>, Serializable {

        int compare(final byte[] buffer1, int offset1, int length1,
                    final byte[] buffer2, int offset2, int length2);
    }

    private static class LexicographicByteArrayComparator extends BytesUtils.LexicographicByteArrayComparator implements ByteArrayComparator {
    }
}
