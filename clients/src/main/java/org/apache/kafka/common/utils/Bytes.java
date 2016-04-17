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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

/**
 * Utility class that handles byte arrays.
 *
 * Its implementation is inspired by com.google.common.primitives.UnsignedBytes
 */
public class Bytes {

    /** When we encode strings, we always specify UTF8 encoding */
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;

    /**
     * A byte array comparator based on lexicograpic ordering.
     */
    public final static Comparator<byte[]> BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

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

            // bring WritableComparator code local
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

    /**
     * Lexicographically compare two arrays.
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static int compareTo(final byte[] buffer1, final byte[] buffer2) {

        return ((ByteArrayComparator) BYTES_LEXICO_COMPARATOR).compare(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
    }

    /**
     * Lexicographically compare two arrays.
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @param offset1 Where to start comparing in the left buffer
     * @param offset2 Where to start comparing in the right buffer
     * @param length1 How much to compare from the left buffer
     * @param length2 How much to compare from the right buffer
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static int compareTo(final byte[] buffer1, final int offset1, final int length1,
                                final byte[] buffer2, final int offset2, final int length2) {

        return ((ByteArrayComparator) BYTES_LEXICO_COMPARATOR).compare(buffer1, offset1, length1, buffer2, offset2, length2);
    }

    /**
     * Check if two byte arrays are lexicographically equal to each other.
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static boolean equals(final byte[] buffer1, final byte[] buffer2) {
        // short circuit case, other shortcuts are in the other equals function
        if (buffer1 == buffer2) return true;

        return equals(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
    }

    /**
     * Check if two byte arrays are lexicographically equal to each other.
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @param offset1 Where to start comparing in the left buffer
     * @param offset2 Where to start comparing in the right buffer
     * @param length1 How much to compare from the left buffer
     * @param length2 How much to compare from the right buffer
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static boolean equals(final byte[] buffer1, int offset1, int length1,
                                 final byte[] buffer2, int offset2, int length2) {
        // short circuit case
        if (buffer1 == null || buffer2 == null)
            return false;
        if (buffer1 == buffer2 &&
                offset1 == offset2 &&
                length1 == length2)
            return true;
        if (length1 != length2)
            return false;
        if (length1 == 0)
            return true;

        // since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (buffer1[offset1 + length1 - 1] != buffer2[offset2 + length2 - 1]) return false;

        return ((ByteArrayComparator) BYTES_LEXICO_COMPARATOR).compare(buffer1, offset1, length1, buffer2, offset2, length2) == 0;
    }

    /**
     * Returns a new byte array, copied from the given {@code buf},
     * from the position (inclusive) to the limit (exclusive).
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return the byte array
     * @see #toBytes(ByteBuffer)
     */
    public static byte[] getBytes(ByteBuffer buf) {
        byte [] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    /**
     * Returns a new byte array, copied from the given {@code buf},
     * from the index 0 (inclusive) to the limit (exclusive),
     * regardless of the current position.
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return the byte array
     * @see #getBytes(ByteBuffer)
     */
    public static byte[] toBytes(ByteBuffer buf) {
        ByteBuffer dup = buf.duplicate();
        dup.position(0);
        return getBytes(dup);
    }

    /**
     * Converts the given byte buffer to a printable representation,
     * from the index 0 (inclusive) to the limit (exclusive),
     * regardless of the current position.
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return a string representation of the buffer's binary contents
     * @see #toBytes(ByteBuffer)
     * @see #getBytes(ByteBuffer)
     */
    public static String toStringBinary(ByteBuffer buf) {
        if (buf == null)
            return "null";
        if (buf.hasArray()) {
            return toStringBinary(buf.array(), buf.arrayOffset(), buf.limit());
        }
        return toStringBinary(toBytes(buf));
    }

    private static final char[] HEX_CHARS_UPPER = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    private static final char[] HEX_CHARS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    /**
     * Convert utf8 encoded bytes into a string.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @return String made from {@code b}
     */
    public static String toString(final byte[] b) {
        return toString(b, 0, b.length);
    }

    /**
     * Convert utf8 encoded bytes into a string. Different from {@link #toString(byte[])} that
     * non-printable characters are hex escaped in the format \\x%02X, eg: \x00 \x05 etc.
     *
     * @see #toString()
     * @see #toStringBinary(byte[], int, int)
     */
    public static String toStringBinary(final byte[] b) {
        return toStringBinary(b, 0, b.length);
    }

    /**
     * Convert utf8 encoded bytes into a string.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @return String made from {@code b} or null
     */
    public static String toString(final byte[] b, int off) {
        return toString(b, off, b.length - off);
    }

    /**
     * Convert utf8 encoded bytes into a string. Different from {@link #toString(byte[])} that
     * non-printable characters are hex escaped in the format \\x%02X, eg: \x00 \x05 etc.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @return String made from {@code b} or null
     */
    public static String toStringBinary(final byte[] b, int off) {
        return toStringBinary(b, off, b.length - off);
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
    public static String toString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, UTF8_CHARSET);
    }

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg:
     * \x00 \x05 etc.
     *
     * @param b array to write out
     * @param off offset to start at
     * @param len length to write
     * @return string output
     */
    public static String toStringBinary(final byte [] b, int off, int len) {
        StringBuilder result = new StringBuilder();

        if (b == null)
            return result.toString();

        // just in case we are passed a 'len' that is > buffer length...
        if (off >= b.length)
            return result.toString();

        if (off + len > b.length)
            len = b.length - off;

        for (int i = off; i < off + len ; ++i) {
            int ch = b[i] & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\') {
                result.append((char)ch);
            } else {
                result.append("\\x");
                result.append(HEX_CHARS_UPPER[ch / 0x10]);
                result.append(HEX_CHARS_UPPER[ch % 0x10]);
            }
        }
        return result.toString();
    }

    /**
     * Convert a byte array into a hex string.
     */
    public static String toHex(byte[] b, int offset, int length) {
        int numChars = length * 2;
        char[] ch = new char[numChars];
        for (int i = 0; i < numChars; i += 2)
        {
            byte d = b[offset + i/2];
            ch[i] = HEX_CHARS[(d >> 4) & 0x0F];
            ch[i+1] = HEX_CHARS[d & 0x0F];
        }
        return new String(ch);
    }

    /**
     * Convert a byte array into a hex string.
     */
    public static String toHex(byte[] b) {
        return toHex(b, 0, b.length);
    }

    private static boolean isHexDigit(char c) {
        return (c >= 'A' && c <= 'F') ||
                (c >= '0' && c <= '9');
    }

    /**
     * Takes a ASCII digit in the range A-F0-9 and returns
     * the corresponding integer/ordinal value.
     *
     * @param ch  The hex digit.
     * @return The converted hex value as a byte.
     */
    public static byte toBinaryFromHex(byte ch) {
        if (ch >= 'A' && ch <= 'F')
            return (byte) ((byte)10 + (byte) (ch - 'A'));
        // else
        return (byte) (ch - '0');
    }

    public static byte[] toBytesBinary(String in) {
        // this may be bigger than we need, but let's be safe.
        byte [] b = new byte[in.length()];
        int size = 0;
        for (int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i+1 && in.charAt(i+1) == 'x') {
                // ok, take next 2 hex digits.
                char hd1 = in.charAt(i+2);
                char hd2 = in.charAt(i+3);

                // they need to be A-F0-9:
                if (!isHexDigit(hd1) ||
                        !isHexDigit(hd2)) {
                    // bogus escape code, ignore:
                    continue;
                }
                // turn hex ASCII digit -> number
                byte d = (byte) ((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));

                b[size++] = d;
                i += 3; // skip 3
            } else {
                b[size++] = (byte) ch;
            }
        }
        // resize:
        byte [] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }

    /**
     * Create a byte array from a string of hash digits. The length of the
     * string must be a multiple of 2.
     *
     * @param hex The string in hex format
     */
    public static byte[] fromHex(String hex) {
        int len = hex.length();
        byte[] b = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            b[i / 2] = hexCharsToByte(hex.charAt(i),hex.charAt(i+1));
        }
        return b;
    }

    private static byte hexCharsToByte(char c1, char c2) {
        return (byte) ((hexCharToNibble(c1) << 4) | hexCharToNibble(c2));
    }

    private static int hexCharToNibble(char ch) {
        if (ch <= '9' && ch >= '0') {
            return ch - '0';
        } else if (ch >= 'a' && ch <= 'f') {
            return ch - 'a' + 10;
        } else if (ch >= 'A' && ch <= 'F') {
            return ch - 'A' + 10;
        }
        throw new IllegalArgumentException("Invalid hex char: " + ch);
    }
}
