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
package org.apache.kafka.common.hash;

/**
 * This class was taken from Hive org.apache.hive.common.util;
 * https://github.com/apache/hive/blob/master/storage-api/src/java/org/apache/hive/common/util/Murmur3.java
 * Commit: dffa3a16588bc8e95b9d0ab5af295a74e06ef702
 *
 *
 * Murmur3 is successor to Murmur2 fast non-crytographic hash algorithms.
 *
 * Murmur3 32 and 128 bit variants.
 * 32-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#94
 * 128-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#255
 *
 * This is a public domain code with no copyrights.
 * From homepage of MurmurHash (https://code.google.com/p/smhasher/),
 * "All MurmurHash versions are public domain software, and the author disclaims all copyright
 * to their code."
 */
@SuppressWarnings("fallthrough")
public class Murmur3 {
    // from 64-bit linear congruential generator
    public static final long NULL_HASHCODE = 2862933555777941757L;

    // Constants for 32 bit variant
    private static final int C1_32 = 0xcc9e2d51;
    private static final int C2_32 = 0x1b873593;
    private static final int R1_32 = 15;
    private static final int R2_32 = 13;
    private static final int M_32 = 5;
    private static final int N_32 = 0xe6546b64;

    // Constants for 128 bit variant
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int R3 = 33;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;
    private static final int N2 = 0x38495ab5;

    public static final int DEFAULT_SEED = 104729;

    public static int hash32(long l0, long l1) {
        return hash32(l0, l1, DEFAULT_SEED);
    }

    public static int hash32(long l0) {
        return hash32(l0, DEFAULT_SEED);
    }

    /**
     * Murmur3 32-bit variant.
     */
    public static int hash32(long l0, int seed) {
        int hash = seed;
        final long r0 = Long.reverseBytes(l0);

        hash = mix32((int) r0, hash);
        hash = mix32((int) (r0 >>> 32), hash);

        return fmix32(Long.BYTES, hash);
    }

    /**
     * Murmur3 32-bit variant.
     */
    public static int hash32(long l0, long l1, int seed) {
        int hash = seed;
        final long r0 = Long.reverseBytes(l0);
        final long r1 = Long.reverseBytes(l1);

        hash = mix32((int) r0, hash);
        hash = mix32((int) (r0 >>> 32), hash);
        hash = mix32((int) (r1), hash);
        hash = mix32((int) (r1 >>> 32), hash);

        return fmix32(Long.BYTES * 2, hash);
    }

    /**
     * Murmur3 32-bit variant.
     *
     * @param data - input byte array
     * @return - hashcode
     */
    public static int hash32(byte[] data) {
        return hash32(data, 0, data.length, DEFAULT_SEED);
    }

    /**
     * Murmur3 32-bit variant.
     *
     * @param data - input byte array
     * @param length - length of array
     * @return - hashcode
     */
    public static int hash32(byte[] data, int length) {
        return hash32(data, 0, length, DEFAULT_SEED);
    }

    /**
     * Murmur3 32-bit variant.
     *
     * @param data   - input byte array
     * @param length - length of array
     * @param seed   - seed. (default 0)
     * @return - hashcode
     */
    public static int hash32(byte[] data, int length, int seed) {
        return hash32(data, 0, length, seed);
    }

    /**
     * Murmur3 32-bit variant.
     *
     * @param data   - input byte array
     * @param offset - offset of data
     * @param length - length of array
     * @param seed   - seed. (default 0)
     * @return - hashcode
     */
    public static int hash32(byte[] data, int offset, int length, int seed) {
        int hash = seed;
        final int nblocks = length >> 2;

        // body
        for (int i = 0; i < nblocks; i++) {
            int i_4 = i << 2;
            int k = (data[offset + i_4] & 0xff)
                    | ((data[offset + i_4 + 1] & 0xff) << 8)
                    | ((data[offset + i_4 + 2] & 0xff) << 16)
                    | ((data[offset + i_4 + 3] & 0xff) << 24);

            hash = mix32(k, hash);
        }

        // tail
        int idx = nblocks << 2;
        int k1 = 0;
        switch (length - idx) {
            case 3:
                k1 ^= data[offset + idx + 2] << 16;
            case 2:
                k1 ^= data[offset + idx + 1] << 8;
            case 1:
                k1 ^= data[offset + idx];

                // mix functions
                k1 *= C1_32;
                k1 = Integer.rotateLeft(k1, R1_32);
                k1 *= C2_32;
                hash ^= k1;
        }

        return fmix32(length, hash);
    }

    private static int mix32(int k, int hash) {
        k *= C1_32;
        k = Integer.rotateLeft(k, R1_32);
        k *= C2_32;
        hash ^= k;
        return Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
    }

    private static int fmix32(int length, int hash) {
        hash ^= length;
        hash ^= (hash >>> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >>> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >>> 16);

        return hash;
    }

    /**
     * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
     *
     * @param data - input byte array
     * @return - hashcode
     */
    public static long hash64(byte[] data) {
        return hash64(data, 0, data.length, DEFAULT_SEED);
    }

    public static long hash64(long data) {
        long hash = DEFAULT_SEED;
        long k = Long.reverseBytes(data);
        int length = Long.BYTES;
        // mix functions
        k *= C1;
        k = Long.rotateLeft(k, R1);
        k *= C2;
        hash ^= k;
        hash = Long.rotateLeft(hash, R2) * M + N1;
        // finalization
        hash ^= length;
        hash = fmix64(hash);
        return hash;
    }

    public static long hash64(int data) {
        long k1 = Integer.reverseBytes(data) & (-1L >>> 32);
        int length = Integer.BYTES;
        long hash = DEFAULT_SEED;
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        hash ^= k1;
        // finalization
        hash ^= length;
        hash = fmix64(hash);
        return hash;
    }

    public static long hash64(short data) {
        long hash = DEFAULT_SEED;
        long k1 = 0;
        k1 ^= ((long) data & 0xff) << 8;
        k1 ^= ((long)((data & 0xFF00) >> 8) & 0xff);
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        hash ^= k1;

        // finalization
        hash ^= Short.BYTES;
        hash = fmix64(hash);
        return hash;
    }

    public static long hash64(byte[] data, int offset, int length) {
        return hash64(data, offset, length, DEFAULT_SEED);
    }

    /**
     * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
     *
     * @param data   - input byte array
     * @param length - length of array
     * @param seed   - seed. (default is 0)
     * @return - hashcode
     */
    public static long hash64(byte[] data, int offset, int length, int seed) {
        long hash = seed;
        final int nblocks = length >> 3;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int i8 = i << 3;
            long k = ((long) data[offset + i8] & 0xff)
                    | (((long) data[offset + i8 + 1] & 0xff) << 8)
                    | (((long) data[offset + i8 + 2] & 0xff) << 16)
                    | (((long) data[offset + i8 + 3] & 0xff) << 24)
                    | (((long) data[offset + i8 + 4] & 0xff) << 32)
                    | (((long) data[offset + i8 + 5] & 0xff) << 40)
                    | (((long) data[offset + i8 + 6] & 0xff) << 48)
                    | (((long) data[offset + i8 + 7] & 0xff) << 56);

            // mix functions
            k *= C1;
            k = Long.rotateLeft(k, R1);
            k *= C2;
            hash ^= k;
            hash = Long.rotateLeft(hash, R2) * M + N1;
        }

        // tail
        long k1 = 0;
        int tailStart = nblocks << 3;
        switch (length - tailStart) {
            case 7:
                k1 ^= ((long) data[offset + tailStart + 6] & 0xff) << 48;
            case 6:
                k1 ^= ((long) data[offset + tailStart + 5] & 0xff) << 40;
            case 5:
                k1 ^= ((long) data[offset + tailStart + 4] & 0xff) << 32;
            case 4:
                k1 ^= ((long) data[offset + tailStart + 3] & 0xff) << 24;
            case 3:
                k1 ^= ((long) data[offset + tailStart + 2] & 0xff) << 16;
            case 2:
                k1 ^= ((long) data[offset + tailStart + 1] & 0xff) << 8;
            case 1:
                k1 ^= ((long) data[offset + tailStart] & 0xff);
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                hash ^= k1;
        }

        // finalization
        hash ^= length;
        hash = fmix64(hash);

        return hash;
    }

    /**
     * Murmur3 128-bit variant.
     *
     * @param data - input byte array
     * @return - hashcode (2 longs)
     */
    public static long[] hash128(byte[] data) {
        return hash128(data, 0, data.length, DEFAULT_SEED);
    }

    /**
     * Murmur3 128-bit variant.
     *
     * @param data   - input byte array
     * @param offset - the first element of array
     * @param length - length of array
     * @param seed   - seed. (default is 0)
     * @return - hashcode (2 longs)
     */
    public static long[] hash128(byte[] data, int offset, int length, int seed) {
        long h1 = seed;
        long h2 = seed;
        final int nblocks = length >> 4;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int i16 = i << 4;
            long k1 = ((long) data[offset + i16] & 0xff)
                    | (((long) data[offset + i16 + 1] & 0xff) << 8)
                    | (((long) data[offset + i16 + 2] & 0xff) << 16)
                    | (((long) data[offset + i16 + 3] & 0xff) << 24)
                    | (((long) data[offset + i16 + 4] & 0xff) << 32)
                    | (((long) data[offset + i16 + 5] & 0xff) << 40)
                    | (((long) data[offset + i16 + 6] & 0xff) << 48)
                    | (((long) data[offset + i16 + 7] & 0xff) << 56);

            long k2 = ((long) data[offset + i16 + 8] & 0xff)
                    | (((long) data[offset + i16 + 9] & 0xff) << 8)
                    | (((long) data[offset + i16 + 10] & 0xff) << 16)
                    | (((long) data[offset + i16 + 11] & 0xff) << 24)
                    | (((long) data[offset + i16 + 12] & 0xff) << 32)
                    | (((long) data[offset + i16 + 13] & 0xff) << 40)
                    | (((long) data[offset + i16 + 14] & 0xff) << 48)
                    | (((long) data[offset + i16 + 15] & 0xff) << 56);

            // mix functions for k1
            k1 *= C1;
            k1 = Long.rotateLeft(k1, R1);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, R2);
            h1 += h2;
            h1 = h1 * M + N1;

            // mix functions for k2
            k2 *= C2;
            k2 = Long.rotateLeft(k2, R3);
            k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, R1);
            h2 += h1;
            h2 = h2 * M + N2;
        }

        // tail
        long k1 = 0;
        long k2 = 0;
        int tailStart = nblocks << 4;
        switch (length - tailStart) {
            case 15:
                k2 ^= (long) (data[offset + tailStart + 14] & 0xff) << 48;
            case 14:
                k2 ^= (long) (data[offset + tailStart + 13] & 0xff) << 40;
            case 13:
                k2 ^= (long) (data[offset + tailStart + 12] & 0xff) << 32;
            case 12:
                k2 ^= (long) (data[offset + tailStart + 11] & 0xff) << 24;
            case 11:
                k2 ^= (long) (data[offset + tailStart + 10] & 0xff) << 16;
            case 10:
                k2 ^= (long) (data[offset + tailStart + 9] & 0xff) << 8;
            case 9:
                k2 ^= (long) (data[offset + tailStart + 8] & 0xff);
                k2 *= C2;
                k2 = Long.rotateLeft(k2, R3);
                k2 *= C1;
                h2 ^= k2;

            case 8:
                k1 ^= (long) (data[offset + tailStart + 7] & 0xff) << 56;
            case 7:
                k1 ^= (long) (data[offset + tailStart + 6] & 0xff) << 48;
            case 6:
                k1 ^= (long) (data[offset + tailStart + 5] & 0xff) << 40;
            case 5:
                k1 ^= (long) (data[offset + tailStart + 4] & 0xff) << 32;
            case 4:
                k1 ^= (long) (data[offset + tailStart + 3] & 0xff) << 24;
            case 3:
                k1 ^= (long) (data[offset + tailStart + 2] & 0xff) << 16;
            case 2:
                k1 ^= (long) (data[offset + tailStart + 1] & 0xff) << 8;
            case 1:
                k1 ^= (long) (data[offset + tailStart] & 0xff);
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        return new long[]{h1, h2};
    }

    private static long fmix64(long h) {
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }

    public static class IncrementalHash32 {
        byte[] tail = new byte[3];
        int tailLen;
        int totalLen;
        int hash;

        public final void start(int hash) {
            tailLen = totalLen = 0;
            this.hash = hash;
        }

        public final void add(byte[] data, int offset, int length) {
            if (length == 0) return;
            totalLen += length;
            if (tailLen + length < 4) {
                System.arraycopy(data, offset, tail, tailLen, length);
                tailLen += length;
                return;
            }
            int offset2 = 0;
            if (tailLen > 0) {
                offset2 = (4 - tailLen);
                int k = -1;
                switch (tailLen) {
                    case 1:
                        k = orBytes(tail[0], data[offset], data[offset + 1], data[offset + 2]);
                        break;
                    case 2:
                        k = orBytes(tail[0], tail[1], data[offset], data[offset + 1]);
                        break;
                    case 3:
                        k = orBytes(tail[0], tail[1], tail[2], data[offset]);
                        break;
                    default: throw new AssertionError(tailLen);
                }
                // mix functions
                k *= C1_32;
                k = Integer.rotateLeft(k, R1_32);
                k *= C2_32;
                hash ^= k;
                hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
            }
            int length2 = length - offset2;
            offset += offset2;
            final int nblocks = length2 >> 2;

            for (int i = 0; i < nblocks; i++) {
                int i_4 = (i << 2) + offset;
                int k = orBytes(data[i_4], data[i_4 + 1], data[i_4 + 2], data[i_4 + 3]);

                // mix functions
                k *= C1_32;
                k = Integer.rotateLeft(k, R1_32);
                k *= C2_32;
                hash ^= k;
                hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
            }

            int consumed = (nblocks << 2);
            tailLen = length2 - consumed;
            if (consumed == length2) return;
            System.arraycopy(data, offset + consumed, tail, 0, tailLen);
        }

        public final int end() {
            int k1 = 0;
            switch (tailLen) {
                case 3:
                    k1 ^= tail[2] << 16;
                case 2:
                    k1 ^= tail[1] << 8;
                case 1:
                    k1 ^= tail[0];

                    // mix functions
                    k1 *= C1_32;
                    k1 = Integer.rotateLeft(k1, R1_32);
                    k1 *= C2_32;
                    hash ^= k1;
            }

            // finalization
            hash ^= totalLen;
            hash ^= (hash >>> 16);
            hash *= 0x85ebca6b;
            hash ^= (hash >>> 13);
            hash *= 0xc2b2ae35;
            hash ^= (hash >>> 16);
            return hash;
        }
    }

    private static int orBytes(byte b1, byte b2, byte b3, byte b4) {
        return (b1 & 0xff) | ((b2 & 0xff) << 8) | ((b3 & 0xff) << 16) | ((b4 & 0xff) << 24);
    }
}
