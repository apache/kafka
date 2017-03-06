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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This classes exposes low-level methods for reading/writing
 */
public final class ByteUtils {

    private ByteUtils() {}

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     *
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return in.read()
                | (in.read() << 8)
                | (in.read() << 16)
                | (in.read() << 24);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset] << 0 & 0xff)
                | ((buffer[offset + 1] & 0xff) << 8)
                | ((buffer[offset + 2] & 0xff) << 16)
                | ((buffer[offset + 3] & 0xff) << 24);
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     *
     * @param out The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value);
        out.write(value >>> 8);
        out.write(value >>> 16);
        out.write(value >>> 24);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3]   = (byte) (value >>> 24);
    }

    /**
     * Read an unsigned integer stored in variable-length format from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this integer.
     *
     * @param in The input to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     *
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static long readUnsignedVarInt(DataInput in) throws IOException {
        return readRawUnsignedVarInt(in) & 0xffffffffL;
    }

    private static int readRawUnsignedVarInt(DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    /**
     * Read an unsigned integer stored in variable-length format from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this integer.
     *
     * @param buffer The buffer to read from
     * @return The integer treated as a long
     */
    public static long readVarUnsignedInt(ByteBuffer buffer) {
        return readRawVarUnsignedInt(buffer) & 0xffffffffL;
    }

    private static int readRawVarUnsignedInt(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    /**
     * Read an unsigned long stored in variable-length format from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this long.
     *
     * @param in The input to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     *
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    private static long readRawVarUnsignedLong(DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80L) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    /**
     * Read an unsigned long stored in variable-length format from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this long.
     *
     * @param buffer The buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     *
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     */
    private static long readRawVarUnsignedLong(ByteBuffer buffer) {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80L) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    /**
     * Read an integer stored in variable-length format using Zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">.
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this integer.
     *
     * @param buffer The buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     *
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     */
    public static int readVarInt(ByteBuffer buffer) {
        int raw = readRawVarUnsignedInt(buffer);
        // This undoes the trick in writeSignedVarInt()
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));
    }

    /**
     * Read an integer stored in variable-length format using Zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">.
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this integer.
     *
     * @param in The input to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     *
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readVarInt(DataInput in) throws IOException {
        int raw = readRawUnsignedVarInt(in);
        // This undoes the trick in writeSignedVarInt()
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));
    }

    /**
     * Read a long stored in variable-length format using Zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this long.
     *
     * @param in The input to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     *
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static long readVarLong(DataInput in) throws IOException {
        long raw = readRawVarUnsignedLong(in);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    /**
     * Read a long stored in variable-length format using Zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Also update the index to indicate how many bytes
     * were used to encode this long.
     *
     * @param buffer The buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     *
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     */
    public static long readVarLong(ByteBuffer buffer)  {
        long raw = readRawVarUnsignedLong(buffer);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    /**
     * Write the given unsigned integer following the variable-length from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a> into the output. Since it the value is not negative Zig-zag is not used.
     *
     * @param longValue The value to write
     * @param out The output to write to
     */
    public static void writeVarUnsignedInt(long longValue, DataOutput out) throws IOException {
        int value = (int) longValue;
        while ((value & 0xffffff80) != 0L) {
            out.writeByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value & 0x7f);
    }

    /**
     * Write the given unsigned integer following the variable-length from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a> into the output. Since the value is not negative, Zig-zag is not used.
     *
     * @param longValue The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarUnsignedInt(long longValue, ByteBuffer buffer) {
        int value = (int) longValue;
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) (value & 0x7f));
    }

    /**
     * Write the given integer following the variable-length from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a> into the output. Zig-zag encoding is not used.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarInt(int value, DataOutput out) throws IOException {
        writeVarUnsignedInt((value << 1) ^ (value >> 31), out);
    }

    /**
     * Write the given integer following the variable-length from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a> into the output. Zig-zag encoding is not used.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeVarInt(int value, ByteBuffer buffer) {
        writeVarUnsignedInt((value << 1) ^ (value >> 31), buffer);
    }

    /**
     * Write the given integer following the variable-length from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a> into the output. Zig-zag encoding is not used.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarLong(long value, DataOutput out) throws IOException {
        value = (value << 1) ^ (value >> 63);
        while ((value & 0xffffffffffffff80L) != 0L) {
            out.writeByte(((int) value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.writeByte((int) value & 0x7f);
    }

    /**
     * Write the given integer following the variable-length from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a> into the output. Zig-zag encoding is not used.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarLong(long value, ByteBuffer buffer) {
        value = (value << 1) ^ (value >> 63);
        while ((value & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) (((int) value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) ((int) value & 0x7f));
    }

    /**
     * Number of bytes needed to encode an unsigned integer in variable-length format.
     *
     * @param longValue The unsigned integer represented as a long
     */
    public static int bytesForVarUnsignedIntEncoding(long longValue) {
        int value = (int) longValue;
        int bytes = 1;
        while ((value & 0xffffff80) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The unsigned integer
     */
    public static int bytesForVarIntEncoding(int value) {
        return bytesForVarUnsignedIntEncoding((value << 1) ^ (value >> 31));
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The unsigned integer
     */
    public static int bytesForVarLongEncoding(long value) {
        value = (value << 1) ^ (value >> 63);
        int bytes = 1;
        while ((value & 0xffffffffffffff80L) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }
}
