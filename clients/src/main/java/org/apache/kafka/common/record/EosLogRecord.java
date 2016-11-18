/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Crc32;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.Record.MAGIC_VALUE_V2;
import static org.apache.kafka.common.utils.Utils.wrapNullable;

public class EosLogRecord implements LogRecord {

    static final int SIZE_OFFSET = 0;
    static final int SIZE_LENGTH = 4;
    static final int OFFSET_OFFSET = SIZE_OFFSET + SIZE_LENGTH;
    static final int OFFSET_LENGTH = 8;
    static final int ATTRIBUTES_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    static final int ATTRIBUTES_LENGTH = 1;
    static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    static final int TIMESTAMP_LENGTH = 8;
    static final int KEY_SIZE_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    static final int KEY_SIZE_LENGTH = 4;
    static final int CRC_LENGTH = 4;

    public static final int RECORD_OVERHEAD = SIZE_LENGTH + OFFSET_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH + CRC_LENGTH;

    private static final int NULL_KEY_MASK = 0x01;
    private static final int NULL_VALUE_MASK = 0x02;

    private final long baseOffset;
    private final long baseSequence;
    private final Long logAppendTime;
    private final ByteBuffer buffer;

    public EosLogRecord(ByteBuffer buffer,
                        long baseOffset,
                        long baseSequence,
                        Long logAppendTime) {
        this.buffer = buffer;
        this.baseOffset = baseOffset;
        this.baseSequence = baseSequence;
        this.logAppendTime = logAppendTime;
    }

    @Override
    public long offset() {
        return baseOffset + buffer.getLong(OFFSET_OFFSET);
    }

    @Override
    public long sequence() {
        return baseSequence + buffer.getLong(OFFSET_OFFSET);
    }

    @Override
    public int sizeInBytes() {
        return buffer.getInt(SIZE_OFFSET);
    }

    private byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    @Override
    public long timestamp() {
        return logAppendTime == null ? buffer.getLong(TIMESTAMP_OFFSET) : logAppendTime;
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return Utils.computeChecksum(buffer, SIZE_OFFSET, checksumOffset());
    }

    /**
     * Retrieve the previously computed CRC for this record
     */
    @Override
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, checksumOffset());
    }

    private int checksumOffset() {
        return sizeInBytes() - CRC_LENGTH;
    }

    @Override
    public boolean isValid() {
        return checksum() == computeChecksum();
    }

    @Override
    public void ensureValid() {
        if (!isValid())
            throw new CorruptRecordException();
    }

    @Override
    public int keySize() {
        if (!hasKey())
            return 0;
        return buffer.getInt(KEY_SIZE_OFFSET);
    }

    private int valueOffset() {
        return hasKey() ? KEY_SIZE_OFFSET + KEY_SIZE_LENGTH + keySize() : KEY_SIZE_OFFSET;
    }

    @Override
    public int valueSize() {
        return sizeInBytes() - CRC_LENGTH - valueOffset();
    }

    @Override
    public boolean hasKey() {
        return (attributes() & NULL_KEY_MASK) == 0;
    }

    @Override
    public ByteBuffer key() {
        if (!hasKey())
            return null;
        return sizeDelimited(KEY_SIZE_OFFSET);
    }

    @Override
    public boolean hasNullValue() {
        return (attributes() & NULL_VALUE_MASK) != 0;
    }

    @Override
    public ByteBuffer value() {
        if (hasNullValue())
            return null;

        int size = valueSize();
        if (size == 0)
            return ByteBuffer.allocate(0);

        ByteBuffer b = buffer.duplicate();
        b.position(valueOffset());
        b = b.slice();
        b.limit(size);
        b.rewind();
        return b;
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sizeDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            throw new InvalidRecordException("Size must be greater than 0");
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    public static long write(DataOutputStream out,
                             long offset,
                             byte attributes,
                             long timestamp,
                             ByteBuffer key,
                             ByteBuffer value) throws IOException {
        int recordSize = sizeOf(key, value);

        if (key == null)
            attributes |= NULL_KEY_MASK;
        else
            attributes &= ~NULL_KEY_MASK;

        if (value == null)
            attributes |= NULL_VALUE_MASK;
        else
            attributes &= ~NULL_VALUE_MASK;

        out.writeInt(recordSize);
        out.writeLong(offset);
        out.writeByte(attributes);
        out.writeLong(timestamp);

        if (key != null) {
            attributes &= ~NULL_KEY_MASK;
            int size = key.remaining();
            out.writeInt(size);
            out.write(key.array(), key.arrayOffset(), size);
        }

        if (value != null)
            out.write(value.array(), value.arrayOffset(), value.remaining());

        long crc = computeChecksum(recordSize, offset, attributes, timestamp, key, value);
        out.writeInt((int) (crc & 0xffffffffL));
        return crc;
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    public static long computeChecksum(int recordSize,
                                       long offset,
                                       byte attributes,
                                       long timestamp,
                                       ByteBuffer key,
                                       ByteBuffer value) {
        Crc32 crc = new Crc32();
        crc.updateInt(recordSize);
        crc.updateLong(offset);
        crc.update(attributes);
        crc.updateLong(timestamp);

        if (key != null) {
            int size = key.remaining();
            crc.updateInt(size);
            crc.update(key.array(), key.arrayOffset(), size);
        }

        if (value != null)
            crc.update(value.array(), value.arrayOffset(), value.remaining());

        return crc.getValue();
    }

    public static int sizeOf(byte[] key, byte[] value) {
        return sizeOf(wrapNullable(key), wrapNullable(value));
    }

    public static int sizeOf(ByteBuffer key, ByteBuffer value) {
        int size = RECORD_OVERHEAD;
        if (key != null)
            size += KEY_SIZE_LENGTH + key.limit();
        if (value != null)
            size += value.limit();
        return size;
    }

    @Override
    public boolean hasMagic(byte magic) {
        return magic >= MAGIC_VALUE_V2;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return false;
    }

    public static EosLogRecord readFrom(DataInputStream input,
                                        long baseOffset,
                                        long baseSequence,
                                        Long logAppendTime) throws IOException {
        int size = input.readInt();
        ByteBuffer recordBuffer = ByteBuffer.allocate(size);
        input.readFully(recordBuffer.array(), recordBuffer.arrayOffset() + 4, size - 4);
        recordBuffer.putInt(EosLogRecord.SIZE_OFFSET, size);
        return new EosLogRecord(recordBuffer, baseOffset, baseSequence, logAppendTime);
    }

    public static EosLogRecord readFrom(ByteBuffer buffer,
                                        long baseOffset,
                                        long baseSequence,
                                        Long logAppendTime) {
        if (buffer.remaining() < RECORD_OVERHEAD)
            return null;

        int size = buffer.getInt(buffer.position() + SIZE_OFFSET);
        if (buffer.remaining() < size)
            return null;

        ByteBuffer recordBuffer = buffer.slice();
        recordBuffer.limit(size);
        return new EosLogRecord(recordBuffer, baseOffset, baseSequence, logAppendTime);
    }
}
