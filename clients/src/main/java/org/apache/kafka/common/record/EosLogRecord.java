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
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Crc32;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.LogEntry.MAGIC_VALUE_V2;
import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class implements the inner record format for magic 2 and above. The schema is as follows:
 *
 * Record =>
 *   Length => Varint
 *   Attributes => Int8
 *   TimestampDelta => Varlong
 *   OffsetDelta => Varint
 *   KeyLen => Varint
 *   Key => data
 *   ValueLen => Varint
 *   Value => data
 *
 * The current record attributes are depicted below:
 *
 *  -----------------------------------
 *  | Unused (1-7) | Control Flag (0) |
 *  -----------------------------------
 *
 * The control flag is used to implement control records (see {@link ControlRecordType}).
 *
 * The offset and timestamp deltas compute the difference relative to the base offset and
 * base timestamp of the log entry that this record is contained in.
 */
public class EosLogRecord implements LogRecord {

    // 5 bytes length + 10 bytes timestamp + 5 bytes offset + 5 bytes keylen + 5 bytes valuelen + 1 byte attributes
    private static final int MAX_RECORD_OVERHEAD = 26;

    private static final int CONTROL_FLAG_MASK = 0x01;
    private static final int NULL_VALUE_SIZE_BYTES = ByteUtils.sizeOfVarint(-1);

    private final int sizeInBytes;
    private final byte attributes;
    private final long offset;
    private final long timestamp;
    private final int sequence;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private Long checksum = null;

    private EosLogRecord(int sizeInBytes,
                         byte attributes,
                         long offset,
                         long timestamp,
                         int sequence,
                         ByteBuffer key,
                         ByteBuffer value) {
        this.sizeInBytes = sizeInBytes;
        this.attributes = attributes;
        this.offset = offset;
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.key = key;
        this.value = value;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long sequence() {
        return sequence;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public byte attributes() {
        return attributes;
    }

    @Override
    public long checksum() {
        if (checksum == null)
            checksum = computeChecksum(timestamp, key, value);
        return checksum;
    }

    @Override
    public boolean isValid() {
        // new versions of the message format (2 and above) do not contain an individual record checksum;
        // instead they are validated with the checksum at the log entry level
        return true;
    }

    @Override
    public void ensureValid() {}

    @Override
    public int keySize() {
        return key == null ? -1 : key.remaining();
    }

    @Override
    public int valueSize() {
        return value == null ? -1 : value.remaining();
    }

    @Override
    public boolean hasKey() {
        return key != null;
    }

    @Override
    public ByteBuffer key() {
        return key == null ? null : key.duplicate();
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public ByteBuffer value() {
        return value == null ? null : value.duplicate();
    }

    public static long writeTo(DataOutputStream out,
                               boolean isControlRecord,
                               int offsetDelta,
                               long timestampDelta,
                               ByteBuffer key,
                               ByteBuffer value) throws IOException {
        int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value);
        ByteUtils.writeVarint(sizeInBytes, out);

        byte attributes = computeAttributes(isControlRecord);
        out.write(attributes);

        ByteUtils.writeVarlong(timestampDelta, out);
        ByteUtils.writeVarint(offsetDelta, out);

        if (key == null) {
            ByteUtils.writeVarint(-1, out);
        } else {
            int keySize = key.remaining();
            ByteUtils.writeVarint(keySize, out);
            out.write(key.array(), key.arrayOffset(), keySize);
        }

        if (value == null) {
            ByteUtils.writeVarint(-1, out);
        } else {
            int valueSize = value.remaining();
            ByteUtils.writeVarint(valueSize, out);
            out.write(value.array(), value.arrayOffset(), valueSize);
        }

        return computeChecksum(timestampDelta, key, value);
    }

    public static long writeTo(ByteBuffer out,
                               boolean isControlRecord,
                               int offsetDelta,
                               long timestampDelta,
                               ByteBuffer key,
                               ByteBuffer value) {
        try {
            return writeTo(new DataOutputStream(new ByteBufferOutputStream(out)), isControlRecord, offsetDelta,
                    timestampDelta, key, value);
        } catch (IOException e) {
            // cannot actually be raised by ByteBufferOutputStream
            throw new RuntimeException(e);
        }
    }

    /**
     * Compute the checksum of the record from the timestamp, key and value payloads
     */
    private static long computeChecksum(long timestamp,
                                        ByteBuffer key,
                                        ByteBuffer value) {
        Crc32 crc = new Crc32();
        crc.updateLong(timestamp);

        if (key != null)
            crc.update(key.array(), key.arrayOffset(), key.remaining());

        if (value != null)
            crc.update(value.array(), value.arrayOffset(), value.remaining());

        return crc.getValue();
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

    @Override
    public boolean isControlRecord() {
        return (attributes & CONTROL_FLAG_MASK) != 0;
    }

    public static EosLogRecord readFrom(DataInputStream input,
                                        long baseOffset,
                                        long baseTimestamp,
                                        int baseSequence,
                                        Long logAppendTime) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        ByteBuffer recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes);
        input.readFully(recordBuffer.array(), recordBuffer.arrayOffset(), sizeOfBodyInBytes);
        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        return readFrom(recordBuffer, totalSizeInBytes, baseOffset, baseTimestamp, baseSequence, logAppendTime);
    }

    public static EosLogRecord readFrom(ByteBuffer buffer,
                                        long baseOffset,
                                        long baseTimestamp,
                                        int baseSequence,
                                        Long logAppendTime) {
        ByteBuffer dup = buffer.duplicate();
        int sizeOfBodyInBytes = ByteUtils.readVarint(dup);
        if (buffer.remaining() < sizeOfBodyInBytes)
            return null;

        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        dup.limit(dup.position() + sizeOfBodyInBytes);
        return readFrom(dup, totalSizeInBytes, baseOffset, baseTimestamp, baseSequence, logAppendTime);
    }

    private static EosLogRecord readFrom(ByteBuffer buffer,
                                         int sizeInBytes,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) {
        byte attributes = buffer.get();
        long timestamp = baseTimestamp + ByteUtils.readVarlong(buffer);
        if (logAppendTime != null)
            timestamp = logAppendTime;

        int delta = ByteUtils.readVarint(buffer);
        long offset = baseOffset + delta;
        int sequence = baseSequence >= 0 ? baseSequence + delta : LogEntry.NO_SEQUENCE;

        final ByteBuffer key;
        int keySize = ByteUtils.readVarint(buffer);
        if (keySize < 0) {
            key = null;
        } else {
            key = buffer.slice();
            key.limit(keySize);
            buffer.position(buffer.position() + keySize);
        }

        final ByteBuffer value;
        int valueSize = ByteUtils.readVarint(buffer);
        if (valueSize < 0) {
            value = null;
        } else {
            value = buffer.slice();
            value.limit(valueSize);
            buffer.position(buffer.position() + valueSize);
        }

        return new EosLogRecord(sizeInBytes, attributes, offset, timestamp, sequence, key, value);
    }

    private static byte computeAttributes(boolean isControlRecord) {
        return isControlRecord ? CONTROL_FLAG_MASK : (byte) 0;
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  byte[] key,
                                  byte[] value) {
        return sizeInBytes(offsetDelta, timestampDelta, wrapNullable(key), wrapNullable(value));
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  ByteBuffer key,
                                  ByteBuffer value) {
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    private static int sizeOfBodyInBytes(int offsetDelta,
                                         long timestampDelta,
                                         ByteBuffer key,
                                         ByteBuffer value) {
        int size = 1; // always one byte for attributes
        size += ByteUtils.sizeOfVarint(offsetDelta);
        size += ByteUtils.sizeOfVarlong(timestampDelta);

        if (key == null) {
            size += NULL_VALUE_SIZE_BYTES;
        } else {
            int keySize = key.remaining();
            size += ByteUtils.sizeOfVarint(keySize);
            size += keySize;
        }

        if (value == null) {
            size += NULL_VALUE_SIZE_BYTES;
        } else {
            int valueSize = value.remaining();
            size += ByteUtils.sizeOfVarint(valueSize);
            size += valueSize;
        }

        return size;
    }

    static int recordSizeUpperBound(byte[] key, byte[] value) {
        int size = MAX_RECORD_OVERHEAD;
        if (key == null)
            size += NULL_VALUE_SIZE_BYTES;
        else
            size += key.length + ByteUtils.sizeOfVarint(key.length);
        if (value == null)
            size += NULL_VALUE_SIZE_BYTES;
        else
            size += value.length + ByteUtils.sizeOfVarint(value.length);
        return size;
    }

}
