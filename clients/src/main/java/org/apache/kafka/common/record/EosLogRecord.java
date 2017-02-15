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
 *   Length => varint
 *   Attributes => int8
 *   TimestampDelta => varlong
 *   OffsetDelta => varint
 *   KeyLen => varint [OPTIONAL]
 *   Key => data [OPTIONAL]
 *   Value => data [OPTIONAL]
 *
 * The record attributes indicate whether the key and value fields are present. The first bit
 * is used to indicate a null key; if set, the key length and key data will be left out of the
 * message. Similarly, if the second bit is set, the value field will be left out.
 *
 * The offset and timestamp deltas compute the difference relative to the base offset and
 * base timestamp of the log entry that this record is contained in.
 */
public class EosLogRecord implements LogRecord {
    private static final int MAX_RECORD_OVERHEAD = 21;
    private static final int NULL_KEY_MASK = 0x01;
    private static final int NULL_VALUE_MASK = 0x02;

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
        return key == null ? 0 : key.remaining();
    }

    @Override
    public int valueSize() {
        return value == null ? 0 : value.remaining();
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
    public boolean hasNullValue() {
        return value == null;
    }

    @Override
    public ByteBuffer value() {
        return value == null ? null : value.duplicate();
    }

    public static long writeTo(DataOutputStream out,
                               int offsetDelta,
                               long timestamp,
                               ByteBuffer key,
                               ByteBuffer value) throws IOException {
        int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestamp, key, value);
        ByteUtils.writeVarint(sizeInBytes, out);

        byte attributes = computeAttributes(key, value);
        out.write(attributes);

        ByteUtils.writeVarlong(timestamp, out);
        ByteUtils.writeVarint(offsetDelta, out);

        if (key != null) {
            int keySize = key.remaining();
            ByteUtils.writeVarint(keySize, out);
            out.write(key.array(), key.arrayOffset(), keySize);
        }

        if (value != null)
            out.write(value.array(), value.arrayOffset(), value.remaining());

        return computeChecksum(timestamp, key, value);
    }

    public static long writeTo(ByteBuffer out,
                               int offsetDelta,
                               long timestamp,
                               ByteBuffer key,
                               ByteBuffer value) {
        try {
            return writeTo(new DataOutputStream(new ByteBufferOutputStream(out)), offsetDelta, timestamp, key, value);
        } catch (IOException e) {
            // cannot actually be raised by ByteBufferOutputStream
            throw new RuntimeException(e);
        }
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    private static long computeChecksum(long timestamp,
                                        ByteBuffer key,
                                        ByteBuffer value) {
        Crc32 crc = new Crc32();
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
                                        int baseSequence,
                                        Long logAppendTime) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        ByteBuffer recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes);
        input.readFully(recordBuffer.array(), recordBuffer.arrayOffset(), sizeOfBodyInBytes);
        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        return readFrom(recordBuffer, totalSizeInBytes, baseOffset, baseSequence, logAppendTime);
    }

    public static EosLogRecord readFrom(ByteBuffer buffer,
                                        long baseOffset,
                                        int baseSequence,
                                        Long logAppendTime) {
        ByteBuffer dup = buffer.duplicate();
        int sizeOfBodyInBytes = ByteUtils.readVarint(dup);
        if (buffer.remaining() < sizeOfBodyInBytes)
            return null;

        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        dup.limit(dup.position() + sizeOfBodyInBytes);
        return readFrom(dup, totalSizeInBytes, baseOffset, baseSequence, logAppendTime);
    }

    private static EosLogRecord readFrom(ByteBuffer buffer,
                                         int sizeInBytes,
                                         long baseOffset,
                                         int baseSequence,
                                         Long logAppendTime) {
        byte attributes = buffer.get();
        long timestamp = ByteUtils.readVarlong(buffer);
        if (logAppendTime != null)
            timestamp = logAppendTime;

        int delta = ByteUtils.readVarint(buffer);
        long offset = baseOffset + delta;
        int sequence = baseSequence >= 0 ? baseSequence + delta : LogEntry.NO_SEQUENCE;

        ByteBuffer key = null;
        if (hasKey(attributes)) {
            int keySizeInBytes = ByteUtils.readVarint(buffer);
            key = buffer.slice();
            key.limit(keySizeInBytes);
            buffer.position(buffer.position() + keySizeInBytes);
        }

        ByteBuffer value = null;
        if (hasValue(attributes))
            value = buffer.slice();
        return new EosLogRecord(sizeInBytes, attributes, offset, timestamp, sequence, key, value);
    }

    private static byte computeAttributes(ByteBuffer key, ByteBuffer value) {
        byte attributes = 0;
        if (key == null)
            attributes |= NULL_KEY_MASK;
        if (value == null)
            attributes |= NULL_VALUE_MASK;
        return attributes;
    }

    private static boolean hasKey(byte attributes) {
        return (attributes & NULL_KEY_MASK) == 0;
    }

    private static boolean hasValue(byte attributes) {
        return (attributes & NULL_VALUE_MASK) == 0;
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestamp,
                                  byte[] key,
                                  byte[] value) {
        return sizeInBytes(offsetDelta, timestamp, wrapNullable(key), wrapNullable(value));
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestamp,
                                  ByteBuffer key,
                                  ByteBuffer value) {
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestamp, key, value);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    private static int sizeOfBodyInBytes(int offsetDelta,
                                         long timestamp,
                                         ByteBuffer key,
                                         ByteBuffer value) {
        int size = 1; // always one byte for attributes
        size += ByteUtils.sizeOfVarint(offsetDelta);
        size += ByteUtils.sizeOfVarlong(timestamp);

        if (key != null) {
            int keySize = key.remaining();
            size += ByteUtils.sizeOfVarint(keySize);
            size += keySize;
        }

        if (value != null)
            size += value.remaining();

        return size;
    }

    static int recordSizeUpperBound(byte[] key, byte[] value) {
        int size = MAX_RECORD_OVERHEAD;
        if (key != null)
            size += key.length + ByteUtils.sizeOfVarint(key.length);
        if (value != null)
            size += value.length;
        return size;
    }

}
