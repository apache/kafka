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
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class implements the inner record format for magic 2 and above. The schema is as follows:
 *
 *
 * Record =>
 *   Length => Varint
 *   Attributes => Int8
 *   TimestampDelta => Varlong
 *   OffsetDelta => Varint
 *   Key => Bytes
 *   Value => Bytes
 *   Headers => [HeaderKey HeaderValue]
 *     HeaderKey => String
 *     HeaderValue => Bytes
 *
 * Note that in this schema, the Bytes and String types use a variable length integer to represent
 * the length of the field. The array type used for the headers also uses a Varint for the number of
 * headers.
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
public class DefaultRecord implements Record {

    // excluding key, value and headers: 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes
    public static final int MAX_RECORD_OVERHEAD = 21;

    private static final int CONTROL_FLAG_MASK = 0x01;
    private static final int NULL_VARINT_SIZE_BYTES = ByteUtils.sizeOfVarint(-1);

    private final int sizeInBytes;
    private final byte attributes;
    private final long offset;
    private final long timestamp;
    private final int sequence;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final Header[] headers;
    private Long checksum = null;

    private DefaultRecord(int sizeInBytes,
                          byte attributes,
                          long offset,
                          long timestamp,
                          int sequence,
                          ByteBuffer key,
                          ByteBuffer value,
                          Header[] headers) {
        this.sizeInBytes = sizeInBytes;
        this.attributes = attributes;
        this.offset = offset;
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.key = key;
        this.value = value;
        this.headers = headers;
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

    @Override
    public Header[] headers() {
        return headers;
    }

    /**
     * Write the record to `out` and return its crc.
     */
    public static long writeTo(DataOutputStream out,
                               boolean isControlRecord,
                               int offsetDelta,
                               long timestampDelta,
                               ByteBuffer key,
                               ByteBuffer value,
                               Header[] headers) throws IOException {
        int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
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
            Utils.writeTo(out, key, keySize);
        }

        if (value == null) {
            ByteUtils.writeVarint(-1, out);
        } else {
            int valueSize = value.remaining();
            ByteUtils.writeVarint(valueSize, out);
            Utils.writeTo(out, value, valueSize);
        }

        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        ByteUtils.writeVarint(headers.length, out);

        for (Header header : headers) {
            String headerKey = header.key();
            if (headerKey == null)
                throw new IllegalArgumentException("Invalid null header key found in headers");

            byte[] utf8Bytes = Utils.utf8(headerKey);
            ByteUtils.writeVarint(utf8Bytes.length, out);
            out.write(utf8Bytes);

            ByteBuffer headerValue = header.value();
            if (headerValue == null) {
                ByteUtils.writeVarint(-1, out);
            } else {
                int headerValueSize = headerValue.remaining();
                ByteUtils.writeVarint(headerValueSize, out);
                Utils.writeTo(out, headerValue, headerValueSize);
            }
        }

        return computeChecksum(timestampDelta, key, value);
    }

    /**
     * Write the record to `out` and return its crc.
     */
    public static long writeTo(ByteBuffer out,
                               boolean isControlRecord,
                               int offsetDelta,
                               long timestampDelta,
                               ByteBuffer key,
                               ByteBuffer value,
                               Header[] headers) {
        try {
            return writeTo(new DataOutputStream(new ByteBufferOutputStream(out)), isControlRecord, offsetDelta,
                    timestampDelta, key, value, headers);
        } catch (IOException e) {
            // cannot actually be raised by ByteBufferOutputStream
            throw new IllegalStateException("Unexpected exception raised from ByteBufferOutputStream", e);
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
            crc.update(key, key.remaining());

        if (value != null)
            crc.update(value, value.remaining());

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

    public static DefaultRecord readFrom(DataInputStream input,
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

    public static DefaultRecord readFrom(ByteBuffer buffer,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) {
        int sizeOfBodyInBytes = ByteUtils.readVarint(buffer);
        if (buffer.remaining() < sizeOfBodyInBytes)
            return null;

        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        return readFrom(buffer, totalSizeInBytes, baseOffset, baseTimestamp, baseSequence, logAppendTime);
    }

    private static DefaultRecord readFrom(ByteBuffer buffer,
                                          int sizeInBytes,
                                          long baseOffset,
                                          long baseTimestamp,
                                          int baseSequence,
                                          Long logAppendTime) {
        byte attributes = buffer.get();
        long timestampDelta = ByteUtils.readVarlong(buffer);
        long timestamp = baseTimestamp + timestampDelta;
        if (logAppendTime != null)
            timestamp = logAppendTime;

        int offsetDelta = ByteUtils.readVarint(buffer);
        long offset = baseOffset + offsetDelta;
        int sequence = baseSequence >= 0 ? baseSequence + offsetDelta : RecordBatch.NO_SEQUENCE;

        ByteBuffer key = null;
        int keySize = ByteUtils.readVarint(buffer);
        if (keySize >= 0) {
            key = buffer.slice();
            key.limit(keySize);
            buffer.position(buffer.position() + keySize);
        }

        ByteBuffer value = null;
        int valueSize = ByteUtils.readVarint(buffer);
        if (valueSize >= 0) {
            value = buffer.slice();
            value.limit(valueSize);
            buffer.position(buffer.position() + valueSize);
        }

        int numHeaders = ByteUtils.readVarint(buffer);
        if (numHeaders < 0)
            throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);

        if (numHeaders == 0)
            return new DefaultRecord(sizeInBytes, attributes, offset, timestamp, sequence, key, value, Record.EMPTY_HEADERS);

        Header[] headers = new Header[numHeaders];
        for (int i = 0; i < numHeaders; i++) {
            int headerKeySize = ByteUtils.readVarint(buffer);
            if (headerKeySize < 0)
                throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);

            String headerKey = Utils.utf8(buffer, headerKeySize);
            buffer.position(buffer.position() + headerKeySize);

            ByteBuffer headerValue = null;
            int headerValueSize = ByteUtils.readVarint(buffer);
            if (headerValueSize >= 0) {
                headerValue = buffer.slice();
                headerValue.limit(headerValueSize);
                buffer.position(buffer.position() + headerValueSize);
            }

            headers[i] = new Header(headerKey, headerValue);
        }

        return new DefaultRecord(sizeInBytes, attributes, offset, timestamp, sequence, key, value, headers);
    }

    private static byte computeAttributes(boolean isControlRecord) {
        return isControlRecord ? CONTROL_FLAG_MASK : (byte) 0;
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  byte[] key,
                                  byte[] value) {
        return sizeInBytes(offsetDelta, timestampDelta, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  ByteBuffer key,
                                  ByteBuffer value,
                                  Header[] headers) {
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    private static int sizeOfBodyInBytes(int offsetDelta,
                                         long timestampDelta,
                                         ByteBuffer key,
                                         ByteBuffer value,
                                         Header[] headers) {
        int size = 1; // always one byte for attributes
        size += ByteUtils.sizeOfVarint(offsetDelta);
        size += ByteUtils.sizeOfVarlong(timestampDelta);

        int keySize = key == null ? -1 : key.remaining();
        int valueSize = value == null ? -1 : value.remaining();
        size += sizeOf(keySize, valueSize, headers);

        return size;
    }

    private static int sizeOf(int keySize, int valueSize, Header[] headers) {
        int size = 0;
        if (keySize < 0)
            size += NULL_VARINT_SIZE_BYTES;
        else
            size += ByteUtils.sizeOfVarint(keySize) + keySize;

        if (valueSize < 0)
            size += NULL_VARINT_SIZE_BYTES;
        else
            size += ByteUtils.sizeOfVarint(valueSize) + valueSize;

        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        size += ByteUtils.sizeOfVarint(headers.length);
        for (Header header : headers) {
            String headerKey = header.key();
            if (headerKey == null)
                throw new IllegalArgumentException("Invalid null header key found in headers");

            int headerKeySize = Utils.utf8Length(headerKey);
            size += ByteUtils.sizeOfVarint(headerKeySize) + headerKeySize;

            ByteBuffer headerValue = header.value();
            if (headerValue == null) {
                size += NULL_VARINT_SIZE_BYTES;
            } else {
                int headerValueSize = headerValue.remaining();
                size += ByteUtils.sizeOfVarint(headerValueSize) + headerValueSize;
            }
        }
        return size;
    }

    static int recordSizeUpperBound(byte[] key, byte[] value, Header[] headers) {
        int keySize = key == null ? -1 : key.length;
        int valueSize = value == null ? -1 : value.length;
        return MAX_RECORD_OVERHEAD + sizeOf(keySize, valueSize, headers);
    }

}
