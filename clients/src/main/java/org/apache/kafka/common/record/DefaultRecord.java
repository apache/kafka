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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.PrimitiveRef.IntRef;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;

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
 *  ----------------
 *  | Unused (0-7) |
 *  ----------------
 *
 * The offset and timestamp deltas compute the difference relative to the base offset and
 * base timestamp of the batch that this record is contained in.
 */
public class DefaultRecord implements Record {

    // excluding key, value and headers: 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes
    public static final int MAX_RECORD_OVERHEAD = 21;

    private static final int NULL_VARINT_SIZE_BYTES = ByteUtils.sizeOfVarint(-1);

    private final int sizeInBytes;
    private final byte attributes;
    private final long offset;
    private final long timestamp;
    private final int sequence;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final Header[] headers;

    DefaultRecord(int sizeInBytes,
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
    public int sequence() {
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
     * Write the record to `out` and return its size.
     */
    public static int writeTo(DataOutputStream out,
                              int offsetDelta,
                              long timestampDelta,
                              ByteBuffer key,
                              ByteBuffer value,
                              Header[] headers) throws IOException {
        int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
        ByteUtils.writeVarint(sizeInBytes, out);

        byte attributes = 0; // there are no used record attributes at the moment
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

            byte[] headerValue = header.value();
            if (headerValue == null) {
                ByteUtils.writeVarint(-1, out);
            } else {
                ByteUtils.writeVarint(headerValue.length, out);
                out.write(headerValue);
            }
        }

        return ByteUtils.sizeOfVarint(sizeInBytes) + sizeInBytes;
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
    public String toString() {
        return String.format("DefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
                offset,
                timestamp,
                key == null ? 0 : key.limit(),
                value == null ? 0 : value.limit());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DefaultRecord that = (DefaultRecord) o;
        return sizeInBytes == that.sizeInBytes &&
                attributes == that.attributes &&
                offset == that.offset &&
                timestamp == that.timestamp &&
                sequence == that.sequence &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Arrays.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = sizeInBytes;
        result = 31 * result + (int) attributes;
        result = 31 * result + Long.hashCode(offset);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + sequence;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(headers);
        return result;
    }

    public static DefaultRecord readFrom(DataInput input,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        ByteBuffer recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes);
        input.readFully(recordBuffer.array(), 0, sizeOfBodyInBytes);
        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        return readFrom(recordBuffer, totalSizeInBytes, sizeOfBodyInBytes, baseOffset, baseTimestamp,
                baseSequence, logAppendTime);
    }

    public static DefaultRecord readFrom(ByteBuffer buffer,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) {
        int sizeOfBodyInBytes = ByteUtils.readVarint(buffer);
        if (buffer.remaining() < sizeOfBodyInBytes)
            throw new InvalidRecordException("Invalid record size: expected " + sizeOfBodyInBytes +
                " bytes in record payload, but instead the buffer has only " + buffer.remaining() +
                " remaining bytes.");

        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        return readFrom(buffer, totalSizeInBytes, sizeOfBodyInBytes, baseOffset, baseTimestamp,
                baseSequence, logAppendTime);
    }

    private static DefaultRecord readFrom(ByteBuffer buffer,
                                          int sizeInBytes,
                                          int sizeOfBodyInBytes,
                                          long baseOffset,
                                          long baseTimestamp,
                                          int baseSequence,
                                          Long logAppendTime) {
        try {
            int recordStart = buffer.position();
            byte attributes = buffer.get();
            long timestampDelta = ByteUtils.readVarlong(buffer);
            long timestamp = baseTimestamp + timestampDelta;
            if (logAppendTime != null)
                timestamp = logAppendTime;

            int offsetDelta = ByteUtils.readVarint(buffer);
            long offset = baseOffset + offsetDelta;
            int sequence = baseSequence >= 0 ?
                    DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
                    RecordBatch.NO_SEQUENCE;

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
            if (numHeaders > buffer.remaining())
                throw new InvalidRecordException("Found invalid number of record headers. " + numHeaders + " is larger than the remaining size of the buffer");

            final Header[] headers;
            if (numHeaders == 0)
                headers = Record.EMPTY_HEADERS;
            else
                headers = readHeaders(buffer, numHeaders);

            // validate whether we have read all header bytes in the current record
            if (buffer.position() - recordStart != sizeOfBodyInBytes)
                throw new InvalidRecordException("Invalid record size: expected to read " + sizeOfBodyInBytes +
                        " bytes in record payload, but instead read " + (buffer.position() - recordStart));

            return new DefaultRecord(sizeInBytes, attributes, offset, timestamp, sequence, key, value, headers);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }

    public static PartialDefaultRecord readPartiallyFrom(DataInput input,
                                                         byte[] skipArray,
                                                         long baseOffset,
                                                         long baseTimestamp,
                                                         int baseSequence,
                                                         Long logAppendTime) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;

        return readPartiallyFrom(input, skipArray, totalSizeInBytes, sizeOfBodyInBytes, baseOffset, baseTimestamp,
            baseSequence, logAppendTime);
    }

    private static PartialDefaultRecord readPartiallyFrom(DataInput input,
                                                          byte[] skipArray,
                                                          int sizeInBytes,
                                                          int sizeOfBodyInBytes,
                                                          long baseOffset,
                                                          long baseTimestamp,
                                                          int baseSequence,
                                                          Long logAppendTime) throws IOException {
        ByteBuffer skipBuffer = ByteBuffer.wrap(skipArray);
        // set its limit to 0 to indicate no bytes readable yet
        skipBuffer.limit(0);

        try {
            // reading the attributes / timestamp / offset and key-size does not require
            // any byte array allocation and therefore we can just read them straight-forwardly
            IntRef bytesRemaining = PrimitiveRef.ofInt(sizeOfBodyInBytes);

            byte attributes = readByte(skipBuffer, input, bytesRemaining);
            long timestampDelta = readVarLong(skipBuffer, input, bytesRemaining);
            long timestamp = baseTimestamp + timestampDelta;
            if (logAppendTime != null)
                timestamp = logAppendTime;

            int offsetDelta = readVarInt(skipBuffer, input, bytesRemaining);
            long offset = baseOffset + offsetDelta;
            int sequence = baseSequence >= 0 ?
                DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
                RecordBatch.NO_SEQUENCE;

            // first skip key
            int keySize = skipLengthDelimitedField(skipBuffer, input, bytesRemaining);

            // then skip value
            int valueSize = skipLengthDelimitedField(skipBuffer, input, bytesRemaining);

            // then skip header
            int numHeaders = readVarInt(skipBuffer, input, bytesRemaining);
            if (numHeaders < 0)
                throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
            for (int i = 0; i < numHeaders; i++) {
                int headerKeySize = skipLengthDelimitedField(skipBuffer, input, bytesRemaining);
                if (headerKeySize < 0)
                    throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);

                // headerValueSize
                skipLengthDelimitedField(skipBuffer, input, bytesRemaining);
            }

            if (bytesRemaining.value > 0 || skipBuffer.remaining() > 0)
                throw new InvalidRecordException("Invalid record size: expected to read " + sizeOfBodyInBytes +
                    " bytes in record payload, but there are still bytes remaining");

            return new PartialDefaultRecord(sizeInBytes, attributes, offset, timestamp, sequence, keySize, valueSize);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }

    private static byte readByte(ByteBuffer buffer, DataInput input, IntRef bytesRemaining) throws IOException {
        if (buffer.remaining() < 1 && bytesRemaining.value > 0) {
            readMore(buffer, input, bytesRemaining);
        }

        return buffer.get();
    }

    private static long readVarLong(ByteBuffer buffer, DataInput input, IntRef bytesRemaining) throws IOException {
        if (buffer.remaining() < 10 && bytesRemaining.value > 0) {
            readMore(buffer, input, bytesRemaining);
        }

        return ByteUtils.readVarlong(buffer);
    }

    private static int readVarInt(ByteBuffer buffer, DataInput input, IntRef bytesRemaining) throws IOException {
        if (buffer.remaining() < 5 && bytesRemaining.value > 0) {
            readMore(buffer, input, bytesRemaining);
        }

        return ByteUtils.readVarint(buffer);
    }

    private static int skipLengthDelimitedField(ByteBuffer buffer, DataInput input, IntRef bytesRemaining) throws IOException {
        boolean needMore = false;
        int sizeInBytes = -1;
        int bytesToSkip = -1;

        while (true) {
            if (needMore) {
                readMore(buffer, input, bytesRemaining);
                needMore = false;
            }

            if (bytesToSkip < 0) {
                if (buffer.remaining() < 5 && bytesRemaining.value > 0) {
                    needMore = true;
                } else {
                    sizeInBytes = ByteUtils.readVarint(buffer);
                    if (sizeInBytes <= 0)
                        return sizeInBytes;
                    else
                        bytesToSkip = sizeInBytes;

                }
            } else {
                if (bytesToSkip > buffer.remaining()) {
                    bytesToSkip -= buffer.remaining();
                    buffer.position(buffer.limit());
                    needMore = true;
                } else {
                    buffer.position(buffer.position() + bytesToSkip);
                    return sizeInBytes;
                }
            }
        }
    }

    private static void readMore(ByteBuffer buffer, DataInput input, IntRef bytesRemaining) throws IOException {
        if (bytesRemaining.value > 0) {
            byte[] array = buffer.array();

            // first copy the remaining bytes to the beginning of the array;
            // at most 4 bytes would be shifted here
            int stepsToLeftShift = buffer.position();
            int bytesToLeftShift = buffer.remaining();
            for (int i = 0; i < bytesToLeftShift; i++) {
                array[i] = array[i + stepsToLeftShift];
            }

            // then try to read more bytes to the remaining of the array
            int bytesRead = Math.min(bytesRemaining.value, array.length - bytesToLeftShift);
            input.readFully(array, bytesToLeftShift, bytesRead);
            buffer.rewind();
            // only those many bytes are readable
            buffer.limit(bytesToLeftShift + bytesRead);

            bytesRemaining.value -= bytesRead;
        } else {
            throw new InvalidRecordException("Invalid record size: expected to read more bytes in record payload");
        }
    }

    private static Header[] readHeaders(ByteBuffer buffer, int numHeaders) {
        Header[] headers = new Header[numHeaders];
        for (int i = 0; i < numHeaders; i++) {
            int headerKeySize = ByteUtils.readVarint(buffer);
            if (headerKeySize < 0)
                throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);

            ByteBuffer headerKeyBuffer = buffer.slice();
            headerKeyBuffer.limit(headerKeySize);
            buffer.position(buffer.position() + headerKeySize);

            ByteBuffer headerValue = null;
            int headerValueSize = ByteUtils.readVarint(buffer);
            if (headerValueSize >= 0) {
                headerValue = buffer.slice();
                headerValue.limit(headerValueSize);
                buffer.position(buffer.position() + headerValueSize);
            }

            headers[i] = new RecordHeader(headerKeyBuffer, headerValue);
        }

        return headers;
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  ByteBuffer key,
                                  ByteBuffer value,
                                  Header[] headers) {
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  int keySize,
                                  int valueSize,
                                  Header[] headers) {
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    private static int sizeOfBodyInBytes(int offsetDelta,
                                         long timestampDelta,
                                         ByteBuffer key,
                                         ByteBuffer value,
                                         Header[] headers) {
        int keySize = key == null ? -1 : key.remaining();
        int valueSize = value == null ? -1 : value.remaining();
        return sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers);
    }

    public static int sizeOfBodyInBytes(int offsetDelta,
                                        long timestampDelta,
                                        int keySize,
                                        int valueSize,
                                        Header[] headers) {
        int size = 1; // always one byte for attributes
        size += ByteUtils.sizeOfVarint(offsetDelta);
        size += ByteUtils.sizeOfVarlong(timestampDelta);
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

            byte[] headerValue = header.value();
            if (headerValue == null) {
                size += NULL_VARINT_SIZE_BYTES;
            } else {
                size += ByteUtils.sizeOfVarint(headerValue.length) + headerValue.length;
            }
        }
        return size;
    }

    static int recordSizeUpperBound(ByteBuffer key, ByteBuffer value, Header[] headers) {
        int keySize = key == null ? -1 : key.remaining();
        int valueSize = value == null ? -1 : value.remaining();
        return MAX_RECORD_OVERHEAD + sizeOf(keySize, valueSize, headers);
    }
}
