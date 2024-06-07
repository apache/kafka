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
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

    public static DefaultRecord readFrom(InputStream input,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        ByteBuffer recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes);
        int bytesRead = Utils.readFully(input, recordBuffer);
        if (bytesRead != sizeOfBodyInBytes)
            throw new InvalidRecordException("Invalid record size: expected " + sizeOfBodyInBytes +
                " bytes in record payload, but the record payload reached EOF.");
        recordBuffer.flip(); // prepare for reading
        return readFrom(recordBuffer, sizeOfBodyInBytes, baseOffset, baseTimestamp,
                baseSequence, logAppendTime);
    }

    public static DefaultRecord readFrom(ByteBuffer buffer,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime) {
        int sizeOfBodyInBytes = ByteUtils.readVarint(buffer);
        return readFrom(buffer, sizeOfBodyInBytes, baseOffset, baseTimestamp,
            baseSequence, logAppendTime);
    }

    private static DefaultRecord readFrom(ByteBuffer buffer,
                                          int sizeOfBodyInBytes,
                                          long baseOffset,
                                          long baseTimestamp,
                                          int baseSequence,
                                          Long logAppendTime) {
        if (buffer.remaining() < sizeOfBodyInBytes)
            throw new InvalidRecordException("Invalid record size: expected " + sizeOfBodyInBytes +
                " bytes in record payload, but instead the buffer has only " + buffer.remaining() +
                " remaining bytes.");
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

            // read key
            int keySize = ByteUtils.readVarint(buffer);
            ByteBuffer key = Utils.readBytes(buffer, keySize);

            // read value
            int valueSize = ByteUtils.readVarint(buffer);
            ByteBuffer value = Utils.readBytes(buffer, valueSize);

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

            int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
            return new DefaultRecord(totalSizeInBytes, attributes, offset, timestamp, sequence, key, value, headers);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }

    public static PartialDefaultRecord readPartiallyFrom(InputStream input,
                                                         long baseOffset,
                                                         long baseTimestamp,
                                                         int baseSequence,
                                                         Long logAppendTime) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;

        return readPartiallyFrom(input, totalSizeInBytes, baseOffset, baseTimestamp,
            baseSequence, logAppendTime);
    }

    private static PartialDefaultRecord readPartiallyFrom(InputStream input,
                                                          int sizeInBytes,
                                                          long baseOffset,
                                                          long baseTimestamp,
                                                          int baseSequence,
                                                          Long logAppendTime) throws IOException {
        try {
            byte attributes = (byte) input.read();
            long timestampDelta = ByteUtils.readVarlong(input);
            long timestamp = baseTimestamp + timestampDelta;
            if (logAppendTime != null)
                timestamp = logAppendTime;

            int offsetDelta = ByteUtils.readVarint(input);
            long offset = baseOffset + offsetDelta;
            int sequence = baseSequence >= 0 ?
                DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
                RecordBatch.NO_SEQUENCE;

            // skip key
            int keySize = ByteUtils.readVarint(input);
            skipBytes(input, keySize);

            // skip value
            int valueSize = ByteUtils.readVarint(input);
            skipBytes(input, valueSize);

            // skip header
            int numHeaders = ByteUtils.readVarint(input);
            if (numHeaders < 0)
                throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
            for (int i = 0; i < numHeaders; i++) {
                int headerKeySize = ByteUtils.readVarint(input);
                if (headerKeySize < 0)
                    throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);
                skipBytes(input, headerKeySize);

                // headerValueSize
                int headerValueSize = ByteUtils.readVarint(input);
                skipBytes(input, headerValueSize);
            }

            return new PartialDefaultRecord(sizeInBytes, attributes, offset, timestamp, sequence, keySize, valueSize);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }


    /**
     * Skips over and discards exactly {@code bytesToSkip} bytes from the input stream.
     *
     * We require a loop over {@link InputStream#skip(long)} because it is possible for InputStream to skip smaller
     * number of bytes than expected (see javadoc for InputStream#skip).
     *
     * No-op for case where bytesToSkip <= 0. This could occur for cases where field is expected to be null.
     * @throws InvalidRecordException if end of stream is encountered before we could skip required bytes.
     * @throws IOException is an I/O error occurs while trying to skip from InputStream.
     * 
     * @see java.io.InputStream#skip(long)
     */
    private static void skipBytes(InputStream in, int bytesToSkip) throws IOException {
        if (bytesToSkip <= 0) return;

        // Starting JDK 12, this implementation could be replaced by InputStream#skipNBytes
        while (bytesToSkip > 0) {
            int ns = (int) in.skip(bytesToSkip);
            if (ns > 0 && ns <= bytesToSkip) {
                // adjust number to skip
                bytesToSkip -= ns;
            } else if (ns == 0) { // no bytes skipped
                // read one byte to check for EOS
                if (in.read() == -1) {
                    throw new InvalidRecordException("Reached end of input stream before skipping all bytes. " +
                        "Remaining bytes:" + bytesToSkip);
                }
                // one byte read so decrement number to skip
                bytesToSkip--;
            } else { // skipped negative or too many bytes
                throw new IOException("Unable to skip exactly");
            }
        }
    }

    private static Header[] readHeaders(ByteBuffer buffer, int numHeaders) {
        Header[] headers = new Header[numHeaders];
        for (int i = 0; i < numHeaders; i++) {
            int headerKeySize = ByteUtils.readVarint(buffer);
            if (headerKeySize < 0)
                throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);

            ByteBuffer headerKeyBuffer = Utils.readBytes(buffer, headerKeySize);

            int headerValueSize = ByteUtils.readVarint(buffer);
            ByteBuffer headerValue = Utils.readBytes(buffer, headerValueSize);

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
