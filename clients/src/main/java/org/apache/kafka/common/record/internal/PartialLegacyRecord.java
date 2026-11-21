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
package org.apache.kafka.common.record.internal;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.internal.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.internal.Records.OFFSET_OFFSET;
import static org.apache.kafka.common.record.internal.Records.SIZE_OFFSET;

/**
 * An inner record of a compressed batch of magic v0 or v1, read without its key and value. It is the
 * counterpart of {@link PartialDefaultRecord} for the older message formats: only the fixed-size fields are
 * decoded, and the key and value are skipped in the decompressed stream, so no buffer sized by the record
 * is ever allocated.
 */
public class PartialLegacyRecord implements Record {

    // the scratch buffer holds the largest fixed-size chunk we read at once: the offset and the size
    static final int SCRATCH_BUFFER_SIZE = LOG_OVERHEAD;

    private final long offset;
    private final int sizeInBytes;
    private final byte magic;
    private final byte attributes;
    private final long timestamp;
    private final TimestampType timestampType;
    private final int keySize;
    private final int valueSize;

    PartialLegacyRecord(long offset,
                        int sizeInBytes,
                        byte magic,
                        byte attributes,
                        long timestamp,
                        TimestampType timestampType,
                        int keySize,
                        int valueSize) {
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.magic = magic;
        this.attributes = attributes;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.keySize = keySize;
        this.valueSize = valueSize;
    }

    /**
     * Read the next inner record from the decompressed stream of a legacy wrapper record, skipping its key
     * and value. The stream is always advanced by the full declared record size, so that it stays aligned
     * with the next record.
     *
     * @param stream the decompressed stream of the wrapper record's value
     * @param scratch a buffer of at least {@link #SCRATCH_BUFFER_SIZE} bytes which is reused across calls
     * @param wrapperTimestamp the timestamp of the wrapper record
     * @param wrapperTimestampType the timestamp type of the wrapper record
     * @return the record, or null if the stream has no further complete record
     */
    static PartialLegacyRecord readFrom(InputStream stream,
                                        ByteBuffer scratch,
                                        long wrapperTimestamp,
                                        TimestampType wrapperTimestampType) throws IOException {
        if (!readFully(stream, scratch, LOG_OVERHEAD))
            return null;
        long offset = scratch.getLong(OFFSET_OFFSET);
        int size = scratch.getInt(SIZE_OFFSET);
        if (size < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", LegacyRecord.RECORD_OVERHEAD_V0));

        if (!readFully(stream, scratch, LegacyRecord.HEADER_SIZE_V0))
            return null;
        byte magic = scratch.get(LegacyRecord.MAGIC_OFFSET);
        byte attributes = scratch.get(LegacyRecord.ATTRIBUTES_OFFSET);

        if (magic == RecordBatch.MAGIC_VALUE_V0)
            return readKeyAndValueSizes(stream, scratch, offset, size, magic, attributes,
                    RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE);

        if (size < LegacyRecord.RECORD_OVERHEAD_V1)
            throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", LegacyRecord.RECORD_OVERHEAD_V1));
        if (!readFully(stream, scratch, LegacyRecord.TIMESTAMP_LENGTH))
            return null;
        // the inner records of a wrapper using LogAppendTime carry the wrapper's timestamp
        long timestamp = wrapperTimestampType == TimestampType.LOG_APPEND_TIME ? wrapperTimestamp : scratch.getLong(0);
        return readKeyAndValueSizes(stream, scratch, offset, size, magic, attributes, timestamp, wrapperTimestampType);
    }

    private static PartialLegacyRecord readKeyAndValueSizes(InputStream stream,
                                                            ByteBuffer scratch,
                                                            long offset,
                                                            int size,
                                                            byte magic,
                                                            byte attributes,
                                                            long timestamp,
                                                            TimestampType timestampType) throws IOException {
        int headerSize = magic == RecordBatch.MAGIC_VALUE_V0 ? LegacyRecord.HEADER_SIZE_V0 : LegacyRecord.HEADER_SIZE_V1;
        int remaining = size - headerSize - LegacyRecord.KEY_SIZE_LENGTH - LegacyRecord.VALUE_SIZE_LENGTH;

        if (!readFully(stream, scratch, LegacyRecord.KEY_SIZE_LENGTH))
            return null;
        int keySize = scratch.getInt(0);
        // a negative size means the key (or the value) is null
        int keyBytes = Math.max(0, keySize);
        if (keyBytes > remaining)
            throw new InvalidRecordException("Invalid key size " + keySize + " in a record of size " + size);
        if (!skipFully(stream, keyBytes))
            return null;
        remaining -= keyBytes;

        if (!readFully(stream, scratch, LegacyRecord.VALUE_SIZE_LENGTH))
            return null;
        int valueSize = scratch.getInt(0);
        if (Math.max(0, valueSize) > remaining)
            throw new InvalidRecordException("Invalid value size " + valueSize + " in a record of size " + size);
        // skip everything up to the declared end of the record rather than just the value
        if (!skipFully(stream, remaining))
            return null;

        return new PartialLegacyRecord(offset, size + LOG_OVERHEAD, magic, attributes, timestamp, timestampType, keySize, valueSize);
    }

    private static boolean readFully(InputStream stream, ByteBuffer scratch, int length) throws IOException {
        scratch.clear().limit(length);
        Utils.readFully(stream, scratch);
        return !scratch.hasRemaining();
    }

    // returns false if the stream ends first, to match how a truncated record ends the full decode
    private static boolean skipFully(InputStream stream, int length) throws IOException {
        int remaining = length;
        while (remaining > 0) {
            long skipped = stream.skip(remaining);
            if (skipped < 0 || skipped > remaining) {
                throw new IOException("Unable to skip exactly");
            } else if (skipped == 0) {
                // skip() may return 0 before the end of the stream, so read one byte to tell them apart
                if (stream.read() == -1)
                    return false;
                remaining--;
            } else {
                remaining -= (int) skipped;
            }
        }
        return true;
    }

    PartialLegacyRecord withOffset(long newOffset) {
        return new PartialLegacyRecord(newOffset, sizeInBytes, magic, attributes, timestamp, timestampType, keySize, valueSize);
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public int sequence() {
        return RecordBatch.NO_SEQUENCE;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public void ensureValid() {
        // the checksum covers the key and value, which are not read
        throw new UnsupportedOperationException("the checksum cannot be verified in PartialLegacyRecord");
    }

    @Override
    public int keySize() {
        return keySize;
    }

    @Override
    public boolean hasKey() {
        return keySize >= 0;
    }

    @Override
    public ByteBuffer key() {
        throw new UnsupportedOperationException("key is skipped in PartialLegacyRecord");
    }

    @Override
    public int valueSize() {
        return valueSize;
    }

    @Override
    public boolean hasValue() {
        return valueSize >= 0;
    }

    @Override
    public ByteBuffer value() {
        throw new UnsupportedOperationException("value is skipped in PartialLegacyRecord");
    }

    @Override
    public boolean hasMagic(byte magic) {
        return this.magic == magic;
    }

    @Override
    public boolean isCompressed() {
        return (attributes & LegacyRecord.COMPRESSION_CODEC_MASK) != 0;
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return this.timestampType == timestampType;
    }

    @Override
    public Header[] headers() {
        return Record.EMPTY_HEADERS;
    }

    @Override
    public String toString() {
        return String.format("PartialLegacyRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
            offset,
            timestamp,
            keySize,
            valueSize);
    }
}
