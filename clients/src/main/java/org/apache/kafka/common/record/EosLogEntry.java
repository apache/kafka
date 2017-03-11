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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * LogEntry implementation for magic 2 and above. The schema is given below:
 *
 * LogEntry =>
 *  BaseOffset => Int64
 *  Length => Int32
 *  CRC => Int32
 *  Magic => Int8
 *  Attributes => Int16
 *  LastOffsetDelta => Int32
 *  BaseTimestamp => Int64
 *  MaxTimestamp => Int64
 *  PID => Int64
 *  Epoch => Int16
 *  BaseSequence => Int32
 *  PartitionLeaderEpoch => Int32
 *  Records => Record1, Record2, … , RecordN
 *
 *  The current attributes are given below:
 *
 *  -----------------------------------------------------------------------------------
 *  | Unused (5-15) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
 *  -----------------------------------------------------------------------------------
 */
public class EosLogEntry extends AbstractLogEntry implements LogEntry.MutableLogEntry {
    static final int BASE_OFFSET_OFFSET = 0;
    static final int BASE_OFFSET_LENGTH = 8;
    static final int SIZE_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    static final int SIZE_LENGTH = 4;
    static final int CRC_OFFSET = SIZE_OFFSET + SIZE_LENGTH;
    static final int CRC_LENGTH = 4;
    static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int MAGIC_LENGTH = 1;
    static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int ATTRIBUTE_LENGTH = 2;
    static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int LAST_OFFSET_DELTA_LENGTH = 4;
    static final int BASE_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    static final int BASE_TIMESTAMP_LENGTH = 8;
    static final int MAX_TIMESTAMP_OFFSET = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
    static final int MAX_TIMESTAMP_LENGTH = 8;
    static final int PID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
    static final int PID_LENGTH = 8;
    static final int EPOCH_OFFSET = PID_OFFSET + PID_LENGTH;
    static final int EPOCH_LENGTH = 2;
    static final int BASE_SEQUENCE_OFFSET = EPOCH_OFFSET + EPOCH_LENGTH;
    static final int BASE_SEQUENCE_LENGTH = 4;
    static final int PARTITION_LEADER_EPOCH_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
    static final int PARTITION_LEADER_EPOCH_LENGTH = 4;
    static final int RECORDS_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
    public static final int LOG_ENTRY_OVERHEAD = RECORDS_OFFSET;

    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;

    private final ByteBuffer buffer;

    EosLogEntry(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    @Override
    public void ensureValid() {
        if (!isValid())
            throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    private long baseTimestamp() {
        return buffer.getLong(BASE_TIMESTAMP_OFFSET);
    }

    @Override
    public long maxTimestamp() {
        return buffer.getLong(MAX_TIMESTAMP_OFFSET);
    }

    @Override
    public TimestampType timestampType() {
        if (magic() == 0)
            return TimestampType.NO_TIMESTAMP_TYPE;
        else
            return TimestampType.forAttributes(attributes());
    }

    @Override
    public long baseOffset() {
        return buffer.getLong(BASE_OFFSET_OFFSET);
    }

    @Override
    public long lastOffset() {
        return baseOffset() + ByteUtils.readUnsignedInt(buffer, LAST_OFFSET_DELTA_OFFSET);
    }

    @Override
    public long pid() {
        return buffer.getLong(PID_OFFSET);
    }

    @Override
    public short epoch() {
        return buffer.getShort(EPOCH_OFFSET);
    }

    @Override
    public int baseSequence() {
        return buffer.getInt(BASE_SEQUENCE_OFFSET);
    }

    @Override
    public int lastSequence() {
        return baseSequence() + buffer.getInt(LAST_OFFSET_DELTA_OFFSET);
    }

    @Override
    public CompressionType compressionType() {
        return CompressionType.forId(attributes() & COMPRESSION_CODEC_MASK);
    }

    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + buffer.getInt(SIZE_OFFSET);
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(this.buffer.duplicate());
    }

    @Override
    public boolean isTransactional() {
        return (attributes() & TRANSACTIONAL_FLAG_MASK) > 0;
    }

    @Override
    public int partitionLeaderEpoch() {
        return buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET);
    }

    private Iterator<LogRecord> compressedIterator() {
        ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);
        DataInputStream stream = new DataInputStream(compressionType().wrapForInput(
                new ByteBufferInputStream(buffer), magic()));

        // TODO: An improvement for the consumer would be to only decompress the records
        // we need to fill max.poll.records and leave the rest compressed.
        Deque<LogRecord> records = new ArrayDeque<>();
        try {
            Long logAppendTime = timestampType() == TimestampType.LOG_APPEND_TIME ? maxTimestamp() : null;
            long baseOffset = baseOffset();
            long baseTimestamp = baseTimestamp();
            int baseSequence = baseSequence();
            long lastOffset = lastOffset();

            while (true) {
                try {
                    EosLogRecord record = EosLogRecord.readFrom(stream, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                    records.add(record);

                    if (record.offset() == lastOffset)
                        break;
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        } finally {
            Utils.closeQuietly(stream, "records iterator stream");
        }

        return records.iterator();
    }

    private Iterator<LogRecord> uncompressedIterator() {
        final ByteBuffer buffer = this.buffer.duplicate();
        return new AbstractIterator<LogRecord>() {
            int position = RECORDS_OFFSET;

            @Override
            protected LogRecord makeNext() {
                if (position >= buffer.limit())
                    return allDone();

                ByteBuffer buf = buffer.duplicate();
                buf.position(position);

                Long logAppendTime = timestampType() == TimestampType.LOG_APPEND_TIME ? maxTimestamp() : null;
                long baseOffset = baseOffset();
                long baseTimestamp = baseTimestamp();
                int baseSequence = baseSequence();

                EosLogRecord record = EosLogRecord.readFrom(buf, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                if (record == null)
                    return allDone();

                position += record.sizeInBytes();
                return record;
            }
        };
    }

    @Override
    public Iterator<LogRecord> iterator() {
        if (isCompressed())
            return compressedIterator();
        else
            return uncompressedIterator();
    }

    public void setOffset(long offset) {
        buffer.putLong(BASE_OFFSET_OFFSET, offset);
    }

    @Override
    public void setMaxTimestamp(TimestampType timestampType, long maxTimestamp) {
        long currentMaxTimestamp = maxTimestamp();
        // We don't need to recompute crc if the timestamp is not updated.
        if (timestampType() == timestampType && currentMaxTimestamp == maxTimestamp)
            return;

        byte attributes = attributes();
        buffer.putShort(ATTRIBUTES_OFFSET, timestampType.updateAttributes(attributes));
        buffer.putLong(MAX_TIMESTAMP_OFFSET, maxTimestamp);
        long crc = computeChecksum();
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);
    }

    @Override
    public void setPartitionLeaderEpoch(int epoch) {
        buffer.putInt(PARTITION_LEADER_EPOCH_LENGTH, epoch);
    }

    @Override
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    public boolean isValid() {
        return sizeInBytes() >= CRC_LENGTH && checksum() == computeChecksum();
    }

    private long computeChecksum() {
        return Utils.computeChecksum(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    private byte attributes() {
        // note we're not using the second byte of attributes
        return (byte) buffer.getShort(ATTRIBUTES_OFFSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EosLogEntry that = (EosLogEntry) o;
        return buffer != null ? buffer.equals(that.buffer) : that.buffer == null;
    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

    private static byte computeAttributes(CompressionType type, TimestampType timestampType, boolean isTransactional) {
        byte attributes = isTransactional ? TRANSACTIONAL_FLAG_MASK : 0;
        if (type.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        return timestampType.updateAttributes(attributes);
    }

    static void writeHeader(ByteBuffer buffer,
                            long baseOffset,
                            int lastOffsetDelta,
                            int size,
                            byte magic,
                            CompressionType compressionType,
                            TimestampType timestampType,
                            long baseTimestamp,
                            long maxTimestamp,
                            long pid,
                            short epoch,
                            int sequence,
                            boolean isTransactional,
                            int partitionLeaderEpoch) {
        if (magic < LogEntry.CURRENT_MAGIC_VALUE)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (baseTimestamp < 0 && baseTimestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + baseTimestamp);

        short attributes = computeAttributes(compressionType, timestampType, isTransactional);

        int position = buffer.position();
        buffer.putLong(position + BASE_OFFSET_OFFSET, baseOffset);
        buffer.putInt(position + SIZE_OFFSET, size - LOG_OVERHEAD);
        buffer.put(position + MAGIC_OFFSET, magic);
        buffer.putShort(position + ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(position + BASE_TIMESTAMP_OFFSET, baseTimestamp);
        buffer.putLong(position + MAX_TIMESTAMP_OFFSET, maxTimestamp);
        buffer.putInt(position + LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
        buffer.putLong(position + PID_OFFSET, pid);
        buffer.putShort(position + EPOCH_OFFSET, epoch);
        buffer.putInt(position + BASE_SEQUENCE_OFFSET, sequence);
        buffer.putInt(position + PARTITION_LEADER_EPOCH_OFFSET, partitionLeaderEpoch);
        long crc = Utils.computeChecksum(buffer, position + MAGIC_OFFSET, size - MAGIC_OFFSET);
        buffer.putInt(position + CRC_OFFSET, (int) (crc & 0xffffffffL));
    }

    @Override
    public String toString() {
        return "LogEntry(magic: " + magic() + ", offsets: [" + baseOffset() + ", " + lastOffset() + "])";
    }

    public static int sizeInBytes(long baseOffset, Iterable<LogRecord> records) {
        Iterator<LogRecord> iterator = records.iterator();
        if (!iterator.hasNext())
            return 0;

        int size = LOG_ENTRY_OVERHEAD;
        Long baseTimestamp = null;
        while (iterator.hasNext()) {
            LogRecord record = iterator.next();
            int offsetDelta = (int) (record.offset() - baseOffset);
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            long timestampDelta = record.timestamp() - baseTimestamp;
            size += EosLogRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value());
        }
        return size;
    }

    public static int sizeInBytes(Iterable<KafkaRecord> records) {
        Iterator<KafkaRecord> iterator = records.iterator();
        if (!iterator.hasNext())
            return 0;

        int size = LOG_ENTRY_OVERHEAD;
        int offsetDelta = 0;
        Long baseTimestamp = null;
        while (iterator.hasNext()) {
            KafkaRecord record = iterator.next();
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            long timestampDelta = record.timestamp() - baseTimestamp;
            size += EosLogRecord.sizeInBytes(offsetDelta++, timestampDelta, record.key(), record.value());
        }
        return size;
    }

    /**
     * Get an upper bound on the size of a log entry with only a single record using a given
     * key and value.
     */
    public static int entrySizeUpperBound(byte[] key, byte[] value) {
        return LOG_ENTRY_OVERHEAD + EosLogRecord.recordSizeUpperBound(key, value);
    }

}
