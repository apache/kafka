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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Crc32C;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * RecordBatch implementation for magic 2 and above. The schema is given below:
 *
 * RecordBatch =>
 *  BaseOffset => Int64
 *  Length => Int32
 *  PartitionLeaderEpoch => Int32
 *  Magic => Int8
 *  CRC => Uint32
 *  Attributes => Int16
 *  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
 *  BaseTimestamp => Int64
 *  MaxTimestamp => Int64
 *  ProducerId => Int64
 *  ProducerEpoch => Int16
 *  BaseSequence => Int32
 *  Records => [Record]
 *
 * Note that when compression is enabled (see attributes below), the compressed record data is serialized
 * directly following the count of the number of records.
 *
 * The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that follow the CRC). It is
 * located after the magic byte, which means that clients must parse the magic byte before deciding how to interpret
 * the bytes between the batch length and the magic byte. The partition leader epoch field is not included in the CRC
 * computation to avoid the need to recompute the CRC when this field is assigned for every batch that is received by
 * the broker. The CRC-32C (Castagnoli) polynomial is used for the computation.
 *
 * On compaction: unlike the older message formats, magic v2 and above preserves the first and last offset/sequence
 * numbers from the original batch when the log is cleaned. This is required in order to be able to restore the
 * producer's state when the log is reloaded. If we did not retain the last sequence number, for example, then
 * after a partition leader failure, the producer might see an OutOfSequence error. The base sequence number must
 * be preserved for duplicate checking (the broker checks incoming Produce requests for duplicates by verifying
 * that the first and last sequence numbers of the incoming batch match the last from that producer).
 *
 * The current attributes are given below:
 *
 *  -------------------------------------------------------------------------------------------------
 *  | Unused (6-15) | Control (5) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
 *  -------------------------------------------------------------------------------------------------
 */
public class DefaultRecordBatch extends AbstractRecordBatch implements MutableRecordBatch {
    static final int BASE_OFFSET_OFFSET = 0;
    static final int BASE_OFFSET_LENGTH = 8;
    static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    static final int LENGTH_LENGTH = 4;
    static final int PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int PARTITION_LEADER_EPOCH_LENGTH = 4;
    static final int MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
    static final int MAGIC_LENGTH = 1;
    static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int CRC_LENGTH = 4;
    static final int ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTE_LENGTH = 2;
    static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int LAST_OFFSET_DELTA_LENGTH = 4;
    static final int BASE_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    static final int BASE_TIMESTAMP_LENGTH = 8;
    static final int MAX_TIMESTAMP_OFFSET = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
    static final int MAX_TIMESTAMP_LENGTH = 8;
    static final int PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
    static final int PRODUCER_ID_LENGTH = 8;
    static final int PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
    static final int PRODUCER_EPOCH_LENGTH = 2;
    static final int BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
    static final int BASE_SEQUENCE_LENGTH = 4;
    static final int RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
    static final int RECORDS_COUNT_LENGTH = 4;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    public static final int RECORD_BATCH_OVERHEAD = RECORDS_OFFSET;

    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;
    private static final int CONTROL_FLAG_MASK = 0x20;
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    private final ByteBuffer buffer;

    DefaultRecordBatch(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    @Override
    public void ensureValid() {
        if (sizeInBytes() < RECORD_BATCH_OVERHEAD)
            throw new InvalidRecordException("Record batch is corrupt (the size " + sizeInBytes() +
                    "is smaller than the minimum allowed overhead " + RECORD_BATCH_OVERHEAD + ")");

        if (!isValid())
            throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * Get the timestamp of the first record in this batch. It is always the create time of the record even if the
     * timestamp type of the batch is log append time.
     *
     * @return The base timestamp
     */
    public long baseTimestamp() {
        return buffer.getLong(BASE_TIMESTAMP_OFFSET);
    }

    @Override
    public long maxTimestamp() {
        return buffer.getLong(MAX_TIMESTAMP_OFFSET);
    }

    @Override
    public TimestampType timestampType() {
        return (attributes() & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }

    @Override
    public long baseOffset() {
        return buffer.getLong(BASE_OFFSET_OFFSET);
    }

    @Override
    public long lastOffset() {
        return baseOffset() + lastOffsetDelta();
    }

    @Override
    public long producerId() {
        return buffer.getLong(PRODUCER_ID_OFFSET);
    }

    @Override
    public short producerEpoch() {
        return buffer.getShort(PRODUCER_EPOCH_OFFSET);
    }

    @Override
    public int baseSequence() {
        return buffer.getInt(BASE_SEQUENCE_OFFSET);
    }

    private int lastOffsetDelta() {
        return buffer.getInt(LAST_OFFSET_DELTA_OFFSET);
    }

    @Override
    public int lastSequence() {
        int baseSequence = baseSequence();
        if (baseSequence == RecordBatch.NO_SEQUENCE)
            return RecordBatch.NO_SEQUENCE;
        return baseSequence() + lastOffsetDelta();
    }

    @Override
    public CompressionType compressionType() {
        return CompressionType.forId(attributes() & COMPRESSION_CODEC_MASK);
    }

    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + buffer.getInt(LENGTH_OFFSET);
    }

    private int count() {
        return buffer.getInt(RECORDS_COUNT_OFFSET);
    }

    @Override
    public Integer countOrNull() {
        return count();
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(this.buffer.duplicate());
    }

    @Override
    public void writeTo(ByteBufferOutputStream outputStream) {
        outputStream.write(this.buffer.duplicate());
    }

    @Override
    public boolean isTransactional() {
        return (attributes() & TRANSACTIONAL_FLAG_MASK) > 0;
    }

    @Override
    public boolean isControlBatch() {
        return (attributes() & CONTROL_FLAG_MASK) > 0;
    }

    @Override
    public int partitionLeaderEpoch() {
        return buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET);
    }

    private CloseableIterator<Record> compressedIterator(BufferSupplier bufferSupplier) {
        ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);
        final DataInputStream inputStream = new DataInputStream(compressionType().wrapForInput(buffer, magic(),
                bufferSupplier));

        return new RecordIterator() {
            @Override
            protected Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) {
                try {
                    return DefaultRecord.readFrom(inputStream, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                } catch (IOException e) {
                    throw new KafkaException("Failed to decompress record stream", e);
                }
            }

            @Override
            public void close() {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new KafkaException("Failed to close record stream", e);
                }
            }
        };
    }

    private CloseableIterator<Record> uncompressedIterator() {
        final ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);
        return new RecordIterator() {
            @Override
            protected Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) {
                return DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, logAppendTime);
            }
            @Override
            public void close() {}
        };
    }

    @Override
    public Iterator<Record> iterator() {
        if (!isCompressed())
            return uncompressedIterator();

        // for a normal iterator, we cannot ensure that the underlying compression stream is closed,
        // so we decompress the full record set here. Use cases which call for a lower memory footprint
        // can use `streamingIterator` at the cost of additional complexity
        try (CloseableIterator<Record> iterator = compressedIterator(BufferSupplier.NO_CACHING)) {
            List<Record> records = new ArrayList<>(count());
            while (iterator.hasNext())
                records.add(iterator.next());
            return records.iterator();
        }
    }


    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
        if (isCompressed())
            return compressedIterator(bufferSupplier);
        else
            return uncompressedIterator();
    }

    @Override
    public void setLastOffset(long offset) {
        buffer.putLong(BASE_OFFSET_OFFSET, offset - lastOffsetDelta());
    }

    @Override
    public void setMaxTimestamp(TimestampType timestampType, long maxTimestamp) {
        long currentMaxTimestamp = maxTimestamp();
        // We don't need to recompute crc if the timestamp is not updated.
        if (timestampType() == timestampType && currentMaxTimestamp == maxTimestamp)
            return;

        byte attributes = computeAttributes(compressionType(), timestampType, isTransactional(), isControlBatch());
        buffer.putShort(ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(MAX_TIMESTAMP_OFFSET, maxTimestamp);
        long crc = computeChecksum();
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);
    }

    @Override
    public void setPartitionLeaderEpoch(int epoch) {
        buffer.putInt(PARTITION_LEADER_EPOCH_OFFSET, epoch);
    }

    @Override
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    public boolean isValid() {
        return sizeInBytes() >= RECORD_BATCH_OVERHEAD && checksum() == computeChecksum();
    }

    private long computeChecksum() {
        return Crc32C.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET);
    }

    private byte attributes() {
        // note we're not using the second byte of attributes
        return (byte) buffer.getShort(ATTRIBUTES_OFFSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DefaultRecordBatch that = (DefaultRecordBatch) o;
        return buffer != null ? buffer.equals(that.buffer) : that.buffer == null;
    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

    private static byte computeAttributes(CompressionType type, TimestampType timestampType,
                                          boolean isTransactional, boolean isControl) {
        if (timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for message " +
                    "format v2 and above");

        byte attributes = isTransactional ? TRANSACTIONAL_FLAG_MASK : 0;
        if (isControl)
            attributes |= CONTROL_FLAG_MASK;
        if (type.id > 0)
            attributes |= COMPRESSION_CODEC_MASK & type.id;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            attributes |= TIMESTAMP_TYPE_MASK;
        return attributes;
    }

    static void writeHeader(ByteBuffer buffer,
                            long baseOffset,
                            int lastOffsetDelta,
                            int sizeInBytes,
                            byte magic,
                            CompressionType compressionType,
                            TimestampType timestampType,
                            long baseTimestamp,
                            long maxTimestamp,
                            long producerId,
                            short epoch,
                            int sequence,
                            boolean isTransactional,
                            boolean isControlBatch,
                            int partitionLeaderEpoch,
                            int numRecords) {
        if (magic < RecordBatch.CURRENT_MAGIC_VALUE)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (baseTimestamp < 0 && baseTimestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + baseTimestamp);

        short attributes = computeAttributes(compressionType, timestampType, isTransactional, isControlBatch);

        int position = buffer.position();
        buffer.putLong(position + BASE_OFFSET_OFFSET, baseOffset);
        buffer.putInt(position + LENGTH_OFFSET, sizeInBytes - LOG_OVERHEAD);
        buffer.putInt(position + PARTITION_LEADER_EPOCH_OFFSET, partitionLeaderEpoch);
        buffer.put(position + MAGIC_OFFSET, magic);
        buffer.putShort(position + ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(position + BASE_TIMESTAMP_OFFSET, baseTimestamp);
        buffer.putLong(position + MAX_TIMESTAMP_OFFSET, maxTimestamp);
        buffer.putInt(position + LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
        buffer.putLong(position + PRODUCER_ID_OFFSET, producerId);
        buffer.putShort(position + PRODUCER_EPOCH_OFFSET, epoch);
        buffer.putInt(position + BASE_SEQUENCE_OFFSET, sequence);
        buffer.putInt(position + RECORDS_COUNT_OFFSET, numRecords);
        long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET);
        buffer.putInt(position + CRC_OFFSET, (int) crc);
    }

    @Override
    public String toString() {
        return "RecordBatch(magic=" + magic() + ", offsets=[" + baseOffset() + ", " + lastOffset() + "], " +
                "compression=" + compressionType() + ", timestampType=" + timestampType() + ", crc=" + checksum() + ")";
    }

    public static int sizeInBytes(long baseOffset, Iterable<Record> records) {
        Iterator<Record> iterator = records.iterator();
        if (!iterator.hasNext())
            return 0;

        int size = RECORD_BATCH_OVERHEAD;
        Long baseTimestamp = null;
        while (iterator.hasNext()) {
            Record record = iterator.next();
            int offsetDelta = (int) (record.offset() - baseOffset);
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            long timestampDelta = record.timestamp() - baseTimestamp;
            size += DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    public static int sizeInBytes(Iterable<SimpleRecord> records) {
        Iterator<SimpleRecord> iterator = records.iterator();
        if (!iterator.hasNext())
            return 0;

        int size = RECORD_BATCH_OVERHEAD;
        int offsetDelta = 0;
        Long baseTimestamp = null;
        while (iterator.hasNext()) {
            SimpleRecord record = iterator.next();
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            long timestampDelta = record.timestamp() - baseTimestamp;
            size += DefaultRecord.sizeInBytes(offsetDelta++, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    /**
     * Get an upper bound on the size of a batch with only a single record using a given key and value.
     */
    static int batchSizeUpperBound(ByteBuffer key, ByteBuffer value, Header[] headers) {
        return RECORD_BATCH_OVERHEAD + DefaultRecord.recordSizeUpperBound(key, value, headers);
    }

    private abstract class RecordIterator implements CloseableIterator<Record> {
        private final Long logAppendTime;
        private final long baseOffset;
        private final long baseTimestamp;
        private final int baseSequence;
        private final int numRecords;
        private int readRecords = 0;

        public RecordIterator() {
            this.logAppendTime = timestampType() == TimestampType.LOG_APPEND_TIME ? maxTimestamp() : null;
            this.baseOffset = baseOffset();
            this.baseTimestamp = baseTimestamp();
            this.baseSequence = baseSequence();
            int numRecords = count();
            if (numRecords < 0)
                throw new InvalidRecordException("Found invalid record count " + numRecords + " in magic v" +
                        magic() + " batch");
            this.numRecords = numRecords;
        }

        @Override
        public boolean hasNext() {
            return readRecords < numRecords;
        }

        @Override
        public Record next() {
            if (readRecords >= numRecords)
                throw new NoSuchElementException();

            readRecords++;
            return readNext(baseOffset, baseTimestamp, baseSequence, logAppendTime);
        }

        protected abstract Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime);

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    static class DefaultFileChannelRecordBatch extends FileLogInputStream.FileChannelRecordBatch {

        DefaultFileChannelRecordBatch(long offset,
                                      byte magic,
                                      FileChannel channel,
                                      int position,
                                      int batchSize) {
            super(offset, magic, channel, position, batchSize);
        }

        @Override
        protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            return new DefaultRecordBatch(buffer);
        }

        @Override
        public long baseOffset() {
            return offset;
        }

        @Override
        public long lastOffset() {
            return loadBatchHeader().lastOffset();
        }

        @Override
        public long producerId() {
            return loadBatchHeader().producerId();
        }

        @Override
        public short producerEpoch() {
            return loadBatchHeader().producerEpoch();
        }

        @Override
        public int baseSequence() {
            return loadBatchHeader().baseSequence();
        }

        @Override
        public int lastSequence() {
            return loadBatchHeader().lastSequence();
        }

        @Override
        public long checksum() {
            return loadBatchHeader().checksum();
        }

        @Override
        public Integer countOrNull() {
            return loadBatchHeader().countOrNull();
        }

        @Override
        public boolean isTransactional() {
            return loadBatchHeader().isTransactional();
        }

        @Override
        public boolean isControlBatch() {
            return loadBatchHeader().isControlBatch();
        }

        @Override
        public int partitionLeaderEpoch() {
            return loadBatchHeader().partitionLeaderEpoch();
        }

        @Override
        protected int headerSize() {
            return RECORD_BATCH_OVERHEAD;
        }
    }

}
