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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;

/**
 * This {@link RecordBatch} implementation is for magic versions 0 and 1. In addition to implementing
 * {@link RecordBatch}, it also implements {@link Record}, which exposes the duality of the old message
 * format in its handling of compressed messages. The wrapper record is considered the record batch in this
 * interface, while the inner records are considered the log records (though they both share the same schema).
 *
 * In general, this class should not be used directly. Instances of {@link Records} provides access to this
 * class indirectly through the {@link RecordBatch} interface.
 */
public abstract class AbstractLegacyRecordBatch extends AbstractRecordBatch implements Record {

    public abstract LegacyRecord outerRecord();

    @Override
    public long lastOffset() {
        return offset();
    }

    @Override
    public boolean isValid() {
        return outerRecord().isValid();
    }

    @Override
    public void ensureValid() {
        outerRecord().ensureValid();
    }

    @Override
    public int keySize() {
        return outerRecord().keySize();
    }

    @Override
    public boolean hasKey() {
        return outerRecord().hasKey();
    }

    @Override
    public ByteBuffer key() {
        return outerRecord().key();
    }

    @Override
    public int valueSize() {
        return outerRecord().valueSize();
    }

    @Override
    public boolean hasValue() {
        return !outerRecord().hasNullValue();
    }

    @Override
    public ByteBuffer value() {
        return outerRecord().value();
    }

    @Override
    public Header[] headers() {
        return Record.EMPTY_HEADERS;
    }

    @Override
    public boolean hasMagic(byte magic) {
        return magic == outerRecord().magic();
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return outerRecord().timestampType() == timestampType;
    }

    @Override
    public long checksum() {
        return outerRecord().checksum();
    }

    @Override
    public long maxTimestamp() {
        return timestamp();
    }

    @Override
    public long timestamp() {
        return outerRecord().timestamp();
    }

    @Override
    public TimestampType timestampType() {
        return outerRecord().timestampType();
    }

    @Override
    public long baseOffset() {
        return iterator().next().offset();
    }

    @Override
    public byte magic() {
        return outerRecord().magic();
    }

    @Override
    public CompressionType compressionType() {
        return outerRecord().compressionType();
    }

    @Override
    public int sizeInBytes() {
        return outerRecord().sizeInBytes() + LOG_OVERHEAD;
    }

    @Override
    public Integer countOrNull() {
        return null;
    }

    @Override
    public String toString() {
        return "LegacyRecordBatch(offset=" + offset() + ", " + outerRecord() + ")";
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        writeHeader(buffer, offset(), outerRecord().sizeInBytes());
        buffer.put(outerRecord().buffer().duplicate());
    }

    @Override
    public long producerId() {
        return RecordBatch.NO_PRODUCER_ID;
    }

    @Override
    public short producerEpoch() {
        return RecordBatch.NO_PRODUCER_EPOCH;
    }

    @Override
    public boolean hasProducerId() {
        return false;
    }

    @Override
    public int sequence() {
        return RecordBatch.NO_SEQUENCE;
    }

    @Override
    public int baseSequence() {
        return RecordBatch.NO_SEQUENCE;
    }

    @Override
    public int lastSequence() {
        return RecordBatch.NO_SEQUENCE;
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public int partitionLeaderEpoch() {
        return RecordBatch.NO_PARTITION_LEADER_EPOCH;
    }

    @Override
    public boolean isControlBatch() {
        return false;
    }

    @Override
    public OptionalLong deleteHorizonMs() {
        return OptionalLong.empty();
    }

    /**
     * Get an iterator for the nested entries contained within this batch. Note that
     * if the batch is not compressed, then this method will return an iterator over the
     * shallow record only (i.e. this object).
     * @return An iterator over the records contained within this batch
     */
    @Override
    public Iterator<Record> iterator() {
        return iterator(BufferSupplier.NO_CACHING);
    }

    CloseableIterator<Record> iterator(BufferSupplier bufferSupplier) {
        if (isCompressed())
            return new DeepRecordsIterator(this, false, Integer.MAX_VALUE, bufferSupplier);

        return new CloseableIterator<>() {
            private boolean hasNext = true;

            @Override
            public void close() {}

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Record next() {
                if (!hasNext)
                    throw new NoSuchElementException();
                hasNext = false;
                return AbstractLegacyRecordBatch.this;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
        // the older message format versions do not support streaming, so we return the normal iterator
        return iterator(bufferSupplier);
    }

    static void writeHeader(ByteBuffer buffer, long offset, int size) {
        buffer.putLong(offset);
        buffer.putInt(size);
    }

    static void writeHeader(DataOutputStream out, long offset, int size) throws IOException {
        out.writeLong(offset);
        out.writeInt(size);
    }

    private static final class DataLogInputStream implements LogInputStream<AbstractLegacyRecordBatch> {
        private final InputStream stream;
        private final int maxMessageSize;
        private final ByteBuffer offsetAndSizeBuffer;

        DataLogInputStream(InputStream stream, int maxMessageSize) {
            this.stream = stream;
            this.maxMessageSize = maxMessageSize;
            this.offsetAndSizeBuffer = ByteBuffer.allocate(Records.LOG_OVERHEAD);
        }

        public AbstractLegacyRecordBatch nextBatch() throws IOException {
            offsetAndSizeBuffer.clear();
            Utils.readFully(stream, offsetAndSizeBuffer);
            if (offsetAndSizeBuffer.hasRemaining())
                return null;

            long offset = offsetAndSizeBuffer.getLong(Records.OFFSET_OFFSET);
            int size = offsetAndSizeBuffer.getInt(Records.SIZE_OFFSET);
            if (size < LegacyRecord.RECORD_OVERHEAD_V0)
                throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", LegacyRecord.RECORD_OVERHEAD_V0));
            if (size > maxMessageSize)
                throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

            ByteBuffer batchBuffer = ByteBuffer.allocate(size);
            Utils.readFully(stream, batchBuffer);
            if (batchBuffer.hasRemaining())
                return null;
            batchBuffer.flip();

            return new BasicLegacyRecordBatch(offset, new LegacyRecord(batchBuffer));
        }
    }

    private static class DeepRecordsIterator extends AbstractIterator<Record> implements CloseableIterator<Record> {
        private final ArrayDeque<AbstractLegacyRecordBatch> innerEntries;
        private final long absoluteBaseOffset;
        private final byte wrapperMagic;

        private DeepRecordsIterator(AbstractLegacyRecordBatch wrapperEntry,
                                    boolean ensureMatchingMagic,
                                    int maxMessageSize,
                                    BufferSupplier bufferSupplier) {
            LegacyRecord wrapperRecord = wrapperEntry.outerRecord();
            this.wrapperMagic = wrapperRecord.magic();
            if (wrapperMagic != RecordBatch.MAGIC_VALUE_V0 && wrapperMagic != RecordBatch.MAGIC_VALUE_V1)
                throw new InvalidRecordException("Invalid wrapper magic found in legacy deep record iterator " + wrapperMagic);

            CompressionType compressionType = wrapperRecord.compressionType();
            if (compressionType == CompressionType.ZSTD)
                throw new InvalidRecordException("Invalid wrapper compressionType found in legacy deep record iterator " + wrapperMagic);
            ByteBuffer wrapperValue = wrapperRecord.value();
            if (wrapperValue == null)
                throw new InvalidRecordException("Found invalid compressed record set with null value (magic = " +
                        wrapperMagic + ")");

            InputStream stream = Compression.of(compressionType).build().wrapForInput(wrapperValue, wrapperRecord.magic(), bufferSupplier);
            LogInputStream<AbstractLegacyRecordBatch> logStream = new DataLogInputStream(stream, maxMessageSize);

            long lastOffsetFromWrapper = wrapperEntry.lastOffset();
            long timestampFromWrapper = wrapperRecord.timestamp();
            this.innerEntries = new ArrayDeque<>();

            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset. For simplicity and because it's a format that is on its way out, we
            // do the same for message format version 0
            try {
                while (true) {
                    AbstractLegacyRecordBatch innerEntry = logStream.nextBatch();
                    if (innerEntry == null)
                        break;

                    LegacyRecord record = innerEntry.outerRecord();
                    byte magic = record.magic();

                    if (ensureMatchingMagic && magic != wrapperMagic)
                        throw new InvalidRecordException("Compressed message magic " + magic +
                                " does not match wrapper magic " + wrapperMagic);

                    if (magic == RecordBatch.MAGIC_VALUE_V1) {
                        LegacyRecord recordWithTimestamp = new LegacyRecord(
                                record.buffer(),
                                timestampFromWrapper,
                                wrapperRecord.timestampType());
                        innerEntry = new BasicLegacyRecordBatch(innerEntry.lastOffset(), recordWithTimestamp);
                    }

                    innerEntries.addLast(innerEntry);
                }

                if (innerEntries.isEmpty())
                    throw new InvalidRecordException("Found invalid compressed record set with no inner records");

                if (wrapperMagic == RecordBatch.MAGIC_VALUE_V1) {
                    if (lastOffsetFromWrapper == 0) {
                        // The outer offset may be 0 if this is produce data from certain versions of librdkafka.
                        this.absoluteBaseOffset = 0;
                    } else {
                        long lastInnerOffset = innerEntries.getLast().offset();
                        if (lastOffsetFromWrapper < lastInnerOffset)
                            throw new InvalidRecordException("Found invalid wrapper offset in compressed v1 message set, " +
                                    "wrapper offset '" + lastOffsetFromWrapper + "' is less than the last inner message " +
                                    "offset '" + lastInnerOffset + "' and it is not zero.");
                        this.absoluteBaseOffset = lastOffsetFromWrapper - lastInnerOffset;
                    }
                } else {
                    this.absoluteBaseOffset = -1;
                }
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                Utils.closeQuietly(stream, "records iterator stream");
            }
        }

        @Override
        protected Record makeNext() {
            if (innerEntries.isEmpty())
                return allDone();

            AbstractLegacyRecordBatch entry = innerEntries.remove();

            // Convert offset to absolute offset if needed.
            if (wrapperMagic == RecordBatch.MAGIC_VALUE_V1) {
                long absoluteOffset = absoluteBaseOffset + entry.offset();
                entry = new BasicLegacyRecordBatch(absoluteOffset, entry.outerRecord());
            }

            if (entry.isCompressed())
                throw new InvalidRecordException("Inner messages must not be compressed");

            return entry;
        }

        @Override
        public void close() {}
    }

    private static class BasicLegacyRecordBatch extends AbstractLegacyRecordBatch {
        private final LegacyRecord record;
        private final long offset;

        private BasicLegacyRecordBatch(long offset, LegacyRecord record) {
            this.offset = offset;
            this.record = record;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public LegacyRecord outerRecord() {
            return record;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            BasicLegacyRecordBatch that = (BasicLegacyRecordBatch) o;

            return offset == that.offset &&
                Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            int result = record != null ? record.hashCode() : 0;
            result = 31 * result + Long.hashCode(offset);
            return result;
        }
    }

    static class ByteBufferLegacyRecordBatch extends AbstractLegacyRecordBatch implements MutableRecordBatch {
        private final ByteBuffer buffer;
        private final LegacyRecord record;

        ByteBufferLegacyRecordBatch(ByteBuffer buffer) {
            this.buffer = buffer;
            buffer.position(LOG_OVERHEAD);
            this.record = new LegacyRecord(buffer.slice());
            buffer.position(OFFSET_OFFSET);
        }

        @Override
        public long offset() {
            return buffer.getLong(OFFSET_OFFSET);
        }

        @Override
        public LegacyRecord outerRecord() {
            return record;
        }

        @Override
        public void setLastOffset(long offset) {
            buffer.putLong(OFFSET_OFFSET, offset);
        }

        @Override
        public void setMaxTimestamp(TimestampType timestampType, long timestamp) {
            if (record.magic() == RecordBatch.MAGIC_VALUE_V0)
                throw new UnsupportedOperationException("Cannot set timestamp for a record with magic = 0");

            long currentTimestamp = record.timestamp();
            // We don't need to recompute crc if the timestamp is not updated.
            if (record.timestampType() == timestampType && currentTimestamp == timestamp)
                return;

            setTimestampAndUpdateCrc(timestampType, timestamp);
        }

        @Override
        public void setPartitionLeaderEpoch(int epoch) {
            throw new UnsupportedOperationException("Magic versions prior to 2 do not support partition leader epoch");
        }

        private void setTimestampAndUpdateCrc(TimestampType timestampType, long timestamp) {
            byte attributes = LegacyRecord.computeAttributes(magic(), compressionType(), timestampType);
            buffer.put(LOG_OVERHEAD + LegacyRecord.ATTRIBUTES_OFFSET, attributes);
            buffer.putLong(LOG_OVERHEAD + LegacyRecord.TIMESTAMP_OFFSET, timestamp);
            long crc = record.computeChecksum();
            ByteUtils.writeUnsignedInt(buffer, LOG_OVERHEAD + LegacyRecord.CRC_OFFSET, crc);
        }

        /**
         * LegacyRecordBatch does not implement this iterator and would hence fallback to the normal iterator.
         *
         * @return An iterator over the records contained within this batch
         */
        @Override
        public CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier) {
            return CloseableIterator.wrap(iterator(bufferSupplier));
        }

        @Override
        public void writeTo(ByteBufferOutputStream outputStream) {
            outputStream.write(buffer.duplicate());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ByteBufferLegacyRecordBatch that = (ByteBufferLegacyRecordBatch) o;

            return Objects.equals(buffer, that.buffer);
        }

        @Override
        public int hashCode() {
            return buffer != null ? buffer.hashCode() : 0;
        }
    }

    static class LegacyFileChannelRecordBatch extends FileLogInputStream.FileChannelRecordBatch {

        LegacyFileChannelRecordBatch(long offset,
                                     byte magic,
                                     FileRecords fileRecords,
                                     int position,
                                     int batchSize) {
            super(offset, magic, fileRecords, position, batchSize);
        }

        @Override
        protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            return new ByteBufferLegacyRecordBatch(buffer);
        }

        @Override
        public long baseOffset() {
            return loadFullBatch().baseOffset();
        }

        @Override
        public OptionalLong deleteHorizonMs() {
            return OptionalLong.empty();
        }

        @Override
        public long lastOffset() {
            return offset;
        }

        @Override
        public long producerId() {
            return RecordBatch.NO_PRODUCER_ID;
        }

        @Override
        public short producerEpoch() {
            return RecordBatch.NO_PRODUCER_EPOCH;
        }

        @Override
        public int baseSequence() {
            return RecordBatch.NO_SEQUENCE;
        }

        @Override
        public int lastSequence() {
            return RecordBatch.NO_SEQUENCE;
        }

        @Override
        public Integer countOrNull() {
            return null;
        }

        @Override
        public boolean isTransactional() {
            return false;
        }

        @Override
        public boolean isControlBatch() {
            return false;
        }

        @Override
        public int partitionLeaderEpoch() {
            return RecordBatch.NO_PARTITION_LEADER_EPOCH;
        }

        @Override
        protected int headerSize() {
            return LOG_OVERHEAD + LegacyRecord.headerSize(magic);
        }

    }

}
