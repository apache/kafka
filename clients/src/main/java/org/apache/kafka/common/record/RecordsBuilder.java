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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class RecordsBuilder {
    private ByteBufferOutputStream out;
    private long nextOffset;
    private long logAppendTime = RecordBatch.NO_TIMESTAMP;
    private int partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
    private byte magic = RecordBatch.CURRENT_MAGIC_VALUE;
    private CompressionType compressionType = CompressionType.NONE;
    private TimestampType timestampType = TimestampType.CREATE_TIME;
    private long producerId = RecordBatch.NO_PRODUCER_ID;
    private short producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
    private int baseSequence = RecordBatch.NO_SEQUENCE;
    private boolean isTransactional = false;

    public RecordsBuilder(long firstOffset) {
        this.nextOffset = firstOffset;
    }

    public RecordsBuilder() {
        this(0L);
    }

    public int bufferPosition() {
        if (out == null)
            return 0;
        return out.position();
    }

    public RecordsBuilder withMagic(byte magic) {
        this.magic = magic;
        return this;
    }

    public RecordsBuilder withProducerMetadata(long producerId, short producerEpoch, int baseSequence) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        return this;
    }

    public RecordsBuilder withProducerMetadata(long producerId, short producerEpoch) {
        return withProducerMetadata(producerId, producerEpoch, RecordBatch.NO_SEQUENCE);
    }

    public RecordsBuilder setTransactional(boolean isTransactional) {
        this.isTransactional = isTransactional;
        return this;
    }

    public RecordsBuilder withTimestampType(TimestampType timestampType) {
        this.timestampType = timestampType;
        return this;
    }

    public RecordsBuilder withLogAppendTime(long logAppendTime) {
        this.logAppendTime = logAppendTime;
        this.timestampType = TimestampType.LOG_APPEND_TIME;
        return this;
    }

    public RecordsBuilder withCompression(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public RecordsBuilder withPartitionLeaderEpoch(int partitionLeaderEpoch) {
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        return this;
    }

    public BatchBuilder newBatch() {
        return newBatchFromOffset(nextOffset);
    }

    public ControlBatchBuilder newControlBatch() {
        return newControlBatchFromOffset(nextOffset);
    }

    public BatchBuilder newBatchFromOffset(long nextOffset) {
        this.nextOffset = nextOffset;
        return new BatchBuilder(magic, nextOffset, partitionLeaderEpoch, compressionType, timestampType)
                .withProducerMetadata(producerId, producerEpoch, baseSequence)
                .setTransactional(isTransactional);
    }

    public ControlBatchBuilder newControlBatchFromOffset(long nextOffset) {
        this.nextOffset = nextOffset;
        return new ControlBatchBuilder(magic, nextOffset, partitionLeaderEpoch, compressionType, timestampType)
                .withProducerMetadata(producerId, producerEpoch);
    }

    public RecordsBuilder addBatch(SimpleRecord ... records) {
        ensureRemaining(AbstractRecords.estimateSizeInBytes(magic, compressionType, new ArrayIterable<>(records)));
        RecordBatchWriter writer = createBatchWriter(false);
        for (SimpleRecord record : records) {
            writer.appendWithOffset(nextOffset, record);
            nextOffset += 1;
        }
        writer.close();
        return this;
    }

    public RecordsBuilder addBatch(Record ... records) {
        ensureRemaining(AbstractRecords.estimateSizeInBytes(magic, nextOffset, compressionType, new ArrayIterable<>(records)));
        RecordBatchWriter writer = createBatchWriter(false);
        for (Record record : records) {
            writer.append(record);
            nextOffset += 1;
        }
        writer.close();
        return this;
    }

    public RecordsBuilder addControlBatch(long timestamp, EndTransactionMarker marker) {
        ControlBatchBuilder batchBuilder = newControlBatch();
        batchBuilder.setTxnMarker(timestamp, marker);
        return batchBuilder.closeBatch();
    }

    private void ensureRemaining(int size) {
        if (out == null) {
            out = new ByteBufferOutputStream(size);
        } else {
            out.ensureRemaining(size);
        }
    }

    private void updateOffset(long offset) {
        if (offset < nextOffset)
            throw new IllegalArgumentException("Cannot append out of order offsets");
        this.nextOffset = offset + 1;
    }

    public MemoryRecords build() {
        if (out == null)
            return MemoryRecords.EMPTY;

        ByteBuffer buf = out.buffer();
        buf.flip();
        return new MemoryRecords(buf);
    }

    RecordBatchWriter createBatchWriter(boolean isControlBatch) {
        return RecordsBuilder.this.createBatchWriter(magic, nextOffset,
                compressionType, timestampType, logAppendTime, isControlBatch,
                producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch);
    }

    private abstract class AbstractBatchBuilder {
        protected final long baseOffset;
        protected byte magic;
        protected CompressionType compressionType;
        protected TimestampType timestampType;
        protected long producerId;
        protected short producerEpoch;
        protected int baseSequence;
        protected boolean isTransactional;
        protected boolean isControlBatch;
        protected int partitionLeaderEpoch;
        protected long logAppendTime;

        private AbstractBatchBuilder(byte magic,
                                     long baseOffset,
                                     int partitionLeaderEpoch,
                                     CompressionType compressionType,
                                     TimestampType timestampType) {
            this.magic = magic;
            this.baseOffset = baseOffset;
            this.partitionLeaderEpoch = partitionLeaderEpoch;
            this.compressionType = compressionType;
            this.magic = magic;
            this.timestampType = timestampType;
            this.producerId = RecordBatch.NO_PRODUCER_ID;
            this.producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
            this.baseSequence = RecordBatch.NO_SEQUENCE;
            this.isTransactional = false;
            this.isControlBatch = false;
            if (timestampType == TimestampType.LOG_APPEND_TIME)
                logAppendTime = System.currentTimeMillis();
            else
                logAppendTime = RecordBatch.NO_TIMESTAMP;
        }

        RecordBatchWriter createBatchWriter() {
            return RecordsBuilder.this.createBatchWriter(magic, baseOffset,
                    compressionType, timestampType, logAppendTime, isControlBatch,
                    producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch);
        }

    }

    RecordBatchWriter createBatchWriter(byte magic,
                                        long baseOffset,
                                        CompressionType compressionType,
                                        TimestampType timestampType,
                                        long logAppendTime,
                                        boolean isControlBatch,
                                        long producerId,
                                        short producerEpoch,
                                        int baseSequence,
                                        boolean isTransactional,
                                        int partitionLeaderEpoch) {
        RecordBatchWriter writer = new RecordBatchWriter(out, magic, compressionType, timestampType,
                baseOffset, logAppendTime, isControlBatch);
        writer.setProducerIdAndEpoch(producerId, producerEpoch);
        writer.setProducerBaseSequence(baseSequence);
        writer.setTransactional(isTransactional);
        writer.setPartitionLeaderEpoch(partitionLeaderEpoch);
        return writer;
    }

    public class ControlBatchBuilder extends AbstractBatchBuilder {
        private long timestamp;
        private EndTransactionMarker marker;

        private ControlBatchBuilder(byte magic,
                                    long baseOffset,
                                    int partitionLeaderEpoch,
                                    CompressionType compressionType,
                                    TimestampType timestampType) {
            super(magic, baseOffset, partitionLeaderEpoch, compressionType, timestampType);
            this.isTransactional = true;
            this.isControlBatch = true;
            this.baseSequence = RecordBatch.NO_SEQUENCE;
        }

        public ControlBatchBuilder setTxnMarker(long timestamp, EndTransactionMarker marker) {
            this.timestamp = timestamp;
            this.marker = marker;
            return this;
        }

        public ControlBatchBuilder withMagic(byte magic) {
            this.magic = magic;
            return this;
        }

        public ControlBatchBuilder withCompression(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public ControlBatchBuilder withTimestampType(TimestampType timestampType) {
            this.timestampType = timestampType;
            return this;
        }

        public ControlBatchBuilder withLogAppendTime(long logAppendTime) {
            this.logAppendTime = logAppendTime;
            this.timestampType = TimestampType.LOG_APPEND_TIME;
            return this;
        }

        public ControlBatchBuilder withProducerMetadata(long producerId, short producerEpoch) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            return this;
        }

        public RecordsBuilder closeBatch() {
            updateOffset(baseOffset);
            ensureRemaining(DefaultRecordBatch.RECORD_BATCH_OVERHEAD +
                    EndTransactionMarker.CURRENT_END_TXN_SCHEMA_RECORD_SIZE);
            RecordBatchWriter builder = createBatchWriter();
            builder.appendEndTxnMarker(timestamp, marker);
            builder.close();
            return RecordsBuilder.this;
        }
    }

    public class BatchBuilder extends AbstractBatchBuilder {
        private List<SimpleRecordAndOffset> records;

        private BatchBuilder(byte magic,
                             long baseOffset,
                             int partitionLeaderEpoch,
                             CompressionType compressionType,
                             TimestampType timestampType) {
            super(magic, baseOffset, partitionLeaderEpoch, compressionType, timestampType);
            this.isControlBatch = false;
            this.records = new ArrayList<>();
        }

        public BatchBuilder withMagic(byte magic) {
            this.magic = magic;
            return this;
        }

        public BatchBuilder withCompression(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public BatchBuilder withTimestampType(TimestampType timestampType) {
            this.timestampType = timestampType;
            return this;
        }

        public BatchBuilder withLogAppendTime(long logAppendTime) {
            this.logAppendTime = logAppendTime;
            this.timestampType = TimestampType.LOG_APPEND_TIME;
            return this;
        }

        public BatchBuilder withProducerMetadata(long producerId, short producerEpoch, int baseSequence) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.baseSequence = baseSequence;
            return this;
        }

        public BatchBuilder setTransactional(boolean isTransactional) {
            this.isTransactional = isTransactional;
            return this;
        }

        public BatchBuilder appendWithOffset(long offset, SimpleRecord record) {
            updateOffset(offset);
            this.records.add(new SimpleRecordAndOffset(record, offset));
            return this;
        }

        public BatchBuilder appendWithOffset(long offset, LegacyRecord record) {
            return appendWithOffset(offset, new SimpleRecord(record.timestamp(), record.key(), record.value()));
        }

        public BatchBuilder append(Record ... records) {
            for (Record record : records)
                appendWithOffset(record.offset(), new SimpleRecord(record));
            return this;
        }

        public BatchBuilder append(SimpleRecord record) {
            appendWithOffset(RecordsBuilder.this.nextOffset, record);
            return this;
        }

        public BatchBuilder append(SimpleRecord ... records) {
            for (SimpleRecord record : records)
                appendWithOffset(RecordsBuilder.this.nextOffset, record);
            return this;
        }

        public RecordsBuilder closeBatch() {
            ensureRemaining(AbstractRecords.estimateSizeInBytes(magic, compressionType, new RecordList(records)));
            RecordBatchWriter builder = createBatchWriter();
            for (SimpleRecordAndOffset recordAndOffset : records)
                builder.appendWithOffset(recordAndOffset.offset, recordAndOffset.record);
            builder.close();
            return RecordsBuilder.this;
        }

    }

    private static class RecordList implements Iterable<SimpleRecord> {
        private final List<SimpleRecordAndOffset> records;

        RecordList(List<SimpleRecordAndOffset> records) {
            this.records = records;
        }

        @Override
        public Iterator<SimpleRecord> iterator() {
            final Iterator<SimpleRecordAndOffset> iterator = records.iterator();
            return new Iterator<SimpleRecord>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public SimpleRecord next() {
                    return iterator.next().record;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    private static class ArrayIterable<T> implements Iterable<T> {
        private final T[] array;

        public ArrayIterable(T[] array) {
            this.array = array;
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                int pos = 0;
                @Override
                public boolean hasNext() {
                    return pos < array.length;
                }

                @Override
                public T next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    T next = array[pos];
                    pos += 1;
                    return next;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    private static class SimpleRecordAndOffset {
        final SimpleRecord record;
        final long offset;

        public SimpleRecordAndOffset(SimpleRecord record, long offset) {
            this.record = record;
            this.offset = offset;
        }
    }

}
