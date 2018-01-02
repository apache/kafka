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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A {@link Records} implementation backed by a ByteBuffer. This is used only for reading or
 * modifying in-place an existing buffer of record batches. To create a new buffer see {@link RecordBatchWriter},
 * or one of the {@link #builder(ByteBuffer, byte, CompressionType, TimestampType, long)} variants.
 */
public class MemoryRecords extends AbstractRecords {
    private static final Logger log = LoggerFactory.getLogger(MemoryRecords.class);
    public static final MemoryRecords EMPTY = new MemoryRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    private final Iterable<MutableRecordBatch> batches = new Iterable<MutableRecordBatch>() {
        @Override
        public Iterator<MutableRecordBatch> iterator() {
            return new RecordBatchIterator<>(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
        }
    };

    private int validBytes = -1;

    public MemoryRecords(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer should not be null");
        this.buffer = buffer;
    }

    @Override
    public int sizeInBytes() {
        return buffer.limit();
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        if (position > Integer.MAX_VALUE)
            throw new IllegalArgumentException("position should not be greater than Integer.MAX_VALUE: " + position);
        if (position + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());

        int pos = (int) position;
        ByteBuffer dup = buffer.duplicate();
        dup.position(pos);
        dup.limit(pos + length);
        return channel.write(dup);
    }

    /**
     * Write all records to the given channel (including partial records).
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages. This
     * may be smaller than what is returned by {@link #sizeInBytes()}.
     * @return The number of valid bytes
     */
    public int validBytes() {
        if (validBytes >= 0)
            return validBytes;

        int bytes = 0;
        for (RecordBatch batch : batches())
            bytes += batch.sizeInBytes();

        this.validBytes = bytes;
        return bytes;
    }

    @Override
    public ConvertedRecords<MemoryRecords> downConvert(byte toMagic, long firstOffset, Time time) {
        return downConvert(batches(), toMagic, firstOffset, time);
    }

    /**
     * Filter the records into the provided ByteBuffer.
     *
     * @param partition                   The partition that is filtered (used only for logging)
     * @param filter                      The filter function
     * @param destinationBuffer           The byte buffer to write the filtered records to
     * @param maxRecordBatchSize          The maximum record batch size. Note this is not a hard limit: if a batch
     *                                    exceeds this after filtering, we log a warning, but the batch will still be
     *                                    created.
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if supported. For small
     *                                    record batches, allocating a potentially large buffer (64 KB for LZ4) will
     *                                    dominate the cost of decompressing and iterating over the records in the
     *                                    batch. As such, a supplier that reuses buffers will have a significant
     *                                    performance impact.
     * @return A FilterResult with a summary of the output (for metrics) and potentially an overflow buffer
     */
    public FilterResult filterTo(TopicPartition partition, RecordFilter filter, ByteBuffer destinationBuffer,
                                 int maxRecordBatchSize, BufferSupplier decompressionBufferSupplier) {
        return filterTo(partition, batches(), filter, destinationBuffer, maxRecordBatchSize, decompressionBufferSupplier);
    }

    private static FilterResult filterTo(TopicPartition partition, Iterable<MutableRecordBatch> batches,
                                         RecordFilter filter, ByteBuffer destinationBuffer, int maxRecordBatchSize,
                                         BufferSupplier decompressionBufferSupplier) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long maxOffset = -1L;
        long shallowOffsetOfMaxTimestamp = -1L;
        int messagesRead = 0;
        int bytesRead = 0;
        int messagesRetained = 0;
        int bytesRetained = 0;

        ByteBufferOutputStream bufferOutputStream = new ByteBufferOutputStream(destinationBuffer);

        for (MutableRecordBatch batch : batches) {
            bytesRead += batch.sizeInBytes();

            BatchRetention batchRetention = filter.checkBatchRetention(batch);
            if (batchRetention == BatchRetention.DELETE)
                continue;

            // We use the absolute offset to decide whether to retain the message or not. Due to KAFKA-4298, we have to
            // allow for the possibility that a previous version corrupted the log by writing a compressed record batch
            // with a magic value not matching the magic of the records (magic < 2). This will be fixed as we
            // recopy the messages to the destination buffer.

            byte batchMagic = batch.magic();
            boolean writeOriginalBatch = true;
            List<Record> retainedRecords = new ArrayList<>();

            try (final CloseableIterator<Record> iterator = batch.streamingIterator(decompressionBufferSupplier)) {
                while (iterator.hasNext()) {
                    Record record = iterator.next();
                    messagesRead += 1;

                    if (filter.shouldRetainRecord(batch, record)) {
                        // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                        // the corrupted batch with correct data.
                        if (!record.hasMagic(batchMagic))
                            writeOriginalBatch = false;

                        if (record.offset() > maxOffset)
                            maxOffset = record.offset();

                        retainedRecords.add(record);
                    } else {
                        writeOriginalBatch = false;
                    }
                }
            }

            if (!retainedRecords.isEmpty()) {
                if (writeOriginalBatch) {
                    batch.writeTo(bufferOutputStream);
                    messagesRetained += retainedRecords.size();
                    bytesRetained += batch.sizeInBytes();
                    if (batch.maxTimestamp() > maxTimestamp) {
                        maxTimestamp = batch.maxTimestamp();
                        shallowOffsetOfMaxTimestamp = batch.lastOffset();
                    }
                } else {
                    RecordBatchWriter builder = buildRetainedRecordsInto(batch, retainedRecords, bufferOutputStream);
                    builder.close();

                    MemoryRecords records = builder.toRecords();
                    int filteredBatchSize = records.sizeInBytes();

                    messagesRetained += retainedRecords.size();
                    bytesRetained += filteredBatchSize;

                    if (filteredBatchSize > batch.sizeInBytes() && filteredBatchSize > maxRecordBatchSize)
                        log.warn("Record batch from {} with last offset {} exceeded max record batch size {} after cleaning " +
                                        "(new size is {}). Consumers with version earlier than 0.10.1.0 may need to " +
                                        "increase their fetch sizes.",
                                partition, batch.lastOffset(), maxRecordBatchSize, filteredBatchSize);

                    RecordBatchWriter.RecordsInfo info = builder.info();
                    if (info.maxTimestamp > maxTimestamp) {
                        maxTimestamp = info.maxTimestamp;
                        shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp;
                    }
                }
            } else if (batchRetention == BatchRetention.RETAIN_EMPTY) {
                if (batchMagic < RecordBatch.MAGIC_VALUE_V2)
                    throw new IllegalStateException("Empty batches are only supported for magic v2 and above");

                bufferOutputStream.ensureRemaining(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
                DefaultRecordBatch.writeEmptyHeader(bufferOutputStream.buffer(), batchMagic, batch.producerId(),
                        batch.producerEpoch(), batch.baseSequence(), batch.baseOffset(), batch.lastOffset(),
                        batch.partitionLeaderEpoch(), batch.timestampType(), batch.maxTimestamp(),
                        batch.isTransactional(), batch.isControlBatch());
            }

            // If we had to allocate a new buffer to fit the filtered output (see KAFKA-5316), return early to
            // avoid the need for additional allocations.
            ByteBuffer outputBuffer = bufferOutputStream.buffer();
            if (outputBuffer != destinationBuffer)
                return new FilterResult(outputBuffer, messagesRead, bytesRead, messagesRetained, bytesRetained,
                        maxOffset, maxTimestamp, shallowOffsetOfMaxTimestamp);
        }

        return new FilterResult(destinationBuffer, messagesRead, bytesRead, messagesRetained, bytesRetained,
                maxOffset, maxTimestamp, shallowOffsetOfMaxTimestamp);
    }

    private static RecordBatchWriter buildRetainedRecordsInto(RecordBatch originalBatch,
                                                              List<Record> retainedRecords,
                                                              ByteBufferOutputStream bufferOutputStream) {
        byte magic = originalBatch.magic();
        TimestampType timestampType = originalBatch.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ?
                originalBatch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;
        long baseOffset = magic >= RecordBatch.MAGIC_VALUE_V2 ?
                originalBatch.baseOffset() : retainedRecords.get(0).offset();

        RecordBatchWriter writer = new RecordBatchWriter(bufferOutputStream, magic,
                originalBatch.compressionType(), timestampType, baseOffset, logAppendTime,
                originalBatch.isControlBatch());

        writer.setProducerIdAndEpoch(originalBatch.producerId(), originalBatch.producerEpoch());
        writer.setProducerBaseSequence(originalBatch.baseSequence());
        writer.setTransactional(originalBatch.isTransactional());
        writer.setPartitionLeaderEpoch(originalBatch.partitionLeaderEpoch());

        for (Record record : retainedRecords)
            writer.append(record);

        writer.closeForAppends();

        if (magic >= RecordBatch.MAGIC_VALUE_V2)
            // we must preserve the last offset from the initial batch in order to ensure that the
            // last sequence number from the batch remains even after compaction. Otherwise, the producer
            // could incorrectly see an out of sequence error.
            writer.setBatchLastOffset(originalBatch.lastOffset());

        return writer;
    }

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public Iterable<MutableRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');

        Iterator<MutableRecordBatch> batchIterator = batches.iterator();
        while (batchIterator.hasNext()) {
            RecordBatch batch = batchIterator.next();
            try (CloseableIterator<Record> recordsIterator = batch.streamingIterator(BufferSupplier.create())) {
                while (recordsIterator.hasNext()) {
                    Record record = recordsIterator.next();
                    appendRecordToStringBuilder(builder, record.toString());
                    if (recordsIterator.hasNext())
                        builder.append(", ");
                }
            } catch (KafkaException e) {
                appendRecordToStringBuilder(builder, "CORRUPTED");
            }
            if (batchIterator.hasNext())
                builder.append(", ");
        }
        builder.append(']');
        return builder.toString();
    }

    private void appendRecordToStringBuilder(StringBuilder builder, String recordAsString) {
        builder.append('(')
            .append("record=")
            .append(recordAsString)
            .append(")");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MemoryRecords that = (MemoryRecords) o;

        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public static abstract class RecordFilter {
        public enum BatchRetention {
            DELETE, // Delete the batch without inspecting records
            RETAIN_EMPTY, // Retain the batch even if it is empty
            DELETE_EMPTY  // Delete the batch if it is empty
        }

        /**
         * Check whether the full batch can be discarded (i.e. whether we even need to
         * check the records individually).
         */
        protected abstract BatchRetention checkBatchRetention(RecordBatch batch);

        /**
         * Check whether a record should be retained in the log. Note that {@link #checkBatchRetention(RecordBatch)}
         * is used prior to checking individual record retention. Only records from batches which were not
         * explicitly discarded with {@link BatchRetention#DELETE} will be considered.
         */
        protected abstract boolean shouldRetainRecord(RecordBatch recordBatch, Record record);
    }

    public static class FilterResult {
        public final ByteBuffer output;
        public final int messagesRead;
        public final int bytesRead;
        public final int messagesRetained;
        public final int bytesRetained;
        public final long maxOffset;
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public FilterResult(ByteBuffer output,
                            int messagesRead,
                            int bytesRead,
                            int messagesRetained,
                            int bytesRetained,
                            long maxOffset,
                            long maxTimestamp,
                            long shallowOffsetOfMaxTimestamp) {
            this.output = output;
            this.messagesRead = messagesRead;
            this.bytesRead = bytesRead;
            this.messagesRetained = messagesRetained;
            this.bytesRetained = bytesRetained;
            this.maxOffset = maxOffset;
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

}
