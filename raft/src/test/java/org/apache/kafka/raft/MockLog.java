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
package org.apache.kafka.raft;

import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MockLog implements ReplicatedLog {
    private final List<EpochStartOffset> epochStartOffsets = new ArrayList<>();
    private final List<LogBatch> log = new ArrayList<>();
    private long highWatermark = 0L;

    @Override
    public void truncateTo(long offset) {
        if (offset < highWatermark) {
            throw new IllegalArgumentException("Illegal attempt to truncate to offset " + offset +
                " which is below the current high watermark " + highWatermark);
        }

        log.removeIf(entry -> entry.lastOffset() >= offset);
        epochStartOffsets.removeIf(epochStartOffset -> epochStartOffset.startOffset >= offset);
    }

    @Override
    public void updateHighWatermark(long offset) {
        if (this.highWatermark > offset)
            throw new IllegalArgumentException("Non-monotonic update of current high watermark " +
                highWatermark + " to new value " + offset);
        this.highWatermark = offset;
    }

    long highWatermark() {
        return highWatermark;
    }

    @Override
    public int lastFetchedEpoch() {
        if (epochStartOffsets.isEmpty())
            return 0;
        return epochStartOffsets.get(epochStartOffsets.size() - 1).epoch;
    }

    @Override
    public Optional<OffsetAndEpoch> endOffsetForEpoch(int epoch) {
        int epochLowerBound = 0;
        for (EpochStartOffset epochStartOffset : epochStartOffsets) {
            if (epochStartOffset.epoch > epoch) {
                return Optional.of(new OffsetAndEpoch(epochStartOffset.startOffset, epochLowerBound));
            }
            epochLowerBound = epochStartOffset.epoch;
        }

        if (!epochStartOffsets.isEmpty()) {
            EpochStartOffset lastEpochStartOffset = epochStartOffsets.get(epochStartOffsets.size() - 1);
            if (lastEpochStartOffset.epoch == epoch)
                return Optional.of(new OffsetAndEpoch(endOffset(), epoch));
        }

        return Optional.empty();
    }

    private Optional<LogEntry> lastEntry() {
        if (log.isEmpty())
            return Optional.empty();
        return Optional.of(log.get(log.size() - 1).last());
    }

    private Optional<LogEntry> firstEntry() {
        if (log.isEmpty())
            return Optional.empty();
        return Optional.of(log.get(0).first());
    }

    @Override
    public long endOffset() {
        return lastEntry().map(entry -> entry.offset + 1).orElse(0L);
    }

    @Override
    public long startOffset() {
        return firstEntry().map(entry -> entry.offset).orElse(0L);
    }

    private List<LogEntry> buildEntries(RecordBatch batch,
                                        Function<Record, Long> offsetSupplier) {
        List<LogEntry> entries = new ArrayList<>();
        for (Record record : batch) {
            long offset = offsetSupplier.apply(record);
            entries.add(new LogEntry(offset, new SimpleRecord(record)));
        }
        return entries;
    }

    @Override
    public LogAppendInfo appendAsLeader(Records records, int epoch) {
        if (records.sizeInBytes() == 0)
            throw new IllegalArgumentException("Attempt to append an empty record set");

        long baseOffset = endOffset();
        AtomicLong offsetSupplier = new AtomicLong(baseOffset);
        for (RecordBatch batch : records.batches()) {
            List<LogEntry> entries = buildEntries(batch, record -> offsetSupplier.getAndIncrement());
            appendBatch(new LogBatch(epoch, batch.isControlBatch(), entries));
        }
        return new LogAppendInfo(baseOffset, offsetSupplier.get() - 1);
    }

    LogAppendInfo appendAsLeader(Collection<SimpleRecord> records, int epoch) {
        long baseOffset = endOffset();
        long offset = baseOffset;

        List<LogEntry> entries = new ArrayList<>();
        for (SimpleRecord record : records) {
            entries.add(new LogEntry(offset, record));
            offset += 1;
        }
        appendBatch(new LogBatch(epoch, false, entries));
        return new LogAppendInfo(baseOffset, offset - 1);
    }

    private Long appendBatch(LogBatch batch) {
        if (batch.epoch > lastFetchedEpoch()) {
            epochStartOffsets.add(new EpochStartOffset(batch.epoch, batch.firstOffset()));
        }
        log.add(batch);
        return batch.firstOffset();
    }

    @Override
    public LogAppendInfo appendAsFollower(Records records) {
        if (records.sizeInBytes() == 0)
            throw new IllegalArgumentException("Attempt to append an empty record set");

        long baseOffset = endOffset();
        long lastOffset = baseOffset;
        for (RecordBatch batch : records.batches()) {
            List<LogEntry> entries = buildEntries(batch, Record::offset);
            appendBatch(new LogBatch(batch.partitionLeaderEpoch(), batch.isControlBatch(), entries));
            lastOffset = entries.get(entries.size() - 1).offset;
        }
        return new LogAppendInfo(baseOffset, lastOffset);
    }

    public List<LogBatch> readBatches(long startOffset, OptionalLong maxOffsetOpt) {
        long maxOffset = maxOffsetOpt.orElse(endOffset());
        if (startOffset > maxOffset) {
            throw new OffsetOutOfRangeException("Requested offset " + startOffset + " is larger than " +
                "the provided end offset " + maxOffsetOpt);
        }

        if (startOffset == maxOffset) {
            return Collections.emptyList();
        }

        return log.stream()
            .filter(batch -> batch.firstOffset() >= startOffset && batch.lastOffset() < maxOffset)
            .collect(Collectors.toList());
    }

    @Override
    public Records read(long startOffset, OptionalLong maxOffsetOpt) {
        ByteBuffer buffer = ByteBuffer.allocate(512);

        long maxOffset = maxOffsetOpt.orElse(endOffset());
        for (LogBatch batch : log) {
            // Note that start offset is inclusive while max offset is exclusive. We only return
            // complete batches, so batches which end at an offset larger than the max offset are
            // filtered, which is effectively the same as having the consumer drop an incomplete
            // batch returned in a fetch response.
            if (batch.lastOffset() >= startOffset && batch.lastOffset() < maxOffset) {
                buffer = batch.writeTo(buffer);
            }
        }

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    @Override
    public void assignEpochStartOffset(int epoch, long startOffset) {
        if (startOffset != endOffset())
            throw new IllegalArgumentException(
                "Can only assign epoch for the end offset " + endOffset() + ", but get offset " + startOffset);
        epochStartOffsets.add(new EpochStartOffset(epoch, startOffset));
    }

    static class LogEntry {
        final long offset;
        final SimpleRecord record;

        LogEntry(long offset, SimpleRecord record) {
            this.offset = offset;
            this.record = record;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LogEntry logEntry = (LogEntry) o;
            return offset == logEntry.offset &&
                Objects.equals(record, logEntry.record);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, record);
        }
    }

    static class LogBatch {
        final List<LogEntry> entries;
        final int epoch;
        final boolean isControlBatch;

        LogBatch(int epoch, boolean isControlBatch, List<LogEntry> entries) {
            if (entries.isEmpty())
                throw new IllegalArgumentException("Empty batches are not supported");
            this.entries = entries;
            this.epoch = epoch;
            this.isControlBatch = isControlBatch;
        }

        long firstOffset() {
            return first().offset;
        }

        LogEntry first() {
            return entries.get(0);
        }

        long lastOffset() {
            return last().offset;
        }

        LogEntry last() {
            return entries.get(entries.size() - 1);
        }

        ByteBuffer writeTo(ByteBuffer buffer) {
            LogEntry first = first();

            MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
                TimestampType.CREATE_TIME, first.offset, first.record.timestamp(),
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false,
                isControlBatch, epoch);

            for (LogEntry entry : entries) {
                if (isControlBatch) {
                    builder.appendControlRecordWithOffset(entry.offset, entry.record);
                } else {
                    builder.appendWithOffset(entry.offset, entry.record);
                }
            }

            builder.close();
            return builder.buffer();
        }
    }

    private static class EpochStartOffset {
        final int epoch;
        final long startOffset;

        private EpochStartOffset(int epoch, long startOffset) {
            this.epoch = epoch;
            this.startOffset = startOffset;
        }
    }

    public void clear() {
        epochStartOffsets.clear();
        log.clear();
        highWatermark = 0L;
    }
}

