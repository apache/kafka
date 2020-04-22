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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class MockLog implements ReplicatedLog {
    private final List<EpochStartOffset> epochStartOffsets = new ArrayList<>();
    private final List<LogEntry> log = new ArrayList<>();
    private long highWatermark = 0L;

    @Override
    public boolean truncateTo(long offset) {
        log.removeIf(entry -> entry.offset >= offset);
        epochStartOffsets.removeIf(epochStartOffset -> epochStartOffset.startOffset >= offset);
        return offset < endOffset();
    }

    @Override
    public void updateHighWatermark(long offset) {
        if (this.highWatermark > offset)
            throw new IllegalArgumentException("Non-monotonic update of current high watermark " +
                highWatermark + " to new value " + offset);
        this.highWatermark = offset;
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
        return Optional.of(log.get(log.size() - 1));
    }

    private Optional<LogEntry> firstEntry() {
        if (log.isEmpty())
            return Optional.empty();
        return Optional.of(log.get(0));
    }

    @Override
    public long endOffset() {
        return lastEntry().map(entry -> entry.offset + 1).orElse(0L);
    }

    @Override
    public long startOffset() {
        return firstEntry().map(entry -> entry.offset).orElse(0L);
    }

    private List<LogEntry> convert(Records records) {
        List<LogEntry> entries = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                int epoch = batch.partitionLeaderEpoch();
                long offset = record.offset();
                entries.add(new LogEntry(offset, epoch, new SimpleRecord(record)));
            }
        }
        return entries;
    }

    @Override
    public Long appendAsLeader(Records records, int epoch) {
        return appendAsLeader(convert(records).stream().map(entry -> entry.record)
                .collect(Collectors.toList()), epoch);
    }

    public Long appendAsLeader(Collection<SimpleRecord> records, int epoch) {
        long firstOffset = endOffset();
        long offset = firstOffset;

        if (epoch > lastFetchedEpoch()) {
            epochStartOffsets.add(new EpochStartOffset(epoch, firstOffset));
        }

        for (SimpleRecord record : records) {
            log.add(new LogEntry(offset, epoch, record));
            offset += 1;
        }
        return firstOffset;
    }

    public void appendAsFollower(Collection<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (entry.epoch > lastFetchedEpoch()) {
                epochStartOffsets.add(new EpochStartOffset(entry.epoch, entry.offset));
            }
        }
        log.addAll(entries);
    }

    @Override
    public void appendAsFollower(Records records) {
        appendAsFollower(convert(records));
    }

    public List<LogEntry> readEntries(long startOffset, long endOffset) {
        return log.stream().filter(entry -> entry.offset >= startOffset && entry.offset < endOffset)
                .collect(Collectors.toList());
    }

    private void writeToBuffer(ByteBuffer buffer, List<LogEntry> entries, int epoch) {
        LogEntry first = entries.get(0);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                first.offset,
                first.record.timestamp(),
                epoch);

        for (LogEntry entry : entries)
            builder.appendWithOffset(entry.offset, entry.record);

        builder.close();
    }

    @Override
    public Records read(long startOffset, OptionalLong endOffset) {
        List<LogEntry> entries = readEntries(startOffset, endOffset.orElseGet(this::endOffset));
        if (entries.isEmpty()) {
            return MemoryRecords.EMPTY;
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int epoch = entries.get(0).epoch;
            List<LogEntry> epochEntries = new ArrayList<>();
            for (LogEntry entry: entries) {
                if (entry.epoch != epoch) {
                    writeToBuffer(buffer, epochEntries, epoch);
                    epochEntries.clear();
                    epoch = entry.epoch;
                }
                epochEntries.add(entry);
            }

            if (!epochEntries.isEmpty())
                writeToBuffer(buffer, epochEntries, epoch);

            buffer.flip();
            return MemoryRecords.readableRecords(buffer);
        }
    }

    @Override
    public Optional<Integer> previousEpoch() {
        if (epochStartOffsets.size() >= 2) {
            return Optional.of(epochStartOffsets.get(epochStartOffsets.size() - 2).epoch);
        } else if (epochStartOffsets.size() == 1) {
            return Optional.of(0);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void assignEpochStartOffset(int epoch, long startOffset) {
        if (startOffset != endOffset())
            throw new IllegalStateException("Can only assign epoch for the end offset");
        epochStartOffsets.add(new EpochStartOffset(epoch, startOffset));
    }

    public static class LogEntry {
        final long offset;
        final int epoch;
        final SimpleRecord record;

        private LogEntry(long offset, int epoch, SimpleRecord record) {
            this.offset = offset;
            this.epoch = epoch;
            this.record = record;
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
}
