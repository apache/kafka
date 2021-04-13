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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MockLog implements ReplicatedLog {
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final List<EpochStartOffset> epochStartOffsets = new ArrayList<>();
    private final List<LogBatch> batches = new ArrayList<>();
    private final NavigableMap<OffsetAndEpoch, MockRawSnapshotReader> snapshots = new TreeMap<>();
    private final TopicPartition topicPartition;
    private final Uuid topicId;

    private long nextId = ID_GENERATOR.getAndIncrement();
    private LogOffsetMetadata highWatermark = new LogOffsetMetadata(0, Optional.empty());
    private long lastFlushedOffset = 0;

    public MockLog(TopicPartition topicPartition, Uuid topicId) {
        this.topicPartition = topicPartition;
        this.topicId = topicId;
    }

    @Override
    public void truncateTo(long offset) {
        if (offset < highWatermark.offset) {
            throw new IllegalArgumentException("Illegal attempt to truncate to offset " + offset +
                " which is below the current high watermark " + highWatermark);
        }

        batches.removeIf(entry -> entry.lastOffset() >= offset);
        epochStartOffsets.removeIf(epochStartOffset -> epochStartOffset.startOffset >= offset);
    }

    @Override
    public boolean truncateToLatestSnapshot() {
        AtomicBoolean truncated = new AtomicBoolean(false);
        latestSnapshotId().ifPresent(snapshotId -> {
            if (snapshotId.epoch > logLastFetchedEpoch().orElse(0) ||
                (snapshotId.epoch == logLastFetchedEpoch().orElse(0) &&
                 snapshotId.offset > endOffset().offset)) {

                batches.clear();
                epochStartOffsets.clear();
                snapshots.headMap(snapshotId, false).clear();
                updateHighWatermark(new LogOffsetMetadata(snapshotId.offset));
                flush();

                truncated.set(true);
            }
        });

        return truncated.get();
    }

    @Override
    public void updateHighWatermark(LogOffsetMetadata offsetMetadata) {
        if (this.highWatermark.offset > offsetMetadata.offset) {
            throw new IllegalArgumentException("Non-monotonic update of current high watermark " +
                highWatermark + " to new value " + offsetMetadata);
        } else if (offsetMetadata.offset > endOffset().offset) {
            throw new IllegalArgumentException("Attempt to update high watermark to " + offsetMetadata +
                " which is larger than the current end offset " + endOffset());
        } else if (offsetMetadata.offset < startOffset()) {
            throw new IllegalArgumentException("Attempt to update high watermark to " + offsetMetadata +
                " which is smaller than the current start offset " + startOffset());
        }

        assertValidHighWatermarkMetadata(offsetMetadata);
        this.highWatermark = offsetMetadata;
    }

    @Override
    public LogOffsetMetadata highWatermark() {
        return highWatermark;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public Uuid topicId() {
        return topicId;
    }

    private Optional<OffsetMetadata> metadataForOffset(long offset) {
        if (offset == endOffset().offset) {
            return endOffset().metadata;
        }

        for (LogBatch batch : batches) {
            if (batch.lastOffset() < offset)
                continue;

            for (LogEntry entry : batch.entries) {
                if (entry.offset == offset) {
                    return Optional.of(entry.metadata);
                }
            }
        }

        return Optional.empty();
    }

    private void assertValidHighWatermarkMetadata(LogOffsetMetadata offsetMetadata) {
        if (!offsetMetadata.metadata.isPresent()) {
            return;
        }

        long id = ((MockOffsetMetadata) offsetMetadata.metadata.get()).id;
        long offset = offsetMetadata.offset;

        metadataForOffset(offset).ifPresent(metadata -> {
            long entryId = ((MockOffsetMetadata) metadata).id;
            if (entryId != id) {
                throw new IllegalArgumentException("High watermark " + offset +
                    " metadata uuid " + id + " does not match the " +
                    " log's record entry maintained uuid " + entryId);
            }
        });
    }

    private OptionalInt logLastFetchedEpoch() {
        if (epochStartOffsets.isEmpty()) {
            return OptionalInt.empty();
        } else {
            return OptionalInt.of(epochStartOffsets.get(epochStartOffsets.size() - 1).epoch);
        }
    }

    @Override
    public int lastFetchedEpoch() {
        return logLastFetchedEpoch().orElseGet(() -> latestSnapshotId().map(id -> id.epoch).orElse(0));
    }

    @Override
    public OffsetAndEpoch endOffsetForEpoch(int epoch) {
        int epochLowerBound = earliestSnapshotId().map(id -> id.epoch).orElse(0);
        for (EpochStartOffset epochStartOffset : epochStartOffsets) {
            if (epochStartOffset.epoch > epoch) {
                return new OffsetAndEpoch(epochStartOffset.startOffset, epochLowerBound);
            }
            epochLowerBound = epochStartOffset.epoch;
        }

        return new OffsetAndEpoch(endOffset().offset, lastFetchedEpoch());
    }

    private Optional<LogEntry> lastEntry() {
        if (batches.isEmpty())
            return Optional.empty();
        return Optional.of(batches.get(batches.size() - 1).last());
    }

    private Optional<LogEntry> firstEntry() {
        if (batches.isEmpty())
            return Optional.empty();
        return Optional.of(batches.get(0).first());
    }

    @Override
    public LogOffsetMetadata endOffset() {
        long nextOffset = lastEntry().map(entry -> entry.offset + 1).orElse(logStartOffset());
        return new LogOffsetMetadata(nextOffset, Optional.of(new MockOffsetMetadata(nextId)));
    }

    @Override
    public long startOffset() {
        return firstEntry().map(entry -> entry.offset).orElse(logStartOffset());
    }

    private long logStartOffset() {
        return earliestSnapshotId().map(id -> id.offset).orElse(0L);
    }

    private List<LogEntry> buildEntries(RecordBatch batch, Function<Record, Long> offsetSupplier) {
        List<LogEntry> entries = new ArrayList<>();
        for (Record record : batch) {
            long offset = offsetSupplier.apply(record);
            long timestamp = record.timestamp();
            ByteBuffer key = copy(record.key());
            ByteBuffer value = copy(record.value());
            entries.add(buildEntry(offset, new SimpleRecord(timestamp, key, value)));
        }
        return entries;
    }

    private ByteBuffer copy(ByteBuffer nullableByteBuffer) {
        if (nullableByteBuffer == null) {
            return null;
        } else {
            byte[] array = Utils.toArray(nullableByteBuffer, nullableByteBuffer.position(), nullableByteBuffer.limit());
            return ByteBuffer.wrap(array);
        }
    }

    private LogEntry buildEntry(Long offset, SimpleRecord record) {
        long id = nextId;
        nextId = ID_GENERATOR.getAndIncrement();
        return new LogEntry(new MockOffsetMetadata(id), offset, record);
    }


    @Override
    public LogAppendInfo appendAsLeader(Records records, int epoch) {
        return append(records, OptionalInt.of(epoch));
    }

    private Long appendBatch(LogBatch batch) {
        if (batch.epoch > lastFetchedEpoch()) {
            epochStartOffsets.add(new EpochStartOffset(batch.epoch, batch.firstOffset()));
        }
        batches.add(batch);
        return batch.firstOffset();
    }

    @Override
    public LogAppendInfo appendAsFollower(Records records) {
        return append(records, OptionalInt.empty());
    }

    private LogAppendInfo append(Records records, OptionalInt epoch) {
        if (records.sizeInBytes() == 0)
            throw new IllegalArgumentException("Attempt to append an empty record set");

        long baseOffset = endOffset().offset;
        long lastOffset = baseOffset;
        for (RecordBatch batch : records.batches()) {
            if (batch.baseOffset() != endOffset().offset) {
                /* KafkaMetadataLog throws an kafka.common.UnexpectedAppendOffsetException this is the
                 * best we can do from this module.
                 */
                throw new RuntimeException(
                    String.format(
                        "Illegal append at offset %s with current end offset of %s",
                        batch.baseOffset(),
                        endOffset().offset
                    )
                );
            }

            List<LogEntry> entries = buildEntries(batch, Record::offset);
            appendBatch(
                new LogBatch(
                    epoch.orElseGet(batch::partitionLeaderEpoch),
                    batch.isControlBatch(),
                    entries
                )
            );
            lastOffset = entries.get(entries.size() - 1).offset;
        }

        return new LogAppendInfo(baseOffset, lastOffset);
    }

    @Override
    public void flush() {
        lastFlushedOffset = endOffset().offset;
    }

    @Override
    public long lastFlushedOffset() {
        return lastFlushedOffset;
    }

    /**
     * Reopening the log causes all unflushed data to be lost.
     */
    public void reopen() {
        batches.removeIf(batch -> batch.firstOffset() >= lastFlushedOffset);
        epochStartOffsets.removeIf(epochStartOffset -> epochStartOffset.startOffset >= lastFlushedOffset);
        highWatermark = new LogOffsetMetadata(0L, Optional.empty());
    }

    public List<LogBatch> readBatches(long startOffset, OptionalLong maxOffsetOpt) {
        verifyOffsetInRange(startOffset);

        long maxOffset = maxOffsetOpt.orElse(endOffset().offset);
        if (startOffset == maxOffset) {
            return Collections.emptyList();
        }

        return batches.stream()
            .filter(batch -> batch.lastOffset() >= startOffset && batch.lastOffset() < maxOffset)
            .collect(Collectors.toList());
    }

    private void verifyOffsetInRange(long offset) {
        if (offset > endOffset().offset) {
            throw new OffsetOutOfRangeException("Requested offset " + offset + " is larger than " +
                "then log end offset " + endOffset().offset);
        }

        if (offset < this.startOffset()) {
            throw new OffsetOutOfRangeException("Requested offset " + offset + " is smaller than " +
                "then log start offset " + this.startOffset());
        }
    }

    @Override
    public LogFetchInfo read(long startOffset, Isolation isolation) {
        OptionalLong maxOffsetOpt = isolation == Isolation.COMMITTED ?
            OptionalLong.of(highWatermark.offset) :
            OptionalLong.empty();

        verifyOffsetInRange(startOffset);

        long maxOffset = maxOffsetOpt.orElse(endOffset().offset);
        if (startOffset >= maxOffset) {
            return new LogFetchInfo(MemoryRecords.EMPTY, new LogOffsetMetadata(
                startOffset, metadataForOffset(startOffset)));
        }

        ByteBuffer buffer = ByteBuffer.allocate(512);
        LogEntry firstEntry = null;

        for (LogBatch batch : batches) {
            // Note that start offset is inclusive while max offset is exclusive. We only return
            // complete batches, so batches which end at an offset larger than the max offset are
            // filtered, which is effectively the same as having the consumer drop an incomplete
            // batch returned in a fetch response.
            if (batch.lastOffset() >= startOffset) {
                if (batch.lastOffset() < maxOffset) {
                    buffer = batch.writeTo(buffer);
                }

                if (firstEntry == null && !batch.entries.isEmpty()) {
                    firstEntry = batch.entries.get(0);
                }
            }
        }

        buffer.flip();
        Records records = MemoryRecords.readableRecords(buffer);

        if (firstEntry == null) {
            throw new RuntimeException("Expected to find at least one entry starting from offset " +
                startOffset + " but found none");
        }

        return new LogFetchInfo(records, firstEntry.logOffsetMetadata());
    }

    @Override
    public void initializeLeaderEpoch(int epoch) {
        long startOffset = endOffset().offset;
        epochStartOffsets.removeIf(epochStartOffset ->
            epochStartOffset.startOffset >= startOffset || epochStartOffset.epoch >= epoch);
        epochStartOffsets.add(new EpochStartOffset(epoch, startOffset));
    }

    @Override
    public RawSnapshotWriter createSnapshot(OffsetAndEpoch snapshotId) {
        return new MockRawSnapshotWriter(snapshotId);
    }

    @Override
    public Optional<RawSnapshotReader> readSnapshot(OffsetAndEpoch snapshotId) {
        return Optional.ofNullable(snapshots.get(snapshotId));
    }

    @Override
    public Optional<OffsetAndEpoch> latestSnapshotId() {
        return Optional.ofNullable(snapshots.lastEntry())
            .map(Map.Entry::getKey);
    }

    @Override
    public Optional<OffsetAndEpoch> earliestSnapshotId() {
        return Optional.ofNullable(snapshots.firstEntry())
            .map(Map.Entry::getKey);
    }

    @Override
    public void onSnapshotFrozen(OffsetAndEpoch snapshotId) {}

    @Override
    public boolean deleteBeforeSnapshot(OffsetAndEpoch logStartSnapshotId) {
        if (logStartOffset() > logStartSnapshotId.offset ||
            highWatermark.offset < logStartSnapshotId.offset) {

            throw new OffsetOutOfRangeException(
                String.format(
                    "New log start (%s) is less than start offset (%s) or is greater than the high watermark (%s)",
                    logStartSnapshotId,
                    logStartOffset(),
                    highWatermark.offset
                )
            );
        }

        boolean updated = false;
        Optional<OffsetAndEpoch> snapshotIdOpt = latestSnapshotId();
        if (snapshotIdOpt.isPresent()) {
            OffsetAndEpoch snapshotId = snapshotIdOpt.get();
            if (startOffset() < logStartSnapshotId.offset &&
                highWatermark.offset >= logStartSnapshotId.offset &&
                snapshotId.offset >= logStartSnapshotId.offset) {

                snapshots.headMap(logStartSnapshotId, false).clear();

                batches.removeIf(entry -> entry.lastOffset() < logStartSnapshotId.offset);

                AtomicReference<Optional<EpochStartOffset>> last = new AtomicReference<>(Optional.empty());
                epochStartOffsets.removeIf(epochStartOffset -> {
                    if (epochStartOffset.startOffset <= logStartSnapshotId.offset) {
                        last.set(Optional.of(epochStartOffset));
                        return true;
                    }

                    return false;
                });

                last.get().ifPresent(epochStartOffset -> {
                    epochStartOffsets.add(0, new EpochStartOffset(epochStartOffset.epoch, logStartSnapshotId.offset));
                });

                updated = true;
            }
        }

        return updated;
    }

    static class MockOffsetMetadata implements OffsetMetadata {
        final long id;

        MockOffsetMetadata(long id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "MockOffsetMetadata(" +
                "id=" + id +
                ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockOffsetMetadata that = (MockOffsetMetadata) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    static class LogEntry {
        final MockOffsetMetadata metadata;
        final long offset;
        final SimpleRecord record;

        LogEntry(MockOffsetMetadata metadata, long offset, SimpleRecord record) {
            this.metadata = metadata;
            this.offset = offset;
            this.record = record;
        }

        LogOffsetMetadata logOffsetMetadata() {
            return new LogOffsetMetadata(offset, Optional.of(metadata));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LogEntry logEntry = (LogEntry) o;
            return offset == logEntry.offset &&
                Objects.equals(metadata, logEntry.metadata) &&
                Objects.equals(record, logEntry.record);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metadata, offset, record);
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

    final class MockRawSnapshotWriter implements RawSnapshotWriter {
        private final OffsetAndEpoch snapshotId;
        private ByteBufferOutputStream data;
        private boolean frozen;

        public MockRawSnapshotWriter(OffsetAndEpoch snapshotId) {
            this.snapshotId = snapshotId;
            this.data = new ByteBufferOutputStream(0);
            this.frozen = false;
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return snapshotId;
        }

        @Override
        public long sizeInBytes() {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }
            return data.position();
        }

        @Override
        public void append(UnalignedMemoryRecords records) {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }
            data.write(records.buffer());
        }

        @Override
        public void append(MemoryRecords records) {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }
            data.write(records.buffer());
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        public void freeze() {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }

            frozen = true;
            ByteBuffer buffer = data.buffer();
            buffer.flip();

            snapshots.putIfAbsent(snapshotId, new MockRawSnapshotReader(snapshotId, buffer));
        }

        @Override
        public void close() {}
    }

    final static class MockRawSnapshotReader implements RawSnapshotReader {
        private final OffsetAndEpoch snapshotId;
        private final MemoryRecords data;

        MockRawSnapshotReader(OffsetAndEpoch snapshotId, ByteBuffer data) {
            this.snapshotId = snapshotId;
            this.data = MemoryRecords.readableRecords(data);
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return snapshotId;
        }

        @Override
        public long sizeInBytes() {
            return data.sizeInBytes();
        }

        @Override
        public Iterator<RecordBatch> iterator() {
            return Utils.covariantCast(data.batchIterator());
        }

        @Override
        public UnalignedRecords read(long position, int size) {
            ByteBuffer buffer = data.buffer();
            buffer.position(Math.toIntExact(position));
            buffer.limit(Math.min(buffer.limit(), Math.toIntExact(position + size)));
            return new UnalignedMemoryRecords(buffer.slice());
        }

        @Override
        public void close() {}
    }
}
