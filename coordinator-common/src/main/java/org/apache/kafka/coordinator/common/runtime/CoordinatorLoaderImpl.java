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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Coordinator loader which reads records from a partition and replays them
 * to a group coordinator.
 *
 * @param <T> The record type.
 */
public class CoordinatorLoaderImpl<T> implements CoordinatorLoader<T> {

    /**
     * The interval between updating the last committed offset during loading, in offsets. Smaller
     * values commit more often at the expense of loading times when the workload is simple and does
     * not create collections that need to participate in {@link CoordinatorPlayback} snapshotting.
     * Larger values commit less often and allow more temporary data to accumulate before the next
     * commit when the workload creates many temporary collections that need to be snapshotted.
     *
     * The value of 16,384 was chosen as a trade-off between the performance of these two workloads.
     *
     * When changing this value, please run the GroupCoordinatorShardLoadingBenchmark to evaluate
     * the relative change in performance.
     */
    public static final long DEFAULT_COMMIT_INTERVAL_OFFSETS = 16384;

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLoaderImpl.class);

    private final Time time;
    private final Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier;
    private final Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier;
    private final Deserializer<T> deserializer;
    private final int loadBufferSize;
    private final long commitIntervalOffsets;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final KafkaScheduler scheduler = new KafkaScheduler(1);

    public CoordinatorLoaderImpl(
        Time time,
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier,
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier,
        Deserializer<T> deserializer,
        int loadBufferSize,
        long commitIntervalOffsets
    ) {
        this.time = time;
        this.partitionLogSupplier = partitionLogSupplier;
        this.partitionLogEndOffsetSupplier = partitionLogEndOffsetSupplier;
        this.deserializer = deserializer;
        this.loadBufferSize = loadBufferSize;
        this.commitIntervalOffsets = commitIntervalOffsets;
        this.scheduler.startup();
    }

    /**
     * Loads the coordinator by reading all the records from the TopicPartition
     * and applying them to the Replayable object.
     *
     * @param tp          The TopicPartition to read from.
     * @param coordinator The object to apply records to.
     */
    @Override
    public CompletableFuture<LoadSummary> load(TopicPartition tp, CoordinatorPlayback<T> coordinator) {
        final CompletableFuture<LoadSummary> future = new CompletableFuture<>();
        long startTimeMs = time.milliseconds();
        try {
            ScheduledFuture<?> result = scheduler.scheduleOnce(String.format("Load coordinator from %s", tp),
                () -> doLoad(tp, coordinator, future, startTimeMs));
            if (result.isCancelled()) {
                future.completeExceptionally(new RuntimeException("Coordinator loader is closed."));
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private void doLoad(
        TopicPartition tp,
        CoordinatorPlayback<T> coordinator,
        CompletableFuture<LoadSummary> future,
        long startTimeMs
    ) {
        long schedulerQueueTimeMs = time.milliseconds() - startTimeMs;
        try {
            Optional<UnifiedLog> logOpt = partitionLogSupplier.apply(tp);
            if (logOpt.isEmpty()) {
                future.completeExceptionally(new NotLeaderOrFollowerException(
                    "Could not load records from " + tp + " because the log does not exist."));
                return;
            }

            UnifiedLog log = logOpt.get();

            // Buffer may not be needed if records are read from memory.
            ByteBuffer buffer = ByteBuffer.allocate(0);
            long currentOffset = log.logStartOffset();
            LoadStats stats = new LoadStats();

            long lastCommittedOffset = -1L;
            while (shouldFetchNextBatch(currentOffset, logEndOffset(tp), stats.readAtLeastOneRecord)) {
                FetchDataInfo fetchDataInfo = log.read(currentOffset, loadBufferSize, FetchIsolation.LOG_END, true);

                stats.readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes() > 0;

                // Reuses a potentially larger buffer by updating it when reading from FileRecords.
                MemoryRecords memoryRecords = toReadableMemoryRecords(tp, fetchDataInfo.records, buffer);
                if (fetchDataInfo.records instanceof FileRecords) {
                    buffer = memoryRecords.buffer();
                }

                ReplayResult replayResult = processMemoryRecords(tp, log, memoryRecords, coordinator, stats, currentOffset, lastCommittedOffset);
                currentOffset = replayResult.nextOffset;
                lastCommittedOffset = replayResult.lastCommittedOffset;
            }

            long endTimeMs = time.milliseconds();

            if (logEndOffset(tp) == -1L) {
                future.completeExceptionally(new NotLeaderOrFollowerException(
                    String.format("Stopped loading records from %s because the partition is not online or is no longer the leader.", tp)));
            } else if (isRunning.get()) {
                future.complete(new LoadSummary(startTimeMs, endTimeMs, schedulerQueueTimeMs, stats.numRecords, stats.numBytes));
            } else {
                future.completeExceptionally(new RuntimeException("Coordinator loader is closed."));
            }
        } catch (Throwable ex) {
            future.completeExceptionally(ex);
        }
    }

    private long logEndOffset(TopicPartition tp) {
        return partitionLogEndOffsetSupplier.apply(tp).orElse(-1L);
    }

    /**
     * Returns true if it's still valid to fetch the next batch of records.
     * <p>
     * This method ensures fetching continues only under safe and meaningful conditions:
     * <ul>
     * <li>The current offset is less than the log end offset.</li>
     * <li>At least one record was read in the previous fetch. This ensures that fetching stops even if the
     * current offset remains smaller than the log end offset but the log is empty. This could happen with compacted topics.</li>
     * <li>The log end offset is not -1L, which ensures the partition is online and is still the leader.</li>
     * <li>The loader is still running.</li>
     * </ul>
     */
    private boolean shouldFetchNextBatch(long currentOffset, long logEndOffset, boolean readAtLeastOneRecord) {
        return currentOffset < logEndOffset && readAtLeastOneRecord && isRunning.get();
    }

    private MemoryRecords toReadableMemoryRecords(TopicPartition tp, Records records, ByteBuffer buffer) throws IOException {
        if (records instanceof MemoryRecords memoryRecords) {
            return memoryRecords;
        } else if (records instanceof FileRecords fileRecords) {
            int sizeInBytes = fileRecords.sizeInBytes();
            int bytesNeeded = Math.max(loadBufferSize, sizeInBytes);

            // "minOneMessage = true in the above log.read() means that the buffer may need to
            // be grown to ensure progress can be made.
            if (buffer.capacity() < bytesNeeded) {
                if (loadBufferSize < bytesNeeded) {
                    LOG.warn("Loaded metadata from {} with buffer larger ({} bytes) than" +
                        " configured buffer size ({} bytes).", tp, bytesNeeded, loadBufferSize);
                }

                buffer = ByteBuffer.allocate(bytesNeeded);
            } else {
                buffer.clear();
            }

            fileRecords.readInto(buffer, 0);
            return MemoryRecords.readableRecords(buffer);
        } else {
            throw new IllegalArgumentException("Unsupported record type: " + records.getClass());
        }
    }

    private ReplayResult processMemoryRecords(
        TopicPartition tp,
        UnifiedLog log,
        MemoryRecords memoryRecords,
        CoordinatorPlayback<T> coordinator,
        LoadStats loadStats,
        long currentOffset,
        long lastCommittedOffset
    ) {
        for (MutableRecordBatch batch : memoryRecords.batches()) {
            if (batch.isControlBatch()) {
                for (Record record : batch) {
                    loadStats.numRecords++;

                    ControlRecordType controlRecord = ControlRecordType.parse(record.key());
                    if (controlRecord == ControlRecordType.COMMIT) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Replaying end transaction marker from {} at offset {} to commit" +
                                " transaction with producer id {} and producer epoch {}.",
                                tp, record.offset(), batch.producerId(), batch.producerEpoch());
                        }
                        coordinator.replayEndTransactionMarker(
                                batch.producerId(),
                                batch.producerEpoch(),
                                TransactionResult.COMMIT
                        );
                    } else if (controlRecord == ControlRecordType.ABORT) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Replaying end transaction marker from {} at offset {} to abort" +
                                " transaction with producer id {} and producer epoch {}.",
                                tp, record.offset(), batch.producerId(), batch.producerEpoch());
                        }
                        coordinator.replayEndTransactionMarker(
                                batch.producerId(),
                                batch.producerEpoch(),
                                TransactionResult.ABORT
                        );
                    }
                }
            } else {
                for (Record record : batch) {
                    loadStats.numRecords++;

                    Optional<T> coordinatorRecordOpt = Optional.empty();
                    try {
                        coordinatorRecordOpt = Optional.ofNullable(deserializer.deserialize(record.key(), record.value()));
                    } catch (Deserializer.UnknownRecordTypeException ex) {
                        LOG.warn("Unknown record type {} while loading offsets and group metadata from {}." +
                            " Ignoring it. It could be a left over from an aborted upgrade.", ex.unknownType(), tp);
                    } catch (RuntimeException ex) {
                        String msg = String.format("Deserializing record %s from %s failed.", record, tp);
                        LOG.error(msg, ex);
                        throw new RuntimeException(msg, ex);
                    }

                    coordinatorRecordOpt.ifPresent(coordinatorRecord -> {
                        try {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Replaying record {} from {} at offset {} with producer id {}" +
                                    " and producer epoch {}.", coordinatorRecord, tp, record.offset(), batch.producerId(), batch.producerEpoch());
                            }
                            coordinator.replay(
                                record.offset(),
                                batch.producerId(),
                                batch.producerEpoch(),
                                coordinatorRecord
                            );
                        } catch (RuntimeException ex) {
                            String msg = String.format("Replaying record %s from %s at offset %d with producer id %d and" +
                                " producer epoch %d failed.", coordinatorRecord, tp, record.offset(),
                                batch.producerId(), batch.producerEpoch());
                            LOG.error(msg, ex);
                            throw new RuntimeException(msg, ex);
                        }
                    });
                }
            }

            // Note that the high watermark can be greater than the current offset but as we load more records
            // the current offset will eventually surpass the high watermark. Also note that the high watermark
            // will continue to advance while loading.
            currentOffset = batch.nextOffset();
            long currentHighWatermark = log.highWatermark();
            if (currentOffset >= currentHighWatermark) {
                coordinator.updateLastWrittenOffset(currentOffset);

                if (currentHighWatermark > lastCommittedOffset) {
                    coordinator.updateLastCommittedOffset(currentHighWatermark);
                    lastCommittedOffset = currentHighWatermark;
                }
            } else if (currentOffset - lastCommittedOffset >= commitIntervalOffsets) {
                coordinator.updateLastWrittenOffset(currentOffset);
                coordinator.updateLastCommittedOffset(currentOffset);
                lastCommittedOffset = currentOffset;
            }
        }
        loadStats.numBytes += memoryRecords.sizeInBytes();
        return new ReplayResult(currentOffset, lastCommittedOffset);
    }

    /**
     * Closes the loader.
     */
    @Override
    public void close() throws Exception {
        if (!isRunning.compareAndSet(true, false)) {
            LOG.warn("Coordinator loader is already shutting down.");
            return;
        }
        scheduler.shutdown();
    }

    /**
     * A helper class to track key metrics during the data loading operation.
     */
    private static class LoadStats {
        private long numRecords = 0L;
        private long numBytes = 0L;
        private boolean readAtLeastOneRecord = true;

        @Override
        public String toString() {
            return "LoadStats(" +
                "numRecords=" + numRecords +
                ", numBytes=" + numBytes +
                ", readAtLeastOneRecord=" + readAtLeastOneRecord +
                ')';
        }
    }

    private record ReplayResult(long nextOffset, long lastCommittedOffset) { }
}
