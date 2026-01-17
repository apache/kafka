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
package org.apache.kafka.storage.internals.log.bookkeeper;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.UnexpectedAppendOffsetException;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.AsyncOffsetReader;
import org.apache.kafka.storage.internals.log.AsyncProducerStateManager;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.CompletedTxn;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.LogValidator;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class BookkeeperUnifiedLog extends UnifiedLog {
    private static final Logger log = LoggerFactory.getLogger(BookkeeperUnifiedLog.class);
    private final BookkeeperLocalLog bookkeeperLocalLog;
    private final AsyncProducerStateManager producerStateManager;
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final CompletableFuture<Void> initializeFuture = new CompletableFuture<>();


    public BookkeeperUnifiedLog(long logStartOffset, BookkeeperLocalLog localLog, BrokerTopicStats brokerTopicStats,
                                int producerIdExpirationCheckIntervalMs, LeaderEpochFileCache leaderEpochCache,
                                AsyncProducerStateManager producerStateManager, Optional<Uuid> topicId,
                                boolean remoteStorageSystemEnable, LogOffsetsListener logOffsetsListener) throws IOException {
        super(logStartOffset, localLog, brokerTopicStats, producerIdExpirationCheckIntervalMs, leaderEpochCache,
                producerStateManager, topicId, remoteStorageSystemEnable, logOffsetsListener);
        this.bookkeeperLocalLog = localLog;
        this.producerStateManager = producerStateManager;
    }

    @Override
    protected void initializePartitionMetadata() {
        // noop
    }

    @Override
    protected void updateLogStartOffset(long offset) throws IOException {
        // noop
    }

    @Override
    public void updateLocalLogStartOffset(long offset) throws IOException {
        // noop
    }

    @Override
    protected void initializeTopicId() {
        // TODO
    }

    @Override
    public int deleteOldSegments() throws IOException {
        return 0;
    }

    @Override
    public long highWatermark() {
        return bookkeeperLocalLog.logEndOffset();
    }

    public CompletableFuture<Void> initialize() {
        if (!recovering.compareAndSet(false, true)) {
            return initializeFuture;
        }

        CompletableFuture<Void> producerRecoverFuture = producerStateManager.recoverSnapshotAsync();
        CompletableFuture<Void> transactionRecoverFuture = bookkeeperLocalLog.txnIndex.recoverSnapshot();
        CompletableFuture.allOf(producerRecoverFuture, transactionRecoverFuture)
                .thenCompose(ignore -> {
                    long producerMapEndOffset = producerStateManager.mapEndOffset();
                    long transactionMapEndOffset = bookkeeperLocalLog.txnIndex.mapEndOffset();
                    if (transactionMapEndOffset < producerMapEndOffset) {
                        // TODO
                        log.warn("Transaction map end offset {} is less than producer map end offset {}, " +
                                "which means that some transactions may be missing. " +
                                "This can happen when the transaction log is truncated before the producer state log.",
                                transactionMapEndOffset, producerMapEndOffset);
                    }
                    long currentOffset = bookkeeperLocalLog.logEndOffset();
                    if (producerMapEndOffset >= currentOffset && transactionMapEndOffset >= currentOffset) {
                        return CompletableFuture.completedFuture(null);
                    }
                    long startRecoverOffset = Math.min(producerMapEndOffset, transactionMapEndOffset);
                    return bookkeeperLocalLog.recoverFrom(startRecoverOffset, new RecoveryRecordsConsumer());
                })
                .thenAccept(ignore -> initializeFuture.complete(null))
                .exceptionally(t -> {
                    log.error("Failed to recover log", t);
                    initializeFuture.completeExceptionally(t);
                    return null;
                });
        return initializeFuture;
    }

    // TRANSLATE FROM: LogSegment.updateProducerState(...)
    private class RecoveryRecordsConsumer implements Consumer<MemoryRecords> {
        @Override
        public void accept(MemoryRecords records) {
            for (RecordBatch batch : records.batches()) {
                long mapEndOffset = batch.lastOffset() + 1;
                long txnMapEndOffset = bookkeeperLocalLog.txnIndex.mapEndOffset();
                if (batch.hasProducerId()) {
                    long producerId = batch.producerId();
                    ProducerAppendInfo appendInfo = producerStateManager.prepareUpdate(producerId, AppendOrigin.REPLICATION);
                    Optional<CompletedTxn> maybeCompletedTxn = appendInfo.append(batch, Optional.empty());
                    producerStateManager.update(appendInfo);
                    if (maybeCompletedTxn.isPresent()) {
                        CompletedTxn completedTxn = maybeCompletedTxn.get();
                        long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                        if (completedTxn.isAborted() && txnMapEndOffset < mapEndOffset) {
                            bookkeeperLocalLog.txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset));
                        }
                        producerStateManager.completeTxn(completedTxn);
                    }
                }
                if (txnMapEndOffset < mapEndOffset) {
                    bookkeeperLocalLog.txnIndex.updateMapEndOffset(mapEndOffset);
                }
                producerStateManager.updateMapEndOffset(mapEndOffset);
            }
        }
    }

    @Override
    public CompletableFuture<LogAppendInfo> appendAsFollowerAsync(MemoryRecords records, int leaderEpoch) {
        return CompletableFuture.failedFuture(Errors.UNSUPPORTED_VERSION.exception());
    }

    @Override
    public CompletableFuture<LogAppendInfo> appendAsLeaderAsync(MemoryRecords records, int leaderEpoch) {
        return appendAsLeaderAsync(records, leaderEpoch, AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN);
    }

    @Override
    public CompletableFuture<LogAppendInfo> appendAsLeaderAsync(MemoryRecords records, int leaderEpoch, AppendOrigin origin) {
        return appendAsLeaderAsync(records, leaderEpoch, origin, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN);
    }

    @Override
    public CompletableFuture<LogAppendInfo> appendAsLeaderAsync(MemoryRecords records, int leaderEpoch, AppendOrigin origin, RequestLocal requestLocal, VerificationGuard verificationGuard, short transactionVersion) {
        boolean validateAndAssignOffsets = origin != AppendOrigin.RAFT_LEADER;
        return appendAsync(records, origin, validateAndAssignOffsets, leaderEpoch, Optional.of(requestLocal),
                verificationGuard, false, RecordBatch.CURRENT_MAGIC_VALUE, transactionVersion);
    }

    @Override
    public CompletableFuture<LogAppendInfo> appendAsLeaderWithRecordVersionAsync(MemoryRecords records, int leaderEpoch, RecordVersion recordVersion) {
        return appendAsync(records, AppendOrigin.CLIENT, true, leaderEpoch, Optional.of(RequestLocal.noCaching()),
                VerificationGuard.SENTINEL, false, recordVersion.value, TransactionVersion.TV_UNKNOWN);
    }

    @Override
    public CompletableFuture<LogAppendInfo> appendAsync(MemoryRecords records, AppendOrigin origin, boolean validateAndAssignOffsets,
                                                        int leaderEpoch, Optional<RequestLocal> requestLocal, VerificationGuard verificationGuard,
                                                        boolean ignoreRecordSize, byte toMagic, short transactionVersion) {
        CompletableFuture<LogAppendInfo> future = new CompletableFuture<>();
        this.bookkeeperLocalLog.mlExecutor.execute(() -> {
            LogAppendInfo appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize, !validateAndAssignOffsets, leaderEpoch);
            if (appendInfo.validBytes() <= 0) {
                future.complete(appendInfo);
                return;
            }
            MemoryRecords trimmedRecords = trimInvalidBytes(records, appendInfo);
            if (validateAndAssignOffsets) {
                PrimitiveRef.LongRef offset = PrimitiveRef.ofLong(bookkeeperLocalLog.logEndOffset());
                appendInfo.setFirstOffset(offset.value);
                Compression targetCompression = BrokerCompressionType.targetCompression(config().compression, appendInfo.sourceCompression());
                LogValidator validator = new LogValidator(trimmedRecords,
                        topicPartition(),
                        time(),
                        appendInfo.sourceCompression(),
                        targetCompression,
                        config().compact,
                        toMagic,
                        config().messageTimestampType,
                        config().messageTimestampBeforeMaxMs,
                        config().messageTimestampAfterMaxMs,
                        leaderEpoch,
                        origin);
                if (requestLocal.isEmpty()) {
                    future.completeExceptionally(new IllegalArgumentException("requestLocal should be defined if assignOffsets is true"));
                    return;
                }
                LogValidator.ValidationResult validateAndOffsetAssignResult = validator.validateMessagesAndAssignOffsets(offset,
                        validatorMetricsRecorder, requestLocal.get().bufferSupplier());
                trimmedRecords = validateAndOffsetAssignResult.validatedRecords();
                appendInfo.setMaxTimestamp(validateAndOffsetAssignResult.maxTimestampMs());
                appendInfo.setLastOffset(offset.value - 1);
                appendInfo.setRecordValidationStats(validateAndOffsetAssignResult.recordValidationStats());
                if (config().messageTimestampType == TimestampType.LOG_APPEND_TIME) {
                    appendInfo.setLogAppendTime(validateAndOffsetAssignResult.logAppendTimeMs());
                }
                if (!ignoreRecordSize && validateAndOffsetAssignResult.messageSizeMaybeChanged()) {
                    for (RecordBatch batch : trimmedRecords.batches()) {
                        if (batch.sizeInBytes() > config().maxMessageSize()) {
                            future.completeExceptionally(new RecordTooLargeException(
                                    "Message batch exceeds the maximum configured message size."));
                            return;
                        }
                    }
                }
            } else {
                if (appendInfo.firstOrLastOffsetOfFirstBatch() < logEndOffset()) {
                    // we may still be able to recover if the log is empty
                    // one example: fetching from log start offset on the leader which is not batch aligned,
                    // which may happen as a result of AdminClient#deleteRecords()
                    boolean hasFirstOffset = appendInfo.firstOffset() != UnifiedLog.UNKNOWN_OFFSET;
                    long firstOffset = hasFirstOffset ? appendInfo.firstOffset() : records.batches().iterator().next().baseOffset();

                    String firstOrLast = hasFirstOffset ? "First offset" : "Last offset of the first batch";
                    List<String> offsets = new ArrayList<>();
                    for (Record record : records.records()) {
                        offsets.add(String.valueOf(record.offset()));
                        if (offsets.size() == 10) break;
                    }
                    // TODO
                    long logStartOffset = 0;
                    future.completeExceptionally(new UnexpectedAppendOffsetException(
                            "Unexpected offset in append to " + topicPartition() + ". " + firstOrLast + " " +
                                    appendInfo.firstOrLastOffsetOfFirstBatch() + " is less than the next offset " + logEndOffset() + ". " +
                                    "First 10 offsets in append: " + String.join(", ", offsets) + ", last offset in" +
                                    " append: " + appendInfo.lastOffset() + ". Log start offset = " + logStartOffset,
                            firstOffset, appendInfo.lastOffset()));
                    return;
                }
            }

            // update the epoch cache with the epoch stamped onto the message by the leader
            trimmedRecords.batches().forEach(batch -> {
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    assignEpochStartOffset(batch.partitionLeaderEpoch(), batch.baseOffset());
                } else {
                    // In partial upgrade scenarios, we may get a temporary regression to the message format. In
                    // order to ensure the safety of leader election, we clear the epoch cache so that we revert
                    // to truncation by high watermark after the next leader election.
                    // TODO
                    // if (leaderEpochCache.nonEmpty()) {
                    //    logger.warn("Clearing leader epoch cache after unexpected append with message format v{}", batch.magic());
                    //  leaderEpochCache.clearAndFlush();
                    // }
                }
            });

            // check messages size does not exceed config.segmentSize
            if (trimmedRecords.sizeInBytes() > config().segmentSize()) {
                future.completeExceptionally(Errors.RECORD_LIST_TOO_LARGE.exception("Message batch size is " + trimmedRecords.sizeInBytes() + " bytes in append " +
                        "to partition " + topicPartition() + ", which exceeds the maximum configured segment size of " + config().segmentSize() + "."));
                return;
            }

            LogOffsetMetadata logOffsetMetadata = new LogOffsetMetadata(appendInfo.firstOrLastOffsetOfFirstBatch(), 0, 1);
            // now that we have valid records, offsets assigned, and timestamps updated, we need to
            // validate the idempotent/transactional state of the producers and collect some metadata
            AnalyzeAndValidateProducerStateResult result = analyzeAndValidateProducerState(
                    logOffsetMetadata, trimmedRecords, origin, verificationGuard, transactionVersion
            );


            if (result.maybeDuplicate().isPresent()) {
                BatchMetadata duplicate = result.maybeDuplicate().get();
                appendInfo.setFirstOffset(duplicate.firstOffset());
                appendInfo.setLastOffset(duplicate.lastOffset());
                appendInfo.setLogAppendTime(duplicate.timestamp());
                // TODO
                // appendInfo.setLogStartOffset(logStartOffset);
                future.complete(appendInfo);
                return;
            }

            bookkeeperLocalLog.appendAsync(appendInfo, trimmedRecords)
                    .thenAccept(offset -> {
                        // Should run on ML executor
                        result.updatedProducers().values().forEach(producerStateManager::update);
                        for (CompletedTxn completedTxn : result.completedTxns()) {
                            long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                            if (completedTxn.isAborted()) {
                                bookkeeperLocalLog.txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset));
                            }
                            producerStateManager.completeTxn(completedTxn);
                        }
                        long mapEndOffset = appendInfo.lastOffset() + 1;
                        bookkeeperLocalLog.txnIndex.updateMapEndOffset(mapEndOffset);
                        producerStateManager.updateMapEndOffset(mapEndOffset);
                        maybeIncrementFirstUnstableOffset();
                        future.complete(appendInfo);
                    })
                    .exceptionally(t -> {
                        future.completeExceptionally(t);
                        return null;
                    });
        });

        return future;
    }

    @Override
    public long logStartOffset() {
        return localLogStartOffset();
    }

    @Override
    public long localLogStartOffset() {
        return bookkeeperLocalLog.asyncGetLogStartOffset().join();
    }

    @Override
    protected void maybeIncrementFirstUnstableOffset() {
        Optional<LogOffsetMetadata> updatedFirstUnstableOffset = producerStateManager.firstUnstableOffset();
        long logStartOffset = logStartOffset();
        if (updatedFirstUnstableOffset.isPresent() &&
                (updatedFirstUnstableOffset.get().messageOffsetOnly() || updatedFirstUnstableOffset.get().messageOffset < logStartOffset)) {
            long offset = Math.max(updatedFirstUnstableOffset.get().messageOffset, logStartOffset);
            updatedFirstUnstableOffset = Optional.of(maybeConvertToOffsetMetadata(offset));
        }

        if (updatedFirstUnstableOffset != this.firstUnstableOffsetMetadata) {
            log.debug("First unstable offset updated to {}", updatedFirstUnstableOffset);
            this.firstUnstableOffsetMetadata = updatedFirstUnstableOffset;
        }
    }

    @Override
    public boolean maybeIncrementLogStartOffset(long newLogStartOffset, LogStartOffsetIncrementReason reason) {
        return true;
    }

    @Override
    public CompletableFuture<FetchDataInfo> readAsync(long startOffset, int maxLength, FetchIsolation isolation, boolean minOneMessage) {
        LogOffsetMetadata maxOffsetMetadata = switch (isolation) {
            case LOG_END,HIGH_WATERMARK -> bookkeeperLocalLog.logEndOffsetMetadata();
            case TXN_COMMITTED -> firstUnstableOffsetMetadata.orElse(bookkeeperLocalLog.logEndOffsetMetadata());
        };
        return bookkeeperLocalLog.readAsync(startOffset, maxLength, minOneMessage, maxOffsetMetadata, isolation == FetchIsolation.TXN_COMMITTED);
    }

    @Override
    public CompletableFuture<OffsetResultHolder> fetchOffsetByTimestampAsync(long targetTimestamp,
                                                                             Optional<AsyncOffsetReader> remoteOffsetReader) {
        if (remoteOffsetReader.isPresent()) {
            return CompletableFuture.failedFuture(Errors.UNSUPPORTED_VERSION.exception());
        }
        if (targetTimestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP
                || targetTimestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP
                || targetTimestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
            return bookkeeperLocalLog.asyncGetLogStartOffset()
                    .thenApply(logStartOffset -> new OffsetResultHolder(
                            new FileRecords.TimestampAndOffset(targetTimestamp, logStartOffset, Optional.empty())));
        } else if (targetTimestamp == ListOffsetsRequest.LATEST_TIMESTAMP
                || targetTimestamp == ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP) {
            return CompletableFuture.completedFuture(new OffsetResultHolder(
                    new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, bookkeeperLocalLog.logEndOffset(), Optional.empty())));
        } else if (targetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
            if (bookkeeperLocalLog.logEndOffset() == 0) {
                return CompletableFuture.completedFuture(new OffsetResultHolder(
                        new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, bookkeeperLocalLog.logEndOffset(), Optional.empty())));
            }
            return bookkeeperLocalLog.readLatestRecordsAsync()
                    .thenApply(records -> {
                        Optional<RecordBatch> lastBatchOpt = records.lastBatch();
                        if (lastBatchOpt.isEmpty()) {
                            return new OffsetResultHolder(
                                    new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, -1L, Optional.empty()));
                        } else {
                            RecordBatch lastBatch = lastBatchOpt.get();
                            return new OffsetResultHolder(
                                    new FileRecords.TimestampAndOffset(lastBatch.maxTimestamp(), lastBatch.lastOffset(), Optional.empty()));
                        }
                    });
        } else {
            return bookkeeperLocalLog.asyncFindTimestampOffset(targetTimestamp)
                    .thenApply(offset -> new OffsetResultHolder(new FileRecords.TimestampAndOffset(targetTimestamp, offset, Optional.empty())));
        }
    }

    @Override
    public LogOffsetMetadata maybeConvertToOffsetMetadata(long offset) {
        return new LogOffsetMetadata(offset);
    }

    @Override
    public CompletableFuture<Void> flushProducerStateSnapshotAsync(Path snapshot) {
        return super.flushProducerStateSnapshotAsync(snapshot);
    }

    @Override
    public CompletableFuture<Void> truncateToAsync(long targetOffset) {
        return super.truncateToAsync(targetOffset);
    }

    @Override
    public CompletableFuture<Void> flushUptoOffsetExclusiveAsync(long offset) {
        return super.flushUptoOffsetExclusiveAsync(offset);
    }

    @Override
    public CompletableFuture<Void> takeProducerSnapshotAsync() {
        return super.takeProducerSnapshotAsync();
    }

    @Override
    public CompletableFuture<Void> truncateFullyAndStartAtAsync(long newOffset, Optional<Long> logStartOffsetOpt) {
        return super.truncateFullyAndStartAtAsync(newOffset, logStartOffsetOpt);
    }

    @Override
    public void close() {
    }

    @Override
    public void closeHandlers() {
    }

    @Override
    public CompletableFuture<Void> deleteAsync() {
        return super.deleteAsync();
    }
}
