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

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.AsyncTransactionIndex;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LocalLog;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.OffsetAndTimestampIndex;
import org.apache.kafka.storage.internals.log.TxnIndexSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class BookkeeperLocalLog extends LocalLog implements AsyncCallbacks.AddEntryCallback, AsyncCallbacks.OpenLedgerCallback {
    private static final Logger log = LoggerFactory.getLogger(BookkeeperLocalLog.class);

    private volatile ManagedLedgerImpl managedLedger;
    private volatile Field currentLedgerTimeoutTriggered;
    private volatile MethodHandle internalAsyncAddEntry;
    private volatile boolean isFenced = false;
    // The Transaction max position is the last position that the read-committed read can read.
    private volatile Position txnMaxPosition = PositionFactory.EARLIEST;
    protected volatile Executor mlExecutor;
    protected volatile OffsetAndTimestampIndex index;

    private final ManagedLedgerConfig managedLedgerConfig;
    private final Time time = Time.SYSTEM;
    private final AtomicInteger pendingAddEntries = new AtomicInteger();
    protected final AsyncTransactionIndex txnIndex;

    private final CompletableFuture<Void> initializeFuture = new CompletableFuture<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * @param dir                  The directory in which log segments are created.
     * @param config               The log configuration settings
     * @param segments             The non-empty log segments recovered from disk
     * @param recoveryPoint        The offset at which to begin the next recovery i.e. the first offset which has not been flushed to disk
     * @param nextOffsetMetadata   The offset where the next message could be appended
     * @param scheduler            The thread pool scheduler used for background actions
     * @param time                 The time instance used for checking the clock
     * @param topicPartition       The topic partition associated with this log
     * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle Log dir failure
     */
    public BookkeeperLocalLog(File dir, LogConfig config, LogSegments segments, long recoveryPoint,
                              LogOffsetMetadata nextOffsetMetadata, Scheduler scheduler, Time time,
                              TopicPartition topicPartition, LogDirFailureChannel logDirFailureChannel,
                              AsyncTransactionIndex txnIndex) {
        super(dir, config, segments, recoveryPoint, nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel);
        this.managedLedgerConfig = buildManagedLedgerConfig(config);
        this.txnIndex = txnIndex;
    }

    @Override
    public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
        this.managedLedger = (ManagedLedgerImpl) ledger;
        try {
            this.internalAsyncAddEntry = MethodHandles.lookup().bind(managedLedger, "internalAsyncAddEntry", MethodType.methodType(Void.class, OpAddEntry.class));
            this.currentLedgerTimeoutTriggered = ManagedLedgerImpl.class.getDeclaredField("currentLedgerTimeoutTriggered");
            this.currentLedgerTimeoutTriggered.setAccessible(true);
            this.index = new OffsetAndTimestampIndex(managedLedger, topicPartition);
            this.mlExecutor = managedLedger.getExecutor();
            initializeFuture.complete(null);
        } catch (Throwable t) {
            log.error("Failed to initialize BookkeeperLocalLog", t);
            initializeFuture.completeExceptionally(t);
        }
    }

    @Override
    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
        log.error("Failed to initialize BookkeeperLocalLog", exception);
        initializeFuture.completeExceptionally(exception);
    }

    public CompletableFuture<Void> initializeAsync(BookkeeperStorageSingleton bookkeeperStorageSingleton) {
        if (!initialized.compareAndSet(false, true)) {
            return initializeFuture;
        }

        String ledgerName = topicPartition.toString();
        ManagedLedgerFactory factory = bookkeeperStorageSingleton.getManagedLedgerFactory();
        factory.asyncOpen(ledgerName, managedLedgerConfig, this, () -> CompletableFuture.completedFuture(true), null);
        return this.initializeFuture;
    }

    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        this.managedLedger.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                closeFuture.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Failed to close BookkeeperLocalLog", exception);
                closeFuture.completeExceptionally(exception);
            }
        }, null);
        return closeFuture;
    }


    public static long getCurrentOffset(ManagedLedger managedLedger) {
        return ((ManagedLedgerInterceptorImpl) managedLedger.getManagedLedgerInterceptor()).getIndex();
    }

    public static long getLogEndOffset(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static ManagedLedgerConfig buildManagedLedgerConfig(LogConfig config) {
        ManagedLedgerConfig ledgerConfig = new ManagedLedgerConfig();
        ledgerConfig.setEnsembleSize(config.ensembleSize);
        ledgerConfig.setWriteQuorumSize(config.writeQuorumSize);
        ledgerConfig.setAckQuorumSize(config.ackQuorumSize);
        ledgerConfig.setDigestType(DigestType.valueOf(config.digestTypeName));
        ledgerConfig.setPassword(config.password);
        ledgerConfig.setMaxEntriesPerLedger(config.maxEntriesPerLedger);
        ledgerConfig.setMaxSizePerLedgerMb(config.maxBytesPerLedgerMB);
        ledgerConfig.setMinimumRolloverTime(config.minRolloverTimeMinutes, TimeUnit.MINUTES);
        ledgerConfig.setMaximumRolloverTime(config.maxRolloverTimeMinutes, TimeUnit.MINUTES);
        ledgerConfig.setMetadataOperationsTimeoutSeconds(config.metadataOperationTimeoutSeconds);
        ledgerConfig.setReadEntryTimeoutSeconds(config.readEntryTimeoutSeconds);
        ledgerConfig.setAddEntryTimeoutSeconds(config.addEntryTimeoutSeconds);
        ledgerConfig.setMetadataEnsembleSize(config.defaultEnsembleSize);
        ledgerConfig.setMetadataWriteQuorumSize(config.defaultWriteQuorumSize);
        ledgerConfig.setMetadataAckQuorumSize(config.defaultAckQuorumSize);
        ledgerConfig.setMetadataMaxEntriesPerLedger(config.defaultMaxEntriesPerLedger);
        ledgerConfig.setLedgerRolloverTimeout(config.ledgerRolloverTimeoutSeconds);
        ledgerConfig.setRetentionTime(config.retentionTimeSeconds, TimeUnit.SECONDS);
        ledgerConfig.setRetentionSizeInMB(config.retentionSizeMb);
        ledgerConfig.setAutoSkipNonRecoverableData(config.autoSkipNonRecoverableData);
        ledgerConfig.setLedgerForceRecovery(config.ledgerForceRecovery);
        ledgerConfig.setInactiveLedgerRollOverTime(config.inactiveLedgerRolloverTimeSeconds, TimeUnit.SECONDS);
        ledgerConfig.setLazyCursorRecovery(true);
        return ledgerConfig;
    }


    @Override
    public LogOffsetMetadata logEndOffsetMetadata() {
        return new LogOffsetMetadata(getLogEndOffset(managedLedger));
    }

    @Override
    public long logEndOffset() {
        return getLogEndOffset(managedLedger);
    }

    @Override
    public void close() {
        closeHandlers();
    }

    @Override
    public List<LogSegment> deleteAllSegments() {
        try {
            managedLedger.delete();
        } catch (Throwable t) {
            //  ignore
        }
        return Collections.emptyList();
    }

    @Override
    public void checkIfMemoryMappedBufferClosed() {
        // no-op
    }

    /**
     * Recover from startOffset.
     *
     * @param startOffset
     * @param consumer
     * @return
     */
    public CompletableFuture<Void> recoverFrom(long startOffset, Consumer<MemoryRecords> consumer) {
        Position lac = managedLedger.getLastConfirmedEntry();
        if (lac == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        String cursorName = "kafka-replay-" + HexFormat.of().formatHex(RandomUtils.insecure().randomBytes(8));
        index.findOffsetPositionAsync(startOffset, false)
                .thenApply(position -> {
                    try {
                        return managedLedger.newNonDurableCursor(position, cursorName);
                    } catch (ManagedLedgerException e) {
                        throw Errors.KAFKA_STORAGE_ERROR.exception(e.getMessage());
                    }
                })
                .thenAccept(cursor -> {
                    recoverEntries(cursor, consumer, future);
                })
                .exceptionally(t -> {
                    log.error("Unable to find the Entry Position of {}", startOffset, t);
                    future.completeExceptionally(t);
                    return null;
                });
        future.thenAccept(v -> {
            asyncDeleteCursor(cursorName);
            this.txnMaxPosition = managedLedger.getLastConfirmedEntry();
        });
        return future;
    }

    /**
     * Delete cursor.
     *
     * @param cursorName
     */
    private void asyncDeleteCursor(String cursorName) {
        managedLedger.asyncDeleteCursor(cursorName, new AsyncCallbacks.DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                // no-op
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("Unable to delete cursor {}", cursorName, exception);
            }
        }, null);
    }

    private void recoverEntries(ManagedCursor cursor, Consumer<MemoryRecords> consumer, CompletableFuture<Void> future) {
        cursor.asyncReadEntries(5, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                if (entries.isEmpty()) {
                    future.complete(null);
                    return;
                }
                try {
                    consumer.accept(KafkaEntryFormatter.decode(entries));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    return;
                }
                Position lastPosition = entries.get(entries.size() - 1).getPosition();
                if (lastPosition.compareTo(managedLedger.getLastConfirmedEntry()) >= 0) {
                    future.complete(null);
                } else {
                    recoverEntries(cursor, consumer, future);
                }
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, managedLedger.getLastConfirmedEntry());
    }

    public void updateTxnMaxPosition(Position txnMaxPosition) {
        if (txnMaxPosition.compareTo(this.txnMaxPosition) > 0) {
            this.txnMaxPosition = txnMaxPosition;
        }
    }

    @Override
    public CompletableFuture<FetchDataInfo> readAsync(long startOffset, int maxLength, boolean minOneMessage,
                                                      LogOffsetMetadata maxOffsetMetadata, boolean includeAbortedTxns) {
        CompletableFuture<FetchDataInfo> future = new CompletableFuture<>();
        Position lac = managedLedger.getLastConfirmedEntry();
        if (lac == null) {
            return CompletableFuture.completedFuture(
                    new FetchDataInfo(new LogOffsetMetadata(startOffset), MemoryRecords.EMPTY));
        }

        MutableObject<Position> mutablePosition = new MutableObject<>();
        index.findOffsetPositionAsync(startOffset, false)
                .thenCompose(position -> {
                    if (position.compareTo(lac) > 0 || maxLength <= 0) {
                        return CompletableFuture.completedFuture(Collections.emptyList());
                    }
                    mutablePosition.setValue(position);
                    int maxEntries = (int) (maxLength / managedLedger.getStats().getEntrySizeAverage());
                    if (maxEntries <= 0 && minOneMessage) {
                        maxEntries = 1;
                    }
                    return maxEntries > 0 ? readEntriesAsync(mutablePosition.get(), maxEntries)
                            : CompletableFuture.completedFuture(Collections.emptyList());
                })
                .thenApply(entries -> {
                    FetchDataInfo fetchDataInfo = entries.isEmpty() ? FetchDataInfo.empty(startOffset)
                            : new FetchDataInfo(new LogOffsetMetadata(startOffset), KafkaEntryFormatter.decode(entries));

                    // Resolve aborted transactions
                    Optional<RecordBatch> lastBatch = fetchDataInfo.records.lastBatch();
                    if (lastBatch.isEmpty() || !includeAbortedTxns) {
                        return fetchDataInfo;
                    }
                    TxnIndexSearchResult txnIndexSearchResult = txnIndex.collectAbortedTxns(startOffset, lastBatch.get().lastOffset());
                    List<AbortedTxn> abortedTxns = txnIndexSearchResult.abortedTransactions();
                    if (!abortedTxns.isEmpty()) {
                        List<FetchResponseData.AbortedTransaction> abortedTransactions = abortedTxns.stream().map(AbortedTxn::asAbortedTransaction).toList();
                        return new FetchDataInfo(fetchDataInfo.fetchOffsetMetadata, fetchDataInfo.records,
                                false, Optional.of(abortedTransactions));
                    }
                    return fetchDataInfo;
                })
                .thenAccept(future::complete)
                .exceptionally(t -> {
                    log.error("Unable to find the Entry Position of {}", startOffset, t);
                    Throwable root = Throwables.getRootCause(t);
                    future.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception(root.getMessage()));
                    return null;
                });
        return future;
    }

    private CompletableFuture<List<Entry>> readEntriesAsync(Position position, int maxEntries) {
        CompletableFuture<List<Entry>> future = new CompletableFuture<>();
        Position maxPosition = this.txnMaxPosition;
        List<Position> positions = new ArrayList<>(maxEntries);
        for (int i = 0; i < maxEntries; i++) {
            if (position.compareTo(maxPosition) > 0) {
                break;
            }
            positions.add(position);
            position = managedLedger.getNextValidPosition(position);
        }

        ReadEntriesCallBack callback = new ReadEntriesCallBack(positions, future);
        for (Position p : positions) {
            managedLedger.asyncReadEntry(p, callback, null);
        }
        return future;
    }


    private static final class ReadEntriesCallBack implements AsyncCallbacks.ReadEntryCallback, Comparator<Entry> {
        private final List<Entry> entries;
        private final List<Position> positions;
        private final CompletableFuture<List<Entry>> future;

        public ReadEntriesCallBack(List<Position> positions, CompletableFuture<List<Entry>> future) {
            this.entries = new ArrayList<>(positions.size());
            this.positions = positions;
            this.future = future;
        }

        @Override
        public void readEntryComplete(Entry entry, Object ctx) {
            entries.add(entry);
            if (entries.size() == positions.size()) {
                entries.sort(this);
                future.complete(entries);
            }
        }

        @Override
        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
            log.error("Failed to read entry", exception);
            for (Entry entry : entries) {
                entry.release();
            }
            future.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception());
        }

        @Override
        public int compare(Entry o1, Entry o2) {
            return o1.getPosition().compareTo(o2.getPosition());
        }
    }

    public CompletableFuture<MemoryRecords> readLatestRecordsAsync() {
        Position lac = managedLedger.getLastConfirmedEntry();
        if (lac == null) {
            return CompletableFuture.completedFuture(MemoryRecords.EMPTY);
        }
        CompletableFuture<MemoryRecords> future = new CompletableFuture<>();
        managedLedger.asyncReadEntry(lac, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    future.complete(KafkaEntryFormatter.decode(List.of(entry)));
                } catch (Throwable t) {
                    log.error("Failed to decode entry", t);
                    future.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception(t.getMessage()));
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Failed to read entry", exception);
                future.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception(exception.getMessage()));
            }
        }, null);
        return future;
    }

    @Override
    public CompletableFuture<Long> appendAsync(LogAppendInfo appendInfo, MemoryRecords records) {
        pendingAddEntries.incrementAndGet();
        if (isFenced) {
            return CompletableFuture.failedFuture(Errors.KAFKA_STORAGE_ERROR.exception());
        }
        CompletableFuture<Long> future = new CompletableFuture<>();
        ByteBuf buf = null;
        try {
            buf = KafkaEntryFormatter.encode(appendInfo, records);
            AtomicBoolean currentLedgerTimeoutTriggered = getCurrentLedgerTimeoutTriggered();
            MessagePublishContext ctx = new MessagePublishContext(future, (int) appendInfo.numMessages(), this);
            OpAddEntry op = OpAddEntry.createNoRetainBuffer(managedLedger, buf, this, ctx, currentLedgerTimeoutTriggered);
            internalAsyncAddEntry.invoke(op);
            return future;
        } catch (Throwable e) {
            log.error("Failed to invoke internalAsyncAddEntry", e);
            decrementPendingWriteOpsAndCheck();
            return CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception());
        } finally {
            if (buf != null) {
                buf.release();
            }
        }
    }


    public CompletableFuture<Long> asyncGetLogStartOffset() {
        return index.asyncGetLogStartOffset();
    }

    public CompletableFuture<Long> asyncFindTimestampOffset(long timestamp) {
        return index.asyncFindTimestampOffset(timestamp);
    }

    @Override
    public void addComplete(Position position, ByteBuf entryData, Object ctx) {
        MessagePublishContext context = (MessagePublishContext) ctx;
        context.setMetadata(entryData);
        decrementPendingWriteOpsAndCheck();
        context.complete(null, position);
    }

    @Override
    public void addFailed(ManagedLedgerException exception, Object ctx) {
        decrementPendingWriteOpsAndCheck();
        ((MessagePublishContext) ctx).complete(exception, PositionFactory.EARLIEST);
    }

    private AtomicBoolean getCurrentLedgerTimeoutTriggered() {
        try {
            return (AtomicBoolean) currentLedgerTimeoutTriggered.get(managedLedger);
        } catch (IllegalAccessException e) {
            // Should never happen
            log.error("Failed to get currentLedgerTimeoutTriggered field", e);
            throw new RuntimeException(e);
        }
    }

    private void decrementPendingWriteOpsAndCheck() {
        long pending = pendingAddEntries.decrementAndGet();
        if (pending == 0 && isFenced) {
            synchronized (this) {
                if (isFenced) {
                    isFenced = false;
                    this.managedLedger.readyToCreateNewLedger();
                }
            }
        }
    }
}
