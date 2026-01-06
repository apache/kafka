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

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BookkeeperLocalLog extends LocalLog implements AsyncCallbacks.AddEntryCallback {
    private static final Logger log = LoggerFactory.getLogger(BookkeeperLocalLog.class);

    private final ManagedLedgerImpl managedLedger;
    private final ManagedLedgerConfig managedLedgerConfig;
    private final Time time = Time.SYSTEM;
    private final Field currentLedgerTimeoutTriggered;
    private final MethodHandle internalAsyncAddEntry;
    private final AtomicInteger pendingAddEntries = new AtomicInteger();
    private volatile boolean isFenced = false;
    protected final OffsetAndTimestampIndex index;
    private final AsyncTransactionIndex txnIndex;
    protected final Executor mlExecutor;

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
                              AsyncTransactionIndex txnIndex) throws Exception {
        super(dir, config, segments, recoveryPoint, nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel);
        this.managedLedgerConfig = buildManagedLedgerConfig(config);
        this.managedLedger = null;
        this.internalAsyncAddEntry = MethodHandles.lookup().bind(managedLedger, "internalAsyncAddEntry", MethodType.methodType(Void.class, OpAddEntry.class));
        this.currentLedgerTimeoutTriggered = ManagedLedgerImpl.class.getDeclaredField("currentLedgerTimeoutTriggered");
        this.currentLedgerTimeoutTriggered.setAccessible(true);
        this.index = new OffsetAndTimestampIndex(managedLedger, topicPartition);
        this.mlExecutor = managedLedger.getExecutor();
        this.txnIndex = txnIndex;
    }


    public static long getCurrentOffset(ManagedLedger managedLedger) {
        return ((ManagedLedgerInterceptorImpl) managedLedger.getManagedLedgerInterceptor()).getIndex();
    }

    public static long getLogEndOffset(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static ManagedLedgerConfig buildManagedLedgerConfig(LogConfig config) {
        // TODO
        return new ManagedLedgerConfig();
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
    public CompletableFuture<FetchDataInfo> readAsync(long startOffset, int maxLength, boolean minOneMessage,
                                                      LogOffsetMetadata maxOffsetMetadata, boolean includeAbortedTxns) {
        // TODO
        // CompletableFuture<Position> ignore = index.findOffsetPositionAsync(startOffset, false);
        return null;
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

    @Override
    public void addComplete(Position position, ByteBuf entryData, Object ctx) {
        MessagePublishContext context = (MessagePublishContext) ctx;
        context.setMetadata(entryData);
        decrementPendingWriteOpsAndCheck();
        context.complete(null, position.getLedgerId(), position.getEntryId());
    }

    @Override
    public void addFailed(ManagedLedgerException exception, Object ctx) {
        decrementPendingWriteOpsAndCheck();
        ((MessagePublishContext) ctx).complete(exception, -1, -1);
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
