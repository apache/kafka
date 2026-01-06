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

import com.google.common.annotations.VisibleForTesting;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;

public class MessageFinder implements AsyncCallbacks.FindEntryCallback {
    protected final ManagedCursor cursor;
    protected final String subName;
    protected final int ledgerCloseTimestampMaxClockSkewMillis;
    protected final String topicName;
    protected long timestamp = 0;

    protected static final int FALSE = 0;
    protected static final int TRUE = 1;
    @SuppressWarnings("unused")
    protected volatile int messageFindInProgress = FALSE;
    protected static final AtomicIntegerFieldUpdater<MessageFinder> MESSAGE_FIND_IN_PROGRESS =
            AtomicIntegerFieldUpdater
                    .newUpdater(MessageFinder.class, "messageFindInProgress");

    public MessageFinder(String topicName, ManagedCursor cursor, int ledgerCloseTimestampMaxClockSkewMillis) {
        this.topicName = topicName;
        this.cursor = cursor;
        this.subName = Codec.decode(cursor.getName());
        this.ledgerCloseTimestampMaxClockSkewMillis = ledgerCloseTimestampMaxClockSkewMillis;
    }

    public void findMessages(final long timestamp, AsyncCallbacks.FindEntryCallback callback) {
        if (!MESSAGE_FIND_IN_PROGRESS.compareAndSet(this, FALSE, TRUE)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore message position find scheduled task, last find is still running",
                        topicName, subName);
            }
            callback.findEntryFailed(
                    new ManagedLedgerException.ConcurrentFindCursorPositionException("last find is still running"),
                    Optional.empty(), null);
            return;
        }

        this.timestamp = timestamp;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Starting message position find at timestamp {}", subName, timestamp);
        }
        Pair<Position, Position> range =
                getFindPositionRange(cursor.getManagedLedger().getLedgersInfo().values(),
                        cursor.getManagedLedger().getLastConfirmedEntry(), timestamp,
                        ledgerCloseTimestampMaxClockSkewMillis);
        // Record the metrics
        Position left = range.getLeft();
        Position right = range.getRight();
        cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
            try {
                // Find the latest entry that is earlier than the target timestamp.
                long entryTimestamp = MessageMetadataUtils.getPublishTime(entry.getDataBuffer());
                return entryTimestamp <= timestamp;
            } catch (Exception e) {
                log.error("[{}][{}] Error deserializing message for message position find",
                        topicName, subName, e);
            } finally {
                entry.release();
            }
            return false;
        }, left, right, this, callback, true);
    }

    /**
     * The range may be across multi ledgers:
     *   - start: the latest ledger that closed before {@param targetTimestamp}.
     *     - only the latest entry is useful.
     *   - end: the earliest ledger that is larger than the target timestamp.
     */
    @VisibleForTesting
    static Pair<Position, Position> getFindPositionRange(Iterable<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfos,
                                                                Position lastConfirmedEntry, long targetTimestamp,
                                                                int ledgerCloseTimestampMaxClockSkewMillis) {
        if (ledgerCloseTimestampMaxClockSkewMillis < 0) {
            // this feature is disabled when the value is negative
            return Pair.of(null, null);
        }

        long targetTimestampMin = targetTimestamp - ledgerCloseTimestampMaxClockSkewMillis;
        long targetTimestampMax = targetTimestamp + ledgerCloseTimestampMaxClockSkewMillis;

        Position start = null;
        Position end = null;

        // We do not use binary search here:
        // Since "managedLedger.ledgers" os a map, we can hardly use a binary search except to copy items to an array,
        // which causes frequently young GC. And "collection.toArray()" also loops the collection once, which does not
        // benefit performance anymore.
        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo info : ledgerInfos) {
            if (!info.hasTimestamp()) {
                // unexpected case, don't set start and end
                return Pair.of(null, null);
            }
            long closeTimestamp = info.getTimestamp();
            // For an open ledger, closeTimestamp is 0
            if (closeTimestamp == 0) {
                end = null;
                break;
            }
            if (closeTimestamp <= targetTimestampMin) {
                // Since we have "broker.conf -> managedLedgerCursorResetLedgerCloseTimestampMaxClockSkewMillis", which
                // already expanded the scope for searching, the entries before the latest one is not useful.
                start = PositionFactory.create(info.getLedgerId(), info.getEntries() - 1);
            } else if (closeTimestamp > targetTimestampMax) {
                // If the close timestamp is greater than the timestamp
                end = PositionFactory.create(info.getLedgerId(), info.getEntries() - 1);
                break;
            }
        }
        return Pair.of(start, end);
    }

    private static final Logger log = LoggerFactory.getLogger(MessageFinder.class);

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (position != null) {
            log.info("[{}][{}] Found position {} closest to provided timestamp {}", topicName, subName, position,
                    timestamp);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] No position found closest to provided timestamp {}", topicName, subName, timestamp);
            }
        }
        messageFindInProgress = FALSE;
        callback.findEntryComplete(position, null);
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] message position find operation failed for provided timestamp {}", topicName, subName,
                    timestamp, exception);
        }
        messageFindInProgress = FALSE;
        callback.findEntryFailed(exception, failedReadPosition, null);
    }
}
