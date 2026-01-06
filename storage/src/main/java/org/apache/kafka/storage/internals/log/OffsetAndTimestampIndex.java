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
package org.apache.kafka.storage.internals.log;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.storage.internals.log.bookkeeper.MessageFinder;
import org.apache.kafka.storage.internals.log.bookkeeper.MessageMetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static org.apache.kafka.storage.internals.log.bookkeeper.MessageMetadataUtils.peekOffsetFromEntry;

public class OffsetAndTimestampIndex {
    private static final Logger log = LoggerFactory.getLogger(OffsetAndTimestampIndex.class);
    private static final String KAFKA_BASE_OFFSET = "kafka.base.offset";

    private final ManagedLedgerImpl managedLedger;
    private final TopicPartition topicPartition;
    private final ManagedCursor cursor;
    private final MessageFinder finder;

    public OffsetAndTimestampIndex(ManagedLedgerImpl managedLedger, TopicPartition tp) throws ManagedLedgerException {
        this.managedLedger = managedLedger;
        this.topicPartition = tp;
        this.cursor = managedLedger.newNonDurableCursor(PositionFactory.EARLIEST, "kafka-offset-index");
        this.finder = new MessageFinder(tp.toString(), cursor, 60_000);
    }

    public CompletableFuture<Long> asyncGetLogStartOffset() {
        MLDataFormats.ManagedLedgerInfo.LedgerInfo info = managedLedger.getLedgersInfo().firstEntry().getValue();
        Position lac = managedLedger.getLastConfirmedEntry();
        if (lac == null) {
            return CompletableFuture.completedFuture(0L);
        }

        long ledgerId = info.getLedgerId();
        Position firstEntryPosition = PositionFactory.create(ledgerId, 0);
        if (firstEntryPosition.compareTo(lac) > 0) {
            return asyncGetLogEndOffset(false);
        }
        CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetLedgerBaseOffset(ledgerId)
                .thenCompose(v -> {
                    if (v != null) {
                        return CompletableFuture.completedFuture(v);
                    }
                    return internalAsyncGetLedgerBaseOffset(ledgerId).thenApply(offset -> {
                        asyncAddLedgerBaseOffset(offset, ledgerId);
                        return offset;
                    });
                })
                .thenAccept(future::complete)
                .exceptionally(t -> {
                    log.error("Failed to get base offset for ledger {}, topic {}", ledgerId, topicPartition, t);
                    future.completeExceptionally(t);
                    return null;
                });
        return future;
    }

    private CompletableFuture<Long> asyncGetLedgerBaseOffset(long ledgerId) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        managedLedger.asyncGetLedgerProperty(ledgerId, KAFKA_BASE_OFFSET)
                .thenAccept(v -> {
                    if (v == null) {
                        future.complete(null);
                    } else if (NumberUtils.isCreatable(v)) {
                        future.complete(NumberUtils.createLong(v));
                    } else {
                        future.completeExceptionally(new IllegalArgumentException("Invalid BaseOffset: " + v));
                    }
                })
                .exceptionally(t -> {
                    log.error("Failed to get base offset for ledger {}, topic {}", ledgerId, topicPartition, t);
                    future.completeExceptionally(t);
                    return null;
                });
        return future;
    }

    /**
     * Get the log start offset.
     *
     * @return
     */
    private CompletableFuture<Long> internalAsyncGetLedgerBaseOffset(long ledgerId) {
        Position lac = managedLedger.getLastConfirmedEntry();
        if (lac == null) {
            return CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception());
        }

        // Should never happen
        if (ledgerId > lac.getLedgerId()) {
            log.warn("[{}] First ledger id {} is greater than lac {}", topicPartition, ledgerId, lac);
            return CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception());
        }

        Position firstEntryPosition = PositionFactory.create(ledgerId, 0);
        if (firstEntryPosition.compareTo(lac) > 0) {
            return asyncGetLogEndOffset(false);
        }

        CompletableFuture<Long> future = new CompletableFuture<>();
        managedLedger.asyncReadEntry(firstEntryPosition, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    future.complete(MessageMetadataUtils.peekBaseOffsetFromEntry(entry));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }

    public CompletableFuture<Long> asyncGetLogEndOffset(boolean readCommitted) {
        return CompletableFuture.completedFuture(MessageMetadataUtils.getLogEndOffset(managedLedger));
    }


    /**
     * Find the offset by timestamp.
     *
     * @param timestamp
     * @return
     */
    public CompletableFuture<Long> asyncFindTimestampOffset(final long timestamp) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        this.finder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position p, Object ctx) {
                try {
                    Position position = p == null ? getFirstValidPosition(managedLedger) : p;
                    if (position == null) {
                        log.warn("Unable find position for topic {} time {}. get NULL position", topicPartition, timestamp);
                        future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception());
                        return;
                    }
                    Position lac = managedLedger.getLastConfirmedEntry();
                    if (position.compareTo(lac) > 0 || MessageMetadataUtils.getCurrentOffset(managedLedger) < 0) {
                        long offset = Math.max(0, MessageMetadataUtils.getCurrentOffset(managedLedger));
                        future.complete(offset);
                        return;
                    }
                    getPositionOffset(position, timestamp)
                            .thenAccept(future::complete)
                            .exceptionally(t -> {
                                log.error("[{}] Failed to get offset for position {}", topicPartition, position, t);
                                future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception());
                                return null;
                            });
                } catch (Throwable t) {
                    log.error("Unable to find position for topic {} time {}. Exception:", topicPartition, timestamp, t);
                    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception());
                }
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception,
                                        Optional<Position> position, Object ctx) {
                if (exception instanceof ManagedLedgerException.NonRecoverableLedgerException) {
                    // The position doesn't exist, it usually happens when the rollover of managed ledger leads to
                    // the deletion of all expired ledgers. In this case, there's only one empty ledger in the managed
                    // ledger. So here we complete it with the latest offset.
                    future.complete(MessageMetadataUtils.getLogEndOffset(managedLedger));
                } else {
                    log.warn("Unable to find position for topic {} time {}. Exception:",
                            topicPartition, timestamp, exception);
                    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception());
                }
            }
        });

        return future;
    }

    /**
     * Get the offset of the position.
     *
     * @param position
     * @param timestamp
     * @return
     */
    private CompletableFuture<Long> getPositionOffset(Position position, long timestamp) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        managedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                if (exception instanceof ManagedLedgerException.NonRecoverableLedgerException) {
                    // The position doesn't exist, it usually happens when the rollover of managed ledger leads to
                    // the deletion of all expired ledgers. In this case, there's only one empty ledger in the managed
                    // ledger. So here we complete it with the latest offset.
                    future.complete(MessageMetadataUtils.getLogEndOffset(managedLedger));
                } else {
                    future.completeExceptionally(exception);
                }
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    long offset = peekOffsetFromEntry(entry);
                    final long publishTime = MessageMetadataUtils.getPublishTime(entry.getDataBuffer());
                    if (publishTime >= timestamp) {
                        future.complete(offset);
                    } else {
                        future.complete(offset + 1);
                    }
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                } finally {
                    if (entry != null) {
                        entry.release();
                    }
                }
            }
        }, null);
        return future;
    }

    public CompletableFuture<Position> findOffsetPositionAsync(long offset, boolean truncate) {
        if (offset < 0) {
            return CompletableFuture.failedFuture(Errors.OFFSET_OUT_OF_RANGE.exception());
        }
        if (offset >= MessageMetadataUtils.getLogEndOffset(managedLedger)) {
            return CompletableFuture.completedFuture(PositionFactory.LATEST);
        }
        CompletableFuture<Position> future = new CompletableFuture<>();
        asyncFindOffsetPosition0(offset)
                .thenApply(position -> {
                    if (!truncate) {
                        return position;
                    }
                    Position lastConfirmedEntry = managedLedger.getLastConfirmedEntry();
                    log.info("Found position {} for offset {}, lastConfirmedEntry {}",
                            position, offset, lastConfirmedEntry);
                    if (lastConfirmedEntry != null
                            && Objects.equals(lastConfirmedEntry.getNext(), position)) {
                        log.debug("Found position {} for offset {}, LAC {} -> RETURN LATEST",
                                position, offset, lastConfirmedEntry);
                        return PositionFactory.LATEST;
                    }
                    return position;
                })
                .thenAccept(future::complete)
                .exceptionally(t -> {
                    Errors errors = Errors.UNKNOWN_SERVER_ERROR;
                    if (t instanceof ManagedLedgerException.ManagedLedgerNotFoundException
                            || t instanceof ManagedLedgerException.ManagedLedgerFencedException
                            || t instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException
                            || t instanceof ManagedLedgerException.CursorAlreadyClosedException) {
                        errors = Errors.NOT_LEADER_OR_FOLLOWER;
                    }
                    future.completeExceptionally(errors.exception());
                    return null;
                });
        return future;
    }

    public CompletableFuture<Void> asyncAddLedgerBaseOffset(long baseOffset, long ledgerId) {
        return managedLedger.asyncAddLedgerProperty(ledgerId, KAFKA_BASE_OFFSET, String.valueOf(baseOffset));
    }

    private CompletableFuture<Position> asyncFindOffsetPosition0(final long offset) {
        Pair<Position, Position> range = getSearchRange(managedLedger, offset);
        Position left = range.getLeft();
        Position right = range.getRight();
        if (left != null && left.equals(right)) {
            return CompletableFuture.completedFuture(left);
        }
        CompletableFuture<Position> future = new CompletableFuture<>();
        Predicate<Entry> predicate = new FindEntryByOffset(managedLedger, offset);
        cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, predicate,
                left, right, new AsyncCallbacks.FindEntryCallback() {
                    @Override
                    public void findEntryComplete(Position position, Object ctx) {
                        if (position == null) {
                            future.complete(managedLedger.getFirstPosition());
                        } else {
                            future.complete(managedLedger.getNextValidPosition(position));
                        }
                    }

                    @Override
                    public void findEntryFailed(ManagedLedgerException e, Optional<Position> p, Object ctx) {
                        log.error("Failed to find entry for offset {} ", offset, e);
                        future.completeExceptionally(e);
                    }
                }, null, true);
        return future;
    }


    private static Pair<Position, Position> getSearchRange(ManagedLedger managedLedger, long offset) {
        NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers = managedLedger.getLedgersInfo();
        Position start = null;
        Position end = null;
        MLDataFormats.ManagedLedgerInfo.LedgerInfo previous = null;
        for (Map.Entry<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> entry : ledgers.entrySet()) {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = entry.getValue();
            if (ledgerInfo.getPropertiesCount() <= 0) {
                return Pair.of(null, null);
            }
            Long ledgerBaseOffset = getLedgerBaseOffset(ledgerInfo);
            if (ledgerBaseOffset == null) {
                return Pair.of(null, null);
            }
            if (ledgerBaseOffset == offset) {
                start = PositionFactory.create(ledgerInfo.getLedgerId(), 0);
                end = PositionFactory.create(ledgerInfo.getLedgerId(), 0);
                return Pair.of(start, end);
            }
            if (ledgerBaseOffset < offset) {
                start = PositionFactory.create(ledgerInfo.getLedgerId(), 0);
            }
            if (ledgerBaseOffset > offset) {
                if (previous != null) {
                    end = PositionFactory.create(previous.getLedgerId(), previous.getEntries() - 1);
                }
                break;
            }
            previous = ledgerInfo;
        }
        return Pair.of(start, end);
    }

    private static Long getLedgerBaseOffset(MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo) {
        if (ledgerInfo.getPropertiesCount() <= 0) {
            return null;
        }
        for (MLDataFormats.KeyValue kv : ledgerInfo.getPropertiesList()) {
            if (kv.hasKey() && kv.getKey().equals(KAFKA_BASE_OFFSET)) {
                if (NumberUtils.isCreatable(kv.getValue())) {
                    return NumberUtils.toLong(kv.getValue());
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    private record FindEntryByOffset(ManagedLedger managedLedger, long offset) implements Predicate<Entry> {
        @Override
        public boolean test(Entry entry) {
            if (entry == null) {
                // `entry` should not be null, add the null check here to fix the spotbugs check
                return false;
            }
            try {
                long offset0 = peekOffsetFromEntry(entry);
                return offset0 < offset;
            } catch (Throwable t) {
                log.error("[{}] Entry {} is corrupted: {}",
                        managedLedger.getName(), entry.getPosition(), t.getMessage());
                return false;
            } finally {
                entry.release();
            }
        }
    }

    public static Position getFirstValidPosition(ManagedLedgerImpl managedLedger) {
        Position firstPosition = managedLedger.getFirstPosition();
        if (firstPosition == null) {
            return null;
        } else {
            final Position validPosition = managedLedger.getNextValidPosition(firstPosition);
            final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers =
                    managedLedger.getLedgersInfo();
            if (!ledgers.containsKey(validPosition.getLedgerId())) {
                // It's a rare case if getNextValidPosition() returns a position that doesn't belong to the ledgers map
                // while the ledgers map contains a non-empty ledger. In this case, return the first position.
                final Map.Entry<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> entry = ledgers.firstEntry();
                if (entry != null && entry.getValue().hasEntries() && entry.getValue().getEntries() > 0) {
                    log.warn("ManagedLedger {} is not empty and doesn't contain {}, return the first position {}:0",
                            managedLedger.getName(), validPosition, entry.getKey());
                    return PositionFactory.create(entry.getKey(), 0);
                }
            }
            return validPosition;
        }
    }
}
