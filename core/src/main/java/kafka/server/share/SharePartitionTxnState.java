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


package kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.share.fetch.InFlightBatch;
import org.apache.kafka.server.share.fetch.InFlightState;
import org.apache.kafka.server.share.fetch.RecordState;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.PartitionAllData;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionIdLeaderEpochData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.server.share.persister.ReadShareGroupStateParameters;
import org.apache.kafka.server.share.persister.TopicData;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class SharePartitionTxnState {
    static CompletableFuture<PartitionAllData> readState(
        Persister persister, String groupId, TopicIdPartition partition, int leaderEpoch, int stateEpoch
    ) {
        return persister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(groupId).setTopicsData(List.of(new TopicData<>(partition.topicId(),
                    List.of(PartitionFactory.newPartitionIdLeaderEpochData(partition.partition(), leaderEpoch)))))
                .build()).build()).thenApply(result -> {
                    if (result == null || result.topicsData() == null || result.topicsData().size() != 1 ||
                        !result.topicsData().get(0).topicId().equals(partition.topicId()) ||
                        result.topicsData().get(0).partitions().size() != 1) {
                        throw new IllegalStateException("Invalid share transaction state response");
                    }
                    PartitionAllData state = result.topicsData().get(0).partitions().get(0);
                    if (state.partition() != partition.partition()) {
                        throw new IllegalStateException("Invalid share transaction partition response");
                    }
                    if (state.errorCode() != Errors.NONE.code()) {
                        throw Errors.forCode(state.errorCode()).exception(state.errorMessage());
                    }
                    if (state.stateEpoch() != stateEpoch) throw Errors.FENCED_STATE_EPOCH.exception();
                    return state;
                });
    }

    static boolean hasPendingRecords(NavigableMap<Long, InFlightBatch> cache, ReadWriteLock lock) {
        lock.readLock().lock();
        try {
            return cache.values().stream().anyMatch(InFlightBatch::hasPendingTransactionalRecords);
        } finally {
            lock.readLock().unlock();
        }
    }
    record ResolvedRecord(long firstOffset, long lastOffset, RecordState state, short deliveryCount, Runnable archive) { }

    private record PendingRecord(long firstOffset, long lastOffset, long producerId, short producerEpoch,
                                 InFlightBatch batch, InFlightState state) {
        InFlightState resolve(RecordState next) {
            return batch == null ? state.applyPersistedTxnState(producerId, producerEpoch, next) :
                batch.applyPersistedTxnState(producerId, producerEpoch, next);
        }

        Runnable archive() {
            return batch == null ? state::archive : batch::archiveBatch;
        }
    }

    private final ReadWriteLock lock;
    private final NavigableMap<Long, InFlightBatch> cache;
    private final BooleanSupplier active;
    private final Supplier<CompletableFuture<PartitionAllData>> read;
    private final Consumer<List<ResolvedRecord>> onResolved;
    private int stagingWrites;
    private CompletableFuture<Void> refresh;

    SharePartitionTxnState(ReadWriteLock lock, NavigableMap<Long, InFlightBatch> cache,
                           BooleanSupplier active, Supplier<CompletableFuture<PartitionAllData>> read,
                           Consumer<List<ResolvedRecord>> onResolved) {
        this.lock = lock;
        this.cache = cache;
        this.active = active;
        this.read = read;
        this.onResolved = onResolved;
    }

    void beginStaging() {
        stagingWrites++;
    }

    void endStaging() {
        lock.writeLock().lock();
        try {
            stagingWrites--;
        } finally {
            lock.writeLock().unlock();
        }
    }

    CompletableFuture<Void> persistStaging(Supplier<CompletableFuture<Void>> write, Runnable rollback) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<Void> persistence;
        try {
            persistence = write.get();
        } catch (Throwable error) {
            persistence = CompletableFuture.failedFuture(error);
        }
        persistence.whenComplete((ignored, error) -> {
            lock.writeLock().lock();
            try {
                if (error != null) rollback.run();
            } catch (Throwable rollbackError) {
                error.addSuppressed(rollbackError);
            } finally {
                stagingWrites--;
                lock.writeLock().unlock();
            }
            if (error == null) result.complete(null);
            else result.completeExceptionally(error);
        });
        return result;
    }

    CompletableFuture<Void> refresh() {
        lock.writeLock().lock();
        try {
            if (refresh != null) return refresh;
            if (!active.getAsBoolean()) {
                return CompletableFuture.failedFuture(new NotLeaderOrFollowerException());
            }
            // A read before a stage becomes durable must not mistake AVAILABLE for an abort.
            if (stagingWrites != 0) return CompletableFuture.completedFuture(null);
            List<PendingRecord> pending = pendingRecords();
            if (pending.isEmpty()) return CompletableFuture.completedFuture(null);
            CompletableFuture<Void> future = new CompletableFuture<>();
            refresh = future;
            try {
                read.get().whenComplete((state, error) -> completeRefresh(pending, state, error, future));
            } catch (Throwable error) {
                completeRefresh(pending, null, error, future);
            }
            return future;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private List<PendingRecord> pendingRecords() {
        List<PendingRecord> pending = new ArrayList<>();
        for (InFlightBatch batch : cache.values()) {
            if (batch.offsetState() == null) {
                if (batch.batchState() == RecordState.TX_PENDING) {
                    pending.add(new PendingRecord(batch.firstOffset(), batch.lastOffset(),
                        batch.batchStagedProducerId(), batch.batchStagedProducerEpoch(), batch, null));
                }
            } else {
                batch.offsetState().forEach((offset, state) -> {
                    if (state.state() == RecordState.TX_PENDING) {
                        pending.add(new PendingRecord(offset, offset, state.stagedTxnOwnerId(),
                            state.stagedTxnOwnerEpoch(), null, state));
                    }
                });
            }
        }
        return pending;
    }

    private void completeRefresh(List<PendingRecord> pending, PartitionAllData persisted,
                                 Throwable error, CompletableFuture<Void> future) {
        List<ResolvedRecord> resolved = new ArrayList<>();
        lock.writeLock().lock();
        try {
            if (error == null) {
                if (!active.getAsBoolean()) throw new NotLeaderOrFollowerException();
                for (PendingRecord record : pending) {
                    RecordState finalState = finalState(record, persisted);
                    if (finalState == null) continue;
                    InFlightState state = record.resolve(finalState);
                    if (state != null) {
                        resolved.add(new ResolvedRecord(record.firstOffset(), record.lastOffset(), finalState,
                            (short) state.deliveryCount(), record.archive()));
                    }
                }
            }
        } catch (Throwable t) {
            error = t;
        } finally {
            refresh = null;
            lock.writeLock().unlock();
        }
        if (error == null) {
            try {
                onResolved.accept(resolved);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        } else {
            future.completeExceptionally(error);
        }
    }

    private RecordState finalState(PendingRecord record, PartitionAllData persisted) {
        if (record.lastOffset() < persisted.startOffset()) return RecordState.ARCHIVED;
        for (PersisterStateBatch batch : persisted.stateBatches()) {
            if (batch.firstOffset() <= record.firstOffset() && batch.lastOffset() >= record.lastOffset()) {
                RecordState state = RecordState.forId(batch.deliveryState());
                return state == RecordState.AVAILABLE || state == RecordState.ACKNOWLEDGED ||
                    state == RecordState.ARCHIVING || state == RecordState.ARCHIVED ? state : null;
            }
        }
        return null;
    }
}
