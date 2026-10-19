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

import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.fetch.DeliveryCountOps;
import org.apache.kafka.server.share.fetch.InFlightBatch;
import org.apache.kafka.server.share.fetch.InFlightState;
import org.apache.kafka.server.share.fetch.RecordState;
import org.apache.kafka.server.share.persister.PartitionAllData;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PersisterStateBatch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharePartitionTxnStateTest {
    private final NavigableMap<Long, InFlightBatch> cache = new TreeMap<>();
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final AtomicInteger reads = new AtomicInteger();
    private final List<SharePartitionTxnState.ResolvedRecord> resolved = new ArrayList<>();
    private CompletableFuture<PartitionAllData> response = new CompletableFuture<>();
    private final SharePartitionTxnState transactions = new SharePartitionTxnState(
        new ReentrantReadWriteLock(), cache, active::get, () -> {
            reads.incrementAndGet();
            return response;
        }, resolved::addAll);

    @Test
    void testCommitPreservesOtherAcquisitionsAndTransactions() {
        InFlightBatch committed = acquired(0, 1);
        committed.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        InFlightBatch acquired = acquired(2, 3);
        InFlightBatch pending = acquired(4, 5);
        pending.stageBatchTxnAcknowledge(20, (short) 2, (byte) 1, RecordState.ACKNOWLEDGED);

        var future = transactions.refresh();
        response.complete(persisted(0, List.of(
            batch(0, 1, RecordState.ACKNOWLEDGED),
            new PersisterStateBatch(4, 5, RecordState.TX_PENDING.id(), (short) 1, 20, (short) 2, (byte) 1))));
        future.join();

        assertEquals(RecordState.ACKNOWLEDGED, committed.batchState());
        assertEquals(RecordState.ACQUIRED, acquired.batchState());
        assertEquals("member", acquired.batchMemberId());
        assertEquals(RecordState.TX_PENDING, pending.batchState());
        assertSame(acquired, cache.get(2L));
        assertEquals(1, resolved.size());
    }

    @Test
    void testSynchronousStageFailureReleasesRefreshGuard() {
        InFlightBatch pending = acquired(0, 0);
        pending.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        transactions.beginStaging();
        var future = transactions.persistStaging(() -> {
            throw new IllegalStateException("write failed");
        }, () -> pending.revertBatchStagedTxnAcknowledge(10, (short) 1));
        assertThrows(Exception.class, future::join);
        assertEquals(RecordState.ACQUIRED, pending.batchState());
        pending.stageBatchTxnAcknowledge(10, (short) 2, (byte) 1, RecordState.ACKNOWLEDGED);
        var refresh = transactions.refresh();
        response.complete(persisted(0, List.of(batch(0, 0, RecordState.ACKNOWLEDGED))));
        refresh.join();
        assertEquals(1, reads.get());
    }

    @Test
    void testResolvedDispositionDoesNotFollowLaterStateMutations() {
        InFlightBatch pending = acquired(0, 0);
        pending.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        var future = transactions.refresh();
        response.complete(persisted(0, List.of(batch(0, 0, RecordState.AVAILABLE))));
        future.join();
        pending.tryUpdateBatchState(RecordState.ACQUIRED, DeliveryCountOps.INCREASE, 10, "other-member", false);
        assertEquals(RecordState.AVAILABLE, resolved.get(0).state());
        assertEquals(1, resolved.get(0).deliveryCount());
    }

    @Test
    void testAbortAllowsRedelivery() {
        InFlightBatch pending = acquired(0, 0);
        pending.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        var future = transactions.refresh();
        response.complete(persisted(0, List.of(batch(0, 0, RecordState.AVAILABLE))));
        future.join();
        assertEquals(RecordState.AVAILABLE, pending.batchState());
        assertEquals(-1L, pending.batchStagedProducerId());
    }

    @Test
    void testRefreshDoesNotReadUnpersistedStaging() {
        InFlightBatch pending = acquired(0, 0);
        pending.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        transactions.beginStaging();
        transactions.refresh().join();
        assertEquals(0, reads.get());
        transactions.endStaging();
        var future = transactions.refresh();
        response.complete(persisted(0, List.of(batch(0, 0, RecordState.ACKNOWLEDGED))));
        future.join();
        assertEquals(RecordState.ACKNOWLEDGED, pending.batchState());
    }

    @Test
    void testDelayedReadCannotResolveNewTransaction() {
        InFlightBatch pending = acquired(0, 0);
        pending.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        var future = transactions.refresh();
        pending.revertBatchStagedTxnAcknowledge(10, (short) 1);
        pending.stageBatchTxnAcknowledge(10, (short) 2, (byte) 1, RecordState.ACKNOWLEDGED);
        response.complete(persisted(0, List.of(batch(0, 0, RecordState.AVAILABLE))));
        future.join();
        assertEquals(RecordState.TX_PENDING, pending.batchState());
        assertEquals(2, pending.batchStagedProducerEpoch());
        assertTrue(resolved.isEmpty());
    }

    @Test
    void testConcurrentRefreshSharesReadAndFailedReadCanRetry() {
        InFlightBatch pending = acquired(0, 0);
        pending.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        var first = transactions.refresh();
        assertSame(first, transactions.refresh());
        response.completeExceptionally(Errors.NOT_COORDINATOR.exception());
        assertTrue(first.isCompletedExceptionally());
        assertEquals(RecordState.TX_PENDING, pending.batchState());
        response = CompletableFuture.completedFuture(persisted(0, List.of(batch(0, 0, RecordState.ACKNOWLEDGED))));
        transactions.refresh().join();
        assertEquals(2, reads.get());
    }

    @Test
    void testFencedPartitionDoesNotApplyRead() {
        InFlightBatch pending = acquired(0, 0);
        pending.stageBatchTxnAcknowledge(10, (short) 1, (byte) 1, RecordState.ACKNOWLEDGED);
        var future = transactions.refresh();
        active.set(false);
        response.complete(persisted(0, List.of(batch(0, 0, RecordState.ACKNOWLEDGED))));
        assertTrue(future.isCompletedExceptionally());
        assertEquals(RecordState.TX_PENDING, pending.batchState());
        assertInstanceOf(NotLeaderOrFollowerException.class, assertThrows(java.util.concurrent.CompletionException.class, future::join).getCause());
    }

    @Test
    void testPerOffsetStateAndPrunedTerminalBatch() {
        InFlightBatch pending = acquired(0, 2);
        pending.maybeInitializeOffsetStateUpdate();
        InFlightState state = pending.offsetState().get(0L);
        state.stageTxnAcknowledge(10, (short) 1, (byte) 3, RecordState.ARCHIVED);
        pending.offsetState().get(1L).stageTxnAcknowledge(20, (short) 2, (byte) 1, RecordState.ACKNOWLEDGED);
        var future = transactions.refresh();
        response.complete(persisted(1, List.of(
            new PersisterStateBatch(1, 1, RecordState.TX_PENDING.id(), (short) 1, 20, (short) 2, (byte) 1))));
        future.join();
        assertEquals(RecordState.ARCHIVED, state.state());
        assertEquals(RecordState.TX_PENDING, pending.offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, pending.offsetState().get(2L).state());
    }

    private InFlightBatch acquired(long first, long last) {
        var batch = new InFlightBatch(null, Time.SYSTEM, "member", first, last, RecordState.ACQUIRED, 1, null, null, null);
        cache.put(first, batch);
        return batch;
    }

    private PersisterStateBatch batch(long first, long last, RecordState state) {
        return new PersisterStateBatch(first, last, state.id(), (short) 1);
    }

    private PartitionAllData persisted(long start, List<PersisterStateBatch> batches) {
        return PartitionFactory.newPartitionAllData(0, 0, start, Errors.NONE.code(), "", batches);
    }
}
