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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.common.TransactionVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.server.common.TransactionVersion.TV_0;
import static org.apache.kafka.server.common.TransactionVersion.TV_2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TransactionMetadataTest {

    private final MockTime time = new MockTime();
    private final long producerId = 23423L;
    private final String transactionalId = "txnlId";

    @Test
    public void testInitializeEpoch() {
        short producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0
        );

        TxnTransitMetadata transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Optional.empty());
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(0, txnMetadata.producerEpoch());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch());
    }

    @Test
    public void testNormalEpochBump() {
        short producerEpoch = 735;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0
        );

        TxnTransitMetadata transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Optional.empty());
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(producerEpoch + 1, txnMetadata.producerEpoch());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch());
    }

    @Test
    public void testBumpEpochNotAllowedIfEpochsExhausted() {
        short producerEpoch = (short) (Short.MAX_VALUE - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0
        );

        assertTrue(txnMetadata.isProducerEpochExhausted());
        assertThrows(IllegalStateException.class, () -> txnMetadata.prepareIncrementProducerEpoch(30000,
            Optional.empty(), time.milliseconds()));
    }

    @Test
    public void testTransitFromEmptyToPrepareAbortInV2() {
        short producerEpoch = 735;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_2
        );

        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() + 1, true);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(producerEpoch + 1, txnMetadata.producerEpoch());
        assertEquals(time.milliseconds() + 1, txnMetadata.txnStartTimestamp());
    }

    @Test
    public void testTransitFromCompleteAbortToPrepareAbortInV2() {
        short producerEpoch = 735;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.COMPLETE_ABORT,
            Set.of(),
            time.milliseconds() - 1,
            time.milliseconds(),
            TV_2
        );

        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() + 1, true);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(producerEpoch + 1, txnMetadata.producerEpoch());
        assertEquals(time.milliseconds() + 1, txnMetadata.txnStartTimestamp());
    }

    @Test
    public void testTransitFromCompleteCommitToPrepareAbortInV2() {
        short producerEpoch = 735;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.COMPLETE_COMMIT,
            Set.of(),
            time.milliseconds() - 1,
            time.milliseconds(),
            TV_2
        );

        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() + 1, true);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(producerEpoch + 1, txnMetadata.producerEpoch());
        assertEquals(time.milliseconds() + 1, txnMetadata.txnStartTimestamp());
    }

    @Test
    public void testTolerateUpdateTimeShiftDuringEpochBump() {
        short producerEpoch = 1;
        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            1L,
            time.milliseconds(),
            TV_0
        );

        // let new time be smaller
        TxnTransitMetadata transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Optional.of(producerEpoch),
            Optional.of(time.milliseconds() - 1));
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(producerEpoch + 1, txnMetadata.producerEpoch());
        assertEquals(producerEpoch, txnMetadata.lastProducerEpoch());
        assertEquals(-1L, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp());
    }

    @Test
    public void testTolerateUpdateTimeResetDuringProducerIdRotation() {
        short producerEpoch = 1;
        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            1L,
            time.milliseconds(),
            TV_0
        );

        // let new time be smaller
        TxnTransitMetadata transitMetadata = txnMetadata.prepareProducerIdRotation(producerId + 1, 30000, time.milliseconds() - 1, true);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId + 1, txnMetadata.producerId());
        assertEquals(producerEpoch, txnMetadata.lastProducerEpoch());
        assertEquals(0, txnMetadata.producerEpoch());
        assertEquals(-1L, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp());
    }

    @Test
    public void testTolerateTimeShiftDuringAddPartitions() {
        short producerEpoch = 1;
        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            time.milliseconds(),
            time.milliseconds(),
            TV_0
        );

        // let new time be smaller; when transiting from TransactionState.EMPTY the start time would be updated to the update-time
        TxnTransitMetadata transitMetadata = txnMetadata.prepareAddPartitions(Set.of(new TopicPartition("topic1", 0)), time.milliseconds() - 1, TV_0);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(Set.of(new TopicPartition("topic1", 0)), txnMetadata.topicPartitions());
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch());
        assertEquals(producerEpoch, txnMetadata.producerEpoch());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp());

        // add another partition, check that in TransactionState.ONGOING state the start timestamp would not change to update time
        transitMetadata = txnMetadata.prepareAddPartitions(Set.of(new TopicPartition("topic2", 0)), time.milliseconds() - 2, TV_0);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(Set.of(new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)), txnMetadata.topicPartitions());
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch());
        assertEquals(producerEpoch, txnMetadata.producerEpoch());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 2, txnMetadata.txnLastUpdateTimestamp());
    }

    @Test
    public void testTolerateTimeShiftDuringPrepareCommit() {
        short producerEpoch = 1;
        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.ONGOING,
            Set.of(),
            1L,
            time.milliseconds(),
            TV_0
        );

        // let new time be smaller
        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds() - 1, false);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(TransactionState.PREPARE_COMMIT, txnMetadata.state());
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch());
        assertEquals(producerEpoch, txnMetadata.producerEpoch());
        assertEquals(1L, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp());
    }

    @Test
    public void testTolerateTimeShiftDuringPrepareAbort() {
        short producerEpoch = 1;
        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.ONGOING,
            Set.of(),
            1L,
            time.milliseconds(),
            TV_0
        );

        // let new time be smaller
        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds() - 1, false);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(TransactionState.PREPARE_ABORT, txnMetadata.state());
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch());
        assertEquals(producerEpoch, txnMetadata.producerEpoch());
        assertEquals(1L, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 2})
    public void testTolerateTimeShiftDuringCompleteCommit(short transactionVersion) {
        TransactionVersion clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion);
        short producerEpoch = 1;
        short lastProducerEpoch = 0;
        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            lastProducerEpoch,
            30000,
            TransactionState.PREPARE_COMMIT,
            Set.of(),
            1L,
            time.milliseconds(),
            clientTransactionVersion
        );

        // let new time be smaller
        TxnTransitMetadata transitMetadata = txnMetadata.prepareComplete(time.milliseconds() - 1);
        txnMetadata.completeTransitionTo(transitMetadata);

        assertEquals(TransactionState.COMPLETE_COMMIT, txnMetadata.state());
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(lastProducerEpoch, txnMetadata.lastProducerEpoch());
        assertEquals(producerEpoch, txnMetadata.producerEpoch());
        assertEquals(1L, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 2})
    public void testTolerateTimeShiftDuringCompleteAbort(short transactionVersion) {
        TransactionVersion clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion);
        short producerEpoch = 1;
        short lastProducerEpoch = 0;
        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            lastProducerEpoch,
            30000,
            TransactionState.PREPARE_ABORT,
            Set.of(),
            1L,
            time.milliseconds(),
            clientTransactionVersion
        );

        // let new time be smaller
        TxnTransitMetadata transitMetadata = txnMetadata.prepareComplete(time.milliseconds() - 1);
        txnMetadata.completeTransitionTo(transitMetadata);

        assertEquals(TransactionState.COMPLETE_ABORT, txnMetadata.state());
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(lastProducerEpoch, txnMetadata.lastProducerEpoch());
        assertEquals(producerEpoch, txnMetadata.producerEpoch());
        assertEquals(1L, txnMetadata.txnStartTimestamp());
        assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp());
    }

    @Test
    public void testFenceProducerAfterEpochsExhausted() {
        short producerEpoch = (short) (Short.MAX_VALUE - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.ONGOING,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);
        assertTrue(txnMetadata.isProducerEpochExhausted());

        TxnTransitMetadata fencingTransitMetadata = txnMetadata.prepareFenceProducerEpoch();
        assertEquals(Short.MAX_VALUE, fencingTransitMetadata.producerEpoch());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, fencingTransitMetadata.lastProducerEpoch());
        assertEquals(Optional.of(TransactionState.PREPARE_EPOCH_FENCE), txnMetadata.pendingState());

        // We should reset the pending state to make way for the abort transition.
        txnMetadata.pendingState(Optional.empty());

        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds(), false);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, transitMetadata.producerId());
    }

    @Test
    public void testInvalidTransitionFromCompleteCommitToFence() {
        short producerEpoch = (short) (Short.MAX_VALUE - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.COMPLETE_COMMIT,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);
        assertTrue(txnMetadata.isProducerEpochExhausted());

        assertThrows(IllegalStateException.class, txnMetadata::prepareFenceProducerEpoch);
    }

    @Test
    public void testInvalidTransitionFromCompleteAbortToFence() {
        short producerEpoch = (short) (Short.MAX_VALUE - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.COMPLETE_ABORT,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);
        assertTrue(txnMetadata.isProducerEpochExhausted());

        assertThrows(IllegalStateException.class, txnMetadata::prepareFenceProducerEpoch);
    }

    @Test
    public void testFenceProducerNotAllowedIfItWouldOverflow() {
        short producerEpoch = Short.MAX_VALUE;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.ONGOING,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);
        assertTrue(txnMetadata.isProducerEpochExhausted());

        // When epoch is at max, prepareFenceProducerEpoch logs an error but doesn't throw
        // This allows graceful recovery through producer ID rotation
        TxnTransitMetadata preparedMetadata = txnMetadata.prepareFenceProducerEpoch();

        // Epoch should remain at Short.MaxValue (not overflow to negative)
        assertEquals(Short.MAX_VALUE, preparedMetadata.producerEpoch());
        assertEquals(TransactionState.PREPARE_EPOCH_FENCE, preparedMetadata.txnState());
    }

    @Test
    public void testRotateProducerId() {
        short producerEpoch = (short) (Short.MAX_VALUE - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);

        long newProducerId = 9893L;
        TxnTransitMetadata transitMetadata = txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds(), true);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(newProducerId, txnMetadata.producerId());
        assertEquals(producerId, txnMetadata.prevProducerId());
        assertEquals(0, txnMetadata.producerEpoch());
        assertEquals(producerEpoch, txnMetadata.lastProducerEpoch());
    }

    @Test
    public void testEpochBumpOnEndTxn() {
        time.sleep(100);
        short producerEpoch = 10;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.ONGOING,
            Set.of(),
            time.milliseconds(),
            time.milliseconds(),
            TV_2);

        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() - 1, false);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals((short) (producerEpoch + 1), txnMetadata.producerEpoch());
        assertEquals(TV_2, txnMetadata.clientTransactionVersion());

        transitMetadata = txnMetadata.prepareComplete(time.milliseconds());
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals((short) (producerEpoch + 1), txnMetadata.producerEpoch());
        assertEquals(TV_2, txnMetadata.clientTransactionVersion());
    }

    @Test
    public void testEpochBumpOnEndTxnOverflow() {
        time.sleep(100);
        short producerEpoch = (short) (Short.MAX_VALUE - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.ONGOING,
            Set.of(),
            time.milliseconds(),
            time.milliseconds(),
            TV_2);
        assertTrue(txnMetadata.isProducerEpochExhausted());

        long newProducerId = 9893L;
        TxnTransitMetadata transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_2, newProducerId, time.milliseconds() - 1, false);
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(Short.MAX_VALUE, txnMetadata.producerEpoch());
        assertEquals(producerEpoch, txnMetadata.lastProducerEpoch());
        assertEquals(TV_2, txnMetadata.clientTransactionVersion());

        transitMetadata = txnMetadata.prepareComplete(time.milliseconds());
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(newProducerId, txnMetadata.producerId());
        assertEquals(0, txnMetadata.producerEpoch());
        assertEquals(producerEpoch, txnMetadata.lastProducerEpoch());
        assertEquals(TV_2, txnMetadata.clientTransactionVersion());
    }

    @Test
    public void testRotateProducerIdInOngoingState() {
        assertThrows(IllegalStateException.class, () -> testRotateProducerIdInOngoingState(TransactionState.ONGOING, TV_0));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 2})
    public void testRotateProducerIdInPrepareAbortState(short transactionVersion) {
        TransactionVersion clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion);
        assertThrows(IllegalStateException.class, () -> testRotateProducerIdInOngoingState(TransactionState.PREPARE_ABORT, clientTransactionVersion));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 2})
    public void testRotateProducerIdInPrepareCommitState(short transactionVersion) {
        TransactionVersion clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion);
        assertThrows(IllegalStateException.class, () -> testRotateProducerIdInOngoingState(TransactionState.PREPARE_COMMIT, clientTransactionVersion));
    }

    @Test
    public void testAttemptedEpochBumpWithNewlyCreatedMetadata() {
        short producerEpoch = 735;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);

        TxnTransitMetadata transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Optional.of(producerEpoch));
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(0, txnMetadata.producerEpoch());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch());
    }

    @Test
    public void testEpochBumpWithCurrentEpochProvided() {
        short producerEpoch = 735;

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);

        TxnTransitMetadata transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Optional.of(producerEpoch));
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(producerEpoch + 1, txnMetadata.producerEpoch());
        assertEquals(producerEpoch, txnMetadata.lastProducerEpoch());
    }

    @Test
    public void testAttemptedEpochBumpWithLastEpoch() {
        short producerEpoch = 735;
        short lastProducerEpoch = (short) (producerEpoch - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            lastProducerEpoch,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);

        TxnTransitMetadata transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Optional.of(lastProducerEpoch));
        txnMetadata.completeTransitionTo(transitMetadata);
        assertEquals(producerId, txnMetadata.producerId());
        assertEquals(producerEpoch, txnMetadata.producerEpoch());
        assertEquals(lastProducerEpoch, txnMetadata.lastProducerEpoch());
    }

    @Test
    public void testAttemptedEpochBumpWithFencedEpoch() {
        short producerEpoch = 735;
        short lastProducerEpoch = (short) (producerEpoch - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            lastProducerEpoch,
            30000,
            TransactionState.EMPTY,
            Set.of(),
            -1,
            time.milliseconds(),
            TV_0);

        assertThrows(
            ProducerFencedException.class, 
            () -> txnMetadata.prepareIncrementProducerEpoch(30000, Optional.of((short) (lastProducerEpoch - 1)), time.milliseconds())
        );
    }

    @Test
    public void testTransactionStateIdAndNameMapping() {
        for (TransactionState state : TransactionState.ALL_STATES) {
            assertEquals(state, TransactionState.fromId(state.id()));
            assertEquals(Optional.of(state), TransactionState.fromName(state.stateName()));

            if (state != TransactionState.DEAD) {
                org.apache.kafka.clients.admin.TransactionState clientTransactionState =
                    org.apache.kafka.clients.admin.TransactionState.parse(state.stateName());
                assertEquals(state.stateName(), clientTransactionState.toString());
                assertNotEquals(org.apache.kafka.clients.admin.TransactionState.UNKNOWN, clientTransactionState);
            }
        }
    }

    @Test
    public void testAllTransactionStatesAreMapped() {
        Set<TransactionState> unmatchedStates = new HashSet<>(Set.of(
            TransactionState.EMPTY,
            TransactionState.ONGOING,
            TransactionState.PREPARE_COMMIT,
            TransactionState.PREPARE_ABORT,
            TransactionState.COMPLETE_COMMIT,
            TransactionState.COMPLETE_ABORT,
            TransactionState.PREPARE_EPOCH_FENCE,
            TransactionState.DEAD
        ));

        // The exhaustive match is intentional here to ensure that we are
        // forced to update the test case if a new state is added.
        for (TransactionState state : TransactionState.ALL_STATES) {
            switch (state) {
                case EMPTY -> assertTrue(unmatchedStates.remove(TransactionState.EMPTY));
                case ONGOING -> assertTrue(unmatchedStates.remove(TransactionState.ONGOING));
                case PREPARE_COMMIT -> assertTrue(unmatchedStates.remove(TransactionState.PREPARE_COMMIT));
                case PREPARE_ABORT -> assertTrue(unmatchedStates.remove(TransactionState.PREPARE_ABORT));
                case COMPLETE_COMMIT -> assertTrue(unmatchedStates.remove(TransactionState.COMPLETE_COMMIT));
                case COMPLETE_ABORT -> assertTrue(unmatchedStates.remove(TransactionState.COMPLETE_ABORT));
                case PREPARE_EPOCH_FENCE -> assertTrue(unmatchedStates.remove(TransactionState.PREPARE_EPOCH_FENCE));
                case DEAD -> assertTrue(unmatchedStates.remove(TransactionState.DEAD));
                default -> fail("Unexpected transaction state " + state);
            }
        }

        assertEquals(Set.of(), unmatchedStates);
    }

    private void testRotateProducerIdInOngoingState(TransactionState state, TransactionVersion clientTransactionVersion) {
        short producerEpoch = (short) (Short.MAX_VALUE - 1);

        TransactionMetadata txnMetadata = new TransactionMetadata(
            transactionalId,
            producerId,
            producerId,
            RecordBatch.NO_PRODUCER_ID,
            producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH,
            30000,
            state,
            Set.of(),
            -1,
            time.milliseconds(),
            clientTransactionVersion
        );
        long newProducerId = 9893L;
        txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds(), false);
    }

    private TxnTransitMetadata prepareSuccessfulIncrementProducerEpoch(
        TransactionMetadata txnMetadata,
        Optional<Short> expectedProducerEpoch
    ) {
        return prepareSuccessfulIncrementProducerEpoch(txnMetadata, expectedProducerEpoch, Optional.empty());
    }

    private TxnTransitMetadata prepareSuccessfulIncrementProducerEpoch(
        TransactionMetadata txnMetadata,
        Optional<Short> expectedProducerEpoch,
        Optional<Long> now
    ) {
        return txnMetadata.prepareIncrementProducerEpoch(30000, expectedProducerEpoch, now.orElse(time.milliseconds()));
    }
}
