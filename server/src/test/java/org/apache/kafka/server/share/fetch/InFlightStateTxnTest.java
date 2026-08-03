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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.requests.TransactionResult;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class InFlightStateTxnTest {

    private static final long TXN_OWNER_ID = 100L;
    private static final short TXN_OWNER_EPOCH = 1;
    private static final String MEMBER_ID = "member-1";

    // ── stageTxnAcknowledge — happy paths ────────────────────────────────────

    @Test
    public void testStageTxnAcknowledge_fromAcquired_accept_succeeds() {
        InFlightState state = acquired();
        InFlightState result = state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.ACCEPT);

        assertNotNull(result);
        assertEquals(RecordState.TX_PENDING, result.state());
        assertEquals(TXN_OWNER_ID, result.stagedTxnOwnerId());
        assertEquals(TXN_OWNER_EPOCH, result.stagedTxnOwnerEpoch());
        assertEquals(AcknowledgeType.ACCEPT.id, result.stagedAckType());
        assertNull(result.acquisitionLockTimeoutTask());
    }

    @Test
    public void testStageTxnAcknowledge_fromAcquired_reject_succeeds() {
        InFlightState state = acquired();
        InFlightState result = state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.REJECT);

        assertNotNull(result);
        assertEquals(RecordState.TX_PENDING, result.state());
        assertEquals(AcknowledgeType.REJECT.id, result.stagedAckType());
    }

    // ── stageTxnAcknowledge — invalid source states ──────────────────────────

    @Test
    public void testStageTxnAcknowledge_fromAvailable_returnsNull() {
        InFlightState state = new InFlightState(RecordState.AVAILABLE, 0, MEMBER_ID);
        assertNull(state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.ACCEPT));
        assertEquals(RecordState.AVAILABLE, state.state());
    }

    @Test
    public void testStageTxnAcknowledge_fromAcknowledged_returnsNull() {
        InFlightState state = new InFlightState(RecordState.ACKNOWLEDGED, 1, MEMBER_ID);
        assertNull(state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.ACCEPT));
        assertEquals(RecordState.ACKNOWLEDGED, state.state());
    }

    @Test
    public void testStageTxnAcknowledge_fromTxPending_returnsNull() {
        InFlightState state = acquired();
        state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.ACCEPT);
        assertNull(state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.ACCEPT));
    }

    @Test
    public void testStageTxnAcknowledge_withRelease_throwsIllegalArgument() {
        InFlightState state = acquired();
        assertThrows(IllegalArgumentException.class,
            () -> state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.RELEASE));
    }

    @Test
    public void testStageTxnAcknowledge_withRenew_throwsIllegalArgument() {
        InFlightState state = acquired();
        assertThrows(IllegalArgumentException.class,
            () -> state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.RENEW));
    }

    // ── applyTxnMarker — COMMIT paths ────────────────────────────────────────

    @Test
    public void testApplyTxnMarker_commit_accept_transitionsToAcknowledged() {
        InFlightState state = staged(AcknowledgeType.ACCEPT);
        InFlightState result = state.applyTxnMarker(TXN_OWNER_ID, TXN_OWNER_EPOCH, TransactionResult.COMMIT);

        assertNotNull(result);
        assertEquals(RecordState.ACKNOWLEDGED, result.state());
        assertEquals(-1L, result.stagedTxnOwnerId());
        assertEquals((byte) -1, result.stagedAckType());
    }

    @Test
    public void testApplyTxnMarker_commit_reject_transitionsToArchiving() {
        InFlightState state = staged(AcknowledgeType.REJECT);
        InFlightState result = state.applyTxnMarker(TXN_OWNER_ID, TXN_OWNER_EPOCH, TransactionResult.COMMIT);

        assertNotNull(result);
        assertEquals(RecordState.ARCHIVING, result.state());
        assertEquals(-1L, result.stagedTxnOwnerId());
    }

    // ── applyTxnMarker — ABORT path ──────────────────────────────────────────

    @Test
    public void testApplyTxnMarker_abort_accept_transitionsToAvailable() {
        InFlightState state = staged(AcknowledgeType.ACCEPT);
        InFlightState result = state.applyTxnMarker(TXN_OWNER_ID, TXN_OWNER_EPOCH, TransactionResult.ABORT);

        assertNotNull(result);
        assertEquals(RecordState.AVAILABLE, result.state());
        assertEquals(InFlightState.EMPTY_MEMBER_ID, result.memberId());
        assertEquals(-1L, result.stagedTxnOwnerId());
    }

    @Test
    public void testApplyTxnMarker_abort_reject_alsoTransitionsToAvailable() {
        InFlightState state = staged(AcknowledgeType.REJECT);
        InFlightState result = state.applyTxnMarker(TXN_OWNER_ID, TXN_OWNER_EPOCH, TransactionResult.ABORT);

        assertNotNull(result);
        assertEquals(RecordState.AVAILABLE, result.state());
    }

    // ── applyTxnMarker — stale / wrong txn owner fencing ─────────────────────

    @Test
    public void testApplyTxnMarker_wrongTxnOwnerId_returnsNull() {
        InFlightState state = staged(AcknowledgeType.ACCEPT);
        assertNull(state.applyTxnMarker(999L, TXN_OWNER_EPOCH, TransactionResult.COMMIT));
        assertEquals(RecordState.TX_PENDING, state.state());
    }

    @Test
    public void testApplyTxnMarker_wrongTxnOwnerEpoch_returnsNull() {
        InFlightState state = staged(AcknowledgeType.ACCEPT);
        assertNull(state.applyTxnMarker(TXN_OWNER_ID, (short) 99, TransactionResult.COMMIT));
        assertEquals(RecordState.TX_PENDING, state.state());
    }

    @Test
    public void testApplyTxnMarker_onNonTxPendingRecord_returnsNull() {
        InFlightState state = acquired();
        assertNull(state.applyTxnMarker(TXN_OWNER_ID, TXN_OWNER_EPOCH, TransactionResult.COMMIT));
        assertEquals(RecordState.ACQUIRED, state.state());
    }

    // ── isolation invariant ──────────────────────────────────────────────────

    @Test
    public void testTxPendingBlocksReacquisition() {
        InFlightState state = staged(AcknowledgeType.ACCEPT);
        // ACQUIRED → TX_PENDING is the only valid path in; TX_PENDING → ACQUIRED is not allowed.
        assertNull(state.tryUpdateState(
            RecordState.ACQUIRED, DeliveryCountOps.INCREASE, 5, "other-member", false));
        assertEquals(RecordState.TX_PENDING, state.state());
    }

    @Test
    public void testStagingClearsAcquisitionLockTimer() {
        AcquisitionLockTimerTask mockTask = mock(AcquisitionLockTimerTask.class);
        InFlightState state = new InFlightState(RecordState.ACQUIRED, 1, MEMBER_ID, mockTask);
        state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, AcknowledgeType.ACCEPT);
        assertNull(state.acquisitionLockTimeoutTask());
        verify(mockTask).cancel();
    }

    // ── equals / hashCode consistency ────────────────────────────────────────

    @Test
    public void testEqualsReflectsStagingFields() {
        InFlightState s1 = staged(AcknowledgeType.ACCEPT);
        InFlightState s2 = staged(AcknowledgeType.ACCEPT);
        assertEquals(s1, s2);
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private InFlightState acquired() {
        return new InFlightState(RecordState.ACQUIRED, 1, MEMBER_ID);
    }

    private InFlightState staged(AcknowledgeType ackType) {
        InFlightState state = acquired();
        state.stageTxnAcknowledge(TXN_OWNER_ID, TXN_OWNER_EPOCH, ackType);
        return state;
    }
}
