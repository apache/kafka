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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.share.metrics.SharePartitionMetrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link InFlightState} state transitions, focusing on the
 * failed-state-transition (rollback) path introduced with the two-phase
 * state transitions used for persistence and DLQ flows.
 */
public class InFlightStateTest {

    private AcquisitionLockTimerTask expiredTask(String memberId) {
        MockTime time = new MockTime();
        AcquisitionLockTimeoutHandler noopHandler = (m, firstOffset, lastOffset, task) -> { };
        AcquisitionLockTimerTask task = new AcquisitionLockTimerTask(
            time, 1000, memberId, 0, 9, noopHandler,
            new SharePartitionMetrics("group", "topic", 0));
        // Mark the task as expired, as the timer would when the acquisition lock times out.
        task.run();
        return task;
    }

    @Test
    public void testDeliveryCountExceededWithDlqSupportTransitionsToArchiving() {
        // Baseline: on the normal release path, when the delivery count limit is reached and
        // DLQ support is enabled, the records move to ARCHIVING (to be written to the DLQ).
        InFlightState state = new InFlightState(RecordState.ACQUIRED, 5, "member-1");
        InFlightState updated = state.tryUpdateState(
            RecordState.AVAILABLE, DeliveryCountOps.INCREASE, 5, InFlightState.EMPTY_MEMBER_ID, true);
        assertEquals(RecordState.ARCHIVING, updated.state());
        assertEquals(6, updated.deliveryCount());
    }

    @Test
    public void testFailedStateTransitionWithExpiredLockArchivesWhenDlqDisabled() {
        // DLQ support disabled: rolling back a failed state transition with an expired
        // acquisition lock and a delivery count at the limit archives the records. This is
        // the expected terminal behaviour when no DLQ is configured.
        InFlightState state = new InFlightState(RecordState.AVAILABLE, 5,
            InFlightState.EMPTY_MEMBER_ID, expiredTask("member-1"));
        InFlightState updated = state.startStateTransition(
            RecordState.ACQUIRED, DeliveryCountOps.INCREASE, 5, "member-1", false);
        assertEquals(RecordState.ACQUIRED, updated.state());
        assertEquals(6, updated.deliveryCount());

        state.completeStateTransition(false, false);
        assertEquals(RecordState.ARCHIVED, state.state());
        assertEquals(InFlightState.EMPTY_MEMBER_ID, state.memberId());
    }

    @Test
    public void testFailedStateTransitionWithExpiredLockDeadLettersWhenDlqSupported() {
        // DLQ support enabled: rolling back a failed state transition with an expired
        // acquisition lock and a delivery count at the limit must move the records to
        // ARCHIVING, exactly like the normal release path does (see
        // testDeliveryCountExceededWithDlqSupportTransitionsToArchiving), so that they are
        // written to the DLQ instead of being silently archived.
        InFlightState state = new InFlightState(RecordState.AVAILABLE, 5,
            InFlightState.EMPTY_MEMBER_ID, expiredTask("member-1"));
        InFlightState updated = state.startStateTransition(
            RecordState.ACQUIRED, DeliveryCountOps.INCREASE, 5, "member-1", true);
        assertEquals(RecordState.ACQUIRED, updated.state());
        assertEquals(6, updated.deliveryCount());

        state.completeStateTransition(false, true);
        assertEquals(RecordState.ARCHIVING, state.state());
        assertEquals(InFlightState.EMPTY_MEMBER_ID, state.memberId());
    }

    @Test
    public void testFailedStateTransitionWithExpiredLockBelowLimitReturnsToAvailable() {
        // Delivery count below the limit: rolling back with an expired acquisition lock
        // makes the records available again, regardless of DLQ support.
        InFlightState state = new InFlightState(RecordState.AVAILABLE, 2,
            InFlightState.EMPTY_MEMBER_ID, expiredTask("member-1"));
        InFlightState updated = state.startStateTransition(
            RecordState.ACQUIRED, DeliveryCountOps.INCREASE, 5, "member-1", true);
        assertEquals(RecordState.ACQUIRED, updated.state());
        assertEquals(3, updated.deliveryCount());

        state.completeStateTransition(false, true);
        assertEquals(RecordState.AVAILABLE, state.state());
        assertEquals(InFlightState.EMPTY_MEMBER_ID, state.memberId());
        assertFalse(state.hasOngoingStateTransition());
    }

    @Test
    public void testFailedStateTransitionWithoutExpiredLockRestoresPreviousState() {
        // Lock not expired: rolling back restores the exact previous state and member.
        InFlightState state = new InFlightState(RecordState.AVAILABLE, 2,
            InFlightState.EMPTY_MEMBER_ID);
        InFlightState updated = state.startStateTransition(
            RecordState.ACQUIRED, DeliveryCountOps.INCREASE, 5, "member-1", true);
        assertEquals(RecordState.ACQUIRED, updated.state());

        state.completeStateTransition(false, true);
        assertEquals(RecordState.AVAILABLE, state.state());
        assertEquals(InFlightState.EMPTY_MEMBER_ID, state.memberId());
        assertFalse(state.hasOngoingStateTransition());
    }
}
