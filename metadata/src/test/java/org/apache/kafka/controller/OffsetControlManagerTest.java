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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.AbortTransactionRecord;
import org.apache.kafka.common.metadata.BeginTransactionRecord;
import org.apache.kafka.common.metadata.EndTransactionRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.TrackingSnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class OffsetControlManagerTest {
    @Test
    public void testInitialValues() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        assertNull(offsetControl.currentSnapshotId());
        assertNull(offsetControl.currentSnapshotName());
        assertEquals(-1L, offsetControl.lastCommittedOffset());
        assertEquals(-1, offsetControl.lastCommittedEpoch());
        assertEquals(-1L, offsetControl.lastStableOffset());
        assertEquals(-1L, offsetControl.transactionStartOffset());
        assertEquals(-1L, offsetControl.nextWriteOffset());
        assertFalse(offsetControl.active());
        assertEquals(Arrays.asList(-1L), offsetControl.snapshotRegistry().epochsList());
    }

    @Test
    public void testActivate() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        offsetControl.activate(1000L);
        assertEquals(1000L, offsetControl.nextWriteOffset());
        assertTrue(offsetControl.active());
        assertTrue(offsetControl.metrics().active());
        assertEquals(Arrays.asList(-1L), offsetControl.snapshotRegistry().epochsList());
    }

    @Test
    public void testActivateFailsIfAlreadyActive() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        offsetControl.activate(1000L);
        assertEquals("Can't activate already active OffsetControlManager.",
            assertThrows(RuntimeException.class,
                () -> offsetControl.activate(2000L)).
                    getMessage());
    }

    @Test
    public void testActivateFailsIfNewNextWriteOffsetIsNegative() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        assertEquals("Invalid negative newNextWriteOffset -2.",
            assertThrows(RuntimeException.class,
                () -> offsetControl.activate(-2)).
                    getMessage());
    }

    @Test
    public void testActivateAndDeactivate() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        offsetControl.activate(1000L);
        assertEquals(1000L, offsetControl.nextWriteOffset());
        offsetControl.deactivate();
        assertEquals(-1L, offsetControl.nextWriteOffset());
    }

    @Test
    public void testDeactivateFailsIfNotActive() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        assertEquals("Can't deactivate inactive OffsetControlManager.",
            assertThrows(RuntimeException.class,
                () -> offsetControl.deactivate()).
                    getMessage());
    }

    private static Batch<ApiMessageAndVersion> newFakeBatch(
        long lastOffset,
        int epoch,
        long appendTimestamp
    ) {
        return Batch.data(
            lastOffset,
            epoch,
            appendTimestamp,
            100,
            Collections.singletonList(new ApiMessageAndVersion(new NoOpRecord(), (short) 0)));
    }

    @Test
    public void testHandleCommitBatch() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();

        offsetControl.handleCommitBatch(newFakeBatch(1000L, 200, 3000L));
        assertEquals(Arrays.asList(1000L), offsetControl.snapshotRegistry().epochsList());
        assertEquals(1000L, offsetControl.lastCommittedOffset());
        assertEquals(200, offsetControl.lastCommittedEpoch());
        assertEquals(1000L, offsetControl.lastStableOffset());
        assertEquals(-1L, offsetControl.transactionStartOffset());
        assertEquals(-1L, offsetControl.nextWriteOffset());
        assertFalse(offsetControl.active());
        assertFalse(offsetControl.metrics().active());
        assertEquals(1000L, offsetControl.metrics().lastAppliedRecordOffset());
        assertEquals(1000L, offsetControl.metrics().lastCommittedRecordOffset());
        assertEquals(3000L, offsetControl.metrics().lastAppliedRecordTimestamp());
    }

    @Test
    public void testHandleScheduleAtomicAppend() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();

        offsetControl.handleScheduleAtomicAppend(2000L);
        assertEquals(2001L, offsetControl.nextWriteOffset());
        assertEquals(2000L, offsetControl.metrics().lastAppliedRecordOffset());
        assertEquals(-1L, offsetControl.lastStableOffset());
        assertEquals(-1L, offsetControl.lastCommittedOffset());
        assertEquals(Arrays.asList(-1L, 2000L), offsetControl.snapshotRegistry().epochsList());

        offsetControl.handleCommitBatch(newFakeBatch(2000L, 200, 3000L));
        assertEquals(2000L, offsetControl.lastStableOffset());
        assertEquals(2000L, offsetControl.lastCommittedOffset());
        assertEquals(Arrays.asList(2000L), offsetControl.snapshotRegistry().epochsList());
    }

    @Test
    public void testHandleLoadSnapshot() {
        TrackingSnapshotRegistry snapshotRegistry = new TrackingSnapshotRegistry(new LogContext());
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                build();

        offsetControl.beginLoadSnapshot(new OffsetAndEpoch(4000L, 300));
        assertEquals(Arrays.asList("snapshot[-1]", "reset"), snapshotRegistry.operations());
        assertEquals(new OffsetAndEpoch(4000L, 300), offsetControl.currentSnapshotId());
        assertEquals("00000000000000004000-0000000300", offsetControl.currentSnapshotName());
        assertEquals(Arrays.asList(), offsetControl.snapshotRegistry().epochsList());

        offsetControl.endLoadSnapshot(3456L);
        assertEquals(Arrays.asList("snapshot[-1]", "reset", "snapshot[4000]"),
            snapshotRegistry.operations());
        assertNull(offsetControl.currentSnapshotId());
        assertNull(offsetControl.currentSnapshotName());
        assertEquals(Arrays.asList(4000L), offsetControl.snapshotRegistry().epochsList());
        assertEquals(4000L, offsetControl.lastCommittedOffset());
        assertEquals(300, offsetControl.lastCommittedEpoch());
        assertEquals(4000L, offsetControl.lastStableOffset());
        assertEquals(-1L, offsetControl.transactionStartOffset());
        assertEquals(-1L, offsetControl.nextWriteOffset());
        assertEquals(4000L, offsetControl.metrics().lastCommittedRecordOffset());
        assertEquals(4000L, offsetControl.metrics().lastAppliedRecordOffset());
        assertEquals(3456L, offsetControl.metrics().lastAppliedRecordTimestamp());
    }

    @Test
    public void testBeginTransactionRecordNotAllowedInSnapshot() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        offsetControl.beginLoadSnapshot(new OffsetAndEpoch(4000L, 300));
        assertEquals("BeginTransactionRecord cannot appear within a snapshot.",
            assertThrows(RuntimeException.class,
                () -> offsetControl.replay(new BeginTransactionRecord(), 1000L)).
                    getMessage());
    }

    @Test
    public void testEndTransactionRecordNotAllowedInSnapshot() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        offsetControl.beginLoadSnapshot(new OffsetAndEpoch(4000L, 300));
        assertEquals("EndTransactionRecord cannot appear within a snapshot.",
            assertThrows(RuntimeException.class,
                () -> offsetControl.replay(new EndTransactionRecord(), 1000L)).
                    getMessage());
    }

    @Test
    public void testAbortTransactionRecordNotAllowedInSnapshot() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        offsetControl.beginLoadSnapshot(new OffsetAndEpoch(4000L, 300));
        assertEquals("AbortTransactionRecord cannot appear within a snapshot.",
            assertThrows(RuntimeException.class,
                () -> offsetControl.replay(new AbortTransactionRecord(), 1000L)).
                    getMessage());
    }

    @Test
    public void testEndLoadSnapshotFailsWhenNotInSnapshot() {
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().build();
        assertEquals("Can't end loading snapshot, because there is no current snapshot.",
            assertThrows(RuntimeException.class,
                () -> offsetControl.endLoadSnapshot(1000L)).
                    getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReplayTransaction(boolean aborted) {
        TrackingSnapshotRegistry snapshotRegistry = new TrackingSnapshotRegistry(new LogContext());
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            build();

        offsetControl.replay(new BeginTransactionRecord(), 1500L);
        assertEquals(1500L, offsetControl.transactionStartOffset());
        assertEquals(Arrays.asList(-1L, 1499L), offsetControl.snapshotRegistry().epochsList());

        offsetControl.handleCommitBatch(newFakeBatch(1550L, 100, 2000L));
        assertEquals(1550L, offsetControl.lastCommittedOffset());
        assertEquals(100, offsetControl.lastCommittedEpoch());
        assertEquals(1499L, offsetControl.lastStableOffset());
        assertEquals(Arrays.asList(1499L), offsetControl.snapshotRegistry().epochsList());

        if (aborted) {
            offsetControl.replay(new AbortTransactionRecord(), 1600L);
            assertEquals(Arrays.asList("snapshot[-1]", "snapshot[1499]", "revert[1499]"),
                snapshotRegistry.operations());
        } else {
            offsetControl.replay(new EndTransactionRecord(), 1600L);
            assertEquals(Arrays.asList("snapshot[-1]", "snapshot[1499]"),
                snapshotRegistry.operations());
        }
        assertEquals(-1L, offsetControl.transactionStartOffset());
        assertEquals(1499L, offsetControl.lastStableOffset());

        offsetControl.handleCommitBatch(newFakeBatch(1650, 100, 2100L));
        assertEquals(1650, offsetControl.lastStableOffset());
        assertEquals(Arrays.asList(1650L), offsetControl.snapshotRegistry().epochsList());
    }

    @Test
    public void testLoadSnapshotClearsTransactionalState() {
        TrackingSnapshotRegistry snapshotRegistry = new TrackingSnapshotRegistry(new LogContext());
        OffsetControlManager offsetControl = new OffsetControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            build();
        offsetControl.replay(new BeginTransactionRecord(), 1500L);
        offsetControl.beginLoadSnapshot(new OffsetAndEpoch(4000L, 300));
        assertEquals(-1L, offsetControl.transactionStartOffset());
        assertEquals(Arrays.asList("snapshot[-1]", "snapshot[1499]", "reset"),
                snapshotRegistry.operations());
    }
}
