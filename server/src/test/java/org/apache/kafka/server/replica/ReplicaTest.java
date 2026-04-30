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
package org.apache.kafka.server.replica;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicaTest {

    private static final int BROKER_ID = 0;
    private static final TopicPartition PARTITION = new TopicPartition("foo", 0);
    private static final long REPLICA_LAG_TIME_MAX_MS = 30000L;

    private MockTime time;
    private Replica replica;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        MetadataCache metadataCache = mock(MetadataCache.class);
        when(metadataCache.getAliveBrokerEpoch(BROKER_ID)).thenReturn(Optional.of(1L));
        replica = new Replica(BROKER_ID, PARTITION, metadataCache);
    }

    private void assertReplicaState(
        long logStartOffset,
        long logEndOffset,
        long lastCaughtUpTimeMs,
        long lastFetchLeaderLogEndOffset,
        long lastFetchTimeMs,
        Optional<Long> brokerEpoch
    ) {
        ReplicaState replicaState = replica.stateSnapshot();
        assertEquals(logStartOffset, replicaState.logStartOffset(),
            "Unexpected Log Start Offset");
        assertEquals(logEndOffset, replicaState.logEndOffset(),
            "Unexpected Log End Offset");
        assertEquals(lastCaughtUpTimeMs, replicaState.lastCaughtUpTimeMs(),
            "Unexpected Last Caught Up Time");
        assertEquals(lastFetchLeaderLogEndOffset, replicaState.lastFetchLeaderLogEndOffset(),
            "Unexpected Last Fetch Leader Log End Offset");
        assertEquals(lastFetchTimeMs, replicaState.lastFetchTimeMs(),
            "Unexpected Last Fetch Time");
        assertEquals(brokerEpoch, replicaState.brokerEpoch(),
            "Broker Epoch Mismatch");
    }

    private void assertReplicaState(
        long logStartOffset,
        long logEndOffset,
        long lastCaughtUpTimeMs,
        long lastFetchLeaderLogEndOffset,
        long lastFetchTimeMs
    ) {
        assertReplicaState(logStartOffset, logEndOffset, lastCaughtUpTimeMs, lastFetchLeaderLogEndOffset,
            lastFetchTimeMs, Optional.of(1L));
    }

    private long updateFetchState(
        long followerFetchOffset,
        long followerStartOffset,
        long leaderEndOffset
    ) {
        long currentTimeMs = time.milliseconds();
        replica.updateFetchStateOrThrow(
            new LogOffsetMetadata(followerFetchOffset),
            followerStartOffset,
            currentTimeMs,
            leaderEndOffset,
            1L
        );
        return currentTimeMs;
    }

    private long resetReplicaState(
        long leaderEndOffset,
        boolean isNewLeader,
        boolean isFollowerInSync
    ) {
        long currentTimeMs = time.milliseconds();
        replica.resetReplicaState(
            currentTimeMs,
            leaderEndOffset,
            isNewLeader,
            isFollowerInSync
        );
        return currentTimeMs;
    }

    private boolean isCaughtUp(long leaderEndOffset) {
        return replica.stateSnapshot().isCaughtUp(
            leaderEndOffset,
            time.milliseconds(),
            REPLICA_LAG_TIME_MAX_MS
        );
    }

    @Test
    public void testInitialState() {
        assertReplicaState(
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            0L,
            0L,
            0L,
            Optional.empty()
        );
    }

    @Test
    public void testUpdateFetchState() {
        long fetchTimeMs1 = updateFetchState(
            5L,
            1L,
            10L
        );

        assertReplicaState(
            1L,
            5L,
            0L,
            10L,
            fetchTimeMs1
        );

        long fetchTimeMs2 = updateFetchState(
            10L,
            2L,
            15L
        );

        assertReplicaState(
            2L,
            10L,
            fetchTimeMs1,
            15L,
            fetchTimeMs2
        );

        long fetchTimeMs3 = updateFetchState(
            15L,
            3L,
            15L
        );

        assertReplicaState(
            3L,
            15L,
            fetchTimeMs3,
            15L,
            fetchTimeMs3
        );
    }

    @Test
    public void testResetReplicaStateWhenLeaderIsReelectedAndReplicaIsInSync() {
        updateFetchState(
            10L,
            1L,
            10L
        );

        long resetTimeMs1 = resetReplicaState(
            11L,
            false,
            true
        );

        assertReplicaState(
            1L,
            10L,
            resetTimeMs1,
            11L,
            resetTimeMs1
        );
    }

    @Test
    public void testResetReplicaStateWhenLeaderIsReelectedAndReplicaIsNotInSync() {
        updateFetchState(
            10L,
            1L,
            10L
        );

        resetReplicaState(
            11L,
            false,
            false
        );

        assertReplicaState(
            1L,
            10L,
            0L,
            11L,
            0L
        );
    }

    @Test
    public void testResetReplicaStateWhenNewLeaderIsElectedAndReplicaIsInSync() {
        updateFetchState(
            10L,
            1L,
            10L
        );

        long resetTimeMs1 = resetReplicaState(
            11L,
            true,
            true
        );

        assertReplicaState(
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            resetTimeMs1,
            UnifiedLog.UNKNOWN_OFFSET,
            0L,
            Optional.empty()
        );
    }

    @Test
    public void testResetReplicaStateWhenNewLeaderIsElectedAndReplicaIsNotInSync() {
        updateFetchState(
            10L,
            1L,
            10L
        );

        resetReplicaState(
            11L,
            true,
            false
        );

        assertReplicaState(
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            0L,
            UnifiedLog.UNKNOWN_OFFSET,
            0L,
            Optional.empty()
        );
    }

    @Test
    public void testIsCaughtUpWhenReplicaIsCaughtUpToLogEnd() {
        assertFalse(isCaughtUp(10L));

        updateFetchState(
            10L,
            1L,
            10L
        );

        assertTrue(isCaughtUp(10L));

        time.sleep(REPLICA_LAG_TIME_MAX_MS + 1);

        assertTrue(isCaughtUp(10L));
    }

    @Test
    public void testIsCaughtUpWhenReplicaIsNotCaughtUpToLogEnd() {
        assertFalse(isCaughtUp(10L));

        updateFetchState(
            5L,
            1L,
            10L
        );

        assertFalse(isCaughtUp(10L));

        updateFetchState(
            10L,
            1L,
            15L
        );

        assertTrue(isCaughtUp(16L));

        time.sleep(REPLICA_LAG_TIME_MAX_MS + 1);

        assertFalse(isCaughtUp(16L));
    }

    @Test
    public void testFenceStaleUpdates() {
        MetadataCache metadataCache = mock(MetadataCache.class);
        when(metadataCache.getAliveBrokerEpoch(BROKER_ID)).thenReturn(Optional.of(2L));

        Replica replica = new Replica(BROKER_ID, PARTITION, metadataCache);
        replica.updateFetchStateOrThrow(
            new LogOffsetMetadata(5L),
            1L,
            1,
            10L,
            2L
        );

        assertThrows(NotLeaderOrFollowerException.class, () -> replica.updateFetchStateOrThrow(
            new LogOffsetMetadata(5L),
            2L,
            3,
            10L,
            1L
        ));

        replica.updateFetchStateOrThrow(
            new LogOffsetMetadata(5L),
            2L,
            4,
            10L,
            -1L
        );
    }
}