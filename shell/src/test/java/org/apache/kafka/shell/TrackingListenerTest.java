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

package org.apache.kafka.shell;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.shell.TrackingBatchReaderTest.MockBatchReader;
import org.apache.kafka.snapshot.SnapshotReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 120000, unit = MILLISECONDS)
public class TrackingListenerTest {
    static class MockRaftClientListener implements RaftClient.Listener<Integer> {
        final AtomicLong lastBaseOffset = new AtomicLong(-1L);
        final AtomicReference<LeaderAndEpoch> leader = new AtomicReference<>(null);
        final AtomicBoolean shuttingDown = new AtomicBoolean(false);

        @Override
        public void handleCommit(BatchReader<Integer> reader) {
            while (reader.hasNext()) {
                Batch<Integer> batch = reader.next();
                lastBaseOffset.set(batch.baseOffset());
            }
        }

        @Override
        public void handleSnapshot(SnapshotReader<Integer> reader) {
            while (reader.hasNext()) {
                Batch<Integer> batch = reader.next();
                lastBaseOffset.set(batch.baseOffset());
            }
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch leader) {
            this.leader.set(leader);
        }

        @Override
        public void beginShutdown() {
            this.shuttingDown.set(true);
        }
    }

    @Test
    public void testCatchUp() {
        CompletableFuture<Void> caughtUpFuture = new CompletableFuture<>();
        MockRaftClientListener mockListener = new MockRaftClientListener();
        AtomicLong highWaterMark = new AtomicLong(1001);
        TrackingListener<Integer> trackingListener = new TrackingListener<>(caughtUpFuture,
            () -> OptionalLong.of(highWaterMark.get()),
            mockListener);
        MockBatchReader mockBatchReader = new MockBatchReader(Arrays.asList(100, 200, 300));
        trackingListener.handleCommit(mockBatchReader);
        assertFalse(mockBatchReader.hasNext());
        assertEquals(300L, mockListener.lastBaseOffset.get());
        assertFalse(caughtUpFuture.isDone());
        trackingListener.handleCommit(new MockBatchReader(Arrays.asList(800, 900, 1000)));
        assertEquals(1000L, mockListener.lastBaseOffset.get());
        assertTrue(caughtUpFuture.isDone());
    }

    @Test
    public void testShuttingDownPropagated() {
        MockRaftClientListener mockListener = new MockRaftClientListener();
        assertFalse(mockListener.shuttingDown.get());
        TrackingListener<Integer> trackingListener = new TrackingListener<>(new CompletableFuture<>(),
                () -> OptionalLong.empty(),
                mockListener);
        assertFalse(mockListener.shuttingDown.get());
        trackingListener.handleLeaderChange(new LeaderAndEpoch(OptionalInt.of(123), 456));
        assertEquals(new LeaderAndEpoch(OptionalInt.of(123), 456), mockListener.leader.get());
        trackingListener.beginShutdown();
        assertTrue(mockListener.shuttingDown.get());
    }
}

