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

package org.apache.kafka.image.publisher;

import org.apache.kafka.image.MetadataImageTest;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class SnapshotEmitterTest {
    static class MockRaftClient implements RaftClient<ApiMessageAndVersion> {
        TreeMap<OffsetAndEpoch, MockSnapshotWriter> writers = new TreeMap<>();

        @Override
        public void initialize() {
            // nothing to do
        }

        @Override
        public void register(Listener<ApiMessageAndVersion> listener) {
            // nothing to do
        }

        @Override
        public void unregister(Listener<ApiMessageAndVersion> listener) {
            // nothing to do
        }

        @Override
        public OptionalLong highWatermark() {
            return OptionalLong.empty();
        }

        @Override
        public LeaderAndEpoch leaderAndEpoch() {
            return LeaderAndEpoch.UNKNOWN;
        }

        @Override
        public OptionalInt nodeId() {
            return OptionalInt.empty();
        }

        @Override
        public long scheduleAppend(int epoch, List<ApiMessageAndVersion> records) {
            return 0;
        }

        @Override
        public long scheduleAtomicAppend(int epoch, List<ApiMessageAndVersion> records) {
            return 0;
        }

        @Override
        public CompletableFuture<Void> shutdown(int timeoutMs) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void resign(int epoch) {
            // nothing to do
        }

        @Override
        public Optional<SnapshotWriter<ApiMessageAndVersion>> createSnapshot(
            OffsetAndEpoch snapshotId,
            long lastContainedLogTime
        ) {
            if (writers.containsKey(snapshotId)) {
                return Optional.empty();
            }
            MockSnapshotWriter writer = new MockSnapshotWriter(snapshotId);
            writers.put(snapshotId, writer);
            return Optional.of(writer);
        }

        @Override
        public Optional<OffsetAndEpoch> latestSnapshotId() {
            NavigableSet<OffsetAndEpoch> descendingSet = writers.descendingKeySet();
            if (descendingSet.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(descendingSet.first());
            }
        }

        @Override
        public void close() throws Exception {
            // nothing to do
        }
    }

    static class MockSnapshotWriter implements SnapshotWriter<ApiMessageAndVersion> {
        private final OffsetAndEpoch snapshotId;
        private boolean frozen = false;
        private boolean closed = false;
        private final List<List<ApiMessageAndVersion>> batches;

        MockSnapshotWriter(OffsetAndEpoch snapshotId) {
            this.snapshotId = snapshotId;
            this.batches = new ArrayList<>();
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return snapshotId;
        }

        @Override
        public long lastContainedLogOffset() {
            return snapshotId.offset();
        }

        @Override
        public int lastContainedLogEpoch() {
            return snapshotId.epoch();
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        public void append(List<ApiMessageAndVersion> records) {
            batches.add(records);
        }

        List<List<ApiMessageAndVersion>> batches() {
            List<List<ApiMessageAndVersion>> results = new ArrayList<>();
            batches.forEach(batch -> results.add(new ArrayList<>(batch)));
            return results;
        }

        @Override
        public void freeze() {
            frozen = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    @Test
    public void testEmit() throws Exception {
        MockRaftClient mockRaftClient = new MockRaftClient();
        SnapshotEmitter emitter = new SnapshotEmitter.Builder().
            setBatchSize(2).
            setRaftClient(mockRaftClient).
            build();
        emitter.maybeEmit(MetadataImageTest.IMAGE1);
        MockSnapshotWriter writer = mockRaftClient.writers.get(
                MetadataImageTest.IMAGE1.highestOffsetAndEpoch());
        assertNotNull(writer);
        assertEquals(MetadataImageTest.IMAGE1.highestOffsetAndEpoch().offset(),
                writer.lastContainedLogOffset());
        assertEquals(MetadataImageTest.IMAGE1.highestOffsetAndEpoch().epoch(),
                writer.lastContainedLogEpoch());
        assertTrue(writer.isFrozen());
        assertTrue(writer.isClosed());

        // Second call to emit does nothing because we already have a snapshot at that offset and epoch.
        emitter.maybeEmit(MetadataImageTest.IMAGE1);
        assertEquals(1, mockRaftClient.writers.size());
    }
}
