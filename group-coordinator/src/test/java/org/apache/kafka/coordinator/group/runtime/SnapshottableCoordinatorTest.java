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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.group.MockCoordinatorTimer;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.coordinator.group.runtime.CoordinatorRuntimeTest.MockCoordinatorShard;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnapshottableCoordinatorTest {

    @Test
    public void testUpdateLastWrittenOffset() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        SnapshottableCoordinator<MockCoordinatorShard, String> coordinator = new SnapshottableCoordinator<>(
            logContext,
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, new MockCoordinatorTimer<>(new MockTime())),
            new TopicPartition("test-topic", 0)
        );

        assertTrue(coordinator.snapshotRegistry().hasSnapshot(0L));
        coordinator.updateLastWrittenOffset(100L);
        assertEquals(100L, coordinator.lastWrittenOffset());
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(100L));
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(0L));
    }

    @Test
    public void testUpdateLastWrittenOffsetFailed() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        SnapshottableCoordinator<MockCoordinatorShard, String> coordinator = new SnapshottableCoordinator<>(
            logContext,
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, new MockCoordinatorTimer<>(new MockTime())),
            new TopicPartition("test-topic", 0)
        );

        assertEquals(0L, coordinator.lastWrittenOffset());
        assertThrows(IllegalStateException.class, () -> coordinator.updateLastWrittenOffset(0L));
        assertEquals(0L, coordinator.lastWrittenOffset());
    }

    @Test
    public void testRevertWrittenOffset() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        SnapshottableCoordinator<MockCoordinatorShard, String> coordinator = new SnapshottableCoordinator<>(
            logContext,
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, new MockCoordinatorTimer<>(new MockTime())),
            new TopicPartition("test-topic", 0)
        );

        coordinator.updateLastWrittenOffset(100L);
        coordinator.updateLastWrittenOffset(200L);
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(0L));
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(100L));
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(200L));

        coordinator.revertLastWrittenOffset(100L);
        assertEquals(100L, coordinator.lastWrittenOffset());
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(100L));
        assertFalse(coordinator.snapshotRegistry().hasSnapshot(200L));
    }

    @Test
    public void testRevertLastWrittenOffsetFailed() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        SnapshottableCoordinator<MockCoordinatorShard, String> coordinator = new SnapshottableCoordinator<>(
            logContext,
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, new MockCoordinatorTimer<>(new MockTime())),
            new TopicPartition("test-topic", 0)
        );

        assertEquals(0, coordinator.lastWrittenOffset());
        assertThrows(IllegalStateException.class, () -> coordinator.revertLastWrittenOffset(1L));
        assertEquals(0, coordinator.lastWrittenOffset());
    }

    @Test
    public void testUpdateLastCommittedOffset() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        SnapshottableCoordinator<MockCoordinatorShard, String> coordinator = new SnapshottableCoordinator<>(
            logContext,
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, new MockCoordinatorTimer<>(new MockTime())),
            new TopicPartition("test-topic", 0)
        );

        coordinator.updateLastWrittenOffset(100L);
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(0L));
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(100L));

        coordinator.updateLastCommittedOffset(100L);
        assertEquals(100L, coordinator.lastCommittedOffset());
        assertFalse(coordinator.snapshotRegistry().hasSnapshot(0L));
        assertTrue(coordinator.snapshotRegistry().hasSnapshot(100L));
    }

    @Test
    public void testUpdateLastCommittedOffsetFailed() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        SnapshottableCoordinator<MockCoordinatorShard, String> coordinator = new SnapshottableCoordinator<>(
            logContext,
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, new MockCoordinatorTimer<>(new MockTime())),
            new TopicPartition("test-topic", 0)
        );

        coordinator.updateLastWrittenOffset(100L);
        coordinator.updateLastCommittedOffset(100L);
        assertEquals(100L, coordinator.lastCommittedOffset());
        assertThrows(IllegalStateException.class, () -> coordinator.updateLastCommittedOffset(99L));
        assertEquals(100L, coordinator.lastCommittedOffset());
        assertThrows(IllegalStateException.class, () -> coordinator.updateLastCommittedOffset(101L));
    }
}
