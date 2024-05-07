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
package org.apache.kafka.snapshot;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NotifyingRawSnapshotWriterTest {
    @Test
    void testFreezeClose() {
        NoopRawSnapshotWriter rawWriter = new NoopRawSnapshotWriter();

        AtomicBoolean called = new AtomicBoolean(false);
        Consumer<OffsetAndEpoch> consumer = offset -> {
            assertEquals(offset, rawWriter.snapshotId());
            called.set(true);
        };

        try (RawSnapshotWriter writer = new NotifyingRawSnapshotWriter(rawWriter, consumer)) {
            writer.freeze();
        }

        assertTrue(called.get());
        assertTrue(rawWriter.isFrozen());
        assertTrue(rawWriter.closed);
    }

    @Test
    void testFailingFreeze() {
        NoopRawSnapshotWriter rawWriter = new NoopRawSnapshotWriter() {
            @Override
            public void freeze() {
                throw new IllegalStateException();
            }
        };

        AtomicBoolean called = new AtomicBoolean(false);
        Consumer<OffsetAndEpoch> consumer = ignored -> called.set(true);

        try (RawSnapshotWriter writer = new NotifyingRawSnapshotWriter(rawWriter, consumer)) {
            assertThrows(IllegalStateException.class, writer::freeze);
        }

        assertFalse(called.get());
        assertFalse(rawWriter.isFrozen());
        assertTrue(rawWriter.closed);
    }

    @Test
    void testCloseWithoutFreeze() {
        NoopRawSnapshotWriter rawWriter = new NoopRawSnapshotWriter();

        AtomicBoolean called = new AtomicBoolean(false);
        Consumer<OffsetAndEpoch> consumer = offset -> called.set(true);

        try (RawSnapshotWriter writer = new NotifyingRawSnapshotWriter(rawWriter, consumer)) {
        }

        assertFalse(called.get());
        assertFalse(rawWriter.isFrozen());
        assertTrue(rawWriter.closed);
    }

    class NoopRawSnapshotWriter implements RawSnapshotWriter {
        boolean frozen = false;
        boolean closed = false;

        @Override
        public OffsetAndEpoch snapshotId() {
            return new OffsetAndEpoch(100, 10);
        }

        @Override
        public long sizeInBytes() {
            return 255;
        }

        @Override
        public void append(UnalignedMemoryRecords records) {
        }

        @Override
        public void append(MemoryRecords records) {
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        public void freeze() {
            frozen = true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
