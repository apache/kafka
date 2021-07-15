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

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.raft.OffsetAndEpoch;

public final class MockRawSnapshotWriter implements RawSnapshotWriter {
    private final ByteBufferOutputStream data = new ByteBufferOutputStream(0);
    private final OffsetAndEpoch snapshotId;
    private final Consumer<ByteBuffer> frozenHandler;

    private boolean frozen = false;
    private boolean closed = false;

    public MockRawSnapshotWriter(
        OffsetAndEpoch snapshotId,
        Consumer<ByteBuffer> frozenHandler
    ) {
        this.snapshotId = snapshotId;
        this.frozenHandler = frozenHandler;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long sizeInBytes() {
        ensureNotFrozenOrClosed();
        return data.position();
    }

    @Override
    public void append(UnalignedMemoryRecords records) {
        ensureNotFrozenOrClosed();
        data.write(records.buffer());
    }

    @Override
    public void append(MemoryRecords records) {
        ensureNotFrozenOrClosed();
        data.write(records.buffer());
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public void freeze() {
        ensureNotFrozenOrClosed();

        frozen = true;
        ByteBuffer buffer = data.buffer();
        buffer.flip();

        frozenHandler.accept(buffer);
    }

    @Override
    public void close() {
        ensureOpen();
        closed = true;
    }

    @Override
    public String toString() {
        return String.format("MockRawSnapshotWriter(snapshotId=%s, data=%s)", snapshotId, data.buffer());
    }

    private void ensureNotFrozenOrClosed() {
        if (frozen) {
            throw new IllegalStateException("Snapshot is already frozen " + snapshotId);
        }
        ensureOpen();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Snapshot is already closed " + snapshotId);
        }
    }
}
