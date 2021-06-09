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

package org.apache.kafka.metadata;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.MockRawSnapshotReader;
import org.apache.kafka.snapshot.MockRawSnapshotWriter;
import org.apache.kafka.snapshot.RaftSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.ByteBuffer;


public class MockSnapshotWriter implements SnapshotWriter<ApiMessageAndVersion> {
    private final long endOffset;
    private boolean ready = true;
    private boolean frozen = false;
    private boolean closed = false;
    private final List<List<ApiMessageAndVersion>> batches = new ArrayList<>();

    public MockSnapshotWriter(long endOffset) {
        this.endOffset = endOffset;
    }

    @Override
    public long endOffset() {
        return endOffset;
    }

    @Override
    public synchronized boolean append(List<ApiMessageAndVersion> batch) {
        if (frozen) throw new RuntimeException("writer has been frozen");
        if (closed) throw new RuntimeException("writer is closed");
        if (!ready) return false;
        batches.add(batch);
        return true;
    }

    public synchronized void setReady(boolean ready) {
        this.ready = ready;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    @Override
    public synchronized void freeze() throws IOException {
        if (closed) throw new RuntimeException("writer is closed");
        this.notifyAll();
        this.frozen = true;
    }

    public synchronized void waitForFreeze() throws InterruptedException {
        while (!frozen) {
            this.wait();
        }
    }

    public synchronized boolean frozen() {
        return frozen;
    }

    public synchronized List<List<ApiMessageAndVersion>> batches() {
        return batches;
    }

    public MockRawSnapshotReader toReader() {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(endOffset, 0);
        AtomicReference<ByteBuffer> buffer = new AtomicReference<>();
        int maxBufferSize = 1024;
        try (RaftSnapshotWriter<ApiMessageAndVersion> snapshotWriter =
                new RaftSnapshotWriter<>(
                    new MockRawSnapshotWriter(snapshotId, buffer::set),
                    maxBufferSize,
                    MemoryPool.NONE,
                    new MockTime(),
                    CompressionType.NONE,
                    new MetadataRecordSerde()
                )
        ) {
            batches.forEach(snapshotWriter::append);
            snapshotWriter.freeze();
        }

        return new MockRawSnapshotReader(snapshotId, buffer.get());
    }
}
