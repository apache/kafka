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

import java.util.function.Consumer;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.raft.OffsetAndEpoch;

/**
 * A thin facade for a {@code RawSnapshotWriter} that notifies a callback when freeze completes successfully.
 */
public final class NotifyingRawSnapshotWriter implements RawSnapshotWriter {
    private final RawSnapshotWriter writer;
    private final Consumer<OffsetAndEpoch> callback;

    /**
     * Constructs a {@code RawSnapshotWriter}.
     *
     * @param writer the raw snapshot writer
     * @param callback the consumer to call when freeze succeeds
     */
    public NotifyingRawSnapshotWriter(RawSnapshotWriter writer, Consumer<OffsetAndEpoch> callback) {
        this.writer = writer;
        this.callback = callback;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return writer.snapshotId();
    }

    @Override
    public long sizeInBytes() {
        return writer.sizeInBytes();
    }

    @Override
    public void append(UnalignedMemoryRecords records) {
        writer.append(records);
    }

    @Override
    public void append(MemoryRecords records) {
        writer.append(records);
    }

    @Override
    public boolean isFrozen() {
        return writer.isFrozen();
    }

    /**
     * Delegates the call to the internal writer and invokes the callback on success.
     */
    @Override
    public void freeze() {
        writer.freeze();
        // Only notify the callback on success
        callback.accept(writer.snapshotId());
    }

    @Override
    public void close() {
        writer.close();
    }

    @Override
    public String toString() {
        return writer.toString();
    }
}
