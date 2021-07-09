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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.raft.OffsetAndEpoch;

public final class MockRawSnapshotReader implements RawSnapshotReader {
    private final OffsetAndEpoch snapshotId;
    private final MemoryRecords data;

    public MockRawSnapshotReader(OffsetAndEpoch snapshotId, ByteBuffer data) {
        this.snapshotId = snapshotId;
        this.data = MemoryRecords.readableRecords(data);
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long sizeInBytes() {
        return data.sizeInBytes();
    }

    @Override
    public UnalignedRecords slice(long position, int size) {
        ByteBuffer buffer = data.buffer();
        buffer.position(Math.toIntExact(position));
        buffer.limit(Math.min(buffer.limit(), Math.toIntExact(position + size)));
        return new UnalignedMemoryRecords(buffer.slice());
    }

    @Override
    public Records records() {
        return data;
    }

    @Override
    public String toString() {
        return String.format("MockRawSnapshotReader(snapshotId=%s, data=%s)", snapshotId, data);
    }
}
