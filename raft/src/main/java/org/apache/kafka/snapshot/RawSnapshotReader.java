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

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.raft.OffsetAndEpoch;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for reading snapshots as a sequence of records.
 */
public interface RawSnapshotReader extends Closeable, Iterable<RecordBatch> {
    /**
     * Returns the end offset and epoch for the snapshot.
     */
    OffsetAndEpoch snapshotId();

    /**
     * Returns the number of bytes for the snapshot.
     *
     * @throws IOException for any IO error while reading the size
     */
    long sizeInBytes() throws IOException;

    /**
     * Reads bytes from position into the given buffer.
     *
     * It is not guarantee that the given buffer will be filled.
     *
     * @param size size to read from snapshot file
     * @param position the starting position in the snapshot to read
     * @return the region read from snapshot
     * @throws IOException for any IO error while reading the snapshot
     */
    UnalignedRecords read(long position, int size) throws IOException;
}
