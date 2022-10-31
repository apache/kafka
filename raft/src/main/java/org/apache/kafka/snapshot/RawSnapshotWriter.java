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

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.raft.OffsetAndEpoch;

/**
 * Interface for writing snapshot as a sequence of records.
 */
public interface RawSnapshotWriter extends AutoCloseable {
    /**
     * Returns the end offset and epoch for the snapshot.
     */
    OffsetAndEpoch snapshotId();

    /**
     * Returns the number of bytes for the snapshot.
     */
    long sizeInBytes();

    /**
     * Fully appends the memory record set to the snapshot.
     *
     * If the method returns without an exception the given record set was fully writing the
     * snapshot.
     *
     * @param records the region to append
     */
    void append(MemoryRecords records);

    /**
     * Fully appends the memory record set to the snapshot, the difference with {@link RawSnapshotWriter#append(MemoryRecords)}
     * is that the record set are fetched from leader by FetchSnapshotRequest, so the records are unaligned.
     *
     * If the method returns without an exception the given records was fully writing the
     * snapshot.
     *
     * @param records the region to append
     */
    void append(UnalignedMemoryRecords records);

    /**
     * Returns true if the snapshot has been frozen, otherwise false is returned.
     *
     * Modification to the snapshot are not allowed once it is frozen.
     */
    boolean isFrozen();

    /**
     * Freezes the snapshot and marking it as immutable.
     */
    void freeze();

    /**
     * Closes the snapshot writer.
     *
     * If close is called without first calling freeze the snapshot is aborted.
     */
    void close();
}
