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

import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.raft.OffsetAndEpoch;

/**
 * Interface for reading snapshots as a sequence of records.
 */
public interface RawSnapshotReader {
    /**
     * Returns the end offset and epoch for the snapshot.
     */
    OffsetAndEpoch snapshotId();

    /**
     * Returns the number of bytes for the snapshot.
     */
    long sizeInBytes();

    /**
     * Creates a slize of unaligned records from the position up to a size.
     *
     * @param position the starting position of the slice in the snapshot
     * @param size the maximum size of the slice
     * @return an unaligned slice of records in the snapshot
     */
    UnalignedRecords slice(long position, int size);

    /**
     * Returns all of the records backing this snapshot reader.
     *
     * @return all of the records for this snapshot
     */
    Records records();
}
