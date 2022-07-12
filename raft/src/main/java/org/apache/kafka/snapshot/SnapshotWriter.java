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

import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.common.message.SnapshotFooterRecord;

import java.util.List;

/**
 * A type for writing a snapshot for a given end offset and epoch.
 *
 * A snapshot writer can be used to append objects until freeze is called. When freeze is
 * called the snapshot is validated and marked as immutable. After freeze is called any
 * append will fail with an exception.
 *
 * It is assumed that the content of the snapshot represents all of the records for the
 * topic partition from offset 0 up to but not including the end offset in the snapshot
 * id.
 *
 * @see org.apache.kafka.raft.KafkaRaftClient#createSnapshot(long, int, long)
 */
public interface SnapshotWriter<T> extends AutoCloseable {
    /**
     * Returns the end offset and epoch for the snapshot.
     */
    OffsetAndEpoch snapshotId();

    /**
     * Returns the last log offset which is represented in the snapshot.
     */
    long lastContainedLogOffset();

    /**
     * Returns the epoch of the last log offset which is represented in the snapshot.
     */
    int lastContainedLogEpoch();

    /**
     * Returns true if the snapshot has been frozen, otherwise false is returned.
     *
     * Modification to the snapshot are not allowed once it is frozen.
     */
    boolean isFrozen();

    /**
     * Appends a list of values to the snapshot.
     *
     * The list of record passed are guaranteed to get written together.
     *
     * @param records the list of records to append to the snapshot
     * @throws IllegalStateException if append is called when isFrozen is true
     */
    void append(List<T> records);

    /**
     * Freezes the snapshot by flushing all pending writes and marking it as immutable.
     *
     * Also adds a {@link SnapshotFooterRecord} to the end of the snapshot
     */
    void freeze();

    /**
     * Closes the snapshot writer.
     *
     * If close is called without first calling freeze the snapshot is aborted.
     */
    void close();

}
