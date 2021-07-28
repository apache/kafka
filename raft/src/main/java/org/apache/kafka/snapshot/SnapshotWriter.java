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

import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.common.message.SnapshotFooterRecord;

import java.util.List;

/**
 * Interface of the snapshot writer
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
     * <p>
     * Modification to the snapshot are not allowed once it is frozen.
     */
    boolean isFrozen();

    /**
     * Appends a list of values to the snapshot.
     * <p>
     * The list of record passed are guaranteed to get written together.
     *
     * @param records the list of records to append to the snapshot
     * @throws IllegalStateException if append is called when isFrozen is true
     */
    void append(List<T> records);

    /**
     * Freezes the snapshot by flushing all pending writes and marking it as immutable.
     * <p>
     * Also adds a {@link SnapshotFooterRecord} to the end of the snapshot
     */
    void freeze();

    /**
     * Closes the snapshot writer.
     *
     * If close is called without first calling freeze the snapshot is aborted.
     */
    void close();

    /**
     * Adds a {@link SnapshotHeaderRecord} to snapshot
     *
     * @throws IllegalStateException if the snapshot is not empty
     */
    void initializeSnapshotWithHeader();
}
