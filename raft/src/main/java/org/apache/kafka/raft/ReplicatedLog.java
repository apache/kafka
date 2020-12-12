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
package org.apache.kafka.raft;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

public interface ReplicatedLog extends Closeable {

    /**
     * Write a set of records to the local leader log. These messages will either
     * be written atomically in a single batch or the call will fail and raise an
     * exception.
     *
     * @return the metadata information of the appended batch
     * @throws IllegalArgumentException if the record set is empty
     */
    LogAppendInfo appendAsLeader(Records records, int epoch);

    /**
     * Append a set of records that were replicated from the leader. The main
     * difference from appendAsLeader is that we do not need to assign the epoch
     * or do additional validation.
     *
     * @return the metadata information of the appended batch
     * @throws IllegalArgumentException if the record set is empty
     */
    LogAppendInfo appendAsFollower(Records records);

    /**
     * Read a set of records within a range of offsets.
     */
    LogFetchInfo read(long startOffsetInclusive, Isolation isolation);

    /**
     * Return the latest epoch. For an empty log, the latest epoch is defined
     * as 0. We refer to this as the "primordial epoch" and it is never allowed
     * to have a leader or any records associated with it (leader epochs always start
     * from 1). Basically this just saves us the trouble of having to use `Option`
     * all over the place.
     */
    int lastFetchedEpoch();

    /**
     * Find the first epoch less than or equal to the given epoch and its end offset,
     * if one exists.
     */
    Optional<OffsetAndEpoch> endOffsetForEpoch(int leaderEpoch);

    /**
     * Get the current log end offset metadata. This is always one plus the offset of the last
     * written record. When the log is empty, the end offset is equal to the start offset.
     */
    LogOffsetMetadata endOffset();

    /**
     * Get the current log start offset. This is the offset of the first written
     * entry, if one exists, or the end offset otherwise.
     */
    long startOffset();

    /**
     * Initialize a new leader epoch beginning at the current log end offset. This API is invoked
     * after becoming a leader and ensures that we can always determine the end offset and epoch
     * with {@link #endOffsetForEpoch(int)} for any previous epoch.
     *
     * @param epoch Epoch of the newly elected leader
     */
    void initializeLeaderEpoch(int epoch);

    /**
     * Truncate the log to the given offset. All records with offsets greater than or equal to
     * the given offset will be removed.
     *
     * @param offset The offset to truncate to
     */
    void truncateTo(long offset);

    /**
     * Update the high watermark and associated metadata (which is used to avoid
     * index lookups when handling reads with {@link #read(long, Isolation)} with
     * the {@link Isolation#COMMITTED} isolation level.
     *
     * @param offsetMetadata The offset and optional metadata
     */
    void updateHighWatermark(LogOffsetMetadata offsetMetadata);

    /**
     * Flush the current log to disk.
     */
    void flush();

    /**
     * Get the last offset which has been flushed to disk.
     */
    long lastFlushedOffset();

    /**
     * Return the topic partition associated with the log.
     */
    TopicPartition topicPartition();

    /**
     * Truncate to an offset and epoch.
     *
     * @param endOffset offset and epoch to truncate to
     * @return the truncation offset or empty if no truncation occurred
     */
    default OptionalLong truncateToEndOffset(OffsetAndEpoch endOffset) {
        final long truncationOffset;
        int leaderEpoch = endOffset.epoch;
        if (leaderEpoch == 0) {
            truncationOffset = endOffset.offset;
        } else {
            Optional<OffsetAndEpoch> localEndOffsetOpt = endOffsetForEpoch(leaderEpoch);
            if (localEndOffsetOpt.isPresent()) {
                OffsetAndEpoch localEndOffset = localEndOffsetOpt.get();
                if (localEndOffset.epoch == leaderEpoch) {
                    truncationOffset = Math.min(localEndOffset.offset, endOffset.offset);
                } else {
                    truncationOffset = Math.min(localEndOffset.offset, endOffset().offset);
                }
            } else {
                // The leader has no epoch which is less than or equal to our own epoch. We simply truncate
                // to the leader offset and begin replication from there.
                truncationOffset = endOffset.offset;
            }
        }

        truncateTo(truncationOffset);
        return OptionalLong.of(truncationOffset);
    }

    /**
     * Create a writable snapshot for the given snapshot id.
     *
     * See {@link RawSnapshotWriter} for details on how to use this object.
     *
     * @param snapshotId the end offset and epoch that identifies the snapshot
     * @return a writable snapshot
     */
    RawSnapshotWriter createSnapshot(OffsetAndEpoch snapshotId) throws IOException;

    /**
     * Opens a readable snapshot for the given snapshot id.
     *
     * Returns an Optional with a readable snapshot, if the snapshot exists, otherwise
     * returns an empty Optional. See {@link RawSnapshotReader} for details on how to
     * use this object.
     *
     * @param snapshotId the end offset and epoch that identifies the snapshot
     * @return an Optional with a readable snapshot, if the snapshot exists, otherwise
     *         returns an empty Optional
     */
    Optional<RawSnapshotReader> readSnapshot(OffsetAndEpoch snapshotId) throws IOException;

    default void close() {}

}
