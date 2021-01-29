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
     * Validate the given offset and epoch against the log and oldest snapshot.
     *
     * Returns the largest valid offset and epoch given `offset` and `epoch` as the upper bound.
     * This can result in three possible values returned:
     *
     * 1. ValidOffsetAndEpoch.valid if the given offset and epoch is valid in the log.
     *
     * 2. ValidOffsetAndEpoch.diverging if the given offset and epoch is not valid; and the
     * largest valid offset and epoch is in the log.
     *
     * 3. ValidOffsetAndEpoch.snapshot if the given offset and epoch is not valid; and the largest
     * valid offset and epoch is less than the oldest snapshot.
     *
     * @param offset the offset to validate
     * @param epoch the epoch of the record at offset - 1
     * @return the largest valid offset and epoch
     */
    default ValidOffsetAndEpoch validateOffsetAndEpoch(long offset, int epoch) {
        if (startOffset() == 0 && offset == 0) {
            return ValidOffsetAndEpoch.valid(new OffsetAndEpoch(0, 0));
        } else if (
                oldestSnapshotId().isPresent() &&
                ((offset < startOffset()) ||
                 (offset == startOffset() && epoch != oldestSnapshotId().get().epoch) ||
                 (epoch < oldestSnapshotId().get().epoch))
        ) {
            /* Send a snapshot if the leader has a snapshot at the log start offset and
             * 1. the fetch offset is less than the log start offset or
             * 2. the fetch offset is equal to the log start offset and last fetch epoch doesn't match
             *    the oldest snapshot or
             * 3. last fetch epoch is less than the oldest snapshot's epoch
             */

            OffsetAndEpoch latestSnapshotId = latestSnapshotId().orElseThrow(() -> {
                return new IllegalStateException(
                    String.format(
                        "Log start offset (%s) is greater than zero but latest snapshot was not found",
                        startOffset()
                    )
                );
            });

            return ValidOffsetAndEpoch.snapshot(latestSnapshotId);
        } else {
            OffsetAndEpoch endOffsetAndEpoch = endOffsetForEpoch(epoch);

            if (endOffsetAndEpoch.epoch != epoch || endOffsetAndEpoch.offset < offset) {
                return ValidOffsetAndEpoch.diverging(endOffsetAndEpoch);
            } else {
                return ValidOffsetAndEpoch.valid(new OffsetAndEpoch(offset, epoch));
            }
        }
    }

    /**
     * Find the first epoch less than or equal to the given epoch and its end offset.
     */
    OffsetAndEpoch endOffsetForEpoch(int epoch);

    /**
     * Get the current log end offset metadata. This is always one plus the offset of the last
     * written record. When the log is empty, the end offset is equal to the start offset.
     */
    LogOffsetMetadata endOffset();

    /**
     * Get the high watermark.
     */
    LogOffsetMetadata highWatermark();

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
     * Fully truncate the log if the latest snapshot is later than the log end offset.
     *
     * In general this operation empties the log and sets the log start offset, high watermark and
     * log end offset to the latest snapshot's end offset.
     *
     * @return true when the log is fully truncated, otherwise returns false
     */
    boolean truncateToLatestSnapshot();

    /**
     * Update the high watermark and associated metadata (which is used to avoid
     * index lookups when handling reads with {@link #read(long, Isolation)} with
     * the {@link Isolation#COMMITTED} isolation level.
     *
     * @param offsetMetadata The offset and optional metadata
     */
    void updateHighWatermark(LogOffsetMetadata offsetMetadata);

    /**
     * Updates the log start offset and delete segments if necessary.
     *
     * The replicated log's start offset can be increased and older segments can be deleted when
     * there is a snapshot greater than the current log start offset.
     */
    boolean deleteBeforeSnapshot(OffsetAndEpoch logStartSnapshotId);

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
     * @return the truncation offset
     */
    default long truncateToEndOffset(OffsetAndEpoch endOffset) {
        final long truncationOffset;
        int leaderEpoch = endOffset.epoch;
        if (leaderEpoch == 0) {
            truncationOffset = Math.min(endOffset.offset, endOffset().offset);
        } else {
            OffsetAndEpoch localEndOffset = endOffsetForEpoch(leaderEpoch);
            if (localEndOffset.epoch == leaderEpoch) {
                truncationOffset = Math.min(localEndOffset.offset, endOffset.offset);
            } else {
                truncationOffset = localEndOffset.offset;
            }
        }

        truncateTo(truncationOffset);
        return truncationOffset;
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

    /**
     * Returns the latest snapshot id if one exists.
     *
     * @return an Optional snapshot id of the latest snashot if one exists, otherwise returns an
     * empty Optional
     */
    Optional<OffsetAndEpoch> latestSnapshotId();

    /**
     * Returns the snapshot id at the log start offset.
     *
     * If the log start offset is nonzero then it is expected that there is a snapshot with an end
     * offset equal to the start offset.
     *
     * @return an Optional snapshot id at the log start offset if nonzero, otherwise returns an empty
     *         Optional
     */
    Optional<OffsetAndEpoch> oldestSnapshotId();

    /**
     * Notifies the replicted log when a new snapshot is available.
     */
    void onSnapshotFrozen(OffsetAndEpoch snapshotId);

    default void close() {}
}
