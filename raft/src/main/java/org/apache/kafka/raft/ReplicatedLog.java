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

import org.apache.kafka.common.record.Records;

import java.util.Optional;
import java.util.OptionalLong;

public interface ReplicatedLog {

    /**
     * Write a set of records to the local leader log. These messages will either
     * be written atomically in a single batch or the call will fail and raise an
     * exception.
     *
     * @return the base offset of the first appended record.
     */
    Long appendAsLeader(Records records, int epoch);

    /**
     * Append a set of records that were replicated from the leader. The main
     * difference from appendAsLeader is that we do not need to assign the epoch
     * or do additional validation.
     */
    void appendAsFollower(Records records);

    /**
     * Read a set of records within a range of offsets.
     */
    Records read(long startOffsetInclusive, OptionalLong endOffsetExclusive);

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
     * Get the current log end offset. This is always one plus the offset of the last
     * written record. When the log is empty, the end offset is equal to the start offset.
     */
    long endOffset();

    /**
     * Get the current log start offset. This is the offset of the first written
     * entry, if one exists, or the end offset otherwise.
     */
    long startOffset();

    /**
     * Assign a start offset to a given epoch.
     */
    void assignEpochStartOffset(int epoch, long startOffset);

    /**
     * Truncate the log to the given offset. Returns true iff targetOffset < logEndOffset.
     */
    boolean truncateTo(long offset);

    void updateHighWatermark(long offset);

    /**
     * Truncate to an offset and epoch.
     *
     * TODO: Do we need the return value here?
     *
     * @param endOffset offset and epoch to truncate to
     * @return true if we truncated to a known point in the requested epoch
     */
    default boolean truncateToEndOffset(OffsetAndEpoch endOffset) {
        int leaderEpoch = endOffset.epoch;
        if (leaderEpoch == 0) {
            truncateTo(endOffset.offset);
            return true;
        } else {
            Optional<OffsetAndEpoch> localEndOffsetOpt = endOffsetForEpoch(leaderEpoch);
            if (localEndOffsetOpt.isPresent()) {
                OffsetAndEpoch localEndOffset = localEndOffsetOpt.get();
                if (localEndOffset.epoch == leaderEpoch) {
                    long truncationOffset = Math.min(localEndOffset.offset, endOffset.offset);
                    truncateTo(truncationOffset);
                    return true;
                } else {
                    long truncationOffset = Math.min(localEndOffset.offset, endOffset());
                    truncateTo(truncationOffset);
                    return false;
                }
            } else {
                // The leader has no epoch which is less than or equal to our own epoch. We simply truncate
                // to the leader offset and begin replication from there.
                truncateTo(endOffset.offset);
                return true;
            }
        }
    }
}
