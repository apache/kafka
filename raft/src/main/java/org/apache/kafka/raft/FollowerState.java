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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

public class FollowerState implements EpochState {
    private final int fetchTimeoutMs;
    private final int epoch;
    private final int leaderId;
    private final Set<Integer> voters;
    // Used for tracking the expiration of both the Fetch and FetchSnapshot requests
    private final Timer fetchTimer;
    private Optional<LogOffsetMetadata> highWatermark;
    /* Used to track the currently fetching snapshot. When fetching snapshot regular
     * Fetch request are paused
     */
    private Optional<RawSnapshotWriter> fetchingSnapshot;

    private final Logger log;

    public FollowerState(
        Time time,
        int epoch,
        int leaderId,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        int fetchTimeoutMs,
        LogContext logContext
    ) {
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.voters = voters;
        this.fetchTimer = time.timer(fetchTimeoutMs);
        this.highWatermark = highWatermark;
        this.fetchingSnapshot = Optional.empty();
        this.log = logContext.logger(FollowerState.class);
    }

    @Override
    public ElectionState election() {
        return new ElectionState(
            epoch,
            OptionalInt.of(leaderId),
            OptionalInt.empty(),
            voters
        );
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public String name() {
        return "Follower";
    }

    public long remainingFetchTimeMs(long currentTimeMs) {
        fetchTimer.update(currentTimeMs);
        return fetchTimer.remainingMs();
    }

    public int leaderId() {
        return leaderId;
    }

    public boolean hasFetchTimeoutExpired(long currentTimeMs) {
        fetchTimer.update(currentTimeMs);
        return fetchTimer.isExpired();
    }

    public void resetFetchTimeout(long currentTimeMs) {
        fetchTimer.update(currentTimeMs);
        fetchTimer.reset(fetchTimeoutMs);
    }

    public void overrideFetchTimeout(long currentTimeMs, long timeoutMs) {
        fetchTimer.update(currentTimeMs);
        fetchTimer.reset(timeoutMs);
    }

    public boolean updateHighWatermark(OptionalLong newHighWatermark) {
        if (!newHighWatermark.isPresent() && highWatermark.isPresent()) {
            throw new IllegalArgumentException(
                String.format("Attempt to overwrite current high watermark %s with unknown value", highWatermark)
            );
        }

        if (highWatermark.isPresent()) {
            long previousHighWatermark = highWatermark.get().offset;
            long updatedHighWatermark = newHighWatermark.getAsLong();

            if (updatedHighWatermark < 0) {
                throw new IllegalArgumentException(
                    String.format("Illegal negative (%d) high watermark update", updatedHighWatermark)
                );
            } else if (previousHighWatermark > updatedHighWatermark) {
                throw new IllegalArgumentException(
                    String.format(
                        "Non-monotonic update of high watermark from %d to %d",
                        previousHighWatermark,
                        updatedHighWatermark
                    )
                );
            } else if (previousHighWatermark == updatedHighWatermark) {
                return false;
            }
        }

        Optional<LogOffsetMetadata> oldHighWatermark = highWatermark;
        highWatermark = newHighWatermark.isPresent() ?
            Optional.of(new LogOffsetMetadata(newHighWatermark.getAsLong())) :
            Optional.empty();

        logHighWatermarkUpdate(oldHighWatermark, highWatermark);

        return true;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    public Optional<RawSnapshotWriter> fetchingSnapshot() {
        return fetchingSnapshot;
    }

    public void setFetchingSnapshot(Optional<RawSnapshotWriter> newSnapshot) {
        if (fetchingSnapshot.isPresent()) {
            fetchingSnapshot.get().close();
        }
        fetchingSnapshot = newSnapshot;
    }

    @Override
    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        log.debug("Rejecting vote request from candidate {} since we already have a leader {} in epoch {}",
                candidateId, leaderId(), epoch);
        return false;
    }

    @Override
    public String toString() {
        return "FollowerState(" +
            "fetchTimeoutMs=" + fetchTimeoutMs +
            ", epoch=" + epoch +
            ", leaderId=" + leaderId +
            ", voters=" + voters +
            ", highWatermark=" + highWatermark +
            ", fetchingSnapshot=" + fetchingSnapshot +
            ')';
    }

    @Override
    public void close() {
        if (fetchingSnapshot.isPresent()) {
            fetchingSnapshot.get().close();
        }
    }

    private void logHighWatermarkUpdate(
        Optional<LogOffsetMetadata> oldHighWatermark,
        Optional<LogOffsetMetadata> newHighWatermark
    ) {
        if (!oldHighWatermark.equals(newHighWatermark)) {
            if (oldHighWatermark.isPresent()) {
                log.trace(
                    "High watermark set to {} from {} for epoch {}",
                    newHighWatermark,
                    oldHighWatermark.get(),
                    epoch
                );
            } else {
                log.info(
                    "High watermark set to {} for the first time for epoch {}",
                    newHighWatermark,
                    epoch
                );
            }
        }
    }
}
