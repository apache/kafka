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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

public class FollowerState implements EpochState {
    private final int fetchTimeoutMs;
    private final int epoch;
    private final int leaderId;
    private final Set<Integer> voters;
    private final Timer fetchTimer;
    private Optional<LogOffsetMetadata> highWatermark;

    public FollowerState(
        Time time,
        int epoch,
        int leaderId,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        int fetchTimeoutMs
    ) {
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.voters = voters;
        this.fetchTimer = time.timer(fetchTimeoutMs);
        this.highWatermark = highWatermark;
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

    public boolean updateHighWatermark(OptionalLong highWatermark) {
        if (!highWatermark.isPresent() && this.highWatermark.isPresent())
            throw new IllegalArgumentException("Attempt to overwrite current high watermark " + this.highWatermark +
                " with unknown value");

        if (this.highWatermark.isPresent()) {
            long previousHighWatermark = this.highWatermark.get().offset;
            long updatedHighWatermark = highWatermark.getAsLong();

            if (updatedHighWatermark < 0)
                throw new IllegalArgumentException("Illegal negative high watermark update");
            if (previousHighWatermark > updatedHighWatermark)
                throw new IllegalArgumentException("Non-monotonic update of high watermark attempted");
            if (previousHighWatermark == updatedHighWatermark)
                return false;
        }

        this.highWatermark = highWatermark.isPresent() ?
            Optional.of(new LogOffsetMetadata(highWatermark.getAsLong())) :
            Optional.empty();
        return true;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public String toString() {
        return "FollowerState(" +
            "fetchTimeoutMs=" + fetchTimeoutMs +
            ", epoch=" + epoch +
            ", leaderId=" + leaderId +
            ", voters=" + voters +
            ')';
    }
}
