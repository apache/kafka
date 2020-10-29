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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A leader or candidate may gracefully resign in the current epoch by sending
 * an `EndQuorumEpoch` request to all of the voters. This state is used to track
 * delivery of this request. A voter will remain in the `Resigned` state until
 * we either learn about another election, or our own election timeout expires
 * and we become a Candidate.
 *
 * Note that vote requests in the same epoch that we have resigned from must be
 * rejected.
 */
public class ResignedState implements EpochState {
    private final ElectionState election;
    private final int localId;
    private final long electionTimeoutMs;
    private final Set<Integer> unackedVoters;
    private final Timer electionTimer;
    private final List<Integer> preferredSuccessors;

    public ResignedState(
        Time time,
        int localId,
        long electionTimeoutMs,
        ElectionState election,
        List<Integer> preferredSuccessors
    ) {
        this.localId = localId;
        this.election = election;
        this.unackedVoters = new HashSet<>(election.voters());
        this.unackedVoters.remove(localId);
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.preferredSuccessors = preferredSuccessors;
    }

    @Override
    public ElectionState election() {
        return election;
    }

    @Override
    public int epoch() {
        return election.epoch;
    }

    /**
     * Get the set of voters which have yet to acknowledge the resignation.
     * This node will send `EndQuorumEpoch` requests to this set until these
     * voters acknowledge the request or we transition to another state.
     *
     * @return the set of unacknowledged voters
     */
    public Set<Integer> unackedVoters() {
        return unackedVoters;
    }

    /**
     * Invoked after receiving a successful `EndQuorumEpoch` response. This
     * is in order to prevent unnecessary retries.
     *
     * @param voterId the ID of the voter that send the successful response
     */
    public void acknowledgeResignation(int voterId) {
        if (!election.voters().contains(voterId)) {
            throw new IllegalArgumentException("Attempt to acknowledge delivery of `EndQuorumEpoch` " +
                "by a non-voter " + voterId);
        }
        unackedVoters.remove(voterId);
    }

    /**
     * Check whether the timeout has expired.
     *
     * @param currentTimeMs current time in milliseconds
     * @return true if the timeout has expired, false otherwise
     */
    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.isExpired();
    }

    /**
     * Check the time remaining until the timeout expires.
     *
     * @param currentTimeMs current time in milliseconds
     * @return the duration in milliseconds from the current time before the timeout expires
     */
    public long remainingElectionTimeMs(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.remainingMs();
    }

    public List<Integer> preferredSuccessors() {
        return preferredSuccessors;
    }

    @Override
    public String name() {
        return "Resigned";
    }

    @Override
    public String toString() {
        return "ResignedState{" +
            "election=" + election +
            ", localId=" + localId +
            ", unackedVoters=" + unackedVoters +
            ", expirationTimeoutMs=" + electionTimeoutMs +
            ", preferredSuccessors=" + preferredSuccessors +
            '}';
    }
}
