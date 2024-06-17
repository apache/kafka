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
import org.apache.kafka.raft.internals.ReplicaKey;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This state represents a leader which has fenced itself either because it
 * is shutting down or because it has encountered a soft failure of some sort.
 * No writes are accepted in this state and we are not permitted to vote for
 * any other candidate in this epoch.
 *
 * A resigned leader may initiate a new election by sending `EndQuorumEpoch`
 * requests to all of the voters. This state tracks delivery of this request
 * in order to prevent unnecessary retries.
 *
 * A voter will remain in the `Resigned` state until we either learn about
 * another election, or our own election timeout expires and we become a
 * Candidate.
 */
public class ResignedState implements EpochState {
    private final int localId;
    private final int epoch;
    private final Set<Integer> voters;
    private final long electionTimeoutMs;
    private final Set<Integer> unackedVoters;
    private final Timer electionTimer;
    private final List<Integer> preferredSuccessors;
    private final Logger log;

    public ResignedState(
        Time time,
        int localId,
        int epoch,
        Set<Integer> voters,
        long electionTimeoutMs,
        List<Integer> preferredSuccessors,
        LogContext logContext
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.voters = voters;
        this.unackedVoters = new HashSet<>(voters);
        this.unackedVoters.remove(localId);
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.preferredSuccessors = preferredSuccessors;
        this.log = logContext.logger(ResignedState.class);
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, localId, voters);
    }

    @Override
    public int epoch() {
        return epoch;
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
        if (!voters.contains(voterId)) {
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
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        log.debug(
            "Rejecting vote request from candidate ({}) since we have resigned as candidate/leader in epoch {}",
            candidateKey,
            epoch
        );

        return false;
    }

    @Override
    public String name() {
        return "Resigned";
    }

    @Override
    public String toString() {
        return "ResignedState(" +
            "localId=" + localId +
            ", epoch=" + epoch +
            ", voters=" + voters +
            ", electionTimeoutMs=" + electionTimeoutMs +
            ", unackedVoters=" + unackedVoters +
            ", preferredSuccessors=" + preferredSuccessors +
            ')';
    }

    @Override
    public void close() {}
}
