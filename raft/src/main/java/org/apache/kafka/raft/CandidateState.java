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

import java.util.HashSet;
import java.util.Set;

public class CandidateState implements EpochState {
    private final int localId;
    private final int epoch;
    private final Set<Integer> voters;
    private final Set<Integer> rejectedVotes = new HashSet<>();
    private final Set<Integer> grantedVotes = new HashSet<>();

    protected CandidateState(int localId, int epoch, Set<Integer> voters) {
        this.localId = localId;
        this.epoch = epoch;
        this.voters = voters;
        this.grantedVotes.add(localId);
    }

    private boolean isNonVoter(int nodeId) {
        return !voters.contains(nodeId);
    }

    public int majoritySize() {
        return voters.size() / 2 + 1;
    }

    public boolean isVoteGranted() {
        return grantedVotes.size() >= majoritySize();
    }

    /**
     * Check if we have received enough rejections that it is no longer possible to reach a
     * majority of grants.
     *
     * @return true if the vote is rejected, false if the vote is already or can still be granted
     */
    public boolean isVoteRejected() {
        return grantedVotes.size() + remainingVoters().size() < majoritySize();
    }

    public boolean voteGrantedBy(int remoteNodeId) {
        if (isNonVoter(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to grant vote to non-voter " + remoteNodeId);
        } else if (rejectedVotes.contains(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to grant vote from node " + remoteNodeId +
                    " which previously rejected our request");
        }

        return grantedVotes.add(remoteNodeId);
    }

    public boolean voteRejectedBy(int remoteNodeId) {
        if (isNonVoter(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to reject vote to non-voter " + remoteNodeId);
        } else if (grantedVotes.contains(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to reject vote from node " + remoteNodeId +
                    " which previously granted our request");
        }
        return rejectedVotes.add(remoteNodeId);
    }

    public Set<Integer> remainingVoters() {
        Set<Integer> remaining = new HashSet<>(voters);
        remaining.removeAll(grantedVotes);
        remaining.removeAll(rejectedVotes);
        return remaining;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withVotedCandidate(epoch, localId);
    }

    @Override
    public int epoch() {
        return epoch;
    }

}
