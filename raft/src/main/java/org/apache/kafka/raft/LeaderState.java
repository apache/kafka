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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderState implements EpochState {
    private final int localId;
    private final int epoch;
    private final long epochStartOffset;
    private OptionalLong highWatermark = OptionalLong.empty();
    private Map<Integer, FollowerState> followers = new HashMap<>();

    protected LeaderState(int localId, int epoch, long epochStartOffset, Set<Integer> voters) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;

        for (int voterId : voters) {
            boolean hasEndorsedLeader = voterId == localId;
            followers.put(voterId, new FollowerState(voterId, hasEndorsedLeader, OptionalLong.empty()));
        }
    }

    @Override
    public OptionalLong highWatermark() {
        return highWatermark;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, localId);
    }

    @Override
    public int epoch() {
        return epoch;
    }

    public Set<Integer> followers() {
        return followers.keySet().stream().filter(id -> id != localId).collect(Collectors.toSet());
    }

    public Set<Integer> nonEndorsingFollowers() {
        Set<Integer> nonEndorsing = new HashSet<>();
        for (FollowerState state : followers.values()) {
            if (!state.hasEndorsedLeader)
                nonEndorsing.add(state.nodeId);
        }
        return nonEndorsing;
    }

    private boolean updateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        ArrayList<FollowerState> followersByFetchOffset = new ArrayList<>(this.followers.values());
        Collections.sort(followersByFetchOffset);
        int indexOfHw = followers.size() / 2 - (followers.size() % 2 == 0 ? 1 : 0);
        OptionalLong highWatermarkUpdateOpt = followersByFetchOffset.get(indexOfHw).endOffset;
        if (highWatermarkUpdateOpt.isPresent()) {
            // When a leader is first elected, it cannot know the high watermark of the previous
            // leader. In order to avoid exposing a non-monotonically increasing value, we have
            // to wait for followers to catch up to the start of the leader's epoch.
            long highWatermarkUpdate = highWatermarkUpdateOpt.getAsLong();
            if (highWatermarkUpdate >= epochStartOffset) {
                highWatermark = OptionalLong.of(highWatermarkUpdate);
                return true;
            }
        }
        return false;
    }

    public boolean updateEndOffset(int remoteNodeId, long endOffset) {
        FollowerState state = ensureValidFollower(remoteNodeId);
        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset > endOffset)
                throw new IllegalArgumentException("Non-monotonic update to end offset for nodeId " + remoteNodeId);
        });
        state.hasEndorsedLeader = true;
        state.endOffset = OptionalLong.of(endOffset);
        return updateHighWatermark();
    }

    public void addEndorsementFrom(int remoteNodeId) {
        FollowerState followerState = ensureValidFollower(remoteNodeId);
        followerState.hasEndorsedLeader = true;
    }

    private FollowerState ensureValidFollower(int remoteNodeId) {
        FollowerState state = followers.get(remoteNodeId);
        if (state == null)
            throw new IllegalArgumentException("Unexpected endorsement from non-voter " + remoteNodeId);
        return state;
    }

    public boolean updateLocalEndOffset(long endOffset) {
        return updateEndOffset(localId, endOffset);
    }

    private static class FollowerState implements Comparable<FollowerState> {
        final int nodeId;
        boolean hasEndorsedLeader;
        OptionalLong endOffset;

        public FollowerState(int nodeId,
                             boolean hasEndorsedLeader,
                             OptionalLong endOffset) {
            this.nodeId = nodeId;
            this.hasEndorsedLeader = hasEndorsedLeader;
            this.endOffset = endOffset;
        }

        @Override
        public int compareTo(FollowerState that) {
            if (this.endOffset == that.endOffset)
                return Integer.compare(this.nodeId, that.nodeId);
            else if (!this.endOffset.isPresent())
                return -1;
            else if (!that.endOffset.isPresent())
                return 1;
            else
                return Long.compare(this.endOffset.getAsLong(), that.endOffset.getAsLong());
        }
    }

}
