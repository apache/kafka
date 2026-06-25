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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.KRaftVersion;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class RaftClientBenchmarkContext {
    private final RaftClientTestContext context;
    private final MockLog log;
    private final MockNetworkChannel channel;
    private final ReplicaKey otherVoter;

    private int lastFlushCount;
    private int lastReadCount;
    private int lastTruncationCount;
    private int lastRequestsSent;
    private int lastQuorumWrites;

    private RaftClientBenchmarkContext(RaftClientTestContext context, ReplicaKey otherVoter) {
        this.context = context;
        this.log = context.log;
        this.channel = context.channel;
        this.otherVoter = otherVoter;
        resetCounters();
    }

    /**
     * Builds an unattached node in a {@code voterCount}-node quorum (the local node is not yet the
     * leader). Use {@link #unattachedToLeader()} as the measured operation to drive a full
     * Unattached &rarr; Leader election. A single-voter quorum is rejected because such a node elects
     * itself at initialization, before any measured poll.
     */
    public static RaftClientBenchmarkContext unattached(int localId, int voterCount) throws Exception {
        if (voterCount < 2) {
            throw new IllegalArgumentException("voterCount must be at least 2; a single voter self-elects at init");
        }
        return new RaftClientBenchmarkContext(buildContext(voterKeys(localId, voterCount)), null);
    }

    public static RaftClientBenchmarkContext leader(int localId, int voterCount) throws Exception {
        List<ReplicaKey> voterKeys = voterKeys(localId, voterCount);
        RaftClientTestContext context = buildContext(voterKeys);
        context.unattachedToLeader();

        return new RaftClientBenchmarkContext(context, voterKeys.get(1));
    }

    /** {@code voterCount} voter keys, each with a random directory id, with {@code localId} first. */
    private static List<ReplicaKey> voterKeys(int localId, int voterCount) {
        return IntStream.range(0, voterCount)
            .mapToObj(i -> ReplicaKey.of(localId + i, Uuid.randomUuid()))
            .collect(Collectors.toList());
    }

    /**
     * Builds an unattached node in a KIP-1186 quorum of {@code voterKeys}, whose first entry is the
     * local node. The local voter's directory id is shared between the voter set and the client so
     * they agree.
     */
    private static RaftClientTestContext buildContext(List<ReplicaKey> voterKeys) throws Exception {
        ReplicaKey local = voterKeys.get(0);
        VoterSet voters = VoterSetTestUtil.voterSet(voterKeys.stream());

        return new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withStartingVoters(voters, KRaftVersion.KRAFT_VERSION_1)
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_1186_PROTOCOL)
            .withPollIntervalMs(0)
            .withUnknownLeader(0)
            .build();
    }

    public void unattachedToLeader() throws Exception {
        context.unattachedToLeader();
    }

    public void advanceLeaderHwmToLogEnd() throws InterruptedException {
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();
    }

    public void pollUntilResponse() throws InterruptedException {
        context.pollUntilResponse();
    }

    public int currentEpoch() {
        return context.currentEpoch();
    }

    public long logEndOffset() {
        return log.endOffset().offset();
    }

    /** A voter other than the local node, e.g. to deliver a FETCH from on the leader. */
    public ReplicaKey otherVoter() {
        if (otherVoter == null) {
            throw new IllegalStateException("This context was not built with an other-voter key");
        }
        return otherVoter;
    }

    public void deliverRequest(ApiMessage request) {
        context.deliverRequest(request);
    }

    public FetchRequestData fetchRequest(
        int epoch,
        ReplicaKey replicaKey,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitMs
    ) {
        return context.fetchRequest(epoch, replicaKey, fetchOffset, lastFetchedEpoch, maxWaitMs);
    }

    /** Re-baselines all counters to the current totals. Call at the end of benchmark setup. */
    public void resetCounters() {
        lastFlushCount = log.flushCount();
        lastReadCount = log.readCount();
        lastTruncationCount = log.truncationCount();
        lastRequestsSent = channel.requestsSent();
        lastQuorumWrites = context.quorumStateWriteCount();
        context.drainAllSentResponses();
    }

    public int drainLogFlushes() {
        int current = log.flushCount();
        int delta = current - lastFlushCount;
        lastFlushCount = current;
        return delta;
    }

    public int drainLogReads() {
        int current = log.readCount();
        int delta = current - lastReadCount;
        lastReadCount = current;
        return delta;
    }

    public int drainLogTruncations() {
        int current = log.truncationCount();
        int delta = current - lastTruncationCount;
        lastTruncationCount = current;
        return delta;
    }

    public int drainRpcRequestsSent() {
        int current = channel.requestsSent();
        int delta = current - lastRequestsSent;
        lastRequestsSent = current;
        return delta;
    }

    public int drainQuorumStateWrites() {
        int current = context.quorumStateWriteCount();
        int delta = current - lastQuorumWrites;
        lastQuorumWrites = current;
        return delta;
    }

    public int drainRpcResponsesSent() {
        return context.drainAllSentResponses();
    }
}
