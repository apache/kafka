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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.raft.RaftClientTestContext.RaftProtocol;
import org.apache.kafka.server.common.KRaftVersion;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class RaftClientBenchmarkContext {
    // Standardized JMH iteration counts, shared by all raft benchmarks so every benchmark of a given
    // mode is configured identically.

    // SingleShotTime measures a single operation per iteration, so it needs many iterations to build
    // a stable distribution.
    public static final int SINGLE_SHOT_WARMUP_ITERATIONS = 50;
    public static final int SINGLE_SHOT_MEASUREMENT_ITERATIONS = 30;
    public static final int SINGLE_SHOT_FORKS = 5;

    // AverageTime averages many operations within each timed iteration, so it needs fewer.
    public static final int AVERAGE_TIME_WARMUP_ITERATIONS = 5;
    public static final int AVERAGE_TIME_MEASUREMENT_ITERATIONS = 10;
    public static final int AVERAGE_TIME_FORKS = 3;

    // Default to the newest version of each (the highest-ordinal enum constant). Enum natural order
    // is ordinal order, so this picks the last-declared constant; relies on the constants being
    // declared oldest-to-newest, which avoids updating these when a new version is added.
    public static final KRaftVersion DEFAULT_KRAFT_VERSION =
        Arrays.stream(KRaftVersion.values()).max(Comparator.naturalOrder()).orElseThrow();
    public static final RaftProtocol DEFAULT_RAFT_PROTOCOL =
        Arrays.stream(RaftProtocol.values()).max(Comparator.naturalOrder()).orElseThrow();

    private final RaftClientTestContext context;
    private final MockLog log;
    private final MockNetworkChannel channel;
    private final List<ReplicaKey> voters;

    // Each tracks one cumulative mock counter as a drainable delta against a baseline. The baseline is
    // reset by zeroCountersOnSetup() at the end of benchmark setup.
    private final DrainableCounter logFlushes;
    private final DrainableCounter logReads;
    private final DrainableCounter logTruncations;
    private final DrainableCounter rpcRequestsSent;
    private final DrainableCounter quorumStateWrites;
    private final DrainableCounter quorumStateReads;

    private RaftClientBenchmarkContext(RaftClientTestContext context, List<ReplicaKey> voters) {
        this.context = context;
        this.log = context.log;
        this.channel = context.channel;
        this.voters = List.copyOf(voters);
        this.logFlushes = new DrainableCounter(log::flushCount);
        this.logReads = new DrainableCounter(log::readCount);
        this.logTruncations = new DrainableCounter(log::truncationCount);
        this.rpcRequestsSent = new DrainableCounter(channel::requestsSent);
        this.quorumStateWrites = new DrainableCounter(context::quorumStateWriteCount);
        this.quorumStateReads = new DrainableCounter(context::quorumStateReadCount);
    }

    /**
     * Builds a local, unattached node in a {@code voterCount}-node cluster (the local node is not yet
     * the leader). Use {@link RaftClientTestContext#unattachedToLeader()} on {@link #testContext()} as
     * the measured operation to drive a full Unattached &rarr; Leader election. A single-voter cluster
     * is rejected because such a node elects itself at initialization, before any measured poll.
     */
    public static RaftClientBenchmarkContext unattached(int voterCount) throws Exception {
        return unattached(voterCount, DEFAULT_KRAFT_VERSION, DEFAULT_RAFT_PROTOCOL);
    }

    public static RaftClientBenchmarkContext unattached(
        int voterCount,
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        if (voterCount < 2) {
            throw new IllegalArgumentException("voterCount must be at least 2; a single voter self-elects at init");
        }
        List<ReplicaKey> voterKeys = voterKeys(voterCount);
        return new RaftClientBenchmarkContext(buildContext(voterKeys, kraftVersion, raftProtocol), voterKeys);
    }

    public static RaftClientBenchmarkContext leader(int voterCount) throws Exception {
        return leader(voterCount, DEFAULT_KRAFT_VERSION, DEFAULT_RAFT_PROTOCOL);
    }

    public static RaftClientBenchmarkContext leader(
        int voterCount,
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        List<ReplicaKey> voterKeys = voterKeys(voterCount);
        RaftClientTestContext context = buildContext(voterKeys, kraftVersion, raftProtocol);
        context.unattachedToLeader();

        return new RaftClientBenchmarkContext(context, voterKeys);
    }

    /**
     * {@code voterCount} voter keys, each with a random directory id, with the local node first.
     */
    private static List<ReplicaKey> voterKeys(int voterCount) {
        int localId = randomReplicaId();
        return IntStream.range(0, voterCount)
            .mapToObj(i -> ReplicaKey.of(localId + i, Uuid.randomUuid()))
            .collect(Collectors.toList());
    }

    private static int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }

    /**
     * Initializes a local, unattached node in a cluster of {@code voterKeys} (first entry is the local
     * node).
     */
    private static RaftClientTestContext buildContext(
        List<ReplicaKey> voterKeys,
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        ReplicaKey local = voterKeys.get(0);
        VoterSet voters = VoterSetTestUtil.voterSet(voterKeys.stream());

        return new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withStartingVoters(voters, kraftVersion)
            .withRaftProtocol(raftProtocol)
            .withPollIntervalMs(0)
            .withUnknownLeader(0)
            .build();
    }

    public RaftClientTestContext testContext() {
        return context;
    }

    /** The local node's log end offset. Kept here because the {@code log} field is package-private. */
    public long logEndOffset() {
        return log.endOffset().offset();
    }

    /**
     * The voters other than the local node, in voter-set order. May be empty (single-voter cluster).
     * Use these as the source of delivered requests, e.g. a FETCH from a follower on the leader.
     */
    public List<ReplicaKey> remoteVoters() {
        return voters.subList(1, voters.size());
    }

    /**
     * Establishes the counter baseline so that work done before this point (building the context,
     * driving an election in {@code leader()}, or anything else a benchmark does in its setup) is not
     * attributed to the measured operation. Call this at the <b>end of benchmark setup</b>, just
     * before the measured region begins.
     */
    public void zeroCountersOnSetup() {
        logFlushes.reset();
        logReads.reset();
        logTruncations.reset();
        rpcRequestsSent.reset();
        quorumStateWrites.reset();
        quorumStateReads.reset();
        channel.drainSendQueue();
        context.drainAllSentResponses();
    }

    public int getLogFlushesDelta() {
        return logFlushes.delta();
    }

    public int getLogReadsDelta() {
        return logReads.delta();
    }

    public int getLogTruncationsDelta() {
        return logTruncations.delta();
    }

    /**
     * Total number of requests (all API keys) the client has sent since the last call. Uses the
     * channel's cumulative counter, so it is unaffected by a test driver draining the send queue.
     */
    public int getRpcRequestsSentDelta() {
        return rpcRequestsSent.delta();
    }

    /**
     * Drains the requests the benchmark expects to be in-flight at the end of the invocation (the
     * given {@code expectedRequest} API key, if any) and asserts the send queue is then empty. A
     * non-empty queue means the client sent more requests than the benchmark accounts for.
     */
    public void drainExpectedRequestsAndAssertEmpty(Optional<ApiKeys> expectedRequest) {
        expectedRequest.ifPresent(apiKey -> channel.drainSentRequests(Optional.of(apiKey)));
        if (channel.hasSentRequests()) {
            throw new IllegalStateException(
                "Unexpected outstanding requests at end of benchmark invocation: " + channel.drainSendQueue());
        }
    }

    public int getQuorumStateWritesDelta() {
        return quorumStateWrites.delta();
    }

    public int getQuorumStateReadsDelta() {
        return quorumStateReads.delta();
    }

    /**
     * Drains the responses the benchmark expects to be in-flight at the end of the invocation (the
     * given {@code apiKey}, if any), counts them, then asserts no other responses remain. Returns the
     * number of expected responses drained (0 if none are expected). A leftover response means the
     * client sent more than the benchmark accounts for.
     */
    public int maybeDrainSentRpcResponses(Optional<ApiKeys> apiKey) {
        int expected = apiKey.map(key -> context.drainSentResponses(key).size()).orElse(0);
        int remaining = context.drainAllSentResponses();
        if (remaining > 0) {
            throw new IllegalStateException(
                "Unexpected outstanding responses at end of benchmark invocation: " + remaining);
        }
        return expected;
    }

    /**
     * Tracks one cumulative mock counter as a delta against a baseline. {@link #reset()} snapshots the
     * current value (so the next {@link #delta()} starts from zero), and {@link #delta()} returns the
     * increase since the last reset/delta and advances the baseline.
     */
    private static final class DrainableCounter {
        private final IntSupplier source;
        private int baseline;

        DrainableCounter(IntSupplier source) {
            this.source = source;
        }

        void reset() {
            baseline = source.getAsInt();
        }

        int delta() {
            int current = source.getAsInt();
            int delta = current - baseline;
            baseline = current;
            return delta;
        }
    }
}
