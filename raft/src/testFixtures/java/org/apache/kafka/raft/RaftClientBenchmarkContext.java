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
import org.apache.kafka.raft.internals.BatchMemoryPool;
import org.apache.kafka.server.common.KRaftVersion;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class RaftClientBenchmarkContext {
    // Standardized JMH iteration counts, shared by all raft benchmarks so every benchmark of a
    // given mode is configured identically.

    // AverageTime averages many operations within each timed iteration.
    public static final int AVERAGE_TIME_WARMUP_ITERATIONS = 5;
    public static final int AVERAGE_TIME_MEASUREMENT_ITERATIONS = 10;
    public static final int AVERAGE_TIME_FORKS = 3;

    /**
     * Keeps mock time in a plausible range. Fixture helpers advance the mock clock to fire timers
     * rather than waiting on them, and a benchmark runs millions of operations, so a production-sized
     * timeout moves the clock weeks ahead within one iteration, which is far enough to reach
     * deadlines meant to be unreachable.
     */
    private static final int BENCHMARK_ELECTION_TIMEOUT_MS = 50;

    public static final KRaftVersion DEFAULT_KRAFT_VERSION = KRaftVersion.LATEST_PRODUCTION;
    // Default to the newest version of each (the highest-ordinal enum constant).
    public static final RaftProtocol DEFAULT_RAFT_PROTOCOL =
        Arrays.stream(RaftProtocol.values()).max(Comparator.naturalOrder()).orElseThrow();

    private final RaftClientTestContext context;
    private final MockLog log;
    private final MockNetworkChannel channel;

    private final ReplicaKey localKey;
    private final List<ReplicaKey> startingVoters;
    private final List<ReplicaKey> startingObservers;

    // Each tracks one cumulative mock counter as a drainable delta against a baseline. The baseline
    // is snapshotted at construction and re-baselined by zeroCountersOnSetup() at the end of
    // benchmark setup.
    private final DrainableCounter logFlushes;
    private final DrainableCounter logReads;
    private final DrainableCounter logTruncations;
    private final DrainableCounter rpcRequestsSent;
    private final DrainableCounter quorumStateWrites;
    private final DrainableCounter quorumStateReads;

    // Responses have no cumulative mock counter. deliverAndAwaitResponse() counts them here instead
    // of appending to RaftClientTestContext.sentResponses, so the collection stays bounded when
    // per-invocation draining is deferred to an iteration teardown.
    private final DrainableCounter rpcResponsesSent;
    private long rpcResponsesSentTotal;
    private RaftResponse.Outbound lastResponse;
    private Throwable lastResponseException;

    private RaftClientBenchmarkContext(
        RaftClientTestContext context,
        ReplicaKey localKey,
        List<ReplicaKey> startingVoters,
        List<ReplicaKey> startingObservers
    ) {
        this.context = context;
        this.log = context.log;
        this.channel = context.channel;
        this.localKey = localKey;
        this.startingVoters = List.copyOf(startingVoters);
        this.startingObservers = List.copyOf(startingObservers);
        this.logFlushes = new DrainableCounter(log::flushCount);
        this.logReads = new DrainableCounter(log::readCount);
        this.logTruncations = new DrainableCounter(log::truncationCount);
        this.rpcRequestsSent = new DrainableCounter(channel::requestsSent);
        this.quorumStateWrites = new DrainableCounter(context::quorumStateWriteCount);
        this.quorumStateReads = new DrainableCounter(context::quorumStateReadCount);
        this.rpcResponsesSent = new DrainableCounter(() -> rpcResponsesSentTotal);
    }

    /**
     * Builds the local node as a voter in the Unattached state (a voter with no leader yet) in a
     * {@code voterCount}-node cluster. The local node is a voter because only a voter can drive the
     * measured operation {@link RaftClientTestContext#unattachedToLeader()} (on
     * {@link #testContext()}) — an Unattached &rarr; Leader election, a path observers cannot
     * take. A single-voter cluster is rejected because such a node elects itself at
     * initialization, before any measured poll.
     */
    public static RaftClientBenchmarkContext unattachedVoter(
        int voterCount,
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws IOException {
        if (voterCount < 2) {
            throw new IllegalArgumentException(
                "voterCount must be at least 2; a single voter self-elects at init");
        }
        List<ReplicaKey> voterKeys = replicaKeys(randomReplicaId(), voterCount);
        ReplicaKey local = voterKeys.get(0);
        RaftClientTestContext context =
            benchmarkContextBuilder(local, voterKeys, kraftVersion, raftProtocol)
                .withUnknownLeader(0)
                .build();
        return new RaftClientBenchmarkContext(context, local, voterKeys, List.of());
    }

    /**
     * Builds a leader in a cluster of {@code voterCount} voters and {@code observerCount}
     * observers.
     */
    public static RaftClientBenchmarkContext leader(
        int voterCount,
        int observerCount,
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        if (voterCount < 2) {
            throw new IllegalArgumentException(
                "voterCount must be at least 2");
        }
        List<ReplicaKey> voterKeys = replicaKeys(randomReplicaId(), voterCount);
        ReplicaKey local = voterKeys.get(0);

        List<ReplicaKey> observerKeys = replicaKeys(local.id() + voterCount, observerCount);
        RaftClientTestContext context =
            benchmarkContextBuilder(local, voterKeys, kraftVersion, raftProtocol)
                .withUnknownLeader(0)
                .build();
        context.unattachedToLeader();

        return new RaftClientBenchmarkContext(context, local, voterKeys, observerKeys);
    }

    /**
     * {@code count} replica keys with consecutive ids starting at {@code startId}, each with a
     * random directory id.
     */
    private static List<ReplicaKey> replicaKeys(int startId, int count) {
        return IntStream.range(0, count)
            .mapToObj(i -> ReplicaKey.of(startId + i, Uuid.randomUuid()))
            .collect(Collectors.toList());
    }

    private static int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }

    /**
     * A {@link RaftClientTestContext.Builder} for {@code local} in a cluster whose voter set is
     * {@code voterKeys}, pre-configured with the settings shared by every raft benchmark.
     */
    private static RaftClientTestContext.Builder benchmarkContextBuilder(
        ReplicaKey local,
        List<ReplicaKey> voterKeys,
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) {
        VoterSet voters = VoterSetTestUtil.voterSet(voterKeys.stream());

        return new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withStartingVoters(voters, kraftVersion)
            .withRaftProtocol(raftProtocol)
            .withElectionTimeoutMs(BENCHMARK_ELECTION_TIMEOUT_MS)
            .withPollIntervalMs(0)
            .withBenchmarking(true)
            .withMemoryPool(new BatchMemoryPool(5, KafkaRaftClient.MAX_BATCH_SIZE_BYTES));
    }

    public RaftClientTestContext testContext() {
        return context;
    }

    /**
     * The local node's log end offset. Kept here because the {@code log} field is package-private.
     */
    public long logEndOffset() {
        return log.endOffset().offset();
    }

    /**
     * The starting voters other than the local node, in voter-set order. May be empty (a
     * single-voter cluster whose only voter is the local node).
     */
    public List<ReplicaKey> remoteVoters() {
        return startingVoters.stream()
            .filter(voter -> !voter.equals(localKey))
            .collect(Collectors.toList());
    }

    public List<ReplicaKey> startingObservers() {
        return startingObservers;
    }

    /**
     * Establishes the counter baseline so that work done before this point (building the context,
     * driving an election in {@code leader()}, or anything else a benchmark does in its setup) is
     * not attributed to the measured operation. Call this at the <b>end of benchmark setup</b>,
     * just before the measured region begins.
     */
    public void zeroCountersOnSetup() {
        logFlushes.drainDelta();
        logReads.drainDelta();
        logTruncations.drainDelta();
        rpcRequestsSent.drainDelta();
        quorumStateWrites.drainDelta();
        quorumStateReads.drainDelta();
        rpcResponsesSent.drainDelta();
        channel.drainSendQueue();
    }

    public long getLogFlushesDelta() {
        return logFlushes.drainDelta();
    }

    public long getLogReadsDelta() {
        return logReads.drainDelta();
    }

    public long getLogTruncationsDelta() {
        return logTruncations.drainDelta();
    }

    /**
     * Total number of requests (all API keys) the client has sent since the last drain. Uses the
     * channel's cumulative counter, so it is unaffected by a test driver draining the send queue.
     */
    public long getRpcRequestsSentDelta() {
        return rpcRequestsSent.drainDelta();
    }

    /**
     * Asserts the client left no outstanding requests on the send queue at the end of the
     * invocation. The benchmark is responsible for completing every request it triggers (by
     * delivering a response), so any leftover request means the client sent more than the benchmark
     * accounts for.
     */
    public void assertNoOutstandingRequests() {
        if (channel.hasSentRequests()) {
            throw new IllegalStateException(
                "Unexpected outstanding requests at end of benchmark invocation: "
                    + channel.drainSendQueue());
        }
    }

    public long getQuorumStateWritesDelta() {
        return quorumStateWrites.drainDelta();
    }

    public long getQuorumStateReadsDelta() {
        return quorumStateReads.drainDelta();
    }

    /**
     * Benchmark deliver path: hands {@code inbound} to the client, polls until the client produces
     * its response, and counts that response.
     */
    public void deliverAndAwaitResponse(
        RaftRequest.Inbound inbound,
        Optional<ApiKeys> expectedResponse
    ) throws InterruptedException {
        long before = rpcResponsesSentTotal;
        context.client.handle(inbound).whenComplete((response, exception) -> {
            if (exception != null) {
                lastResponseException = exception;
            } else {
                rpcResponsesSentTotal++;
                lastResponse = response;
            }
        });
        context.pollUntil(() -> rpcResponsesSentTotal > before || lastResponseException != null);
        if (lastResponseException != null) {
            Throwable failure = lastResponseException;
            lastResponseException = null;
            throw new IllegalStateException("benchmark request handling failed", failure);
        }
        if (expectedResponse.isPresent()
            && lastResponse.data().apiKey() != expectedResponse.get().id) {
            throw new IllegalStateException(
                "expected " + expectedResponse.get() + " response, got apiKey "
                    + lastResponse.data().apiKey());
        }
    }

    /**
     * Number of responses the client has produced via {@link #deliverAndAwaitResponse} since the
     * last drain.
     */
    public long getRpcResponsesSentDelta() {
        return rpcResponsesSent.drainDelta();
    }

    /**
     * Commits the local leader's epoch by delivering a fetch at the log end offset from every other
     * voter, which advances the high watermark over the {@code LeaderChange} record the leader wrote
     * when it took the epoch.
     */
    public void commitEpoch() throws InterruptedException {
        int epoch = context.currentEpoch();
        long logEndOffset = log.endOffset().offset();
        for (ReplicaKey voter : remoteVoters()) {
            deliverAndAwaitResponse(
                context.inboundRequest(
                    context.fetchRequest(epoch, voter, logEndOffset, epoch, 0)),
                Optional.of(ApiKeys.FETCH)
            );
        }
        if (context.client.highWatermark().orElse(-1) < logEndOffset) {
            throw new IllegalStateException(
                "expected the high watermark to reach the log end offset " + logEndOffset
                    + ", but it is " + context.client.highWatermark());
        }
    }

    /**
     * Drives the local node to the Unattached state by delivering a vote request one epoch ahead.
     * A raft node transitions to Unattached whenever it observes an RPC with a higher epoch, so this
     * is safe and works from any attached role, not just leader.
     */
    public void toUnattachedWithHigherEpoch() throws InterruptedException {
        if (context.client.quorum().isUnattached()) {
            throw new IllegalStateException(
                "toUnattachedWithHigherEpoch() expects an attached node, but it is already Unattached");
        }
        ReplicaKey candidate = remoteVoters().get(0);
        deliverAndAwaitResponse(
            context.inboundRequest(context.voteRequest(context.currentEpoch() + 1, candidate, 0, 0)),
            Optional.of(ApiKeys.VOTE)
        );
    }

}
