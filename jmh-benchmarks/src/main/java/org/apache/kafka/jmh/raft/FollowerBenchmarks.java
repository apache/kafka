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
package org.apache.kafka.jmh.raft;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.RaftClientBenchmarkContext;
import org.apache.kafka.raft.RaftClientTestContext;
import org.apache.kafka.raft.RaftRequest;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for the follower path.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_WARMUP_ITERATIONS)
@Measurement(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_MEASUREMENT_ITERATIONS)
@Fork(RaftClientBenchmarkContext.AVERAGE_TIME_FORKS)
public class FollowerBenchmarks {

    /**
     * Starting state: the local node is a follower of another voter in a {@code voterCount}-node
     * cluster.
     */
    @State(Scope.Thread)
    public static class FollowerWithMultipleVoters {
        @Param({"3", "5"})
        public int voterCount;

        RaftClientBenchmarkContext benchmark;
        RaftClientTestContext context;
        @Setup(Level.Iteration)
        public void setup() throws IOException {
            benchmark = RaftClientBenchmarkContext.follower(
                voterCount,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            context = benchmark.testContext();
        }
    }

    /**
     * The follower handles a {@code BEGIN_QUORUM_EPOCH} one epoch ahead, which moves it to the next
     * epoch of the same leader.
     */
    @Benchmark
    public void handleBeginQuorumEpoch(
        FollowerWithMultipleVoters state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        int leaderId = state.benchmark.leaderKey().id();
        state.benchmark.deliverAndAwaitResponse(
            state.context.inboundRequest(
                state.context.beginEpochRequest(state.context.currentEpoch() + 1, leaderId)),
            Optional.of(ApiKeys.BEGIN_QUORUM_EPOCH)
        );
        /* Each invocation's transition clears the follower's connections, so the next poll opens a
         * fresh fetch to the leader. Nothing here answers fetches, so take it off the send queue.
         */
        state.context.assertSentFetchRequest();
        counters.recordInvocation(state.benchmark);
    }

    /**
     * Starting state: a follower, plus the {@code END_QUORUM_EPOCH} it will be handed. Handling
     * that request changes neither epoch nor leader, so one request stays valid for every
     * invocation.
     */
    @State(Scope.Thread)
    public static class FollowerWithPreparedEndQuorumEpoch {
        @Param({"3", "5"})
        public int voterCount;

        RaftClientBenchmarkContext benchmark;
        RaftClientTestContext context;
        RaftRequest.Inbound request;

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            benchmark = RaftClientBenchmarkContext.follower(
                voterCount,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            context = benchmark.testContext();
            request = context.inboundRequest(
                context.endEpochRequest(
                    context.currentEpoch(),
                    benchmark.leaderKey().id(),
                    benchmark.remoteVoters()));

            /* The follower fetches from its leader on its first poll, and since handling
             * END_QUORUM_EPOCH does not reset its connections that is the only fetch it ever sends.
             * Poll here so it is sent, then let zeroCountersOnSetup() take it off the send queue:
             * the measured operation is expected to leave nothing queued.
             */
            context.pollUntilRequest();
            benchmark.zeroCountersOnSetup();
        }
    }

    /**
     * The follower handles an {@code END_QUORUM_EPOCH} from its leader, which overrides its fetch
     * timeout so that it starts an election sooner than it otherwise would.
     */
    @Benchmark
    public void handleEndQuorumEpoch(
        FollowerWithPreparedEndQuorumEpoch state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.benchmark.deliverAndAwaitResponse(
            state.request, Optional.of(ApiKeys.END_QUORUM_EPOCH));
        counters.recordInvocation(state.benchmark);
    }

    /**
     * Starting state: the local node is a follower fetching from its leader.
     */
    @State(Scope.Thread)
    public static class FollowerFetchingFromLeader {
        static final int VOTER_COUNT = 3;
        static final List<String> RECORDS = List.of("a", "b", "c");

        RaftClientBenchmarkContext benchmark;
        RaftClientTestContext context;
        int leaderId;

        @Setup(Level.Iteration)
        public void setup() throws IOException {
            benchmark = RaftClientBenchmarkContext.follower(
                VOTER_COUNT,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            context = benchmark.testContext();
            leaderId = benchmark.leaderKey().id();
        }
    }

    /**
     * The follower appends the records from a fetch response. It is told the high watermark it
     * already has, so nothing commits and no listener is notified.
     */
    @Benchmark
    public void handleFetchResponseWithRecords(
        FollowerFetchingFromLeader state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        RaftClientTestContext context = state.context;
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();

        // Records have to start at the follower's log end offset, so the batch is built per
        // invocation; that construction is fixture cost inside the measured region.
        long baseOffset = state.benchmark.logEndOffset();
        int epoch = context.currentEpoch();
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(
                epoch,
                state.leaderId,
                context.buildBatch(baseOffset, epoch, FollowerFetchingFromLeader.RECORDS),
                0L,
                Errors.NONE)
        );
        context.client.poll();
        counters.recordInvocation(state.benchmark);
    }

    /**
     * The follower appends the records from a fetch response and is told a high watermark that
     * commits what it already had, so it also advances its own high watermark and notifies its
     * listener (which, on a follower, means reading those records back from the log).
     */
    @Benchmark
    public void handleFetchResponseAdvancingHighWatermark(
        FollowerFetchingFromLeader state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        RaftClientTestContext context = state.context;
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();

        long baseOffset = state.benchmark.logEndOffset();
        int epoch = context.currentEpoch();
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(
                epoch,
                state.leaderId,
                context.buildBatch(baseOffset, epoch, FollowerFetchingFromLeader.RECORDS),
                baseOffset,
                Errors.NONE)
        );
        context.client.poll();
        counters.recordInvocation(state.benchmark);
    }
}
