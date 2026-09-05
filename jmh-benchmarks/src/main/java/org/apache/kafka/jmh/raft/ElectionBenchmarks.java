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
import org.apache.kafka.raft.RaftClientBenchmarkContext;
import org.apache.kafka.raft.RaftClientTestContext;

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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for the leader-election path.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_WARMUP_ITERATIONS)
@Measurement(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_MEASUREMENT_ITERATIONS)
@Fork(RaftClientBenchmarkContext.AVERAGE_TIME_FORKS)
public class ElectionBenchmarks {

    /**
     * Starting state: the local node is Unattached in a {@code voterCount}-node cluster.
     */
    @State(Scope.Thread)
    public static class UnattachedWithMultipleVoters {
        @Param({"3", "5"})
        public int voterCount;

        RaftClientBenchmarkContext benchmark;

        @Setup(Level.Iteration)
        public void setup() throws IOException {
            benchmark = RaftClientBenchmarkContext.unattachedVoter(
                voterCount,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
        }
    }

    /**
     * The local node wins an election, commits the epoch it just took, then resigns back to
     * Unattached.
     */
    @Benchmark
    public void electLeader(
        UnattachedWithMultipleVoters state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.benchmark.testContext().unattachedToLeader();
        state.benchmark.commitEpoch();
        state.benchmark.toUnattachedWithHigherEpoch();
        counters.recordInvocation(state.benchmark);
    }

    /**
     * The local node times out and starts an election, then a higher-epoch vote request knocks it
     * back to Unattached for the next invocation.
     */
    @Benchmark
    public void unattachedToProspective(
        UnattachedWithMultipleVoters state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.benchmark.unattachedToProspective();
        state.benchmark.toUnattachedWithHigherEpoch();
        state.benchmark.drainSentRequests();             // discard the abandoned pre-votes
        counters.recordInvocation(state.benchmark);
    }

    /**
     * The local node times out and starts an election, then that election times out without a
     * winner, and it falls back to Unattached for the next invocation.
     */
    @Benchmark
    public void prospectiveToUnattachedOnTimeout(
        UnattachedWithMultipleVoters state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.benchmark.unattachedToProspective();
        state.benchmark.prospectiveToUnattached();
        counters.recordInvocation(state.benchmark);
    }

    /**
     * Starting state: the local node is Prospective in a {@code voterCount}-node cluster, with its
     * pre-vote requests already on the send queue.
     */
    @State(Scope.Thread)
    public static class ProspectiveWithMultipleVoters {
        @Param({"3", "5"})
        public int voterCount;

        RaftClientBenchmarkContext benchmark;

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            benchmark = RaftClientBenchmarkContext.unattachedVoter(
                voterCount,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            benchmark.unattachedToProspective();
            // Keep the queued pre-votes (the measured transition answers them); only zero counters.
            benchmark.zeroCounters();
        }
    }

    /**
     * The local node, a prospective, wins its pre-vote and becomes a candidate, then its election
     * timeout returns it to prospective for the next invocation.
     */
    @Benchmark
    public void prospectiveToCandidate(
        ProspectiveWithMultipleVoters state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.benchmark.prospectiveToCandidate();
        state.benchmark.drainSentRequests();         // discard the candidate's vote requests
        state.benchmark.candidateToProspective();
        counters.recordInvocation(state.benchmark);
    }

    /**
     * Starting state: the local node is a follower of another voter in a {@code voterCount}-node
     * cluster.
     */
    @State(Scope.Thread)
    public static class FollowerWithMultipleVoters {
        @Param({"3", "5"})
        public int voterCount;

        RaftClientBenchmarkContext benchmark;

        @Setup(Level.Iteration)
        public void setup() throws IOException {
            benchmark = RaftClientBenchmarkContext.follower(
                voterCount,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
        }
    }

    /**
     * The local node, a follower, times out on its leader and starts an election, then a
     * BeginQuorumEpoch one epoch ahead returns it to a follower for the next invocation.
     */
    @Benchmark
    public void followerToProspective(
        FollowerWithMultipleVoters state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        RaftClientTestContext context = state.benchmark.testContext();
        state.benchmark.followerToProspective();
        state.benchmark.deliverAndAwaitResponse(
            context.inboundRequest(
                context.beginEpochRequest(context.currentEpoch() + 1, state.benchmark.leaderKey().id())),
            Optional.of(ApiKeys.BEGIN_QUORUM_EPOCH));
        state.benchmark.drainSentRequests();         // discard abandoned pre-votes + the new fetch
        counters.recordInvocation(state.benchmark);
    }
}
