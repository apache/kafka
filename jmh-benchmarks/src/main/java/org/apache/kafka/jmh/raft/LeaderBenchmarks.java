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
import org.apache.kafka.common.protocol.ApiMessage;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for the leader request-handling path.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_WARMUP_ITERATIONS)
@Measurement(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_MEASUREMENT_ITERATIONS)
@Fork(RaftClientBenchmarkContext.AVERAGE_TIME_FORKS)
public class LeaderBenchmarks {

    /**
     * <p>Only read-only, non-mutating RPCs belong here.
     */
    public enum LeaderInboundRpc implements BenchmarkRpc {
        /**
         * A no-wait FETCH from a fully caught-up follower.
         */
        NO_WAIT_FETCH_AT_HWM(Optional.empty(), Optional.of(ApiKeys.FETCH)) {
            @Override
            public ApiMessage build(RaftClientBenchmarkContext benchmark) {
                RaftClientTestContext context = benchmark.testContext();
                return context.fetchRequest(
                    context.currentEpoch(),
                    benchmark.remoteVoters().get(0),
                    benchmark.logEndOffset(),
                    context.currentEpoch(),
                    0);
            }
        },

        /**
         * A FETCH from a follower that is behind.
         */
        FETCH_FROM_BEHIND(Optional.empty(), Optional.of(ApiKeys.FETCH)) {
            @Override
            public ApiMessage build(RaftClientBenchmarkContext benchmark) {
                RaftClientTestContext context = benchmark.testContext();
                return context.fetchRequest(
                    context.currentEpoch(),
                    benchmark.remoteVoters().get(0),
                    1L,
                    context.currentEpoch(),
                    0);
            }
        },

        DESCRIBE_QUORUM(Optional.empty(), Optional.of(ApiKeys.DESCRIBE_QUORUM)) {
            @Override
            public ApiMessage build(RaftClientBenchmarkContext benchmark) {
                return benchmark.testContext().describeQuorumRequest();
            }
        };

        private final Optional<ApiKeys> expectedRequest;
        private final Optional<ApiKeys> expectedResponse;

        LeaderInboundRpc(Optional<ApiKeys> expectedRequest, Optional<ApiKeys> expectedResponse) {
            this.expectedRequest = expectedRequest;
            this.expectedResponse = expectedResponse;
        }

        @Override
        public Optional<ApiKeys> expectedRequest() {
            return expectedRequest;
        }

        @Override
        public Optional<ApiKeys> expectedResponse() {
            return expectedResponse;
        }
    }

    /**
     * Starting state: the local node is leader with the high watermark at the log end. The
     * {@code rpc} param selects which inbound request the benchmark delivers. The request is built
     * and round-tripped through serialization once here, so the measured region starts from a
     * parsed request on the queue and excludes request parsing.
     */
    @State(Scope.Thread)
    public static class LeaderWithHwmAtLogEnd {
        static final int VOTER_COUNT = 3;

        @Param
        public LeaderInboundRpc rpc;

        RaftClientBenchmarkContext benchmark;
        RaftClientTestContext context;
        RaftRequest.Inbound request;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            benchmark = RaftClientBenchmarkContext.leader(
                VOTER_COUNT,
                0,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            context = benchmark.testContext();
            context.client.prepareAppend(context.currentEpoch(), List.of("a", "b", "c", "d", "e"));
            context.client.schedulePreparedAppend();
            context.poll();
            context.advanceLocalLeaderHighWatermarkToLogEndOffset();
            request = context.inboundRequest(rpc.build(benchmark));
            benchmark.zeroCountersOnSetup();
        }
    }

    /**
     * Leader handles one inbound RPC (selected by the {@code rpc} param) and replies.
     */
    @Benchmark
    public void handleInboundRpc(
        LeaderWithHwmAtLogEnd state,
        KRaftBenchmarkingCounters counters
    ) throws InterruptedException {
        state.benchmark.deliverAndCount(state.request, state.rpc.expectedResponse());
        counters.recordInvocation(state.benchmark, state.rpc.expectedRequest());
    }

    /**
     * Inbound RPCs that transition a leader out of its term, one benchmark row each. Each constant
     * mutates the node's durable state.
     */
    public enum LeaderTransitioningRpc implements BenchmarkRpc {
        /**
         * A BEGIN_QUORUM_EPOCH at a higher epoch: the leader learns of a new leader, steps down to
         * follower, and acknowledges with a BEGIN_QUORUM_EPOCH response.
         */
        HIGHER_EPOCH_BEGIN_QUORUM_EPOCH(Optional.empty(), Optional.of(ApiKeys.BEGIN_QUORUM_EPOCH)) {
            @Override
            public ApiMessage build(RaftClientBenchmarkContext benchmark) {
                RaftClientTestContext context = benchmark.testContext();
                int newLeaderId = benchmark.remoteVoters().get(0).id();
                return context.beginEpochRequest(context.currentEpoch() + 1, newLeaderId);
            }
        };

        private final Optional<ApiKeys> expectedRequest;
        private final Optional<ApiKeys> expectedResponse;

        LeaderTransitioningRpc(
            Optional<ApiKeys> expectedRequest,
            Optional<ApiKeys> expectedResponse
        ) {
            this.expectedRequest = expectedRequest;
            this.expectedResponse = expectedResponse;
        }

        @Override
        public Optional<ApiKeys> expectedRequest() {
            return expectedRequest;
        }

        @Override
        public Optional<ApiKeys> expectedResponse() {
            return expectedResponse;
        }
    }

    /**
     * Starting state: a freshly elected leader, rebuilt per invocation because the measured RPC
     * transitions it out of its term (state-consuming).
     */
    @State(Scope.Thread)
    public static class LeaderBeforeTransition {
        static final int VOTER_COUNT = 3;

        @Param
        public LeaderTransitioningRpc rpc;

        RaftClientBenchmarkContext benchmark;
        RaftClientTestContext context;
        RaftRequest.Inbound request;

        @Setup(Level.Invocation)
        public void setup() throws Exception {
            benchmark = RaftClientBenchmarkContext.leader(
                VOTER_COUNT,
                0,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            context = benchmark.testContext();
            request = context.inboundRequest(rpc.build(benchmark));
            benchmark.zeroCountersOnSetup();
        }
    }

    /**
     * Leader handles an inbound RPC that transitions it out of its term.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = RaftClientBenchmarkContext.SINGLE_SHOT_WARMUP_ITERATIONS)
    @Measurement(iterations = RaftClientBenchmarkContext.SINGLE_SHOT_MEASUREMENT_ITERATIONS)
    @Fork(RaftClientBenchmarkContext.SINGLE_SHOT_FORKS)
    public void handleTransitioningRpc(
        LeaderBeforeTransition state,
        KRaftBenchmarkingCounters counters
    ) throws InterruptedException {
        state.benchmark.deliverAndCount(state.request, state.rpc.expectedResponse());
        counters.recordInvocation(state.benchmark, state.rpc.expectedRequest());
    }
}
