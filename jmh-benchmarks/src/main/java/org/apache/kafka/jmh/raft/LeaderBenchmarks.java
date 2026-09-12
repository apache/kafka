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
    public enum LeaderInboundRpc implements ParameterizedRpc {
        /**
         * A FETCH from a caught-up voter follower ({@code fetchOffset} = log end offset) with
         * {@code maxWaitMs = 0}, so the leader replies immediately instead of parking the fetch
         * until new data arrives; this measures the immediate-reply path.
         */
        NO_WAIT_FETCH_AT_HWM(Optional.of(ApiKeys.FETCH)) {
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
         A FETCH from a follower that is lagging behind the leader's log end offset.
         */
        FETCH_FROM_LAGGING_FOLLOWER(Optional.of(ApiKeys.FETCH)) {
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

        DESCRIBE_QUORUM(Optional.of(ApiKeys.DESCRIBE_QUORUM)) {
            @Override
            public ApiMessage build(RaftClientBenchmarkContext benchmark) {
                return benchmark.testContext().describeQuorumRequest();
            }
        };

        private final Optional<ApiKeys> expectedResponse;

        LeaderInboundRpc(Optional<ApiKeys> expectedResponse) {
            this.expectedResponse = expectedResponse;
        }

        @Override
        public Optional<ApiKeys> expectedResponse() {
            return expectedResponse;
        }
    }

    /**
     * Starting state: the local node is leader with the high watermark at the log end. The
     * {@code rpc} param selects which inbound request the benchmark delivers. The request is built
     * once here, so the measured region starts from a request already on the queue and excludes
     * building it.
     */
    @State(Scope.Thread)
    public static class LeaderWithHwmAtLogEnd {
        static final int VOTER_COUNT = 3;

        @Param
        public LeaderInboundRpc rpc;

        RaftClientBenchmarkContext benchmark;
        RaftRequest.Inbound request;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            benchmark = RaftClientBenchmarkContext.leader(
                VOTER_COUNT,
                0,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            RaftClientTestContext context = benchmark.testContext();
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
        state.benchmark.deliverAndAwaitResponse(state.request, state.rpc.expectedResponse());
        counters.recordInvocation(state.benchmark);
    }

}