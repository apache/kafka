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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for the leader request-handling path. The outer class is intentionally not a JMH
 * {@code @State}: each benchmark declares the starting state it needs as a nested {@code @State}
 * parameter, so future leader scenarios (e.g. a lagging-follower fetch or a commit) can have their own
 * setup without forcing a single shared {@code @Setup} on the whole class.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_WARMUP_ITERATIONS)
@Measurement(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_MEASUREMENT_ITERATIONS)
@Fork(RaftClientBenchmarkContext.AVERAGE_TIME_FORKS)
public class LeaderBenchmarks {

    /**
     * Starting state: the local node is Leader with the high watermark at the log end and a caught-up
     * follower ready to fetch.
     */
    @State(Scope.Thread)
    public static class LeaderWithCaughtUpFollower {
        static final int VOTER_COUNT = 3;

        RaftClientBenchmarkContext benchmark;
        RaftClientTestContext context;

        int epoch;
        long endOffset;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            benchmark = RaftClientBenchmarkContext.leader(VOTER_COUNT);
            context = benchmark.testContext();
            context.advanceLocalLeaderHighWatermarkToLogEndOffset();
            epoch = context.currentEpoch();
            endOffset = benchmark.logEndOffset();
            benchmark.zeroCountersOnSetup();
        }
    }

    /**
     * Leader handles a FETCH from a fully caught-up follower (fetch offset == log end offset) that asks
     * not to wait ({@code maxWaitMs = 0}), so the leader replies immediately rather than deferring.
     *
     * <p>Note: a real caught-up follower long-polls with {@code maxWaitMs > 0}, and such a fetch is
     * <em>deferred</em> (held until new data arrives or the wait times out) — it would not produce an
     * immediate response. This benchmark deliberately uses {@code maxWaitMs = 0} to measure the immediate-reply path
     */
    @Benchmark
    public void handleNoWaitFetchFromCaughtUpFollower(
        LeaderWithCaughtUpFollower state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.context.deliverRequest(
            state.context.fetchRequest(
                state.epoch, state.benchmark.remoteVoters().get(0), state.endOffset, state.epoch, 0));
        state.context.pollUntilResponse();

        counters.collectDeltasAndDrainRPCs(state.benchmark, Optional.empty(), Optional.of(ApiKeys.FETCH));
    }
}
