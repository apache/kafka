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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_WARMUP_ITERATIONS)
@Measurement(iterations = RaftClientBenchmarkContext.AVERAGE_TIME_MEASUREMENT_ITERATIONS)
@Fork(RaftClientBenchmarkContext.AVERAGE_TIME_FORKS)
public class LeaderBenchmarks {

    /**
     * Starting state: the local node is Leader with the high watermark at the log end and a caught-up
     * follower ready to fetch. Built once per trial and reused across invocations, since handling a
     * caught-up fetch does not mutate it.
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
     * Leader handles a valid FETCH from a fully caught-up follower (fetch offset == log end offset),
     * which does not advance the high watermark — the steady-state heartbeat-style fetch.
     */
    @Benchmark
    public void handleFetchFromCaughtUpFollower(
        LeaderWithCaughtUpFollower state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.context.deliverRequest(
            state.context.fetchRequest(
                state.epoch, state.benchmark.remoteVoters().get(0), state.endOffset, state.epoch, 0));
        state.context.pollUntilResponse();

        counters.drainFrom(state.benchmark, Optional.empty(), Optional.of(ApiKeys.FETCH));
    }
}
