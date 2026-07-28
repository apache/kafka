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
        RaftClientTestContext context;

        @Setup(Level.Iteration)
        public void setup() throws IOException {
            benchmark = RaftClientBenchmarkContext.unattachedVoter(
                voterCount,
                RaftClientBenchmarkContext.DEFAULT_KRAFT_VERSION,
                RaftClientBenchmarkContext.DEFAULT_RAFT_PROTOCOL);
            context = benchmark.testContext();
        }
    }

    /** The local node wins an election and becomes leader, then resigns back to Unattached. */
    @Benchmark
    public void electLeader(
        UnattachedWithMultipleVoters state,
        KRaftBenchmarkingCounters counters
    ) throws Exception {
        state.context.unattachedToLeader();
        state.benchmark.resignToUnattached();
        counters.recordInvocation(state.benchmark);
    }
}
