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

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.Optional;

/**
 * Secondary, machine-independent work counters reported by the raft benchmarks alongside the timing
 * score, as {@code benchmark:counter} rows.
 *
 * <p>Throughout this class, an <em>operation</em> is JMH's unit of work: a single invocation of a
 * {@code @Benchmark}-annotated method. (One operation equals one invocation here because we don't
 * use {@code @OperationsPerInvocation}.) JMH reports the timing score in {@code ns/op}, and these
 * work counters are reported {@code PerOp} to match.
 *
 * <p>The per-operation values are integer-exact and should be stable across a correct refactor of
 * {@code KafkaRaftClient}: a flush count moving from 1 to 2 per operation is a behavioral diff, not
 * measurement noise. The counters that are zero on a path (e.g. log flushes on a caught-up fetch)
 * are the most useful tripwires, since zero is speed-independent.
 */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class KRaftBenchmarkingCounters {
    // Private accumulators: not reported directly (we report the per-op values below). Being
    // private, JMH does not touch them between iterations, so reset() must zero them.
    private long logFlushesTotal;
    private long logReadsTotal;
    private long logTruncationsTotal;
    private long rpcRequestsSentTotal;
    private long rpcResponsesSentTotal;
    private long quorumStateWritesTotal;
    private long quorumStateReadsTotal;

    // The number of operations (i.e. @Benchmark method invocations) measured in the iteration, and
    // the divisor for the per-operation values below.
    private long operations;

    // The divisor for the per-op methods below: (forks x measurement iterations). Set once per fork
    // by captureRunShape().
    private double measurementDataPoints = 1.0;

    // Stashed by recordInvocation() each invocation so the iteration teardown can read the deltas
    // off the benchmark's mocks and drain any request the scenario expects in-flight.
    private RaftClientBenchmarkContext context;
    private Optional<ApiKeys> expectedRequest = Optional.empty();

    /**
     * Captures the number of measurement data points — {@code forks x measurement iterations} —
     * that JMH will SUM the {@code *PerOp()} methods over ({@code Type.EVENTS} secondary results
     * are SUM-aggregated across iterations and forks). Each per-op method pre-divides by this count
     * so that the SUM reports the exact per-operation value (e.g. {@code logReadsPerOp = 1.0}) in
     * the summary row. Reading it from {@link BenchmarkParams} tracks the actual run shape
     * (including {@code -f}/{@code -i} overrides) rather than hardcoding the annotation values.
     */
    @Setup(Level.Trial)
    public void captureRunShape(BenchmarkParams params) {
        if (params.getThreads() != 1) {
            throw new IllegalStateException(
                "raft benchmarks are single-threaded (one client over shared mocks); got "
                    + params.getThreads() + " threads");
        }
        // forks() is 0 when forking is disabled (in-process), which is still one set of iterations.
        int forks = Math.max(1, params.getForks());
        measurementDataPoints = (double) forks * params.getMeasurement().getCount();
    }

    @Setup(Level.Iteration)
    public void reset() {
        logFlushesTotal = 0;
        logReadsTotal = 0;
        logTruncationsTotal = 0;
        rpcRequestsSentTotal = 0;
        rpcResponsesSentTotal = 0;
        quorumStateWritesTotal = 0;
        quorumStateReadsTotal = 0;
        operations = 0;
        context = null;
        expectedRequest = Optional.empty();
    }

    /**
     * Counts one completed benchmark operation.
     */
    public void recordInvocation(
        RaftClientBenchmarkContext context,
        Optional<ApiKeys> expectedRequest
    ) {
        if (this.context == null) {
            this.context = context;
            this.expectedRequest = expectedRequest;
        }
        operations += 1;
    }

    /**
     * Runs at iteration end (untimed), before the {@code *PerOp()} aux counters are read,
     * and reads this iteration's work-counters and drains the requests the scenario expects in-flight.
     */
    @TearDown(Level.Iteration)
    public void collect() {
        if (context == null || operations == 0) {
            throw new IllegalStateException(
                "iteration ended with no recorded invocations (operations=" + operations + "); "
                    + "every @Benchmark method must call recordInvocation() so the work counters "
                    + "can be attributed to an operation");
        }
        logFlushesTotal = context.getLogFlushesDelta();
        logReadsTotal = context.getLogReadsDelta();
        logTruncationsTotal = context.getLogTruncationsDelta();
        rpcRequestsSentTotal = context.getRpcRequestsSentDelta();
        rpcResponsesSentTotal = context.getRpcResponsesSentDelta();
        quorumStateWritesTotal = context.getQuorumStateWritesDelta();
        quorumStateReadsTotal = context.getQuorumStateReadsDelta();
        context.drainExpectedRequestsAndAssertEmpty(expectedRequest);
    }

    public double logFlushesPerOp() {
        return perOperation(logFlushesTotal);
    }

    public double logReadsPerOp() {
        return perOperation(logReadsTotal);
    }

    public double logTruncationsPerOp() {
        return perOperation(logTruncationsTotal);
    }

    public double rpcRequestsSentPerOp() {
        return perOperation(rpcRequestsSentTotal);
    }

    public double rpcResponsesSentPerOp() {
        return perOperation(rpcResponsesSentTotal);
    }

    public double quorumStateWritesPerOp() {
        return perOperation(quorumStateWritesTotal);
    }

    public double quorumStateReadsPerOp() {
        return perOperation(quorumStateReadsTotal);
    }

    private double perOperation(long counter) {
        if (operations == 0) {
            return 0.0;
        }
        return (double) counter / operations / measurementDataPoints;
    }
}
