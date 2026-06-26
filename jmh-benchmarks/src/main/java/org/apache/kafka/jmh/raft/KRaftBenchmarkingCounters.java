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
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.Optional;

/**
 * Secondary, machine-independent work counters reported by the raft benchmarks alongside the timing
 * score, as {@code benchmark:counter} rows.
 *
 * <p>Throughout this class, an <em>operation</em> is JMH's unit of work: a single invocation of a
 * {@code @Benchmark}-annotated method. (One operation equals one invocation here because we don't use
 * {@code @OperationsPerInvocation}.) JMH reports the timing score in {@code ns/op}, and these work
 * counters are reported {@code PerOp} to match.
 *
 * <p>Each benchmark calls {@link #drainFrom} every invocation to accumulate the work deltas drained
 * from {@link RaftClientBenchmarkContext}. The raw totals are private accumulators; what we report
 * are the per-operation values from the {@code *PerOp()} methods (the quantity of interest), plus
 * {@link #operations}.
 *
 * <p>JMH aggregates {@code Type.EVENTS} secondary results with {@code SUM} across all measurement
 * data points i.e {@code forks x measurement iterations}. To make the <em>summary</em> row
 * report the true per-operation value rather than that value multiplied by the data-point count, each
 * method pre-divides by the data-point count obtained from {@link BenchmarkParams} in
 * {@link #captureRunShape}. The SUM then reconstitutes the exact per-operation value (e.g.
 * {@code logReadsPerOp = 1.0}) in the summary, for any {@code -f}/{@code -i} configuration. (The
 * per-iteration console values are correspondingly a small fraction of the per-op value; read the
 * summary row.)
 *
 * <p>The per-operation values are integer-exact and should be stable across a correct refactor of
 * {@code KafkaRaftClient}: a flush count moving from 1 to 2 per operation is a behavioral diff, not
 * measurement noise. The counters that are zero on a path (e.g. log flushes on a caught-up fetch)
 * are the most useful tripwires, since zero is speed-independent.
 */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class KRaftBenchmarkingCounters {
    private long logFlushesTotal;
    private long logReadsTotal;
    private long logTruncationsTotal;
    private long rpcRequestsSentTotal;
    private long rpcResponsesSentTotal;
    private long quorumStateWritesTotal;
    private long quorumStateReadsTotal;

    // Reported: the number of operations (i.e. @Benchmark method invocations) measured in the
    // iteration, and the divisor for the per-operation values below.
    public long operations;

    // The number of measurement data points JMH will SUM the per-op methods over, i.e.
    // (forks x measurement iterations) for this run. Captured from BenchmarkParams so it tracks the
    // actual run shape (including -f/-i overrides) rather than being hardcoded.
    private double measurementDataPoints = 1.0;

    @Setup(Level.Trial)
    public void captureRunShape(BenchmarkParams params) {
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
    }

    /**
     * Accumulates this invocation's work deltas drained from {@code context} into these counters.
     * {@code expectedRequest}/{@code expectedResponse}, if present, restrict the RPC request/response
     * counts to that API key (e.g. {@code FETCH}); empty counts all.
     */
    public void drainFrom(
        RaftClientBenchmarkContext context,
        Optional<ApiKeys> expectedRequest,
        Optional<ApiKeys> expectedResponse
    ) {
        logFlushesTotal += context.drainLogFlushes();
        logReadsTotal += context.drainLogReads();
        logTruncationsTotal += context.drainLogTruncations();
        rpcRequestsSentTotal += context.drainRpcRequestsSent(expectedRequest);
        rpcResponsesSentTotal += context.drainRpcResponsesSent(expectedResponse);
        quorumStateWritesTotal += context.drainQuorumStateWrites();
        quorumStateReadsTotal += context.drainQuorumStateReads();
        operations += 1;
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