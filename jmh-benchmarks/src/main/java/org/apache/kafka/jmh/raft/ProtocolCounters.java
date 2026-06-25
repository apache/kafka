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

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Secondary, machine-independent work counters reported by the raft benchmarks alongside the timing
 * score, as {@code benchmark:counter} rows.
 *
 * <p>Each benchmark calls {@link #drainFrom} every invocation to accumulate the work deltas drained
 * from {@link RaftClientBenchmarkContext}. JMH reads the public numeric methods at the end of each
 * measurement iteration. The public fields are raw totals intended for JSON post-processing; divide
 * them by {@link #operations} to get the exact per-operation value. The public methods return
 * per-operation values for each iteration, so the per-iteration console output shows stable values
 * such as {@code logReadsPerOp = 1.0}. JMH aggregates {@code Type.EVENTS} secondary results with
 * {@code SUM}, so the final summary row will add per-iteration method values together when more
 * than one measurement iteration is used.
 *
 * <p>The per-operation values are integer-exact and should be stable across a correct refactor of
 * {@code KafkaRaftClient}: a flush count moving from 1 to 2 per operation is a behavioral diff, not
 * measurement noise. The counters that are zero on a path (e.g. log flushes on a caught-up fetch)
 * are the most useful tripwires, since zero is speed-independent.
 */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class ProtocolCounters {
    public long logFlushesTotal;
    public long logReadsTotal;
    public long logTruncationsTotal;
    public long rpcRequestsSentTotal;
    public long rpcResponsesSentTotal;
    public long quorumStateWritesTotal;
    public long operations;

    @Setup(Level.Iteration)
    public void reset() {
        logFlushesTotal = 0;
        logReadsTotal = 0;
        logTruncationsTotal = 0;
        rpcRequestsSentTotal = 0;
        rpcResponsesSentTotal = 0;
        quorumStateWritesTotal = 0;
        operations = 0;
    }

    /** Accumulates this invocation's work deltas drained from {@code context} into these counters. */
    public void drainFrom(RaftClientBenchmarkContext context) {
        logFlushesTotal += context.drainLogFlushes();
        logReadsTotal += context.drainLogReads();
        logTruncationsTotal += context.drainLogTruncations();
        rpcRequestsSentTotal += context.drainRpcRequestsSent();
        rpcResponsesSentTotal += context.drainRpcResponsesSent();
        quorumStateWritesTotal += context.drainQuorumStateWrites();
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

    private double perOperation(long counter) {
        if (operations == 0) {
            return 0.0;
        }
        return (double) counter / operations;
    }
}