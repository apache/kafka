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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Secondary, machine-independent work counters reported by the raft benchmarks alongside the timing
 * score, as {@code benchmark:counter} rows.
 *
 * <p>Each benchmark calls {@link #drainFrom} every invocation to accumulate the work deltas drained
 * from {@link RaftClientBenchmarkContext}. {@code Type.EVENTS} reports the total accumulated over the
 * iteration (not normalized to time), which scales with throughput and so isn't directly comparable
 * across runs. To get the stable per-operation value, divide any counter by {@link #operations} (the
 * count of benchmark invocations in the same iteration): e.g. {@code logReads / operations}. This
 * works uniformly for every benchmark, including the single-shot election where no work counter
 * equals the operation count.
 *
 * <p>The per-operation values are integer-exact and should be stable across a correct refactor of
 * {@code KafkaRaftClient}: a flush count moving from 1 to 2 per operation is a behavioral diff, not
 * measurement noise. The counters that are zero on a path (e.g. log flushes on a caught-up fetch)
 * are the most useful tripwires, since zero is speed-independent.
 */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class ProtocolCounters {
    public long logFlushes;
    public long logReads;
    public long rpcRequestsSent;
    public long rpcResponsesSent;
    public long quorumStateWrites;

    /** Number of benchmark invocations in the iteration; the divisor for the per-operation values. */
    public long operations;

    public long totalLogFlushes;
    public long totalLogReads;

    /** Accumulates this invocation's work deltas drained from {@code context} into these counters. */
    public void drainFrom(RaftClientBenchmarkContext context) {
        logFlushes += context.drainLogFlushes();
        logReads += context.drainLogReads();
        rpcRequestsSent += context.drainRpcRequestsSent();
        rpcResponsesSent += context.drainRpcResponsesSent();
        quorumStateWrites += context.drainQuorumStateWrites();
        operations += 1;
    }

    public void logFlushesPerOp() {
        totalLogFlushes += (long) logFlushes / operations;
    }

    public void logReadsPerOp() {
         totalLogReads += (long) logReads / operations;
    }
}
