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

package org.apache.kafka.jmh.fetchsession;

import kafka.server.FetchContext;
import kafka.server.FetchManager;
import kafka.server.FetchSessionCache;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FetchSessionCacheBenchmark {

    @State(Scope.Thread)
    public static class TestState {
        @Param(value = {"1000", "2000", "5000"})
        private int cacheSize;

        @Param(value = {"99", "100"})
        private int cacheUtilization;

        @Param(value = {"10"})
        private int percentPrivileged;

        @Param(value = {"false", "true"})
        private boolean newSession;

        @Param(value = {"false", "true"})
        private boolean fetchSessionAddedPartitions;

        @Param(value = {"false", "true"})
        private boolean benchPrivileged;

        @Param(value = {"0", "1", "10"})
        private int numEvictableEntries;

        // private final AtomicLong msSinceEpoch = new AtomicLong(0);
        private AtomicLong msSinceEpoch;

        private final Time time = new Time() {
            @Override
            public long milliseconds() {
                return msSinceEpoch.get();
            }

            @Override
            public long nanoseconds() {
                return msSinceEpoch.get();
            }

            @Override
            public void sleep(long ms) { }

            @Override
            public void waitObject(Object obj, Supplier<Boolean> condition, long timeoutMs) { }
        };

        private LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData;
        private LinkedHashMap<TopicPartition, FetchResponse.PartitionData<Records>> respData;
        private LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData2;
        private LinkedHashMap<TopicPartition, FetchResponse.PartitionData<Records>> respData2;
        FetchSessionCache cache;
        FetchManager fetchManager;
        List<Integer> sessions;
        List<Boolean> sessionFollower;
        FetchMetadata fetchMetadata;

        @Setup(Level.Invocation)
        public void setUp() throws Exception {
            msSinceEpoch = new AtomicLong(0);
            sessionFollower = new ArrayList<>();
            sessions = new ArrayList<>();
            int EVICTION_MS = 500;
            cache = new FetchSessionCache(cacheSize, EVICTION_MS);
            fetchManager = new FetchManager(time, cache);
            reqData = new LinkedHashMap<>();
            reqData.put(new TopicPartition("foo", 0),
                    new FetchRequest.PartitionData(0, 0, 100, Optional.empty()));
            reqData.put(new TopicPartition("foo", 1),
                    new FetchRequest.PartitionData(10, 0, 100, Optional.empty()));
            respData = new LinkedHashMap<>();
            respData.put(new TopicPartition("foo", 0),
                    new FetchResponse.PartitionData<>(
                    Errors.NONE, 10, 10, 10, null,
                    MemoryRecords.readableRecords(ByteBuffer.allocate(0))));
            respData.put(new TopicPartition("foo", 1),
                    new FetchResponse.PartitionData<>(
                            Errors.NONE, 10, 10, 10, null,
                            MemoryRecords.readableRecords(ByteBuffer.allocate(0))));

            reqData2 = new LinkedHashMap<>();
            reqData2.put(new TopicPartition("foo", 0),
                    new FetchRequest.PartitionData(10, 0, 100, Optional.empty()));
            reqData2.put(new TopicPartition("foo", 1),
                    new FetchRequest.PartitionData(20, 0, 100, Optional.empty()));

            // the fetch session cache has a different performance profile if the size TreeMap
            // needs to be updated
            if (fetchSessionAddedPartitions)
                reqData2.put(new TopicPartition("foo", 2),
                        new FetchRequest.PartitionData(15, 0, 100, Optional.empty()));

            respData2 = new LinkedHashMap<>();
            respData2.put(new TopicPartition("foo", 1),
                new FetchResponse.PartitionData<>(
                    Errors.NONE, 20, 20, 0, null,
                    MemoryRecords.readableRecords(ByteBuffer.allocate(0))));

            int sessionCount = (int)Math.ceil(cacheUtilization / 100.0 * cacheSize);
            for (int i = 0; i < sessionCount; i++) {
                boolean isFollower = new Random().nextFloat() < percentPrivileged / 100.0;
                FetchContext context = fetchManager.newContext(FetchMetadata.INITIAL, reqData,
                        new ArrayList<>(), isFollower);
                FetchResponse<Records> resp = context.updateAndGenerateResponseData(respData);

                if (resp.sessionId() == 0)
                    throw new Exception("failed to establish session");

                sessions.add(resp.sessionId());
                sessionFollower.add(isFollower);

                if (i == sessionCount - numEvictableEntries - 1) {
                    msSinceEpoch.set(EVICTION_MS + 1);
                }
            }

            if (newSession) {
                fetchMetadata = FetchMetadata.INITIAL;
            } else {
                // randomly choose session of the right type to update
                boolean fetchIsFollower;
                do {
                    int sessionToUpdate = new Random().nextInt(sessions.size());
                    fetchMetadata = new FetchMetadata(sessions.get(sessionToUpdate), 1);
                    fetchIsFollower = sessionFollower.get(sessionToUpdate);
                } while (fetchIsFollower != benchPrivileged);
            }

            if (cache.size() != Math.min(sessionCount, cacheSize)) {
                throw new Exception("cache did not contain the expected session count");
            }
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void sessionCacheBench(TestState state) {
        // FetchContext context = state.fetchManager.newContext(state.fetchMetadata, state.reqData,
        //         new ArrayList<>(), state.benchPrivileged);
        // FetchResponse<Records> resp = context.updateAndGenerateResponseData(state.respData);
        FetchContext context = state.fetchManager.newContext(state.fetchMetadata, state.reqData2,
                new ArrayList<>(), state.benchPrivileged);
        FetchResponse<Records> resp = context.updateAndGenerateResponseData(state.respData2);
    }
}