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
import kafka.server.FetchSession;
import kafka.server.CachedPartition;
import kafka.server.FetchSessionCache;
import kafka.utils.MockTime;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class IncrementalFetchContextBenchmark {
    @Param({"10", "20", "50"})
    private int topicCount;
    @Param({"10", "20", "50"})
    private int partitionCount;
    @Param({"5", "10", "50"})
    private int toForgetPercentage;

    private MockTime time;
    private FetchManager fetchManager;
    private Map<TopicPartition, FetchRequest.PartitionData> fullReqData;
    private LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> fullRespData;
    private FetchResponse fullResponse;
    private FetchContext incrementalFetchContext;
    private Map<TopicPartition, FetchRequest.PartitionData> incrementalReqData;
    private List<TopicPartition> toForget;
    private FetchMetadata incrementalMetadata;
    private LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> incrementalRespData;
    private FetchSessionCache cache;
    private FetchSession session;
    private int sessionId;


    @Setup(Level.Trial)
    public void setup() {
        time = new MockTime();
        cache = new FetchSessionCache(1000, 120000);
        fetchManager = new FetchManager(time, cache);

        // FullFetchContext setup
        List<TopicPartition> topics = new ArrayList<>();
        IntStream.range(0, topicCount).forEach(topicNum -> {
            String topicName = "topic" + topicNum;
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                topics.add(new TopicPartition(topicName, partitionId));
            }
        });
        fullReqData = new HashMap<>();
        fullRespData = new LinkedHashMap<>();
        topics.forEach(tp -> {
            FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(0,
                    0, 4096, Optional.empty());
            fullReqData.put(tp, partitionData);

            FetchResponseData.PartitionData respPartitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(tp.partition())
                    .setLastStableOffset(0)
                    .setHighWatermark(0)
                    .setLogStartOffset(0);
            fullRespData.put(tp, respPartitionData);
        });

        // IncrementalFetchContext setup
        // Let's add a fraction of the current topicPartitions to the session. Say, 1/10.
        List<TopicPartition> incrementalTopics = new ArrayList<>();
        IntStream.range(0, topicCount).forEach(topicNum -> {
            String topicName = "incrementalTopic" + topicNum;
            for (int partitionId = 0; partitionId < partitionCount; partitionId += 10) {
                incrementalTopics.add(new TopicPartition(topicName, partitionId));
            }
        });
        incrementalReqData = new HashMap<>();
        incrementalRespData = new LinkedHashMap<>();
        incrementalTopics.forEach(tp -> {
            FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                    0, 0, 4096, Optional.empty());
            incrementalReqData.put(tp, partitionData);

            FetchResponseData.PartitionData respPartitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(tp.partition())
                    .setLastStableOffset(0)
                    .setHighWatermark(0)
                    .setLogStartOffset(0);
            incrementalRespData.put(tp, respPartitionData);
        });
        // Add responses from session
        incrementalRespData.putAll(fullRespData);

        // Add some topicPartitions toForget
        toForget = new ArrayList<>();
        IntStream.range(0, topics.size()).forEach(tpNum -> {
            if (tpNum % (100 / toForgetPercentage) == 0) {
                toForget.add(topics.get(tpNum));
                incrementalRespData.remove(topics.get(tpNum));
            }
        });
    }
    @Setup(Level.Iteration)
    public void setupIncrementalInfo() {
        FetchContext fullFetchContext = fetchManager.newContext(FetchMetadata.INITIAL, fullReqData, Collections.emptyList(), false);
        fullResponse = fullFetchContext.updateAndGenerateResponseData(fullRespData);

        FetchMetadata newIncremental = FetchMetadata.newIncremental(fullResponse.sessionId());
        incrementalFetchContext = fetchManager.newContext(newIncremental, incrementalReqData, toForget, false);

        sessionId = cache.maybeCreateSession(time.milliseconds(), false, fullRespData.size(), this::createPartitions);
        incrementalMetadata = newIncremental.nextIncremental();
        session = cache.get(sessionId).get();
    }

    @TearDown(Level.Iteration)
    public void removeSessions() {
        cache.remove(fullResponse.sessionId());
        cache.remove(sessionId);
    }

    private ImplicitLinkedHashCollection<CachedPartition> createPartitions() {
        ImplicitLinkedHashCollection<CachedPartition> cachedPartitions = new ImplicitLinkedHashCollection<>(fullRespData.size());
        fullRespData.forEach((part, respData) -> {
            FetchRequest.PartitionData reqData = fullReqData.get(part);
            cachedPartitions.mustAdd(new CachedPartition(part, reqData, respData));
        });
        return cachedPartitions;
    }

    @Benchmark
    public void updateSession() {
        session.update(incrementalReqData, toForget, incrementalMetadata);

    }

    @Benchmark
    public void updateAndGenerateResponseDataForIncrementalContext() {
        incrementalFetchContext.updateAndGenerateResponseData(incrementalRespData);

    }
}
