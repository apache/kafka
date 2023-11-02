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

import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FetchSessionBenchmark {
    private static final LogContext LOG_CONTEXT = new LogContext("[BenchFetchSessionHandler]=");

    @Param(value = {"10", "100", "1000"})
    private int partitionCount;

    @Param(value = {"0", "10", "100"})
    private int updatedPercentage;

    @Param(value = {"false", "true"})
    private boolean presize;

    private LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetches;
    private FetchSessionHandler handler;
    private Map<String, Uuid> topicIds;

    @Setup(Level.Trial)
    public void setUp() {
        fetches = new LinkedHashMap<>();
        handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        topicIds = new HashMap<>();
        FetchSessionHandler.Builder builder = handler.newBuilder();

        Uuid id = Uuid.randomUuid();
        topicIds.put("foo", id);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respMap = new LinkedHashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            TopicPartition tp = new TopicPartition("foo", i);
            FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(id, 0, 0, 200, Optional.empty());
            fetches.put(tp, partitionData);
            builder.add(tp, partitionData);
            respMap.put(new TopicIdPartition(id, tp), new FetchResponseData.PartitionData()
                            .setPartitionIndex(tp.partition())
                            .setLastStableOffset(0)
                            .setLogStartOffset(0));
        }
        builder.build();
        // build and handle an initial response so that the next fetch will be incremental
        handler.handleResponse(FetchResponse.of(Errors.NONE, 0, 1, respMap), ApiKeys.FETCH.latestVersion());

        int counter = 0;
        for (TopicPartition topicPartition: new ArrayList<>(fetches.keySet())) {
            if (updatedPercentage != 0 && counter % (100 / updatedPercentage) == 0) {
                // reorder in fetch session, and update log start offset
                fetches.remove(topicPartition);
                fetches.put(topicPartition, new FetchRequest.PartitionData(id, 50, 40, 200,
                        Optional.empty()));
            }
            counter++;
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void incrementalFetchSessionBuild() {
        FetchSessionHandler.Builder builder;
        if (presize)
            builder = handler.newBuilder(fetches.size(), true);
        else
            builder = handler.newBuilder();

        for (Map.Entry<TopicPartition, FetchRequest.PartitionData> entry: fetches.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            builder.add(topicPartition, entry.getValue());
        }

        builder.build();
    }
}
