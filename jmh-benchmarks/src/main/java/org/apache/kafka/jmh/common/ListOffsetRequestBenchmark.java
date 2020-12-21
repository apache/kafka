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

package org.apache.kafka.jmh.common;

import kafka.network.RequestConvertToJson;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ListOffsetsRequest;
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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ListOffsetRequestBenchmark {
    @Param({"10", "500", "1000"})
    private int topicCount;

    @Param({"3", "10", "20"})
    private int partitionCount;

    Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> offsetData;

    ListOffsetsRequest offsetRequest;

    @Setup(Level.Trial)
    public void setup() {
        this.offsetData = new HashMap<>();
        for (int topicIdx = 0; topicIdx < topicCount; topicIdx++) {
            String topic = UUID.randomUUID().toString();
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                ListOffsetsRequestData.ListOffsetsPartition data = new ListOffsetsRequestData.ListOffsetsPartition();
                this.offsetData.put(new TopicPartition(topic, partitionId), data);
            }
        }

        this.offsetRequest = ListOffsetsRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
                .build(ApiKeys.LIST_OFFSETS.latestVersion());
    }

    @Benchmark
    public String testRequestToJson() {
        return RequestConvertToJson.request(offsetRequest).toString();
    }
}
