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

package org.apache.kafka.jmh.metadata;


import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MetadataResponseBenchmark {
    @Param({"500", "1000", "5000"})
    private int topicCount;
    @Param({"10", "20", "50"})
    private int partitionCount;

    private MetadataResponseData metadataResponseData;

    @Setup(Level.Trial)
    public void setup() {

        List<MetadataResponseData.MetadataResponseTopic> topicStates = new ArrayList<>();
        IntStream.range(0, topicCount).forEach(topicIndex -> {
            String topicName = "topic-" + topicIndex;

            List<MetadataResponseData.MetadataResponsePartition> partitionStates = new ArrayList<>();
            IntStream.range(0, partitionCount).forEach(partitionId -> partitionStates.add(
                new MetadataResponseData.MetadataResponsePartition()
                    .setPartitionIndex(1)
                    .setLeaderId(1)
                    .setLeaderEpoch(1)
                    .setReplicaNodes(Arrays.asList(1, 2, 3))
                    .setIsrNodes(Arrays.asList(1, 2, 3))
                    .setOfflineReplicas(Collections.emptyList())
                    .setErrorCode(Errors.NONE.code())));

            MetadataResponseData.MetadataResponseTopic topicMetadata = new MetadataResponseData.MetadataResponseTopic()
                .setErrorCode(Errors.NONE.code())
                .setName(topicName)
                .setTopicId(Uuid.randomUuid())
                .setPartitions(partitionStates)
                .setIsInternal(false);

            topicStates.add(topicMetadata);
        });


        MetadataResponseData.MetadataResponseTopicCollection topics =
            new MetadataResponseData.MetadataResponseTopicCollection(topicStates.iterator());

        metadataResponseData = new MetadataResponseData()
            .setClusterId("clusterId")
            .setControllerId(0)
            .setTopics(topics)
            .setBrokers(new MetadataResponseData.MetadataResponseBrokerCollection());
    }

    @Benchmark
    public void constructMetadataResponse() {
        new MetadataResponse(metadataResponseData, MetadataResponseData.HIGHEST_SUPPORTED_VERSION).topicMetadata();
    }
}
