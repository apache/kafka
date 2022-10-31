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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.RequestHeader;
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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FetchRequestBenchmark {

    @Param({"10", "500", "1000"})
    private int topicCount;

    @Param({"3", "10", "20"})
    private int partitionCount;

    Map<TopicPartition, FetchRequest.PartitionData> fetchData;

    Map<Uuid, String> topicNames;

    RequestHeader header;

    FetchRequest consumerRequest;

    FetchRequest replicaRequest;

    ByteBuffer requestBuffer;

    @Setup(Level.Trial)
    public void setup() {
        this.fetchData = new HashMap<>();
        this.topicNames = new HashMap<>();
        for (int topicIdx = 0; topicIdx < topicCount; topicIdx++) {
            String topic = Uuid.randomUuid().toString();
            Uuid id = Uuid.randomUuid();
            topicNames.put(id, topic);
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                    id, 0, 0, 4096, Optional.empty());
                fetchData.put(new TopicPartition(topic, partitionId), partitionData);
            }
        }

        this.header = new RequestHeader(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion(), "jmh-benchmark", 100);
        this.consumerRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 0, 0, fetchData)
            .build(ApiKeys.FETCH.latestVersion());
        this.replicaRequest = FetchRequest.Builder.forReplica(ApiKeys.FETCH.latestVersion(), 1, 0, 0, fetchData)
            .build(ApiKeys.FETCH.latestVersion());
        this.requestBuffer = this.consumerRequest.serialize();

    }

    @Benchmark
    public short testFetchRequestFromBuffer() {
        return AbstractRequest.parseRequest(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion(), requestBuffer).request.version();
    }

    @Benchmark
    public int testFetchRequestForConsumer() {
        FetchRequest fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 0, 0, fetchData)
            .build(ApiKeys.FETCH.latestVersion());
        return fetchRequest.fetchData(topicNames).size();
    }

    @Benchmark
    public int testFetchRequestForReplica() {
        FetchRequest fetchRequest = FetchRequest.Builder.forReplica(
            ApiKeys.FETCH.latestVersion(), 1, 0, 0, fetchData)
                .build(ApiKeys.FETCH.latestVersion());
        return fetchRequest.fetchData(topicNames).size();
    }

    @Benchmark
    public int testSerializeFetchRequestForConsumer() throws IOException {
        Send send = consumerRequest.toSend(header);
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        return channel.buffer().limit();
    }

    @Benchmark
    public int testSerializeFetchRequestForReplica() throws IOException {
        Send send = replicaRequest.toSend(header);
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        return channel.buffer().limit();
    }

    @Benchmark
    public String testRequestToJson() {
        return RequestConvertToJson.request(consumerRequest).toString();
    }
}
