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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ResponseHeader;
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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FetchResponseBenchmark {
    @Param({"10", "500", "1000"})
    private int topicCount;

    @Param({"3", "10", "20"})
    private int partitionCount;

    LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData;

    ResponseHeader header;

    FetchResponse<MemoryRecords> fetchResponse;

    @Setup(Level.Trial)
    public void setup() {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord(1000, "key1".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(1001, "key2".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(1002, "key3".getBytes(StandardCharsets.UTF_8), "value3".getBytes(StandardCharsets.UTF_8)));

        this.responseData = new LinkedHashMap<>();
        for (int topicIdx = 0; topicIdx < topicCount; topicIdx++) {
            String topic = UUID.randomUUID().toString();
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                FetchResponse.PartitionData<MemoryRecords> partitionData = new FetchResponse.PartitionData<>(
                    Errors.NONE, 0, 0, 0, Optional.empty(), Collections.emptyList(), records);
                responseData.put(new TopicPartition(topic, partitionId), partitionData);
            }
        }

        this.header = new ResponseHeader(100, ApiKeys.FETCH.responseHeaderVersion(ApiKeys.FETCH.latestVersion()));
        this.fetchResponse = new FetchResponse<>(Errors.NONE, responseData, 0, 0);
    }

    @Benchmark
    public int testConstructFetchResponse() {
        FetchResponse<MemoryRecords> fetchResponse = new FetchResponse<>(Errors.NONE, responseData, 0, 0);
        return fetchResponse.responseData().size();
    }

    @Benchmark
    public int testSerializeFetchResponse() throws IOException {
        Send send = fetchResponse.toSend(header, ApiKeys.FETCH.latestVersion());
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        return channel.buffer().limit();
    }
}
