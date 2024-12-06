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

package org.apache.kafka.jmh.producer;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProducerRequestBenchmark {
    private static final int NUMBER_OF_PARTITIONS = 3;
    private static final int NUMBER_OF_RECORDS = 3;
    private static final List<ProduceRequestData.TopicProduceData> TOPIC_PRODUCE_DATA = Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName("tp")
            .setPartitionData(IntStream.range(0, NUMBER_OF_PARTITIONS).mapToObj(partitionIndex -> new ProduceRequestData.PartitionProduceData()
                .setIndex(partitionIndex)
                .setRecords(MemoryRecords.withRecords(Compression.NONE, IntStream.range(0, NUMBER_OF_RECORDS)
                    .mapToObj(recordIndex -> new SimpleRecord(100, "hello0".getBytes(StandardCharsets.UTF_8)))
                    .collect(Collectors.toList())
                    .toArray(new SimpleRecord[0]))))
                .collect(Collectors.toList()))
    );
    private static final ProduceRequestData PRODUCE_REQUEST_DATA = new ProduceRequestData()
            .setTimeoutMs(100)
            .setAcks((short) 1)
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(TOPIC_PRODUCE_DATA.iterator()));

    private static ProduceRequest request() {
        return ProduceRequest.forMagic(RecordBatch.CURRENT_MAGIC_VALUE, PRODUCE_REQUEST_DATA, false).build();
    }

    private static final ProduceRequest REQUEST = request();

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public ProduceRequest constructorProduceRequest() {
        return request();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public ProduceResponse constructorErrorResponse() {
        return REQUEST.getErrorResponse(0, Errors.INVALID_REQUEST.exception());
    }

}
