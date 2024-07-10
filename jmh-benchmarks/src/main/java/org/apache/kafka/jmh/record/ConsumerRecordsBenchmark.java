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
package org.apache.kafka.jmh.record;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class ConsumerRecordsBenchmark {
    private ConsumerRecords<Integer, String> records;
    private LegacyConsumerRecords<Integer, String> legacyRecords;

    @Setup(Level.Trial)
    public void setUp() {
        List<String> topics = Arrays.asList("topic1", "topic2", "topic3");
        int recordSize = 10000;
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> partitionToRecords = new LinkedHashMap<>();
        for (String topic : topics) {
            for (int partition = 0; partition < 10; partition++) {
                List<ConsumerRecord<Integer, String>> records = new ArrayList<>(recordSize);
                for (int offset = 0; offset < recordSize; offset++) {
                    records.add(
                        new ConsumerRecord<>(topic, partition, offset, 0L, TimestampType.CREATE_TIME,
                            0, 0, offset, String.valueOf(offset), new RecordHeaders(), Optional.empty())
                    );
                }
                partitionToRecords.put(new TopicPartition(topic, partition), records);
            }
        }

        records = new ConsumerRecords<>(partitionToRecords);
        legacyRecords = new LegacyConsumerRecords<>(partitionToRecords);
    }

    @Benchmark
    public void records(Blackhole blackhole) {
        blackhole.consume(records.records("topic2"));
    }

    @Benchmark
    public void iteratorRecords(Blackhole blackhole) {
        for (ConsumerRecord<Integer, String> record : records.records("topic2")) {
            blackhole.consume(record);
        }
    }

    @Benchmark
    public void recordsWithLegacyImplementation(Blackhole blackhole) {
        blackhole.consume(legacyRecords.records("topic2"));
    }

    @Benchmark
    public void iteratorRecordsByLegacyImplementation(Blackhole blackhole) {
        for (ConsumerRecord<Integer, String> record : legacyRecords.records("topic2")) {
            blackhole.consume(record);
        }
    }
}
