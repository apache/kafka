package org.apache.kafka.jmh.record;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ConsumerRecordsBenchmark {
    private ConsumerRecords<Integer, String> records;

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
    }

    @Benchmark
    public void records() {
        records.records("topic2");
    }

    @Benchmark
    public void recordsForEach() {
        records.recordsForEach("topic2");
    }

    @Benchmark
    public void recordsWithEntrySetAndStream() {
        records.recordsWithEntrySetAndStream("topic2");
    }
}
