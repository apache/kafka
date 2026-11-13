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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.kafka.clients.ClientsTestUtils.consumeRecords;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    }
)
public class ProducerIncrementalAllocationTest {

    private static final String TOPIC = "topic";
    private static final int NUM_RECORDS = 1000;
    private static final int VALUE_SIZE = 1000;

    /**
     * With the incremental allocation strategy, a batch spanning several chunks is sent straight from its
     * chunks. The broker must accept those batches, and a consumer must read back exactly what was sent.
     */
    @ClusterTest
    public void testSendBatchesSpanningSeveralChunks(ClusterInstance cluster) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        TopicPartition tp = new TopicPartition(TOPIC, 0);

        List<byte[]> values = new ArrayList<>(NUM_RECORDS);
        for (int i = 0; i < NUM_RECORDS; i++) {
            values.add(ByteBuffer.allocate(VALUE_SIZE).putInt(0, i).array());
        }

        Map<String, Object> producerProps = Map.of(
            ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG, ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL,
            ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024,
            ProducerConfig.LINGER_MS_CONFIG, 100);
        try (Producer<byte[], byte[]> producer = cluster.producer(producerProps)) {
            List<Future<RecordMetadata>> futures = new ArrayList<>(NUM_RECORDS);
            for (byte[] value : values) {
                futures.add(producer.send(new ProducerRecord<>(TOPIC, tp.partition(), null, value)));
            }
            for (int i = 0; i < NUM_RECORDS; i++) {
                assertEquals(i, futures.get(i).get().offset());
            }

            // Batches larger than a chunk span several chunks, so the multi-chunk send path was exercised.
            double batchSizeAvg = (double) metric(producer, "batch-size-avg").metricValue();
            assertTrue(batchSizeAvg > ChunkedRecordAccumulator.CHUNK_SIZE,
                "expected batches spanning several chunks, but the average batch size was " + batchSizeAvg);
        }

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of())) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            List<ConsumerRecord<byte[], byte[]>> records =
                consumeRecords(consumer, NUM_RECORDS, Integer.MAX_VALUE, TestUtils.DEFAULT_MAX_WAIT_MS);
            assertEquals(NUM_RECORDS, records.size());
            for (int i = 0; i < NUM_RECORDS; i++) {
                assertEquals(i, records.get(i).offset());
                assertArrayEquals(values.get(i), records.get(i).value());
            }
        }
    }

    private static Metric metric(Producer<byte[], byte[]> producer, String name) {
        for (Map.Entry<MetricName, ? extends Metric> entry : producer.metrics().entrySet()) {
            if (entry.getKey().name().equals(name) && entry.getKey().group().equals("producer-metrics")) {
                return entry.getValue();
            }
        }
        throw new AssertionError("producer metric " + name + " not found");
    }
}
