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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.ClientsTestUtils.awaitAssignment;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.consumeRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = PlaintextConsumerFetchTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
    }
)
public class PlaintextConsumerFetchTest {

    public static final int BROKER_COUNT = 3;
    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);
    private final TopicPartition tp2 = new TopicPartition(topic, 1);

    public PlaintextConsumerFetchTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        cluster.createTopic(topic, 2, (short) BROKER_COUNT);
    }

    @ClusterTest
    public void testClassicConsumerFetchInvalidOffset() {
        testFetchInvalidOffset(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchInvalidOffset() {
        testFetchInvalidOffset(GroupProtocol.CONSUMER);
    }

    private void testFetchInvalidOffset(GroupProtocol groupProtocol) {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "none"
        );
        try (var consumer = cluster.consumer(config)) {
            // produce one record
            var totalRecords = 2;
            sendRecords(cluster, tp, totalRecords);
            consumer.assign(List.of(tp));

            // poll should fail because there is no offset reset strategy set.
            // we fail only when resetting positions after coordinator is known, so using a long timeout.
            assertThrows(NoOffsetForPartitionException.class, () -> consumer.poll(Duration.ofMillis(15000)));

            // seek to out of range position
            var outOfRangePos = totalRecords + 1;
            consumer.seek(tp, outOfRangePos);
            var e = assertThrows(OffsetOutOfRangeException.class, () -> consumer.poll(Duration.ofMillis(20000)));
            var outOfRangePartitions = e.offsetOutOfRangePartitions();
            assertNotNull(outOfRangePartitions);
            assertEquals(1, outOfRangePartitions.size());
            assertEquals(outOfRangePos, outOfRangePartitions.get(tp).longValue());
        }
    }

    @ClusterTest
    public void testClassicConsumerFetchOutOfRangeOffsetResetConfigEarliest() throws InterruptedException {
        testFetchOutOfRangeOffsetResetConfigEarliest(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchOutOfRangeOffsetResetConfigEarliest() throws InterruptedException {
        testFetchOutOfRangeOffsetResetConfigEarliest(GroupProtocol.CONSUMER);
    }

    private void testFetchOutOfRangeOffsetResetConfigEarliest(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            // ensure no in-flight fetch request so that the offset can be reset immediately
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var totalRecords = 10;
            var startingTimestamp = 0;
            sendRecords(cluster, tp, totalRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            consumeAndVerifyRecords(consumer, tp, totalRecords, 0);
            // seek to out of range position
            var outOfRangePos = totalRecords + 1;
            consumer.seek(tp, outOfRangePos);
            // assert that poll resets to the beginning position
            consumeAndVerifyRecords(consumer, tp, 1, 0);
        }
    }

    @ClusterTest
    public void testClassicConsumerFetchOutOfRangeOffsetResetConfigLatest() throws InterruptedException {
        testFetchOutOfRangeOffsetResetConfigLatest(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchOutOfRangeOffsetResetConfigLatest() throws InterruptedException {
        testFetchOutOfRangeOffsetResetConfigLatest(GroupProtocol.CONSUMER);
    }

    private void testFetchOutOfRangeOffsetResetConfigLatest(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "latest",
            // ensure no in-flight fetch request so that the offset can be reset immediately
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config);
             Producer<byte[], byte[]> producer = cluster.producer()
        ) {
            var totalRecords = 10;
            var startingTimestamp = 0;
            sendRecords(producer, tp, totalRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            consumer.seek(tp, 0);
            
            // consume some, but not all the records
            consumeAndVerifyRecords(consumer, tp, totalRecords / 2, 0);
            // seek to out of range position
            var outOfRangePos = totalRecords + 17; // arbitrary, much higher offset
            consumer.seek(tp, outOfRangePos);
            // assert that poll resets to the ending position
            assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty());
            sendRecords(producer, tp, totalRecords, totalRecords);
            var nextRecord = consumer.poll(Duration.ofMillis(50)).iterator().next();
            // ensure the seek went to the last known record at the time of the previous poll
            assertEquals(totalRecords, nextRecord.offset());
        }
    }

    @ClusterTest
    public void testClassicConsumerFetchOutOfRangeOffsetResetConfigByDuration() throws InterruptedException {
        testFetchOutOfRangeOffsetResetConfigByDuration(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchOutOfRangeOffsetResetConfigByDuration() throws InterruptedException {
        testFetchOutOfRangeOffsetResetConfigByDuration(GroupProtocol.CONSUMER);
    }

    private void testFetchOutOfRangeOffsetResetConfigByDuration(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "by_duration:PT1H",
            // ensure no in-flight fetch request so that the offset can be reset immediately
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );
        try (Consumer<byte[], byte[]> consumer1 = cluster.consumer(config);
             Consumer<byte[], byte[]> consumer2 = cluster.consumer(config)
        ) {
            var totalRecords = 10;
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(cluster, tp, totalRecords, startingTimestamp);
            consumer1.assign(List.of(tp));
            consumeAndVerifyRecords(
                consumer1,
                tp,
                totalRecords,
                0,
                0,
                startingTimestamp
            );

            // seek to out of range position
            var outOfRangePos = totalRecords + 1;
            consumer1.seek(tp, outOfRangePos);
            // assert that poll resets to the beginning position
            consumeAndVerifyRecords(
                consumer1,
                tp,
                1,
                0,
                0,
                startingTimestamp
            );

            // Test the scenario where starting offset is earlier than the requested duration
            var totalRecords2 = 25;
            startingTimestamp = Instant.now().minus(Duration.ofHours(24)).toEpochMilli();

            // generate records with 1 hour interval for 1 day
            var hourMillis = Duration.ofHours(1).toMillis();
            sendRecords(cluster, tp2, totalRecords2, startingTimestamp, hourMillis);
            consumer2.assign(List.of(tp2));
            // consumer should read one record from last one hour
            consumeAndVerifyRecords(
                consumer2,
                tp2,
                1,
                24,
                24,
                startingTimestamp + 24 * hourMillis,
                hourMillis
            );

            // seek to out of range position
            outOfRangePos = totalRecords2 + 1;
            consumer2.seek(tp2, outOfRangePos);
            // assert that poll resets to the duration offset. consumer should read one record from last one hour
            consumeAndVerifyRecords(
                consumer2,
                tp2,
                1,
                24,
                24,
                startingTimestamp + 24 * hourMillis,
                hourMillis
            );
        }
    }

    @ClusterTest
    public void testClassicConsumerFetchRecordLargerThanFetchMaxBytes() throws InterruptedException {
        testFetchRecordLargerThanFetchMaxBytes(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchRecordLargerThanFetchMaxBytes() throws InterruptedException {
        testFetchRecordLargerThanFetchMaxBytes(GroupProtocol.CONSUMER);
    }

    private void testFetchRecordLargerThanFetchMaxBytes(GroupProtocol groupProtocol) throws InterruptedException {
        int maxFetchBytes = 10 * 1024;
        checkLargeRecord(Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            FETCH_MAX_BYTES_CONFIG, maxFetchBytes
        ), maxFetchBytes + 1);
    }

    @ClusterTest
    public void testClassicConsumerFetchRecordLargerThanMaxPartitionFetchBytes() throws InterruptedException {
        testFetchRecordLargerThanMaxPartitionFetchBytes(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchRecordLargerThanMaxPartitionFetchBytes() throws InterruptedException {
        testFetchRecordLargerThanMaxPartitionFetchBytes(GroupProtocol.CONSUMER);
    }

    private void testFetchRecordLargerThanMaxPartitionFetchBytes(GroupProtocol groupProtocol) throws InterruptedException {
        int maxFetchBytes = 10 * 1024;
        checkLargeRecord(Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            MAX_PARTITION_FETCH_BYTES_CONFIG, maxFetchBytes
        ), maxFetchBytes + 1);
    }

    private void checkLargeRecord(Map<String, Object> config, int producerRecordSize) throws InterruptedException {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config);
             Producer<byte[], byte[]> producer = cluster.producer()
        ) {
            // produce a record that is larger than the configured fetch size
            var record = new ProducerRecord<>(
                tp.topic(),
                tp.partition(),
                "key".getBytes(),
                new byte[producerRecordSize]
            );
            producer.send(record);

            // consuming a record that is too large should succeed since KIP-74
            consumer.assign(List.of(tp));
            var records = consumeRecords(consumer, 1);
            assertEquals(1, records.size());
            var consumerRecord = records.iterator().next();
            assertEquals(0L, consumerRecord.offset());
            assertEquals(tp.topic(), consumerRecord.topic());
            assertEquals(tp.partition(), consumerRecord.partition());
            assertArrayEquals(record.key(), consumerRecord.key());
            assertArrayEquals(record.value(), consumerRecord.value());
        }
    }

    @ClusterTest
    public void testClassicConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst() throws ExecutionException, InterruptedException {
        testFetchHonoursFetchSizeIfLargeRecordNotFirst(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst() throws ExecutionException, InterruptedException {
        testFetchHonoursFetchSizeIfLargeRecordNotFirst(GroupProtocol.CONSUMER);
    }

    private void testFetchHonoursFetchSizeIfLargeRecordNotFirst(GroupProtocol groupProtocol) throws ExecutionException, InterruptedException {
        int maxFetchBytes = 10 * 1024;
        checkFetchHonoursSizeIfLargeRecordNotFirst(Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            FETCH_MAX_BYTES_CONFIG, maxFetchBytes
        ), maxFetchBytes);
    }

    @ClusterTest
    public void testClassicConsumerFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst() throws ExecutionException, InterruptedException {
        testFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst() throws ExecutionException, InterruptedException {
        testFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst(GroupProtocol.CONSUMER);
    }

    private void testFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst(GroupProtocol groupProtocol) throws ExecutionException, InterruptedException {
        int maxFetchBytes = 10 * 1024;
        checkFetchHonoursSizeIfLargeRecordNotFirst(Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            MAX_PARTITION_FETCH_BYTES_CONFIG, maxFetchBytes
        ), maxFetchBytes);
    }

    private void checkFetchHonoursSizeIfLargeRecordNotFirst(
        Map<String, Object> config, 
        int largeProducerRecordSize
    ) throws ExecutionException, InterruptedException {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config);
             Producer<byte[], byte[]> producer = cluster.producer()
        ) {
            var smallRecord = new ProducerRecord<>(
                tp.topic(),
                tp.partition(),
                "small".getBytes(),
                "value".getBytes()
            );
            var largeRecord = new ProducerRecord<>(
                tp.topic(),
                tp.partition(),
                "large".getBytes(),
                new byte[largeProducerRecordSize]
            );

            producer.send(smallRecord).get();
            producer.send(largeRecord).get();

            // we should only get the small record in the first `poll`
            consumer.assign(List.of(tp));
            
            var records = consumeRecords(consumer, 1);
            assertEquals(1, records.size());
            var consumerRecord = records.iterator().next();
            assertEquals(0L, consumerRecord.offset());
            assertEquals(tp.topic(), consumerRecord.topic());
            assertEquals(tp.partition(), consumerRecord.partition());
            assertArrayEquals(smallRecord.key(), consumerRecord.key());
            assertArrayEquals(smallRecord.value(), consumerRecord.value());
        }
    }

    @ClusterTest
    public void testClassicConsumerLowMaxFetchSizeForRequestAndPartition() throws InterruptedException {
        testLowMaxFetchSizeForRequestAndPartition(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerLowMaxFetchSizeForRequestAndPartition() throws InterruptedException {
        testLowMaxFetchSizeForRequestAndPartition(GroupProtocol.CONSUMER);
    }

    private void testLowMaxFetchSizeForRequestAndPartition(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            // one of the effects of this is that there will be some log reads where `0 > remaining limit bytes < message size`
            // and we don't return the message because it's not the first message in the first non-empty partition of the fetch
            // this behaves a little different from when remaining limit bytes is 0, and it's important to test it
            FETCH_MAX_BYTES_CONFIG, 500,
            MAX_PARTITION_FETCH_BYTES_CONFIG, 100,
            // Avoid a rebalance while the records are being sent (the default is 6 seconds)
            MAX_POLL_INTERVAL_MS_CONFIG, 20000
        );
        
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config);
             Producer<byte[], byte[]> producer = cluster.producer()
        ) {
            var partitionCount = 30;
            var topics = List.of("topic1", "topic2", "topic3");

            for (var topicName : topics) {
                cluster.createTopic(topicName, partitionCount, (short) BROKER_COUNT);
            }

            Set<TopicPartition> partitions = new HashSet<>();
            for (var topic : topics) {
                for (var i = 0; i < partitionCount; i++) {
                    partitions.add(new TopicPartition(topic, i));
                }
            }

            assertEquals(0, consumer.assignment().size());
            consumer.subscribe(topics);
            awaitAssignment(consumer, partitions);

            List<ProducerRecord<byte[], byte[]>> producerRecords = new ArrayList<>();
            for (var partition : partitions) {
                producerRecords.addAll(sendRecords(producer, partition, partitionCount, System.currentTimeMillis(), -1));
            }

            List<ConsumerRecord<byte[], byte[]>> consumerRecords = consumeRecords(consumer, producerRecords.size());

            Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumedByPartition = new HashMap<>();
            for (var record : consumerRecords) {
                var tp = new TopicPartition(record.topic(), record.partition());
                consumedByPartition.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
            }

            Map<TopicPartition, List<ProducerRecord<byte[], byte[]>>> producedByPartition = new HashMap<>();
            for (var record : producerRecords) {
                var tp = new TopicPartition(record.topic(), record.partition());
                producedByPartition.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
            }

            for (var partition : partitions) {
                var produced = producedByPartition.getOrDefault(partition, List.of());
                var consumed = consumedByPartition.getOrDefault(partition, List.of());

                assertEquals(produced.size(), consumed.size(), "Records count mismatch for " + partition);

                for (var i = 0; i < produced.size(); i++) {
                    var producerRecord = produced.get(i);
                    var consumerRecord = consumed.get(i);

                    assertEquals(producerRecord.topic(), consumerRecord.topic());
                    assertEquals(producerRecord.partition(), consumerRecord.partition());
                    assertArrayEquals(producerRecord.key(), consumerRecord.key());
                    assertArrayEquals(producerRecord.value(), consumerRecord.value());
                    assertEquals(producerRecord.timestamp(), consumerRecord.timestamp());
                }
            }
        }
    }
}
