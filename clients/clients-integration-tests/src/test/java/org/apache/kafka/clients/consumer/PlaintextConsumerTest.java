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

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.ClientsTestUtils.TestConsumerReassignmentListener;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Flaky;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.quota.QuotaType;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.TOPIC;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.TP;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testClusterResourceListener;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testCoordinatorFailover;
import static org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testSimpleConsumption;
import static org.apache.kafka.clients.ClientsTestUtils.awaitAssignment;
import static org.apache.kafka.clients.ClientsTestUtils.awaitRebalance;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecordsWithTimeTypeLogAppend;
import static org.apache.kafka.clients.ClientsTestUtils.consumeRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendAndAwaitAsyncCommit;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_INSTANCE_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, value = "60000"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
    }
)
public class PlaintextConsumerTest {

    private final ClusterInstance cluster;
    public static final double EPSILON = 0.1;

    public PlaintextConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testClassicConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testClassicConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testClassicConsumerCoordinatorFailover() throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            SESSION_TIMEOUT_MS_CONFIG, 5001,
            HEARTBEAT_INTERVAL_MS_CONFIG, 1000,
            // Use higher poll timeout to avoid consumer leaving the group due to timeout
            MAX_POLL_INTERVAL_MS_CONFIG, 15000
        );
        testCoordinatorFailover(cluster, config);
    }

    @ClusterTest
    public void testAsyncConsumeCoordinatorFailover() throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            // Use higher poll timeout to avoid consumer leaving the group due to timeout
            MAX_POLL_INTERVAL_MS_CONFIG, 15000
        );
        testCoordinatorFailover(cluster, config);
    }

    @ClusterTest(
        brokers = 1,
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1")
        }
    )
    public void testClassicConsumerCloseOnBrokerShutdown() {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        );
        testConsumerCloseOnBrokerShutdown(config);
    }

    @ClusterTest(
        brokers = 1,
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1")
        }
    )
    public void testAsyncConsumerCloseOnBrokerShutdown() {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, false
        );
        // Disabling auto commit so that commitSync() does not block the close timeout.
        testConsumerCloseOnBrokerShutdown(config);
    }

    private void testConsumerCloseOnBrokerShutdown(Map<String, Object> consumerConfig) {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            consumer.subscribe(List.of(TOPIC));

            // Force consumer to discover coordinator by doing a poll
            // This ensures coordinator is discovered before we shutdown the broker
            consumer.poll(Duration.ofMillis(100));

            // Now shutdown broker.
            assertEquals(1, cluster.brokers().size());
            KafkaBroker broker = cluster.brokers().get(0);
            cluster.shutdownBroker(0);
            broker.awaitShutdown();

            // Do another poll to force the consumer to retry finding the coordinator.
            consumer.poll(Duration.ofMillis(100));

            // Close should not hang waiting for retries when broker is already down
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> consumer.close(),
                    "Consumer close should not wait for full timeout when broker is already shutdown");
        }
    }

    @ClusterTest
    public void testClassicConsumerHeaders() throws Exception {
        testHeaders(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerHeaders() throws Exception {
        testHeaders(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testHeaders(Map<String, Object> consumerConfig) throws Exception {
        var numRecords = 1;

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var record = new ProducerRecord<>(TP.topic(), TP.partition(), null, "key".getBytes(), "value".getBytes());
            record.headers().add("headerKey", "headerValue".getBytes());
            record.headers().add("headerKey2", "headerValue2".getBytes());
            record.headers().add("headerKey3", "headerValue3".getBytes());
            producer.send(record);
            producer.flush();

            assertEquals(0, consumer.assignment().size());
            consumer.assign(List.of(TP));
            assertEquals(1, consumer.assignment().size());

            consumer.seek(TP, 0);
            var records = consumeRecords(consumer, numRecords);
            assertEquals(numRecords, records.size());

            var header = records.get(0).headers().lastHeader("headerKey");
            assertEquals("headerValue", header == null ? null : new String(header.value()));

            // Test the order of headers in a record is preserved when producing and consuming
            Header[] headers = records.get(0).headers().toArray();
            assertEquals("headerKey", headers[0].key());
            assertEquals("headerKey2", headers[1].key());
            assertEquals("headerKey3", headers[2].key());
        }
    }

    @ClusterTest
    public void testClassicConsumerHeadersSerializerDeserializer() throws Exception {
        testHeadersSerializeDeserialize(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerHeadersSerializerDeserializer() throws Exception {
        testHeadersSerializeDeserialize(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testHeadersSerializeDeserialize(Map<String, Object> config) throws InterruptedException {
        var numRecords = 1;
        Map<String, Object> consumerConfig = new HashMap<>(config);
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, DeserializerImpl.class);
        Map<String, Object> producerConfig = Map.of(
            VALUE_SERIALIZER_CLASS_CONFIG, SerializerImpl.class.getName()
        );

        try (Producer<byte[], byte[]> producer = cluster.producer(producerConfig);
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            producer.send(new ProducerRecord<>(
                TP.topic(), 
                TP.partition(), 
                null, 
                "key".getBytes(), 
                "value".getBytes())
            );
            
            assertEquals(0, consumer.assignment().size());
            consumer.assign(List.of(TP));
            assertEquals(1, consumer.assignment().size());

            consumer.seek(TP, 0);
            assertEquals(numRecords, consumeRecords(consumer, numRecords).size());
        }
    }

    @ClusterTest
    public void testClassicConsumerAutoOffsetReset() throws Exception {
        testAutoOffsetReset(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerAutoOffsetReset() throws Exception {
        testAutoOffsetReset(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testAutoOffsetReset(Map<String, Object> consumerConfig) throws Exception {
        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, TP, 1, startingTimestamp);
            consumer.assign(List.of(TP));
            consumeAndVerifyRecords(consumer, TP, 1, 0, 0, startingTimestamp);
        }
    }

    @ClusterTest
    public void testClassicConsumerGroupConsumption() throws Exception {
        testGroupConsumption(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerGroupConsumption() throws Exception {
        testGroupConsumption(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testGroupConsumption(Map<String, Object> consumerConfig) throws Exception {
        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, TP, 10, startingTimestamp);
            consumer.subscribe(List.of(TOPIC));
            consumeAndVerifyRecords(consumer, TP, 1, 0, 0, startingTimestamp);
        }
    }

    @ClusterTest
    public void testClassicConsumerPartitionsFor() throws Exception {
        testPartitionsFor(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPartitionsFor() throws Exception {
        testPartitionsFor(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testPartitionsFor(Map<String, Object> consumerConfig) throws Exception {
        var numParts = 2;
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
        cluster.createTopic("part-test", numParts, (short) 1);

        try (var consumer = cluster.consumer(consumerConfig)) {
            var partitions = consumer.partitionsFor(TOPIC);
            assertNotNull(partitions);
            assertEquals(2, partitions.size());
        }
    }

    @ClusterTest
    public void testClassicConsumerPartitionsForAutoCreate() throws Exception {
        testPartitionsForAutoCreate(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPartitionsForAutoCreate() throws Exception {
        testPartitionsForAutoCreate(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testPartitionsForAutoCreate(Map<String, Object> consumerConfig) throws Exception {
        try (var consumer = cluster.consumer(consumerConfig)) {
            // First call would create the topic
            consumer.partitionsFor("non-exist-topic");
            TestUtils.waitForCondition(
                () -> !consumer.partitionsFor("non-exist-topic").isEmpty(), 
                "Timed out while awaiting non empty partitions."
            );
        }
    }

    @ClusterTest
    public void testClassicConsumerPartitionsForInvalidTopic() {
        testPartitionsForInvalidTopic(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPartitionsForInvalidTopic() {
        testPartitionsForInvalidTopic(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testPartitionsForInvalidTopic(Map<String, Object> consumerConfig) {
        try (var consumer = cluster.consumer(consumerConfig)) {
            assertThrows(InvalidTopicException.class, () -> consumer.partitionsFor(";3# ads,{234"));
        }
    }

    @ClusterTest
    public void testClassicConsumerSeek() throws Exception {
        testSeek(
            Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerSeek() throws Exception {
        testSeek(
            Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testSeek(Map<String, Object> consumerConfig) throws Exception {
        var totalRecords = 50;
        var mid = totalRecords / 2;
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var startingTimestamp = 0;
            sendRecords(producer, TP, totalRecords, startingTimestamp);

            consumer.assign(List.of(TP));
            consumer.seekToEnd(List.of(TP));
            assertEquals(totalRecords, consumer.position(TP));
            assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty());

            consumer.seekToBeginning(List.of(TP));
            assertEquals(0, consumer.position(TP));
            consumeAndVerifyRecords(consumer, TP, 1, 0, 0, startingTimestamp);

            consumer.seek(TP, mid);
            assertEquals(mid, consumer.position(TP));

            consumeAndVerifyRecords(consumer, TP, 1, mid, mid, mid);

            // Test seek compressed message
            var tp2 = new TopicPartition(TOPIC, 1);
            cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
            sendCompressedMessages(totalRecords, tp2);
            consumer.assign(List.of(tp2));

            consumer.seekToEnd(List.of(tp2));
            assertEquals(totalRecords, consumer.position(tp2));
            assertTrue(consumer.poll(Duration.ofMillis(50)).isEmpty());

            consumer.seekToBeginning(List.of(tp2));
            assertEquals(0L, consumer.position(tp2));
            consumeAndVerifyRecords(consumer, tp2, 1, 0);

            consumer.seek(tp2, mid);
            assertEquals(mid, consumer.position(tp2));
            consumeAndVerifyRecords(consumer, tp2, 1, mid, mid, mid);
        }
    }

    @ClusterTest
    public void testClassicConsumerPartitionPauseAndResume() throws Exception {
        testPartitionPauseAndResume(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPartitionPauseAndResume() throws Exception {
        testPartitionPauseAndResume(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testPartitionPauseAndResume(Map<String, Object> consumerConfig) throws Exception {
        var partitions = List.of(TP);
        var numRecords = 5;

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, TP, numRecords, startingTimestamp);

            consumer.assign(partitions);
            consumeAndVerifyRecords(consumer, TP, numRecords, 0, 0, startingTimestamp);
            consumer.pause(partitions);
            startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, TP, numRecords, startingTimestamp);
            assertTrue(consumer.poll(Duration.ofMillis(100)).isEmpty());
            consumer.resume(partitions);
            consumeAndVerifyRecords(consumer, TP, numRecords, 5, 0, startingTimestamp);
        }
    }

    @ClusterTest
    public void testClassicConsumerInterceptors() throws Exception {
        testInterceptors(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerInterceptors() throws Exception {
        testInterceptors(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testInterceptors(Map<String, Object> consumerConfig) throws Exception {
        var appendStr = "mock";
        MockConsumerInterceptor.resetCounters();
        MockProducerInterceptor.resetCounters();

        // create producer with interceptor
        Map<String, Object> producerConfig = Map.of(
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName(),
            KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            "mock.interceptor.append", appendStr
        );
        // create consumer with interceptor
        Map<String, Object> consumerConfigOverride = new HashMap<>(consumerConfig);
        consumerConfigOverride.put(INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());
        consumerConfigOverride.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigOverride.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        try (Producer<String, String> producer = cluster.producer(producerConfig);
             Consumer<String, String> consumer = cluster.consumer(consumerConfigOverride)
        ) {
            // produce records
            var numRecords = 10;
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (var i = 0; i < numRecords; i++) {
                Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>(TP.topic(), TP.partition(), "key " + i, "value " + i)
                );
                futures.add(future);
            }

            // Wait for all sends to complete
            futures.forEach(future -> assertDoesNotThrow(() -> future.get()));

            assertEquals(numRecords, MockProducerInterceptor.ONSEND_COUNT.intValue());
            assertEquals(numRecords, MockProducerInterceptor.ON_SUCCESS_COUNT.intValue());

            // send invalid record
            assertThrows(
                Throwable.class,
                () -> producer.send(null), 
                "Should not allow sending a null record"
            );
            assertEquals(
                1, 
                MockProducerInterceptor.ON_ERROR_COUNT.intValue(), 
                "Interceptor should be notified about exception"
            );
            assertEquals(
                0, 
                MockProducerInterceptor.ON_ERROR_WITH_METADATA_COUNT.intValue(),
                "Interceptor should not receive metadata with an exception when record is null"
            );
            
            consumer.assign(List.of(TP));
            consumer.seek(TP, 0);

            // consume and verify that values are modified by interceptors
            var records = consumeRecords(consumer, numRecords);
            for (var i = 0; i < numRecords; i++) {
                ConsumerRecord<String, String> record = records.get(i);
                assertEquals("key " + i, record.key());
                assertEquals(("value " + i + appendStr).toUpperCase(Locale.ROOT), record.value());
            }

            // commit sync and verify onCommit is called
            var commitCountBefore = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue();
            consumer.commitSync(Map.of(TP, new OffsetAndMetadata(2L, "metadata")));
            OffsetAndMetadata metadata = consumer.committed(Set.of(TP)).get(TP);
            assertEquals(2, metadata.offset());
            assertEquals("metadata", metadata.metadata());
            assertEquals(commitCountBefore + 1, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue());

            // commit async and verify onCommit is called
            var offsetsToCommit = Map.of(TP, new OffsetAndMetadata(5L, null));
            sendAndAwaitAsyncCommit(consumer, Optional.of(offsetsToCommit));
            metadata = consumer.committed(Set.of(TP)).get(TP);
            assertEquals(5, metadata.offset());
            // null metadata will be converted to an empty string
            assertEquals("", metadata.metadata());
            assertEquals(commitCountBefore + 2, MockConsumerInterceptor.ON_COMMIT_COUNT.intValue());
        }
        // cleanup
        MockConsumerInterceptor.resetCounters();
        MockProducerInterceptor.resetCounters();
    }

    @ClusterTest
    public void testClassicConsumerInterceptorsWithWrongKeyValue() throws Exception {
        testInterceptorsWithWrongKeyValue(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerInterceptorsWithWrongKeyValue() throws Exception {
        testInterceptorsWithWrongKeyValue(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testInterceptorsWithWrongKeyValue(Map<String, Object> consumerConfig) throws Exception {
        var appendStr = "mock";
        // create producer with interceptor that has different key and value types from the producer
        Map<String, Object> producerConfig = Map.of(
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName(),
            "mock.interceptor.append", appendStr
        );
        // create consumer with interceptor that has different key and value types from the consumer
        Map<String, Object> consumerConfigOverride = new HashMap<>(consumerConfig);
        consumerConfigOverride.put(INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());

        try (Producer<byte[], byte[]> producer = cluster.producer(producerConfig);
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfigOverride)
        ) {
            // producing records should succeed
            producer.send(new ProducerRecord<>(
                TP.topic(),
                TP.partition(),
                "key".getBytes(),
                "value will not be modified".getBytes()
            ));

            consumer.assign(List.of(TP));
            consumer.seek(TP, 0);
            // consume and verify that values are not modified by interceptors -- their exceptions are caught and logged, but not propagated
            var records = consumeRecords(consumer, 1);
            var record = records.get(0);
            assertEquals("value will not be modified", new String(record.value()));
        }
    }

    @ClusterTest
    public void testClassicConsumerConsumeMessagesWithCreateTime() throws Exception {
        testConsumeMessagesWithCreateTime(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerConsumeMessagesWithCreateTime() throws Exception {
        testConsumeMessagesWithCreateTime(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testConsumeMessagesWithCreateTime(Map<String, Object> consumerConfig) throws Exception {
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
        var numRecords = 50;
        var tp2 = new TopicPartition(TOPIC, 1);

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            // Test non-compressed messages
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, TP, numRecords, startingTimestamp);
            consumer.assign(List.of(TP));
            consumeAndVerifyRecords(consumer, TP, numRecords, 0, 0, startingTimestamp);

            // Test compressed messages
            sendCompressedMessages(numRecords, tp2);
            consumer.assign(List.of(tp2));
            consumeAndVerifyRecords(consumer, tp2, numRecords, 0, 0, 0);
        }
    }

    @ClusterTest
    public void testClassicConsumerConsumeMessagesWithLogAppendTime() throws Exception {
        testConsumeMessagesWithLogAppendTime(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerConsumeMessagesWithLogAppendTime() throws Exception {
        testConsumeMessagesWithLogAppendTime(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testConsumeMessagesWithLogAppendTime(Map<String, Object> consumerConfig) throws Exception {
        var topicName = "testConsumeMessagesWithLogAppendTime";
        var startTime = System.currentTimeMillis();
        var numRecords = 50;
        cluster.createTopic(topicName, 2, (short) 2, Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime"));

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            // Test non-compressed messages
            var tp1 = new TopicPartition(topicName, 0);
            sendRecords(cluster, tp1, numRecords);
            consumer.assign(List.of(tp1));
            consumeAndVerifyRecordsWithTimeTypeLogAppend(consumer, tp1, numRecords, startTime);

            // Test compressed messages
            var tp2 = new TopicPartition(topicName, 1);
            sendCompressedMessages(numRecords, tp2);
            consumer.assign(List.of(tp2));
            consumeAndVerifyRecordsWithTimeTypeLogAppend(consumer, tp2, numRecords, startTime);
        }
    }

    @ClusterTest
    public void testClassicConsumerListTopics() throws Exception {
        testListTopics(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerListTopics() throws Exception {
        testListTopics(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testListTopics(Map<String, Object> consumerConfig) throws Exception {
        var numParts = 2;
        var topic1 = "part-test-topic-1";
        var topic2 = "part-test-topic-2";
        var topic3 = "part-test-topic-3";
        cluster.createTopic(topic1, numParts, (short) 1);
        cluster.createTopic(topic2, numParts, (short) 1);
        cluster.createTopic(topic3, numParts, (short) 1);

        sendRecords(cluster, new TopicPartition(topic1, 0), 1);

        try (var consumer = cluster.consumer(consumerConfig)) {
            // consumer some messages, and we can list the internal topic __consumer_offsets
            consumer.subscribe(List.of(topic1));
            consumer.poll(Duration.ofMillis(100));
            var topics = consumer.listTopics();
            assertNotNull(topics);
            assertEquals(4, topics.size());
            assertEquals(2, topics.get(topic1).size());
            assertEquals(2, topics.get(topic2).size());
            assertEquals(2, topics.get(topic3).size());
        }
    }

    @ClusterTest
    public void testClassicConsumerPauseStateNotPreservedByRebalance() throws Exception {
        testPauseStateNotPreservedByRebalance(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            SESSION_TIMEOUT_MS_CONFIG, 100,
            HEARTBEAT_INTERVAL_MS_CONFIG, 30
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPauseStateNotPreservedByRebalance() throws Exception {
        testPauseStateNotPreservedByRebalance(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testPauseStateNotPreservedByRebalance(Map<String, Object> consumerConfig) throws Exception {
        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, TP, 5, startingTimestamp);
            consumer.subscribe(List.of(TOPIC));
            consumeAndVerifyRecords(consumer, TP, 5, 0, 0, startingTimestamp);
            consumer.pause(List.of(TP));

            // subscribe to a new topic to trigger a rebalance
            consumer.subscribe(List.of("topic2"));

            // after rebalance, our position should be reset and our pause state lost,
            // so we should be able to consume from the beginning
            consumeAndVerifyRecords(consumer, TP, 0, 5, 0, startingTimestamp);
        }
    }

    @ClusterTest
    public void testClassicConsumerPerPartitionLeadMetricsCleanUpWithSubscribe() throws Exception {
        String consumerClientId = "testClassicConsumerPerPartitionLeadMetricsCleanUpWithSubscribe";
        testPerPartitionLeadMetricsCleanUpWithSubscribe(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    @ClusterTest
    public void testAsyncConsumerPerPartitionLeadMetricsCleanUpWithSubscribe() throws Exception {
        String consumerClientId = "testAsyncConsumerPerPartitionLeadMetricsCleanUpWithSubscribe";
        testPerPartitionLeadMetricsCleanUpWithSubscribe(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    private void testPerPartitionLeadMetricsCleanUpWithSubscribe(
        Map<String, Object> consumerConfig,
        String consumerClientId
    ) throws Exception {
        var numMessages = 1000;
        var topic2 = "topic2";
        var tp2 = new TopicPartition(TOPIC, 1);
        cluster.createTopic(topic2, 2, (short) BROKER_COUNT);
        
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            // send some messages.
            sendRecords(cluster, TP, numMessages);

            // Test subscribe
            // Create a consumer and consumer some messages.
            var listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(TOPIC, topic2), listener);
            var records = awaitNonEmptyRecords(consumer, TP);
            assertEquals(1, listener.callsToAssigned, "should be assigned once");

            // Verify the metric exist.
            Map<String, String> tags1 = Map.of(
                "client-id", consumerClientId,
                "topic", TP.topic(),
                "partition", String.valueOf(TP.partition())
            );

            Map<String, String> tags2 = Map.of(
                "client-id", consumerClientId,
                "topic", tp2.topic(),
                "partition", String.valueOf(tp2.partition())
            );

            var fetchLead0 = consumer.metrics().get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags1));
            assertNotNull(fetchLead0);
            assertEquals((double) records.count(), fetchLead0.metricValue(), "The lead should be " + records.count());

            // Remove topic from subscription
            consumer.subscribe(List.of(topic2), listener);
            awaitRebalance(consumer, listener);

            // Verify the metric has gone
            assertNull(consumer.metrics().get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags1)));
            assertNull(consumer.metrics().get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags2)));
        }
    }

    @ClusterTest
    public void testClassicConsumerPerPartitionLagMetricsCleanUpWithSubscribe() throws Exception {
        String consumerClientId = "testClassicConsumerPerPartitionLagMetricsCleanUpWithSubscribe";
        testPerPartitionLagMetricsCleanUpWithSubscribe(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    @ClusterTest
    public void testAsyncConsumerPerPartitionLagMetricsCleanUpWithSubscribe() throws Exception {
        String consumerClientId = "testAsyncConsumerPerPartitionLagMetricsCleanUpWithSubscribe";
        testPerPartitionLagMetricsCleanUpWithSubscribe(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    private void testPerPartitionLagMetricsCleanUpWithSubscribe(
        Map<String, Object> consumerConfig,
        String consumerClientId
    ) throws Exception {
        int numMessages = 1000;
        var topic2 = "topic2";
        var tp2 = new TopicPartition(TOPIC, 1);
        cluster.createTopic(topic2, 2, (short) BROKER_COUNT);

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            // send some messages.
            sendRecords(cluster, TP, numMessages);
            
            // Test subscribe
            // Create a consumer and consumer some messages.
            var listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(TOPIC, topic2), listener);
            var records = awaitNonEmptyRecords(consumer, TP);
            assertEquals(1, listener.callsToAssigned, "should be assigned once");

            // Verify the metric exist.
            Map<String, String> tags1 = Map.of(
                "client-id", consumerClientId,
                "topic", TP.topic(),
                "partition", String.valueOf(TP.partition())
            );

            Map<String, String> tags2 = Map.of(
                "client-id", consumerClientId,
                "topic", tp2.topic(),
                "partition", String.valueOf(tp2.partition())
            );

            var fetchLag0 = consumer.metrics().get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags1));
            assertNotNull(fetchLag0);
            var expectedLag = numMessages - records.count();
            assertEquals(expectedLag, (double) fetchLag0.metricValue(), EPSILON, "The lag should be " + expectedLag);

            // Remove topic from subscription
            consumer.subscribe(List.of(topic2), listener);
            awaitRebalance(consumer, listener);

            // Verify the metric has gone
            assertNull(consumer.metrics().get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags1)));
            assertNull(consumer.metrics().get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags2)));
        }
    }

    @ClusterTest
    public void testClassicConsumerPerPartitionLeadMetricsCleanUpWithAssign() throws Exception {
        String consumerClientId = "testClassicConsumerPerPartitionLeadMetricsCleanUpWithAssign";
        testPerPartitionLeadMetricsCleanUpWithAssign(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    @ClusterTest
    public void testAsyncConsumerPerPartitionLeadMetricsCleanUpWithAssign() throws Exception {
        String consumerClientId = "testAsyncConsumerPerPartitionLeadMetricsCleanUpWithAssign";
        testPerPartitionLeadMetricsCleanUpWithAssign(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    private void testPerPartitionLeadMetricsCleanUpWithAssign(
        Map<String, Object> consumerConfig,
        String consumerClientId
    ) throws Exception {
        var numMessages = 1000;
        var tp2 = new TopicPartition(TOPIC, 1);
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
        
        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            // Test assign send some messages.
            sendRecords(producer, TP, numMessages, System.currentTimeMillis());
            sendRecords(producer, tp2, numMessages, System.currentTimeMillis());
            
            consumer.assign(List.of(TP));
            var records = awaitNonEmptyRecords(consumer, TP);

            // Verify the metric exist.
            Map<String, String> tags = Map.of(
                "client-id", consumerClientId,
                "topic", TP.topic(),
                "partition", String.valueOf(TP.partition()) 
            );

            var fetchLead = consumer.metrics().get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags));
            assertNotNull(fetchLead);
            assertEquals((double) records.count(), fetchLead.metricValue(), "The lead should be " + records.count());

            consumer.assign(List.of(tp2));
            awaitNonEmptyRecords(consumer, tp2);
            assertNull(consumer.metrics().get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags)));
        }
    }

    @ClusterTest
    public void testClassicConsumerPerPartitionLagMetricsCleanUpWithAssign() throws Exception {
        String consumerClientId = "testClassicConsumerPerPartitionLagMetricsCleanUpWithAssign";
        testPerPartitionLagMetricsCleanUpWithAssign(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    @ClusterTest
    public void testAsyncConsumerPerPartitionLagMetricsCleanUpWithAssign() throws Exception {
        String consumerClientId = "testAsyncConsumerPerPartitionLagMetricsCleanUpWithAssign";
        testPerPartitionLagMetricsCleanUpWithAssign(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId
        ), consumerClientId);
    }

    private void testPerPartitionLagMetricsCleanUpWithAssign(
        Map<String, Object> consumerConfig,
        String consumerClientId
    ) throws Exception {
        var numMessages = 1000;
        var tp2 = new TopicPartition(TOPIC, 1);
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            // Test assign send some messages.
            sendRecords(producer, TP, numMessages, System.currentTimeMillis());
            sendRecords(producer, tp2, numMessages, System.currentTimeMillis());

            consumer.assign(List.of(TP));
            var records = awaitNonEmptyRecords(consumer, TP);

            // Verify the metric exist.
            Map<String, String> tags = Map.of(
                "client-id", consumerClientId,
                "topic", TP.topic(),
                "partition", String.valueOf(TP.partition())
            );

            var fetchLag = consumer.metrics().get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags));
            assertNotNull(fetchLag);

            var expectedLag = numMessages - records.count();
            assertEquals(expectedLag, (double) fetchLag.metricValue(), EPSILON, "The lag should be " + expectedLag);
            consumer.assign(List.of(tp2));
            awaitNonEmptyRecords(consumer, tp2);
            assertNull(consumer.metrics().get(new MetricName(TP + ".records-lag", "consumer-fetch-manager-metrics", "", tags)));
            assertNull(consumer.metrics().get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags)));
        }
    }

    @ClusterTest
    public void testClassicConsumerPerPartitionLagMetricsWhenReadCommitted() throws Exception {
        String consumerClientId = "testClassicConsumerPerPartitionLagMetricsWhenReadCommitted";
        testPerPartitionLagMetricsWhenReadCommitted(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId,
            ISOLATION_LEVEL_CONFIG, "read_committed"
        ), consumerClientId);
    }

    @ClusterTest
    public void testAsyncConsumerPerPartitionLagMetricsWhenReadCommitted() throws Exception {
        String consumerClientId = "testAsyncConsumerPerPartitionLagMetricsWhenReadCommitted";
        testPerPartitionLagMetricsWhenReadCommitted(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId,
            ISOLATION_LEVEL_CONFIG, "read_committed"
        ), consumerClientId);
    }

    private void testPerPartitionLagMetricsWhenReadCommitted(
        Map<String, Object> consumerConfig,
        String consumerClientId
    ) throws Exception {
        var numMessages = 1000;
        var tp2 = new TopicPartition(TOPIC, 1);
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            // Test assign send some messages.
            sendRecords(producer, TP, numMessages, System.currentTimeMillis());
            sendRecords(producer, tp2, numMessages, System.currentTimeMillis());

            consumer.assign(List.of(TP));
            awaitNonEmptyRecords(consumer, TP);

            // Verify the metric exist.
            Map<String, String> tags = Map.of(
                "client-id", consumerClientId,
                "topic", TP.topic(),
                "partition", String.valueOf(TP.partition())
            );

            var fetchLag = consumer.metrics().get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags));
            assertNotNull(fetchLag);
        }
    }

    @ClusterTest
    public void testClassicConsumerQuotaMetricsNotCreatedIfNoQuotasConfigured() throws Exception {
        var consumerClientId = "testClassicConsumerQuotaMetricsNotCreatedIfNoQuotasConfigured";
        testQuotaMetricsNotCreatedIfNoQuotasConfigured(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId,
            ISOLATION_LEVEL_CONFIG, "read_committed"
        ), consumerClientId);
    }

    @ClusterTest
    public void testAsyncConsumerQuotaMetricsNotCreatedIfNoQuotasConfigured() throws Exception {
        var consumerClientId = "testAsyncConsumerQuotaMetricsNotCreatedIfNoQuotasConfigured";
        testQuotaMetricsNotCreatedIfNoQuotasConfigured(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, consumerClientId,
            CLIENT_ID_CONFIG, consumerClientId,
            ISOLATION_LEVEL_CONFIG, "read_committed"
        ), consumerClientId);
    }

    private void testQuotaMetricsNotCreatedIfNoQuotasConfigured(
        Map<String, Object> consumerConfig, 
        String consumerClientId
    ) throws Exception {
        var producerClientId = UUID.randomUUID().toString();
        var numRecords = 1000;
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);

        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of(ProducerConfig.CLIENT_ID_CONFIG, producerClientId));
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, TP, numRecords, startingTimestamp);

            consumer.assign(List.of(TP));
            consumer.seek(TP, 0);
            consumeAndVerifyRecords(consumer, TP, numRecords, 0, 0, startingTimestamp);
            
            var brokers = cluster.brokers().values();
            brokers.forEach(broker -> assertNoMetric(broker, "byte-rate", QuotaType.PRODUCE, producerClientId));
            brokers.forEach(broker -> assertNoMetric(broker, "throttle-time", QuotaType.PRODUCE, producerClientId));
            brokers.forEach(broker -> assertNoMetric(broker, "byte-rate", QuotaType.FETCH, consumerClientId));
            brokers.forEach(broker -> assertNoMetric(broker, "throttle-time", QuotaType.FETCH, consumerClientId));
            brokers.forEach(broker -> assertNoMetric(broker, "request-time", QuotaType.REQUEST, producerClientId));
            brokers.forEach(broker -> assertNoMetric(broker, "throttle-time", QuotaType.REQUEST, producerClientId));
            brokers.forEach(broker -> assertNoMetric(broker, "request-time", QuotaType.REQUEST, consumerClientId));
            brokers.forEach(broker -> assertNoMetric(broker, "throttle-time", QuotaType.REQUEST, consumerClientId));
        }
    }

    private void assertNoMetric(KafkaBroker broker, String name, QuotaType quotaType, String clientId) {
        var metricName = broker.metrics().metricName(name, quotaType.toString(), "", "user", "", "client-id", clientId);
        assertNull(broker.metrics().metric(metricName), "Metric should not have been created " + metricName);
    }
    
    @ClusterTest
    public void testClassicConsumerSeekThrowsIllegalStateIfPartitionsNotAssigned() throws Exception {
        testSeekThrowsIllegalStateIfPartitionsNotAssigned(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerSeekThrowsIllegalStateIfPartitionsNotAssigned() throws Exception {
        testSeekThrowsIllegalStateIfPartitionsNotAssigned(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testSeekThrowsIllegalStateIfPartitionsNotAssigned(Map<String, Object> consumerConfig) throws Exception {
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
        try (var consumer = cluster.consumer(consumerConfig)) {
            var e = assertThrows(IllegalStateException.class, () -> consumer.seekToEnd(List.of(TP)));
            assertEquals("No current assignment for partition " + TP, e.getMessage());
        }
    }

    @ClusterTest
    public void testClassicConsumingWithNullGroupId() throws Exception {
        testConsumingWithNullGroupId(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()
        ));
    }

    @ClusterTest
    public void testAsyncConsumerConsumingWithNullGroupId() throws Exception {
        testConsumingWithNullGroupId(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()
        ));
    }

    private void testConsumingWithNullGroupId(Map<String, Object> consumerConfig) throws Exception {
        var partition = 0;
        cluster.createTopic(TOPIC, 1, (short) 1);

        // consumer 1 uses the default group id and consumes from earliest offset
        Map<String, Object> consumer1Config = new HashMap<>(consumerConfig);
        consumer1Config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer1Config.put(CLIENT_ID_CONFIG, "consumer1");

        // consumer 2 uses the default group id and consumes from latest offset
        Map<String, Object> consumer2Config = new HashMap<>(consumerConfig);
        consumer2Config.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer2Config.put(CLIENT_ID_CONFIG, "consumer2");

        // consumer 3 uses the default group id and starts from an explicit offset
        Map<String, Object> consumer3Config = new HashMap<>(consumerConfig);
        consumer3Config.put(CLIENT_ID_CONFIG, "consumer3");

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer1 = new KafkaConsumer<>(consumer1Config);
             Consumer<byte[], byte[]> consumer2 = new KafkaConsumer<>(consumer2Config);
             Consumer<byte[], byte[]> consumer3 = new KafkaConsumer<>(consumer3Config)
        ) {
            producer.send(new ProducerRecord<>(TOPIC, partition, "k1".getBytes(), "v1".getBytes())).get();
            producer.send(new ProducerRecord<>(TOPIC, partition, "k2".getBytes(), "v2".getBytes())).get();
            producer.send(new ProducerRecord<>(TOPIC, partition, "k3".getBytes(), "v3".getBytes())).get();

            consumer1.assign(List.of(TP));
            consumer2.assign(List.of(TP));
            consumer3.assign(List.of(TP));
            consumer3.seek(TP, 1);

            var numRecords1 = consumer1.poll(Duration.ofMillis(5000)).count();
            assertThrows(InvalidGroupIdException.class, consumer1::commitSync);
            assertThrows(InvalidGroupIdException.class, () -> consumer2.committed(Set.of(TP)));

            var numRecords2 = consumer2.poll(Duration.ofMillis(5000)).count();
            var numRecords3 = consumer3.poll(Duration.ofMillis(5000)).count();

            consumer1.unsubscribe();
            consumer2.unsubscribe();
            consumer3.unsubscribe();

            assertTrue(consumer1.assignment().isEmpty());
            assertTrue(consumer2.assignment().isEmpty());
            assertTrue(consumer3.assignment().isEmpty());

            consumer1.close();
            consumer2.close();
            consumer3.close();

            assertEquals(3, numRecords1, "Expected consumer1 to consume from earliest offset");
            assertEquals(0, numRecords2, "Expected consumer2 to consume from latest offset");
            assertEquals(2, numRecords3, "Expected consumer3 to consume from offset 1");
        }
    }

    @ClusterTest
    public void testClassicConsumerNullGroupIdNotSupportedIfCommitting() throws Exception {
        testNullGroupIdNotSupportedIfCommitting(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            CLIENT_ID_CONFIG, "consumer1"
        ));
    }

    @ClusterTest
    public void testAsyncConsumerNullGroupIdNotSupportedIfCommitting() throws Exception {
        testNullGroupIdNotSupportedIfCommitting(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            CLIENT_ID_CONFIG, "consumer1"
        ));
    }

    private void testNullGroupIdNotSupportedIfCommitting(Map<String, Object> consumerConfig) throws Exception {
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
        try (var consumer = new KafkaConsumer<>(consumerConfig)) {
            consumer.assign(List.of(TP));
            assertThrows(InvalidGroupIdException.class, consumer::commitSync);
        }
    }

    @ClusterTest
    public void testClassicConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart() throws Exception {
        testStaticConsumerDetectsNewPartitionCreatedAfterRestart(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, "my-group-id",
            GROUP_INSTANCE_ID_CONFIG, "my-instance-id",
            METADATA_MAX_AGE_CONFIG, 100,
            MAX_POLL_INTERVAL_MS_CONFIG, 6000
        ));
    }

    @ClusterTest
    public void testAsyncConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart() throws Exception {
        testStaticConsumerDetectsNewPartitionCreatedAfterRestart(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, "my-group-id",
            GROUP_INSTANCE_ID_CONFIG, "my-instance-id",
            METADATA_MAX_AGE_CONFIG, 100,
            MAX_POLL_INTERVAL_MS_CONFIG, 6000
        ));
    }

    private void testStaticConsumerDetectsNewPartitionCreatedAfterRestart(Map<String, Object> consumerConfig) throws Exception {
        var foo = "foo";
        var foo0 = new TopicPartition(foo, 0);
        var foo1 = new TopicPartition(foo, 1);
        cluster.createTopic(foo, 1, (short) 1);

        try (Consumer<byte[], byte[]> consumer1 = cluster.consumer(consumerConfig);
             Consumer<byte[], byte[]> consumer2 = cluster.consumer(consumerConfig);
             var admin = cluster.admin()
        ) {
            consumer1.subscribe(List.of(foo));
            awaitAssignment(consumer1, Set.of(foo0));
            consumer1.close();

            consumer2.subscribe(List.of(foo));
            awaitAssignment(consumer2, Set.of(foo0));

            admin.createPartitions(Map.of(foo, NewPartitions.increaseTo(2))).all().get();
            awaitAssignment(consumer2, Set.of(foo0, foo1));
        }
    }

    @ClusterTest
    public void testClassicConsumerEndOffsets() throws Exception {
        testEndOffsets(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            METADATA_MAX_AGE_CONFIG, 100,
            MAX_POLL_INTERVAL_MS_CONFIG, 6000
        ));
    }

    @ClusterTest
    public void testAsyncConsumerEndOffsets() throws Exception {
        testEndOffsets(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            METADATA_MAX_AGE_CONFIG, 100,
            MAX_POLL_INTERVAL_MS_CONFIG, 6000
        ));
    }

    private void testEndOffsets(Map<String, Object> consumerConfig) throws Exception {
        var numRecords = 10000;
        var tp2 = new TopicPartition(TOPIC, 1);
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            var startingTimestamp = System.currentTimeMillis();
            for (var i = 0; i < numRecords; i++) {
                var timestamp = startingTimestamp + (long) i;
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    TP.topic(),
                    TP.partition(),
                    timestamp,
                    ("key " + i).getBytes(),
                    ("value " + i).getBytes()
                );
                producer.send(record);
            }
            producer.flush();

            consumer.subscribe(List.of(TOPIC));
            awaitAssignment(consumer, Set.of(TP, tp2));

            var endOffsets = consumer.endOffsets(Set.of(TP));
            assertEquals(numRecords, endOffsets.get(TP));
        }
    }

    @ClusterTest
    public void testClassicConsumerFetchOffsetsForTime() throws Exception {
        testFetchOffsetsForTime(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerFetchOffsetsForTime() throws Exception {
        testFetchOffsetsForTime(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testFetchOffsetsForTime(Map<String, Object> consumerConfig) throws Exception {
        var numPartitions = 2;
        var tp2 = new TopicPartition(TOPIC, 1);
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);

        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)
        ) {
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (int part = 0, i = 0; part < numPartitions; part++, i++) {
                var tp = new TopicPartition(TOPIC, part);
                // key, val, and timestamp equal to the sequence number.
                sendRecords(producer, tp, 100, 0);
                timestampsToSearch.put(tp, i * 20L);
            }
            // Test negative target time
            assertThrows(IllegalArgumentException.class, () -> consumer.offsetsForTimes(Map.of(TP, -1L)));
            var timestampOffsets = consumer.offsetsForTimes(timestampsToSearch);

            var timestampTp0 = timestampOffsets.get(TP);
            assertEquals(0, timestampTp0.offset());
            assertEquals(0, timestampTp0.timestamp());
            assertEquals(Optional.of(0), timestampTp0.leaderEpoch());

            var timestampTp1 = timestampOffsets.get(tp2);
            assertEquals(20, timestampTp1.offset());
            assertEquals(20, timestampTp1.timestamp());
            assertEquals(Optional.of(0), timestampTp1.leaderEpoch());
        }
    }
    
    @ClusterTest
    public void testClassicConsumerPositionRespectsTimeout() {
        testPositionRespectsTimeout(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPositionRespectsTimeout() {
        testPositionRespectsTimeout(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testPositionRespectsTimeout(Map<String, Object> consumerConfig) {
        var topicPartition = new TopicPartition(TOPIC, 15);
        try (var consumer = cluster.consumer(consumerConfig)) {
            consumer.assign(List.of(topicPartition));
            // When position() is called for a topic/partition that doesn't exist, the consumer will repeatedly update the
            // local metadata. However, it should give up after the user-supplied timeout has past.
            assertThrows(TimeoutException.class, () -> consumer.position(topicPartition, Duration.ofSeconds(3)));
        }
    }
    
    @ClusterTest
    public void testClassicConsumerPositionRespectsWakeup() {
        testPositionRespectsWakeup(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPositionRespectsWakeup() {
        testPositionRespectsWakeup(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testPositionRespectsWakeup(Map<String, Object> consumerConfig) {
        var topicPartition = new TopicPartition(TOPIC, 15);
        try (var consumer = cluster.consumer(consumerConfig)) {
            consumer.assign(List.of(topicPartition));
            CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.SECONDS.sleep(1);
                    consumer.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThrows(WakeupException.class, () -> consumer.position(topicPartition, Duration.ofSeconds(3)));
        }
    }
    
    @ClusterTest
    public void testClassicConsumerPositionWithErrorConnectionRespectsWakeup() {
        testPositionWithErrorConnectionRespectsWakeup(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            // make sure the connection fails
            BOOTSTRAP_SERVERS_CONFIG, "localhost:12345"
        ));
    }

    @ClusterTest
    public void testAsyncConsumerPositionWithErrorConnectionRespectsWakeup() {
        testPositionWithErrorConnectionRespectsWakeup(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            // make sure the connection fails
            BOOTSTRAP_SERVERS_CONFIG, "localhost:12345"
        ));
    }

    private void testPositionWithErrorConnectionRespectsWakeup(Map<String, Object> consumerConfig) {
        var topicPartition = new TopicPartition(TOPIC, 15);
        try (var consumer = cluster.consumer(consumerConfig)) {
            consumer.assign(List.of(topicPartition));
            CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.SECONDS.sleep(1);
                    consumer.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThrows(WakeupException.class, () -> consumer.position(topicPartition, Duration.ofSeconds(100)));
        }
    }

    @Flaky("KAFKA-18031")
    @ClusterTest
    public void testClassicConsumerCloseLeavesGroupOnInterrupt() throws Exception {
        testCloseLeavesGroupOnInterrupt(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            GROUP_ID_CONFIG, "group_test,",
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()
        ));
    }

    @Flaky("KAFKA-18031")
    @ClusterTest
    public void testAsyncConsumerCloseLeavesGroupOnInterrupt() throws Exception {
        testCloseLeavesGroupOnInterrupt(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            GROUP_ID_CONFIG, "group_test,",
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()
        ));
    }

    private void testCloseLeavesGroupOnInterrupt(Map<String, Object> consumerConfig) throws Exception {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            var listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(TOPIC), listener);
            awaitRebalance(consumer, listener);

            assertEquals(1, listener.callsToAssigned);
            assertEquals(0, listener.callsToRevoked);

            try {
                Thread.currentThread().interrupt();
                assertThrows(InterruptException.class, consumer::close);
            } finally {
                // Clear the interrupted flag so we don't create problems for subsequent tests.
                Thread.interrupted();
            }

            assertEquals(1, listener.callsToAssigned);
            assertEquals(1, listener.callsToRevoked);

            Map<String, Object> consumerConfigMap = new HashMap<>(consumerConfig);
            var config = new ConsumerConfig(consumerConfigMap);

            // Set the wait timeout to be only *half* the configured session timeout. This way we can make sure that the
            // consumer explicitly left the group as opposed to being kicked out by the broker.
            var leaveGroupTimeoutMs = config.getInt(SESSION_TIMEOUT_MS_CONFIG) / 2;

            TestUtils.waitForCondition(
                () -> checkGroupMemberEmpty(config), 
                leaveGroupTimeoutMs, 
                "Consumer did not leave the consumer group within " + leaveGroupTimeoutMs + " ms of close"
            );
        }
    }

    private boolean checkGroupMemberEmpty(ConsumerConfig config) {
        try (var admin = cluster.admin()) {
            var groupId = config.getString(GROUP_ID_CONFIG);
            var result = admin.describeConsumerGroups(List.of(groupId));
            var groupDescription = result.describedGroups().get(groupId).get();
            return groupDescription.members().isEmpty();
        } catch (ExecutionException | InterruptedException e) {
            return false;
        }
    }

    @ClusterTest
    public void testClassicConsumerOffsetRelatedWhenTimeoutZero() throws Exception {
        testOffsetRelatedWhenTimeoutZero(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)
        ));
    }

    @ClusterTest
    public void testAsyncConsumerOffsetRelatedWhenTimeoutZero() throws Exception {
        testOffsetRelatedWhenTimeoutZero(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testOffsetRelatedWhenTimeoutZero(Map<String, Object> consumerConfig) throws Exception {
        cluster.createTopic(TOPIC, 2, (short) BROKER_COUNT);
        try (var consumer = cluster.consumer(consumerConfig)) {
            var result1 = consumer.beginningOffsets(List.of(TP), Duration.ZERO);
            assertNotNull(result1);
            assertEquals(0, result1.size());

            var result2 = consumer.endOffsets(List.of(TP), Duration.ZERO);
            assertNotNull(result2);
            assertEquals(0, result2.size());

            var result3 = consumer.offsetsForTimes(Map.of(TP, 0L), Duration.ZERO);
            assertNotNull(result3);
            assertEquals(1, result3.size());
            assertNull(result3.get(TP));
        }
    }

    private void sendCompressedMessages(int numRecords, TopicPartition tp) {
        Map<String, Object> config = Map.of(
            COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name,
            LINGER_MS_CONFIG, Integer.MAX_VALUE
        );
        try (Producer<byte[], byte[]> producer = cluster.producer(config)) {
            IntStream.range(0, numRecords).forEach(i -> producer.send(new ProducerRecord<>(
                tp.topic(),
                tp.partition(),
                (long) i,
                ("key " + i).getBytes(),
                ("value " + i).getBytes()
            )));
        }
    }

    @ClusterTest
    public void testClassicConsumerStallBetweenPoll() throws Exception {
        testStallBetweenPoll(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerStallBetweenPoll() throws Exception {
        testStallBetweenPoll(GroupProtocol.CONSUMER);
    }

    /**
     * This test is to prove that the intermittent stalling that has been experienced when using the asynchronous
     * consumer, as filed under KAFKA-19259, have been fixed.
     *
     * <p/>
     *
     * The basic idea is to have one thread that produces a record every 500 ms. and the main thread that consumes
     * records without pausing between polls for much more than the produce delay. In the test case filed in
     * KAFKA-19259, the consumer sometimes pauses for up to 5-10 seconds despite records being produced every second.
     */
    private void testStallBetweenPoll(GroupProtocol groupProtocol) throws Exception {
        var testTopic = "stall-test-topic";
        var numPartitions = 6;
        cluster.createTopic(testTopic, numPartitions, (short) BROKER_COUNT);

        // The producer must produce slowly to tickle the scenario.
        var produceDelay = 500;

        var executor = Executors.newScheduledThreadPool(1);

        try (var producer = cluster.producer()) {
            // Start a thread running that produces records at a relative trickle.
            executor.scheduleWithFixedDelay(
                () -> producer.send(new ProducerRecord<>(testTopic, TestUtils.randomBytes(64))),
                0,
                produceDelay,
                TimeUnit.MILLISECONDS
            );

            Map<String, Object> consumerConfig = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));

            // Assign a tolerance for how much time is allowed to pass between Consumer.poll() calls given that there
            // should be *at least* one record to read every second.
            var pollDelayTolerance = 2000;

            try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
                consumer.subscribe(List.of(testTopic));

                // This is here to allow the consumer time to settle the group membership/assignment.
                awaitNonEmptyRecords(consumer, new TopicPartition(testTopic, 0));

                // Keep track of the last time the poll is invoked to ensure the deltas between invocations don't
                // exceed the delay threshold defined above.
                var beforePoll = System.currentTimeMillis();
                consumer.poll(Duration.ofSeconds(5));
                consumer.poll(Duration.ofSeconds(5));
                var afterPoll = System.currentTimeMillis();
                var pollDelay = afterPoll - beforePoll;

                if (pollDelay > pollDelayTolerance)
                    fail("Detected a stall of " + pollDelay + " ms between Consumer.poll() invocations despite a Producer producing records every " + produceDelay + " ms");
            } finally {
                executor.shutdownNow();
                // Wait for any active tasks to terminate to ensure consumer is not closed while being used from another thread
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate");
            }
        }
    }

    private ConsumerRecords<byte[], byte[]> awaitNonEmptyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp
    ) throws Exception {
        AtomicReference<ConsumerRecords<byte[], byte[]>> result = new AtomicReference<>();

        TestUtils.waitForCondition(() -> {
            var polledRecords = consumer.poll(Duration.ofSeconds(10));
            boolean hasRecords = !polledRecords.isEmpty();
            if (hasRecords) {
                result.set(polledRecords);
            }
            return hasRecords;
        }, "Timed out waiting for non-empty records from topic " + tp.topic() + " partition " + tp.partition());

        return result.get();
    }

    public static class SerializerImpl implements Serializer<byte[]> {
        private final ByteArraySerializer serializer = new ByteArraySerializer();

        @Override
        public byte[] serialize(String topic, byte[] data) {
            throw new RuntimeException("This method should not be called");
        }

        @Override
        public byte[] serialize(String topic, Headers headers, byte[] data) {
            headers.add("content-type", "application/octet-stream".getBytes());
            return serializer.serialize(topic, headers, data);
        }
    }

    public static class DeserializerImpl implements Deserializer<byte[]> {
        private final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            throw new RuntimeException("This method should not be called");
        }

        @Override
        public byte[] deserialize(String topic, Headers headers, byte[] data) {
            Header contentType = headers.lastHeader("content-type");
            assertNotNull(contentType);
            assertEquals("application/octet-stream", new String(contentType.value()));
            return deserializer.deserialize(topic, headers, data);
        }
    }
}
