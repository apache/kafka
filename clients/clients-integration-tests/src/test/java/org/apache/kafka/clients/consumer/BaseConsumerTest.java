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
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.BaseConsumerTest.Testcase.testClusterResourceListener;
import static org.apache.kafka.clients.consumer.BaseConsumerTest.Testcase.testCoordinatorFailover;
import static org.apache.kafka.clients.consumer.BaseConsumerTest.Testcase.testSimpleConsumption;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BaseConsumerTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
    }
)
public class BaseConsumerTest {

    private final ClusterInstance cluster;
    public static final AtomicInteger UPDATE_PRODUCER_COUNT = new AtomicInteger();
    public static final AtomicInteger UPDATE_CONSUMER_COUNT = new AtomicInteger();
    public static final int BROKER_COUNT = 3;

    public BaseConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }
    
    @BeforeEach
    public void setUp() throws InterruptedException {
        cluster.createTopic(Testcase.TOPIC, 2, (short) BROKER_COUNT);
    }

    @ClusterTest
    public void testClassicConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)));
    }

    @ClusterTest
    public void testAsyncConsumerSimpleConsumption() throws InterruptedException {
        testSimpleConsumption(cluster, Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)));
    }

    @ClusterTest
    public void testClassicConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT)));
    }

    @ClusterTest
    public void testAsyncConsumerClusterResourceListener() throws InterruptedException {
        testClusterResourceListener(cluster, Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)));
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

    public static class Testcase {

        private static final String TOPIC = "topic";
        private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
        
        public static void testSimpleConsumption(
            ClusterInstance cluster, 
            Map<String, Object> config
        ) throws InterruptedException {
            var numRecords = 10000;
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(cluster, TP, numRecords, startingTimestamp);
            try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
                assertEquals(0, consumer.assignment().size());
                consumer.assign(List.of(TP));
                assertEquals(1, consumer.assignment().size());

                consumer.seek(TP, 0);
                consumeAndVerifyRecords(consumer, TP, numRecords, 0, 0, startingTimestamp);
                // check async commit callbacks
                sendAndAwaitAsyncCommit(consumer, Optional.empty());
            }
        }

        public static void testClusterResourceListener(
            ClusterInstance cluster,
            Map<String, Object> consumerConfig
        ) throws InterruptedException {
            var numRecords = 100;
            Map<String, Object> producerConfig = Map.of(
                KEY_SERIALIZER_CLASS_CONFIG, TestClusterResourceListenerSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, TestClusterResourceListenerSerializer.class
            );
            Map<String, Object> consumerConfigOverrides = new HashMap<>(consumerConfig);
            consumerConfigOverrides.put(KEY_DESERIALIZER_CLASS_CONFIG, TestClusterResourceListenerDeserializer.class);
            consumerConfigOverrides.put(VALUE_DESERIALIZER_CLASS_CONFIG, TestClusterResourceListenerDeserializer.class);
            try (Producer<byte[], byte[]> producer = cluster.producer(producerConfig);
                 Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfigOverrides)
            ) {
                var startingTimestamp = System.currentTimeMillis();
                sendRecords(producer, TP, numRecords, startingTimestamp, -1);

                consumer.subscribe(List.of(TP.topic()));
                consumeAndVerifyRecords(consumer, TP, numRecords, 0, 0, startingTimestamp);
                assertNotEquals(0, BaseConsumerTest.UPDATE_PRODUCER_COUNT.get());
                assertNotEquals(0, BaseConsumerTest.UPDATE_CONSUMER_COUNT.get());
            }
        }
        
        public static void testCoordinatorFailover(
            ClusterInstance cluster, 
            Map<String, Object> consumerConfig
        ) throws InterruptedException {
            var listener = new TestConsumerReassignmentListener();
            try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
                consumer.subscribe(List.of(TOPIC), listener);
                // the initial subscription should cause a callback execution
                awaitRebalance(consumer, listener);
                assertEquals(1, listener.callsToAssigned);

                // get metadata for the topic
                List<PartitionInfo> parts = null;
                while (parts == null) {
                    parts = consumer.partitionsFor(Topic.GROUP_METADATA_TOPIC_NAME);
                }
                assertEquals(1, parts.size());
                assertNotNull(parts.get(0).leader());

                // shutdown the coordinator
                int coordinator = parts.get(0).leader().id();
                cluster.shutdownBroker(coordinator);

                // the failover should not cause a rebalance
                ensureNoRebalance(consumer, listener);
            }
        }
    }

    private static void sendAndAwaitAsyncCommit(
        Consumer<byte[], byte[]> consumer,
        Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
    ) throws InterruptedException {

        var commitCallback = new RetryCommitCallback(consumer, offsetsOpt);
        sendAsyncCommit(consumer, commitCallback, offsetsOpt);
        
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return commitCallback.isComplete;
        }, "Failed to observe commit callback before timeout");

        assertEquals(Optional.empty(), commitCallback.error);
    }

    private static void sendAsyncCommit(
        Consumer<byte[], byte[]> consumer,
        OffsetCommitCallback callback,
        Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
    ) {
        offsetsOpt.ifPresentOrElse(
            offsets -> consumer.commitAsync(offsets, callback), 
            () -> consumer.commitAsync(callback)
        );
    }

    public static class TestClusterResourceListenerSerializer implements Serializer<byte[]>, ClusterResourceListener {

        @Override
        public void onUpdate(ClusterResource clusterResource) {
            UPDATE_PRODUCER_COUNT.incrementAndGet();
        }

        @Override
        public byte[] serialize(String topic, byte[] data) {
            return data;
        }
    }

    public static class TestClusterResourceListenerDeserializer implements Deserializer<byte[]>, ClusterResourceListener {

        @Override
        public void onUpdate(ClusterResource clusterResource) {
            UPDATE_CONSUMER_COUNT.incrementAndGet();
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            return data;
        }
    }
    
    private static class RetryCommitCallback implements OffsetCommitCallback {
        boolean isComplete = false;
        Optional<Exception> error = Optional.empty();
        Consumer<byte[], byte[]> consumer;
        Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt;
        
        public RetryCommitCallback(
            Consumer<byte[], byte[]> consumer, 
            Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
        ) {
            this.consumer = consumer;
            this.offsetsOpt = offsetsOpt;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception instanceof RetriableCommitFailedException) {
                sendAsyncCommit(consumer, this, offsetsOpt);
            } else {
                isComplete = true;
                error = Optional.ofNullable(exception);
            }
        }
    }

    public static class TestConsumerReassignmentListener implements ConsumerRebalanceListener {
        int callsToAssigned = 0;
        int callsToRevoked = 0;

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            callsToAssigned += 1;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            callsToRevoked += 1;
        }
    }

    private static void awaitRebalance(
        Consumer<byte[], byte[]> consumer,
        TestConsumerReassignmentListener rebalanceListener
    ) throws InterruptedException {
        var numReassignments = rebalanceListener.callsToAssigned;
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return rebalanceListener.callsToAssigned > numReassignments;
        }, "Timed out before expected rebalance completed");
    }

    private static void ensureNoRebalance(
        Consumer<byte[], byte[]> consumer,
        TestConsumerReassignmentListener rebalanceListener
    ) throws InterruptedException {
        // The best way to verify that the current membership is still active is to commit offsets.
        // This would fail if the group had rebalanced.
        var initialRevokeCalls = rebalanceListener.callsToRevoked;
        sendAndAwaitAsyncCommit(consumer, Optional.empty());
        assertEquals(initialRevokeCalls, rebalanceListener.callsToRevoked);
    }
    
}
