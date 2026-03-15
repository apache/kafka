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

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.internal.DefaultRecord;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC;
import static org.apache.kafka.server.config.ServerConfigs.MESSAGE_MAX_BYTES_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 2,
    serverProperties = {
        @ClusterConfigProperty(key = AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        //  15000 is filed serverMessageMaxBytes
        @ClusterConfigProperty(key = MESSAGE_MAX_BYTES_CONFIG, value = "15000"),
        //  15200 is filed replicaFetchMaxBytes
        @ClusterConfigProperty(key = REPLICA_FETCH_MAX_BYTES_CONFIG, value = "15200"),
        //  15400 is filed replicaFetchMaxResponseBytes
        @ClusterConfigProperty(key = REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC, value = "15400"),
        // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
        // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    }
)
public class ProducerFailureHandlingTest {

    private final int producerBufferSize = 30000;
    private final int serverMessageMaxBytes = producerBufferSize / 2;
    private final int replicaFetchMaxPartitionBytes = serverMessageMaxBytes + 200;
    private final int replicaFetchMaxResponseBytes = replicaFetchMaxPartitionBytes + 200;
    private final String topic1 = "topic-1";
    private final String topic2 = "topic-2";


    /**
     * With ack == 0 the future metadata will have no exceptions with offset -1
     */
    @ClusterTest
    void testTooLargeRecordWithAckZero(ClusterInstance clusterInstance) throws InterruptedException,
            ExecutionException {
        clusterInstance.createTopic(topic1, 1, (short) clusterInstance.brokers().size());
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig(0))) {
            // send a too-large record
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(topic1, null, "key".getBytes(), new byte[serverMessageMaxBytes + 1]);

            RecordMetadata recordMetadata = producer.send(record).get();
            assertNotNull(recordMetadata);
            assertFalse(recordMetadata.hasOffset());
            assertEquals(-1L, recordMetadata.offset());
        }
    }

    /**
     * With ack == 1 the future metadata will throw ExecutionException caused by RecordTooLargeException
     */
    @ClusterTest
    void testTooLargeRecordWithAckOne(ClusterInstance clusterInstance) throws InterruptedException {
        clusterInstance.createTopic(topic1, 1, (short) clusterInstance.brokers().size());

        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig(1))) {
            // send a too-large record
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(topic1, null, "key".getBytes(), new byte[serverMessageMaxBytes + 1]);
            assertThrows(ExecutionException.class, () -> producer.send(record).get());
        }
    }


    /**
     * This should succeed as the replica fetcher thread can handle oversized messages since KIP-74
     */
    @ClusterTest
    void testPartitionTooLargeForReplicationWithAckAll(ClusterInstance clusterInstance) throws InterruptedException,
            ExecutionException {
        checkTooLargeRecordForReplicationWithAckAll(clusterInstance, replicaFetchMaxPartitionBytes);
    }

    /**
     * This should succeed as the replica fetcher thread can handle oversized messages since KIP-74
     */
    @ClusterTest
    void testResponseTooLargeForReplicationWithAckAll(ClusterInstance clusterInstance) throws InterruptedException,
            ExecutionException {
        checkTooLargeRecordForReplicationWithAckAll(clusterInstance, replicaFetchMaxResponseBytes);
    }


    /**
     * With non-exist-topic the future metadata should return ExecutionException caused by TimeoutException
     */
    @ClusterTest
    void testNonExistentTopic(ClusterInstance clusterInstance) {
        // send a record with non-exist topic
        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic2, null, "key".getBytes(), "value".getBytes());
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig(0))) {
            assertThrows(ExecutionException.class, () -> producer.send(record).get());
        }
    }


    /**
     * With incorrect broker-list the future metadata should return ExecutionException caused by TimeoutException
     */
    @ClusterTest
    void testWrongBrokerList(ClusterInstance clusterInstance) throws InterruptedException {
        clusterInstance.createTopic(topic1, 1, (short) 1);
        // producer with incorrect broker list
        Map<String, Object> producerConfig = new HashMap<>(producerConfig(1));
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8686,localhost:4242");
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig)) {
            // send a record with incorrect broker list
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(topic1, null, "key".getBytes(), "value".getBytes());
            assertThrows(ExecutionException.class, () -> producer.send(record).get());
        }
    }

    /**
     * Send with invalid partition id should return ExecutionException caused by TimeoutException
     * when partition is higher than the upper bound of partitions.
     */
    @ClusterTest
    void testInvalidPartition(ClusterInstance clusterInstance) throws InterruptedException {
        // create topic with a single partition
        clusterInstance.createTopic(topic1, 1, (short) clusterInstance.brokers().size());

        // create a record with incorrect partition id (higher than the number of partitions), send should fail
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig(0))) {
            ProducerRecord<byte[], byte[]> higherRecord =
                    new ProducerRecord<>(topic1, 1, "key".getBytes(), "value".getBytes());
            Exception e = assertThrows(ExecutionException.class, () -> producer.send(higherRecord).get());
            assertEquals(TimeoutException.class, e.getCause().getClass());
        }
    }


    /**
     * The send call after producer closed should throw IllegalStateException
     */
    @ClusterTest
    void testSendAfterClosed(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        // create topic
        clusterInstance.createTopic(topic1, 1, (short) clusterInstance.brokers().size());

        Producer<byte[], byte[]> producer1 = clusterInstance.producer(producerConfig(0));
        Producer<byte[], byte[]> producer2 = clusterInstance.producer(producerConfig(1));
        Producer<byte[], byte[]> producer3 = clusterInstance.producer(producerConfig(-1));

        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic1, null, "key".getBytes(), "value".getBytes());
        // first send a message to make sure the metadata is refreshed
        producer1.send(record).get();
        producer2.send(record).get();
        producer3.send(record).get();

        producer1.close();
        assertThrows(IllegalStateException.class, () -> producer1.send(record));
        producer2.close();
        assertThrows(IllegalStateException.class, () -> producer2.send(record));
        producer3.close();
        assertThrows(IllegalStateException.class, () -> producer3.send(record));
    }

    @ClusterTest
    void testCannotSendToInternalTopic(ClusterInstance clusterInstance) throws InterruptedException {
        try (Admin admin = clusterInstance.admin()) {
            Map<String, String> topicConfig = new HashMap<>();
            clusterInstance.brokers().get(0)
                    .groupCoordinator()
                    .groupMetadataTopicConfigs()
                    .forEach((k, v) -> topicConfig.put(k.toString(), v.toString()));
            admin.createTopics(List.of(new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 1, (short) 1).configs(topicConfig)));
            clusterInstance.waitTopicDeletion(Topic.GROUP_METADATA_TOPIC_NAME);
        }

        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig(1))) {
            Exception thrown = assertThrows(ExecutionException.class,
                    () -> producer.send(new ProducerRecord<>(Topic.GROUP_METADATA_TOPIC_NAME, "test".getBytes(),
                            "test".getBytes())).get());
            assertInstanceOf(InvalidTopicException.class, thrown.getCause(),
                    () -> "Unexpected exception while sending to an invalid topic " + thrown.getCause());
        }
    }

    @ClusterTest
    void testNotEnoughReplicasAfterBrokerShutdown(ClusterInstance clusterInstance) throws InterruptedException,
            ExecutionException {
        String topicName = "minisrtest2";
        int brokerNum = clusterInstance.brokers().size();
        Map<String, String> topicConfig = Map.of(MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(brokerNum));
        try (Admin admin = clusterInstance.admin()) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) brokerNum).configs(topicConfig)));
        }

        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topicName, null, "key".getBytes(), "value".getBytes());

        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig(-1))) {
            // this should work with all brokers up and running
            producer.send(record).get();
            // shut down one broker
            KafkaBroker oneBroker = clusterInstance.brokers().get(0);
            oneBroker.shutdown();
            oneBroker.awaitShutdown();

            Exception e = assertThrows(ExecutionException.class, () -> producer.send(record).get());
            assertTrue(e.getCause() instanceof NotEnoughReplicasException ||
                    e.getCause() instanceof NotEnoughReplicasAfterAppendException ||
                    e.getCause() instanceof TimeoutException);

            // restart the server
            oneBroker.startup();
        }
    }

    private void checkTooLargeRecordForReplicationWithAckAll(ClusterInstance clusterInstance, int maxFetchSize) throws InterruptedException, ExecutionException {
        int maxMessageSize = maxFetchSize + 100;
        int brokerSize = clusterInstance.brokers().size();
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put(MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(brokerSize));
        topicConfig.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(maxMessageSize));

        // create topic
        String topic10 = "topic10";
        clusterInstance.createTopic(topic10, brokerSize, (short) brokerSize, topicConfig);

        // send a record that is too large for replication, but within the broker max message limit
        byte[] value = new byte[maxMessageSize - DefaultRecordBatch.RECORD_BATCH_OVERHEAD - DefaultRecord.MAX_RECORD_OVERHEAD];
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(producerConfig(-1))) {
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic10, null, value);
            RecordMetadata recordMetadata = producer.send(producerRecord).get();

            assertEquals(topic10, recordMetadata.topic());
        }
    }

    private Map<String, Object> producerConfig(int acks) {
        return Map.of(
                ACKS_CONFIG, String.valueOf(acks),
                RETRIES_CONFIG, 0,
                REQUEST_TIMEOUT_MS_CONFIG, 30000,
                MAX_BLOCK_MS_CONFIG, 10000,
                BUFFER_MEMORY_CONFIG, producerBufferSize);
    }

}
