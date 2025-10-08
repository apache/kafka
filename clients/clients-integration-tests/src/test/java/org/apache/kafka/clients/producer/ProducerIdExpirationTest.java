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
import kafka.utils.TestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;

import org.opentest4j.AssertionFailedError;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static kafka.utils.TestUtils.consumeRecords;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        // Set a smaller value for the number of partitions for the __consumer_offsets topic
        // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively 
        // long.
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "3"),
        @ClusterConfigProperty(key = TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        //  ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG is not a constant 
        @ClusterConfigProperty(key = "unclean.leader.election.enable", value = "false"),
        @ClusterConfigProperty(key = AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = 
            "200"),
        @ClusterConfigProperty(key = TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG, value = "5000"),
        @ClusterConfigProperty(key = TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG, value =
            "500"),
        @ClusterConfigProperty(key = PRODUCER_ID_EXPIRATION_MS_CONFIG, value = "10000"),
        @ClusterConfigProperty(key = PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG, value = "500")
    }
)
public class ProducerIdExpirationTest {
    private final String topic1 = "topic1";
    private final int numPartitions = 1;
    private final short replicationFactor = 3;
    private final TopicPartition tp0 = new TopicPartition(topic1, 0);
    private final String transactionalId = "transactionalProducer";
    private final ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");

    @ClusterTest
    void testProducerIdExpirationWithNoTransactions(ClusterInstance cluster) throws InterruptedException, ExecutionException {
        cluster.createTopic(topic1, numPartitions, replicationFactor);
        Producer<byte[], byte[]> producer = cluster.producer(Map.of(ENABLE_IDEMPOTENCE_CONFIG, true));
        // Send records to populate producer state cache.
        producer.send(new ProducerRecord<>(topic1, 0, null, "key".getBytes(), "value".getBytes()));
        producer.flush();
        try (Admin admin = cluster.admin(); producer) {
            assertEquals(1, producerStates(admin).size());

            waitProducerIdExpire(admin);

            // Send more records to send producer ID back to brokers.
            producer.send(new ProducerRecord<>(topic1, 0, null, "key".getBytes(), "value".getBytes()));
            producer.flush();

            // Producer IDs should repopulate.
            assertEquals(1, producerStates(admin).size());
        }
    }

    @ClusterTest
    void testTransactionAfterTransactionIdExpiresButProducerIdRemains(ClusterInstance cluster) throws InterruptedException, ExecutionException {
        cluster.createTopic(topic1, numPartitions, replicationFactor);
        Producer<byte[], byte[]> producer = cluster.producer(transactionalProducerConfig());
        producer.initTransactions();

        // Start and then abort a transaction to allow the producer ID to expire.
        producer.beginTransaction();
        producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "2", "2", false));
        producer.flush();
        Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(ISOLATION_LEVEL_CONFIG, "read_committed"));

        try (Admin admin = cluster.admin()) {
            // Ensure producer IDs are added.
            TestUtils.waitUntilTrue(() -> {
                try {
                    return producerStates(admin).size() == 1;
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, () -> "Producer IDs were not added.", DEFAULT_MAX_WAIT_MS, 100);

            producer.abortTransaction();

            // Wait for the transactional ID to expire.
            waitUntilTransactionalStateExpires(admin);

            // Producer IDs should be retained.
            assertEquals(1, producerStates(admin).size());

            // Start a new transaction and attempt to send, triggering an AddPartitionsToTxnRequest that will fail
            // due to the expired transactional ID, resulting in a fatal error.
            producer.beginTransaction();
            Future<RecordMetadata> failedFuture =
                producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "1", "1", false));
            TestUtils.waitUntilTrue(failedFuture::isDone, () -> "Producer future never completed.",
                DEFAULT_MAX_WAIT_MS, 100);
            assertFutureThrows(InvalidPidMappingException.class, failedFuture);

            // Assert that aborting the transaction throws a KafkaException due to the fatal error.
            assertThrows(KafkaException.class, producer::abortTransaction);

            // Close the producer and reinitialize to recover from the fatal error.
            producer.close();
            producer = cluster.producer(transactionalProducerConfig());
            producer.initTransactions();

            producer.beginTransaction();
            producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "4", "4", true));
            producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "3", "3", true));

            // Producer IDs should be retained.
            assertFalse(producerStates(admin).isEmpty());

            producer.commitTransaction();

            // Check we can still consume the transaction.
            consumer.subscribe(List.of(topic1));
            consumeRecords(consumer, 2, DEFAULT_MAX_WAIT_MS).foreach(TestUtils::assertCommittedAndGetValue);
        } finally {
            producer.close();
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(1)));
        }
    }

    @ClusterTest
    void testDynamicProducerIdExpirationMs(ClusterInstance cluster) throws InterruptedException, ExecutionException {
        cluster.createTopic(topic1, numPartitions, replicationFactor);
        Producer<byte[], byte[]> producer = cluster.producer(Map.of(ENABLE_IDEMPOTENCE_CONFIG, true));

        // Send records to populate producer state cache.
        producer.send(new ProducerRecord<>(topic1, 0, null, "key".getBytes(), "value".getBytes()));
        producer.flush();

        try (Admin admin = cluster.admin(); producer) {
            assertEquals(1, producerStates(admin).size());

            waitProducerIdExpire(admin);

            // Update the producer ID expiration ms to a very high value.
            admin.incrementalAlterConfigs(producerIdExpirationConfig("100000"));

            cluster.brokers().values().forEach(broker ->
                TestUtils.waitUntilTrue(() -> broker.logManager().producerStateManagerConfig().producerIdExpirationMs() == 100000,
                    () -> "Configuration was not updated.", DEFAULT_MAX_WAIT_MS, 100)
            );
            // Send more records to send producer ID back to brokers.
            producer.send(new ProducerRecord<>(topic1, 0, null, "key".getBytes(), "value".getBytes()));
            producer.flush();

            // Producer IDs should repopulate.
            assertEquals(1, producerStates(admin).size());

            // Ensure producer ID does not expire within 4 seconds.
            assertThrows(AssertionFailedError.class, () -> waitProducerIdExpire(admin, TimeUnit.SECONDS.toMillis(4)));

            // Update the expiration time to a low value again.
            admin.incrementalAlterConfigs(producerIdExpirationConfig("100")).all().get();

            KafkaBroker kafkaBroker = cluster.brokers().get(0);
            kafkaBroker.shutdown(Duration.ofMinutes(5));
            kafkaBroker.awaitShutdown();
            kafkaBroker.startup();
            cluster.waitForReadyBrokers();
            cluster.brokers().values().forEach(broker ->
                TestUtils.waitUntilTrue(() -> broker.logManager().producerStateManagerConfig().producerIdExpirationMs() == 100,
                    () -> "Configuration was not updated.", DEFAULT_MAX_WAIT_MS, 100)
            );

            // Ensure producer ID expires quickly again.
            waitProducerIdExpire(admin);
        }
    }


    private void waitProducerIdExpire(Admin admin) {
        waitProducerIdExpire(admin, DEFAULT_MAX_WAIT_MS);
    }

    private void waitProducerIdExpire(Admin admin, long timeout) {
        TestUtils.waitUntilTrue(() -> {
            try {
                return producerStates(admin).isEmpty();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, () -> "Producer ID expired.", timeout, 100);
    }

    private Map<ConfigResource, Collection<AlterConfigOp>> producerIdExpirationConfig(String configValue) {
        ConfigEntry producerIdCfg = new ConfigEntry(PRODUCER_ID_EXPIRATION_MS_CONFIG, configValue);
        return Map.of(configResource, List.of(new AlterConfigOp(producerIdCfg, AlterConfigOp.OpType.SET)));
    }


    private void waitUntilTransactionalStateExpires(Admin admin) {
        TestUtils.waitUntilTrue(() -> {
            boolean removedTransactionState = false;
            try {
                admin.describeTransactions(List.of(transactionalId))
                    .description(transactionalId)
                    .get();
            } catch (Exception e) {
                removedTransactionState = e.getCause() instanceof TransactionalIdNotFoundException;
            }
            return removedTransactionState;
        }, () -> "Transaction state never expired.", DEFAULT_MAX_WAIT_MS, 100);
    }

    private Map<String, Object> transactionalProducerConfig() {
        return Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.ACKS_CONFIG, "all");
    }

    private List<ProducerState> producerStates(Admin admin) throws ExecutionException, InterruptedException {
        return admin.describeProducers(Collections.singletonList(tp0))
            .partitionResult(tp0)
            .get()
            .activeProducers();
    }
}
