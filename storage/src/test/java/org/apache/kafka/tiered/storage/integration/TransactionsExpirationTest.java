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
package org.apache.kafka.tiered.storage.integration;

import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.TOPIC1;
import static org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.TOPIC2;
import static org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.getActiveProducers;
import static org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.producerRecordWithExpectedTransactionStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Placed in the storage module to avoid a cross-module dependency from clients-integration-tests on storage.
@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        // Set a smaller value for the number of partitions for the __consumer_offsets topic
        // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long.
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "3"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = "group.initial.rebalance.delay.ms", value = "0"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "200"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG, value = "10000"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG, value = "500"),
        @ClusterConfigProperty(key = TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG, value = "5000"),
        @ClusterConfigProperty(key = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG, value = "500"),
    }
)
public class TransactionsExpirationTest {
    private static final String TRANSACTION_ID = "transactionalProducer";
    private static final TopicPartition TOPIC1_PARTITION0 = new TopicPartition(TOPIC1, 0);

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)})
    public void testFatalErrorAfterInvalidProducerIdMappingWithTV1(ClusterInstance clusterInstance) throws InterruptedException {
        testFatalErrorAfterInvalidProducerIdMapping(clusterInstance);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    public void testFatalErrorAfterInvalidProducerIdMappingWithTV2(ClusterInstance clusterInstance) throws InterruptedException {
        testFatalErrorAfterInvalidProducerIdMapping(clusterInstance);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)})
    public void testTransactionAfterProducerIdExpiresWithTV1(ClusterInstance clusterInstance) throws InterruptedException {
        testTransactionAfterProducerIdExpires(clusterInstance, false);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    public void testTransactionAfterProducerIdExpiresWithTV2(ClusterInstance clusterInstance) throws InterruptedException {
        testTransactionAfterProducerIdExpires(clusterInstance, true);
    }

    private void testFatalErrorAfterInvalidProducerIdMapping(ClusterInstance clusterInstance) throws InterruptedException {
        clusterInstance.createTopic(TOPIC1, 4, (short) 3);
        clusterInstance.createTopic(TOPIC2, 4, (short) 3);
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(Map.of(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION_ID
            ))
        ) {
            producer.initTransactions();
            // Start and then abort a transaction to allow the transactional ID to expire.
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "2", "2", false));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 0, "4", "4", false));
            producer.abortTransaction();

            // Check the transactional state exists and then wait for it to expire.
            waitUntilTransactionalStateExists(clusterInstance);
            waitUntilTransactionalStateExpires(clusterInstance);

            // Start a new transaction and attempt to send, triggering an AddPartitionsToTxnRequest that will fail
            // due to the expired transactional ID, resulting in a fatal error.
            producer.beginTransaction();
            var failedFuture = producer.send(
                producerRecordWithExpectedTransactionStatus(TOPIC1, 3, "1", "1", false));
            TestUtils.waitForCondition(failedFuture::isDone, "Producer future never completed.");
            org.apache.kafka.test.TestUtils.assertFutureThrows(InvalidPidMappingException.class, failedFuture);

            // Assert that aborting the transaction throws a KafkaException due to the fatal error.
            assertThrows(KafkaException.class, producer::abortTransaction);
        }

        // Reinitialize to recover from the fatal error.
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(Map.of(
                 ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION_ID
             ))
        ) {
            producer.initTransactions();
            // Proceed with a new transaction after reinitializing.
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 2, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 3, "3", "3", true));
            producer.commitTransaction();

            waitUntilTransactionalStateExists(clusterInstance);
        }

        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            TransactionsTestHelper.assertConsumeCommittedRecords(
                    clusterInstance, List.of(TOPIC1, TOPIC2), 4, groupProtocol);
        }
    }

    private void testTransactionAfterProducerIdExpires(ClusterInstance clusterInstance, boolean isTV2Enabled) throws InterruptedException {
        clusterInstance.createTopic(TOPIC1, 4, (short) 3);
        long oldProducerId;
        long oldProducerEpoch;

        try (Producer<byte[], byte[]> producer = clusterInstance.producer(Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION_ID
        ))
        ) {
            producer.initTransactions();

            // Start and then abort a transaction to allow the producer ID to expire.
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "2", "2", false));
            producer.flush();

            // Ensure producer IDs are added.
            var producerStates = new ArrayList<ProducerState>();
            TestUtils.waitForCondition(() -> {
                try {
                    producerStates.addAll(getActiveProducers(clusterInstance, TOPIC1_PARTITION0));
                    return !producerStates.isEmpty();
                } catch (ExecutionException | InterruptedException e) {
                    return false;
                }
            }, "Producer IDs for " + TOPIC1_PARTITION0 + " did not propagate quickly");
            assertEquals(1, producerStates.size(), "Unexpected producer to " + TOPIC1_PARTITION0);
            oldProducerId = producerStates.get(0).producerId();
            oldProducerEpoch = producerStates.get(0).producerEpoch();

            producer.abortTransaction();

            // Wait for the producer ID to expire.
            TestUtils.waitForCondition(() -> {
                try {
                    return getActiveProducers(clusterInstance, TOPIC1_PARTITION0).isEmpty();
                } catch (ExecutionException | InterruptedException e) {
                    return false;
                }
            }, "Producer IDs for " + TOPIC1_PARTITION0 + " did not expire.");
        }

        // Create a new producer to check that we retain the producer ID in transactional state.
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION_ID
        ))
        ) {
            producer.initTransactions();

            // Start a new transaction and attempt to send. This should work since only the producer ID was removed from its mapping in ProducerStateManager.
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 3, "3", "3", true));
            producer.commitTransaction();

            // Producer IDs should repopulate.
            var producerStates = new ArrayList<ProducerState>();
            TestUtils.waitForCondition(() -> {
                try {
                    producerStates.addAll(getActiveProducers(clusterInstance, TOPIC1_PARTITION0));
                    return !producerStates.isEmpty();
                } catch (ExecutionException | InterruptedException e) {
                    return false;
                }
            }, "Producer IDs for " + TOPIC1_PARTITION0 + " did not propagate quickly");
            assertEquals(1, producerStates.size(), "Unexpected producer to " + TOPIC1_PARTITION0);
            var newProducerId = producerStates.get(0).producerId();
            var newProducerEpoch = producerStates.get(0).producerEpoch();

            // Because the transaction IDs outlive the producer IDs, creating a producer with the same transactional id
            // soon after the first will re-use the same producerId, while bumping the epoch to indicate that they are distinct.
            assertEquals(oldProducerId, newProducerId);
            if (isTV2Enabled) {
                // TV2 bumps epoch on EndTxn, and the final commit may or may not have bumped the epoch in the producer state.
                // The epoch should be at least oldProducerEpoch + 2 for the first commit and the restarted producer.
                assertTrue(oldProducerEpoch + 2 <= newProducerEpoch);
            } else {
                assertEquals(oldProducerEpoch + 1, newProducerEpoch);
            }

            for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
                TransactionsTestHelper.assertConsumeCommittedRecords(
                        clusterInstance, List.of(TOPIC1), 2, groupProtocol);
            }
        }
    }

    private void waitUntilTransactionalStateExists(ClusterInstance clusterInstance) throws InterruptedException {
        try (var admin = clusterInstance.admin()) {
            TestUtils.waitForCondition(() -> {
                try {
                    admin.describeTransactions(List.of(TRANSACTION_ID)).description(TRANSACTION_ID).get();
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }, "Transactional state was never added.");
        }
    }

    private void waitUntilTransactionalStateExpires(ClusterInstance clusterInstance) throws InterruptedException {
        try (var admin = clusterInstance.admin()) {
            TestUtils.waitForCondition(() -> {
                try {
                    admin.describeTransactions(List.of(TRANSACTION_ID)).description(TRANSACTION_ID).get();
                    return false;
                } catch (Exception e) {
                    return e.getCause() instanceof TransactionalIdNotFoundException;
                }
            }, "Transaction state never expired.");
        }
    }
}
