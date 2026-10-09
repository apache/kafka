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

import kafka.server.BrokerServer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionMetadata;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for KIP-939 two-phase commit (2PC) support: InitProducerId(keepPreparedTxn=true),
 * prepareTransaction() / completeTransaction() and producer ID rotation with dual identity.
 */
@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "3"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = "group.initial.rebalance.delay.ms", value = "0"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "200"),
        // Enable unstable API versions to support KIP-939 2PC (InitProducerId v6 with keepPreparedTxn)
        @ClusterConfigProperty(key = ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, value = "true"),
        // Enable 2PC support on the broker side
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_2PC_ENABLED_CONFIG, value = "true"),
    }
)
public class TransactionsWith2PCTest {

    private static final Map<String, String> TOPIC_CONFIG = Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
    private static final long CONSUME_DURATION_MS = 1000;

    private final ClusterInstance clusterInstance;
    private Admin adminClient;

    public TransactionsWith2PCTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setUp() {
        adminClient = clusterInstance.admin();
    }

    @AfterEach
    public void tearDown() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @ClusterTest
    public void testFencingWithDualIdentity() throws Exception {
        // Test that zombie producer is properly fenced when dual identity is established
        // after crash recovery with 2PC keepPreparedTxn.
        var transactionalId = "test-fencing-dual-identity";
        var testTopic = "test-fencing-topic-" + System.nanoTime();
        clusterInstance.createTopic(testTopic, 1, TransactionsTestHelper.BROKER_COUNT, TOPIC_CONFIG);

        try (var consumer = createReadCommittedConsumer();
             var producer1 = create2PCProducer(transactionalId);
             var producer2 = create2PCProducer(transactionalId)) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofMillis(100));

            // Start transaction and send records (simulating crash before commit)
            producer1.initTransactions();
            producer1.beginTransaction();
            var numRecords = 5;
            for (var i = 0; i < numRecords; i++) {
                producer1.send(new ProducerRecord<>(testTopic, 0, ("key-" + i).getBytes(), ("value-" + i).getBytes()));
            }
            producer1.flush();
            // Simulate crash - don't commit (leave transaction prepared)

            // Recover with initTransactions(keepPreparedTxn=true), this establishes dual identity
            producer2.initTransactions(true);

            // Old (zombie) producer tries to abort - should get ProducerFencedException
            assertThrows(ProducerFencedException.class, producer1::abortTransaction,
                "Zombie producer should be fenced when trying to abort");

            // New producer commits the prepared transaction
            producer2.commitTransaction();

            // Wait for transaction to reach COMPLETE_COMMIT state
            waitForTransactionState(transactionalId, TransactionState.COMPLETE_COMMIT);

            // Verify consumer sees the records from the first producer
            consumer.seekToBeginning(consumer.assignment());
            var consumedRecords = TransactionsTestHelper.consumeRecordsFor(consumer, CONSUME_DURATION_MS);
            assertEquals(numRecords, consumedRecords.size(),
                "Consumer should see all records from first producer after commit");

            // Verify transaction state
            var txnDescription = adminClient.describeTransactions(List.of(transactionalId))
                .description(transactionalId).get();
            assertEquals(TransactionState.COMPLETE_COMMIT, txnDescription.state(),
                "Transaction should be in COMPLETE_COMMIT state");
        }
    }

    @ClusterTest
    public void testPrepareAndCompleteTransactionCommit() throws Exception {
        // Test the 2PC API: prepareTransaction() and completeTransaction() for commit path.
        // This tests the client-side 2PC API where the producer explicitly calls prepareTransaction()
        // and then completeTransaction() to commit.
        var transactionalId = "test-prepare-complete-commit";
        var testTopic = "test-prepare-complete-topic-" + System.nanoTime();
        clusterInstance.createTopic(testTopic, 1, TransactionsTestHelper.BROKER_COUNT, TOPIC_CONFIG);

        try (var consumer = createReadCommittedConsumer();
             var producer1 = create2PCProducer(transactionalId);
             var producer2 = create2PCProducer(transactionalId)) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofMillis(100));

            // Start transaction and produce records
            producer1.initTransactions();
            producer1.beginTransaction();
            var numRecords = 5;
            for (var i = 0; i < numRecords; i++) {
                producer1.send(new ProducerRecord<>(testTopic, 0, ("key-" + i).getBytes(), ("value-" + i).getBytes()));
            }
            producer1.flush();

            // Prepare the transaction (moves to PREPARED state)
            var preparedState = producer1.prepareTransaction();
            assertNotNull(preparedState, "prepareTransaction should return prepared state");

            // Simulate crash - don't call commit/abort, just leave in prepared state
            // (In real scenario, producer would crash here)

            // New producer recovers and completes the transaction
            producer2.initTransactions(true);  // keepPreparedTxn=true

            // Complete the transaction with the prepared state (should commit)
            producer2.completeTransaction(preparedState);

            // Wait for transaction to reach COMPLETE_COMMIT state
            waitForTransactionState(transactionalId, TransactionState.COMPLETE_COMMIT);

            // Verify consumer sees the records
            consumer.seekToBeginning(consumer.assignment());
            var consumedRecords = TransactionsTestHelper.consumeRecordsFor(consumer, CONSUME_DURATION_MS);
            assertEquals(numRecords, consumedRecords.size(),
                "Consumer should see all records after commit");

            // Verify transaction state
            var txnDescription = adminClient.describeTransactions(List.of(transactionalId))
                .description(transactionalId).get();
            assertEquals(TransactionState.COMPLETE_COMMIT, txnDescription.state(),
                "Transaction should be in COMPLETE_COMMIT state");
        }
    }

    @ClusterTest
    public void testPrepareAndCompleteTransactionAbort() throws Exception {
        // Test the 2PC API: prepareTransaction() and completeTransaction() for abort path.
        // This tests that completeTransaction() with a non-matching state triggers abort.
        var transactionalId = "test-prepare-complete-abort";
        var testTopic = "test-prepare-complete-abort-topic-" + System.nanoTime();
        clusterInstance.createTopic(testTopic, 1, TransactionsTestHelper.BROKER_COUNT, TOPIC_CONFIG);

        try (var consumer = createReadCommittedConsumer();
             var producer1 = create2PCProducer(transactionalId);
             var producer2 = create2PCProducer(transactionalId)) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofMillis(100));

            producer1.initTransactions();

            // First transaction - prepare but then abort
            producer1.beginTransaction();
            for (var i = 0; i < 3; i++) {
                producer1.send(new ProducerRecord<>(testTopic, 0, ("key1-" + i).getBytes(), ("value1-" + i).getBytes()));
            }
            producer1.flush();

            // Get marker for first transaction
            var marker1 = producer1.prepareTransaction();
            assertNotNull(marker1, "prepareTransaction should return prepared state");

            // Abort the first prepared transaction
            producer1.abortTransaction();

            // Second transaction - prepare and leave in prepared state
            producer1.beginTransaction();
            var numRecords = 5;
            for (var i = 0; i < numRecords; i++) {
                producer1.send(new ProducerRecord<>(testTopic, 0, ("key2-" + i).getBytes(), ("value2-" + i).getBytes()));
            }
            producer1.flush();

            // Get marker for second transaction
            var marker2 = producer1.prepareTransaction();
            assertNotNull(marker2, "prepareTransaction should return prepared state");

            // Simulate crash - leave second transaction in prepared state

            // New producer recovers
            producer2.initTransactions(true);  // keepPreparedTxn=true

            // Complete with marker1 (old, aborted transaction) - should trigger abort
            // because it doesn't match the current prepared state (marker2)
            producer2.completeTransaction(marker1);

            // Wait for transaction to reach COMPLETE_ABORT state
            waitForTransactionState(transactionalId, TransactionState.COMPLETE_ABORT);

            // Verify consumer sees NO records (both transactions aborted)
            consumer.seekToBeginning(consumer.assignment());
            var consumedRecords = TransactionsTestHelper.consumeRecordsFor(consumer, CONSUME_DURATION_MS);
            assertEquals(0, consumedRecords.size(),
                "Consumer should see no records after abort");

            // Verify transaction state
            var txnDescription = adminClient.describeTransactions(List.of(transactionalId))
                .description(transactionalId).get();
            assertEquals(TransactionState.COMPLETE_ABORT, txnDescription.state(),
                "Transaction should be in COMPLETE_ABORT state");
        }
    }

    @ClusterTest
    public void testProducerCrashAndRecoverWith2PC() throws Exception {
        // Test producer crash and recovery with 2PC keepPrepared transaction flow.
        // Note: 2PC is enabled server-side and triggered by calling
        // initTransactions(keepPreparedTxn=true) after crash.

        // Test single crash with commit
        test2PCRecovery(1, true);

        // Test single crash with abort
        test2PCRecovery(1, false);

        // Test multiple crashes before final commit (validates epoch progression)
        test2PCRecovery(3, true);
    }

    private void test2PCRecovery(int numCrashes, boolean shouldCommit) throws Exception {
        var transactionalId = "test-2pc-recovery-" + System.nanoTime();
        var testTopic = "test-2pc-topic-" + System.nanoTime();
        clusterInstance.createTopic(testTopic, 1, TransactionsTestHelper.BROKER_COUNT, TOPIC_CONFIG);

        try (var consumer = createReadCommittedConsumer()) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofMillis(100));  // Trigger assignment

            // Create producer and send records in a transaction
            var producer = create2PCProducer(transactionalId);
            producer.initTransactions();
            producer.beginTransaction();

            var numRecords = 5;
            for (var i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(testTopic, 0, ("key-" + i).getBytes(), ("value-" + i).getBytes()));
            }
            producer.flush();

            // Verify records not visible to read_committed consumer
            consumer.poll(Duration.ofMillis(100));
            assertEquals(0, consumer.assignment().stream().mapToLong(consumer::position).sum(),
                "Records should not be visible before commit");

            // Simulate crash by closing without committing
            producer.close(Duration.ZERO);

            short baseEpoch = 0;

            // Crash and recover numCrashes times
            for (var crashNum = 1; crashNum <= numCrashes; crashNum++) {
                var recoveredProducer = create2PCProducer(transactionalId);
                // Use keepPreparedTxn=true to preserve in-flight transaction
                recoveredProducer.initTransactions(true);

                // Crash the transaction coordinator to test resilience during 2PC recovery
                crashAndRestartTransactionCoordinator(transactionalId);

                // Verify dual identity after recovery
                var txnDescription = adminClient.describeTransactions(List.of(transactionalId))
                    .description(transactionalId).get();

                // After crash recovery, server should set up dual identity
                assertTrue(txnDescription.producerEpoch() >= baseEpoch,
                    "Crash " + crashNum + ": client epoch should be >= base epoch");

                // For simplicity in this test, we just verify epoch progression.
                // Detailed dual identity verification is in unit tests.

                if (crashNum < numCrashes) {
                    // Simulate another crash
                    recoveredProducer.close(Duration.ZERO);
                } else {
                    // Last recovery - complete the transaction
                    producer = recoveredProducer;
                }
            }

            try {
                // Complete prepared transaction directly.  Cannot call beginTransaction()
                // in prepared state - must call commitTransaction() or abortTransaction() directly.
                if (shouldCommit) {
                    producer.commitTransaction();
                } else {
                    producer.abortTransaction();
                }

                // Wait for the transaction to fully complete on the server before proceeding.
                // The client-side commitTransaction() returns after receiving the EndTxn response,
                // but the server might still be writing markers and transitioning to COMPLETE_* state.
                var expectedState = shouldCommit ? TransactionState.COMPLETE_COMMIT : TransactionState.COMPLETE_ABORT;
                waitForTransactionState(transactionalId, expectedState);

                // Verify consumer sees correct records
                consumer.seekToBeginning(consumer.assignment());
                var consumedRecords = TransactionsTestHelper.consumeRecordsFor(consumer, CONSUME_DURATION_MS);

                if (shouldCommit) {
                    assertEquals(numRecords, consumedRecords.size(),
                        "Consumer should see all records after commit");
                } else {
                    assertEquals(0, consumedRecords.size(),
                        "Consumer should see no records after abort");
                }

                // Verify fresh transaction works with bumped epoch
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(testTopic, 0, "fresh-key".getBytes(), "fresh-value".getBytes()));
                producer.commitTransaction();

                // Seek to beginning to read all records (both 2PC and fresh)
                consumer.seekToBeginning(consumer.assignment());
                var allRecords = TransactionsTestHelper.consumeRecordsFor(consumer, CONSUME_DURATION_MS);
                var expectedTotal = shouldCommit ? numRecords + 1 : 1;
                assertEquals(expectedTotal, allRecords.size(),
                    "Should have " + expectedTotal + " total records after fresh transaction");
            } finally {
                producer.close();
            }
        }
    }

    @ClusterTest
    public void testProducerIdRotationWithEpochExhaustion() throws Exception {
        // Test producer ID rotation when client epoch is exhausted.  This tests both scenarios:
        // 1. Rotation during initTransactions(keepPreparedTxn=true)
        // 2. Rotation during commitTransaction()

        // Scenario 1: Rotation happens during initTransactions(keepPreparedTxn=true) call
        // Example flow:
        //  1. After setProducerEpoch: epoch=32765
        //  2. First initTransactions() + beginTransaction(): epoch=32766, transaction ONGOING
        //  3. initTransactions(keepPreparedTxn=true): tries 32766+1=32767 → rotation triggered
        //     → Creates dual identity: producerId unchanged, nextProducerId=<new>, nextProducerEpoch=0
        //  4. commitTransaction(): Completes transition with nextProducerEpoch bumped 0→1
        //     → Final state: producerId=<new>, epoch=1
        testRotation((short) (Short.MAX_VALUE - 2), false);  // 32765

        // Scenario 1 with double rotation: First rotation during iteration, second during final init/commit
        testRotation((short) (Short.MAX_VALUE - 2), true);

        // Scenario 2: Rotation happens during commitTransaction() call
        // Example flow:
        //  1. After setProducerEpoch: epoch=32764
        //  2. First initTransactions() + beginTransaction(): epoch=32765, transaction ONGOING
        //  3. initTransactions(keepPreparedTxn=true): bumps to 32766 (not exhausted yet)
        //     → Creates dual identity: producerId unchanged, nextProducerId=<producerId>, nextProducerEpoch=32766
        //  4. commitTransaction(): tries 32766+1=32767 → rotation triggered
        //     → Final state: producerId=<new>, epoch=0
        testRotation((short) (Short.MAX_VALUE - 3), false);  // 32764

        // Scenario 2 with double rotation: First rotation during iteration, second during final init/commit
        testRotation((short) (Short.MAX_VALUE - 3), true);
    }

    private void testRotation(short startEpoch, boolean doubleRotation) throws Exception {
        var transactionalId = "test-rotation-" + System.nanoTime();
        var testTopic = "test-rotation-topic-" + System.nanoTime();
        clusterInstance.createTopic(testTopic, 1, TransactionsTestHelper.BROKER_COUNT, TOPIC_CONFIG);

        try (var consumer = createReadCommittedConsumer()) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofMillis(100));

            // Establish transactional ID
            try (var producer = create2PCProducer(transactionalId)) {
                producer.initTransactions();
            }

            // Set epoch to trigger rotation at desired point
            setProducerEpoch(transactionalId, startEpoch);

            // Create producer and start a prepared transaction
            try (var producer = create2PCProducer(transactionalId)) {
                producer.initTransactions();
                producer.beginTransaction();
                var numRecords = 3;
                for (var i = 0; i < numRecords; i++) {
                    producer.send(new ProducerRecord<>(testTopic, 0, ("key-" + i).getBytes(), ("value-" + i).getBytes()));
                }
                producer.flush();
                // Don't commit - leave transaction prepared (simulates crash)

                var originalProducerId = getProducerId(transactionalId);

                if (doubleRotation) {
                    // First rotation: loop calling initTransactions(true) until rotation occurs
                    var rotationCount = 0;
                    var currentClientId = originalProducerId;
                    var iteration = 0;
                    while (rotationCount == 0 && iteration < 20) {
                        iteration++;
                        var recoveryProducer = create2PCProducer(transactionalId);
                        recoveryProducer.initTransactions(true);  // keepPreparedTxn

                        var clientId = getClientProducerId(transactionalId);
                        if (clientId != currentClientId) {
                            rotationCount = 1;
                            currentClientId = clientId;
                        }
                        recoveryProducer.close(Duration.ZERO);
                    }

                    assertTrue(rotationCount >= 1, "First rotation should have occurred after " + iteration + " iterations");
                    assertEquals((short) 0, getClientProducerEpoch(transactionalId),
                        "After first rotation, client epoch should be exactly 0");

                    // Set client epoch high again to trigger second rotation
                    setClientProducerEpoch(transactionalId, startEpoch);

                    // Do a compensating epoch bump so that we get at the same epoch as w/o double rotation
                    var recoveryProducer = create2PCProducer(transactionalId);
                    recoveryProducer.initTransactions(true);
                    recoveryProducer.close(Duration.ZERO);
                }

                // We do 3 total epoch increments from the initial startEpoch:
                //   1. First initTransactions(): startEpoch → startEpoch + 1
                //   2. Second initTransactions(true): startEpoch + 1 → startEpoch + 2
                //   3. commitTransaction(): startEpoch + 2 → startEpoch + 3
                // Rotation may happen during step 2 or 3 depending on startEpoch.
                try (var finalProducer = create2PCProducer(transactionalId)) {
                    finalProducer.initTransactions(true);  // keepPreparedTxn - may rotate here

                    // Complete the transaction - may rotate here
                    finalProducer.commitTransaction();

                    // Wait for transaction to reach COMPLETE_COMMIT state
                    waitForTransactionState(transactionalId, TransactionState.COMPLETE_COMMIT);

                    // Verify that rotation happened (regardless of whether it occurred during
                    // initTransactions() or commitTransaction()). After transaction completes, verify:
                    //  - Producer ID changed (rotation occurred)
                    //  - Total epoch increments = 3 (accounting for overflow)
                    var finalProducerId = getProducerId(transactionalId);
                    assertNotEquals(originalProducerId, finalProducerId,
                        "Producer ID should have rotated by transaction completion (original=" + originalProducerId +
                            ", final=" + finalProducerId + ")");

                    // Verify epoch overflow occurred and total epoch increments.
                    var finalEpoch = getProducerEpoch(transactionalId);

                    // Final epoch is less than startEpoch (overflow occurred)
                    assertTrue(finalEpoch < startEpoch,
                        "Overflow should have occurred: finalEpoch=" + finalEpoch + " < startEpoch=" + startEpoch);

                    // Total epoch increments = 3 (accounting for overflow)
                    // Formula: finalEpoch + (MaxValue - startEpoch) = total increments
                    // This accounts for increments from startEpoch to MaxValue boundary, then
                    // wrap to 0 and increments to finalEpoch.
                    var totalIncrements = finalEpoch + Short.MAX_VALUE - startEpoch;
                    assertEquals(3, totalIncrements,
                        "Should have 3 total epoch increments: finalEpoch=" + finalEpoch + " + " +
                            "(MaxValue=" + Short.MAX_VALUE + " - startEpoch=" + startEpoch + ") = " + totalIncrements);

                    // Verify consumer sees all records
                    consumer.seekToBeginning(consumer.assignment());
                    var consumedRecords = TransactionsTestHelper.consumeRecordsFor(consumer, CONSUME_DURATION_MS);
                    assertEquals(numRecords, consumedRecords.size(),
                        "Consumer should see all records despite rotation");
                }
            }
        }
    }

    private Producer<byte[], byte[]> create2PCProducer(String transactionalId) {
        // The transaction timeout is not set: it must not be configured when 2PC is enabled.
        return clusterInstance.producer(Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000",
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000",
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000",
            ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG, "true"
        ));
    }

    private Consumer<byte[], byte[]> createReadCommittedConsumer() {
        return TransactionsTestHelper.createReadCommittedConsumer(
            clusterInstance, GroupProtocol.CONSUMER, "transactional-group");
    }

    /**
     * Wait until a transaction reaches a specific state.
     * Polls listTransactions API until the transaction with given ID reaches the expected state.
     */
    private void waitForTransactionState(String transactionalId, TransactionState expectedState) throws InterruptedException {
        TestUtils.waitForCondition(() -> adminClient.listTransactions().all().get().stream()
                .anyMatch(txn -> txn.transactionalId().equals(transactionalId) && txn.state() == expectedState),
            "Transaction did not reach " + expectedState + " state");
    }

    /**
     * Access the transaction metadata for a given transactional ID.
     * Finds the transaction coordinator and applies the provided function to the transaction metadata.
     */
    private <T> T withTransactionMetadata(String transactionalId, Function<TransactionMetadata, T> f) throws Exception {
        var coordinatorId = findTransactionCoordinatorId(transactionalId);
        var brokerServer = (BrokerServer) clusterInstance.brokers().get(coordinatorId);
        var metadata = brokerServer.transactionCoordinator().transactionManager().getTransactionMetadata(transactionalId)
            .orElseThrow(() -> new IllegalStateException("Transaction metadata not found for " + transactionalId));
        return f.apply(metadata);
    }

    /**
     * Manually set producer epoch to a high value for testing epoch exhaustion.
     */
    private void setProducerEpoch(String transactionalId, short epoch) throws Exception {
        withTransactionMetadata(transactionalId, metadata -> metadata.inLock(() -> {
            metadata.setProducerEpoch(epoch);
            return null;
        }));
    }

    /**
     * Manually set client producer epoch (nextProducerEpoch) for testing 2PC epoch exhaustion.
     * This is used when testing epoch exhaustion during prepared transactions where client credentials
     * are stored in nextProducerId/nextProducerEpoch.
     */
    private void setClientProducerEpoch(String transactionalId, short epoch) throws Exception {
        withTransactionMetadata(transactionalId, metadata -> metadata.inLock(() -> {
            metadata.setNextProducerEpoch(epoch);
            return null;
        }));
    }

    /**
     * Get current producer ID from DescribeTransactions API.
     */
    private long getProducerId(String transactionalId) throws Exception {
        return adminClient.describeTransactions(List.of(transactionalId))
            .description(transactionalId).get().producerId();
    }

    /**
     * Get current producer epoch from DescribeTransactions API.
     */
    private short getProducerEpoch(String transactionalId) throws Exception {
        return (short) adminClient.describeTransactions(List.of(transactionalId))
            .description(transactionalId).get().producerEpoch();
    }

    /**
     * Get client-facing producer ID (for 2PC dual identity scenarios).
     * When a transaction is prepared, the ongoing transaction keeps its original ID/epoch,
     * but the client gets new credentials (nextProducerId/nextProducerEpoch).
     */
    private long getClientProducerId(String transactionalId) throws Exception {
        return withTransactionMetadata(transactionalId, TransactionMetadata::clientProducerId);
    }

    /**
     * Get client-facing producer epoch (for 2PC dual identity scenarios).
     * When a transaction is prepared, the ongoing transaction keeps its original ID/epoch,
     * but the client gets new credentials (nextProducerId/nextProducerEpoch).
     */
    private short getClientProducerEpoch(String transactionalId) throws Exception {
        return withTransactionMetadata(transactionalId, TransactionMetadata::clientProducerEpoch);
    }

    /**
     * Find the transaction coordinator broker ID for a given transactional ID.
     */
    private int findTransactionCoordinatorId(String transactionalId) throws Exception {
        return adminClient.describeTransactions(List.of(transactionalId))
            .description(transactionalId).get().coordinatorId();
    }

    /**
     * Crash the transaction coordinator, wait for a new coordinator to be elected,
     * restart the crashed broker, and wait for leadership to stabilize.
     * This simulates a coordinator failure during transaction processing.
     */
    private void crashAndRestartTransactionCoordinator(String transactionalId) throws Exception {
        // Find the current coordinator
        var oldCoordinatorId = findTransactionCoordinatorId(transactionalId);
        var transactionPartitionId = ((BrokerServer) clusterInstance.brokers().get(oldCoordinatorId))
            .transactionCoordinator().partitionFor(transactionalId);
        var transactionPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, transactionPartitionId);

        // Shutdown the coordinator broker
        clusterInstance.shutdownBroker(oldCoordinatorId);

        // Wait for a new coordinator to be elected (coordinator ID should change)
        TestUtils.waitForCondition(() -> {
            try {
                return findTransactionCoordinatorId(transactionalId) != oldCoordinatorId;
            } catch (Exception e) {
                return false;  // Coordinator not yet available
            }
        }, "New coordinator was not elected after shutting down broker " + oldCoordinatorId);

        // Restart the old broker
        clusterInstance.startBroker(oldCoordinatorId);

        // Wait for the broker to fully start and metadata to propagate
        clusterInstance.ensureConsistentMetadata();

        // Wait for the old coordinator to be in ISR before triggering preferred leader election
        TestUtils.waitForCondition(() -> {
            try {
                var topicDesc = adminClient.describeTopics(Set.of(Topic.TRANSACTION_STATE_TOPIC_NAME)).allTopicNames().get();
                return topicDesc.get(Topic.TRANSACTION_STATE_TOPIC_NAME).partitions().stream()
                    .filter(p -> p.partition() == transactionPartitionId)
                    .anyMatch(p -> p.isr().stream().anyMatch(node -> node.id() == oldCoordinatorId));
            } catch (Exception e) {
                return false;
            }
        }, "Old coordinator " + oldCoordinatorId + " did not rejoin ISR for partition " + transactionPartition);

        // Trigger preferred leader election for the transaction state topic partition
        adminClient.electLeaders(ElectionType.PREFERRED, Set.of(transactionPartition)).all().get();

        // Wait until the old coordinator regains leadership.
        TestUtils.waitForCondition(() -> {
            try {
                return findTransactionCoordinatorId(transactionalId) == oldCoordinatorId;
            } catch (Exception e) {
                return false;  // Coordinator not yet available
            }
        }, "Old coordinator " + oldCoordinatorId + " did not regain leadership after restart");
    }
}
