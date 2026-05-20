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

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.config.TopicConfig;
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

import java.util.Map;

// Placed in the storage module to avoid a cross-module dependency from clients-integration-tests on storage.
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
    }
)
public class TransactionsTest {

    private static final Map<String, String> TOPIC_CONFIG = Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");

    private final ClusterInstance clusterInstance;

    public TransactionsTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testBasicTransactions() throws Exception {
        TransactionsTestHelper.testBasicTransactions(
                clusterInstance, GroupProtocol.CONSUMER, TransactionsTestHelper.NO_OP_HOOKS, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testReadCommittedConsumerShouldNotSeeUndecidedData() throws Exception {
        TransactionsTestHelper.testReadCommittedConsumerShouldNotSeeUndecidedData(
                clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testDelayedFetchIncludesAbortedTransaction() throws Exception {
        TransactionsTestHelper.testDelayedFetchIncludesAbortedTransaction(
                clusterInstance, GroupProtocol.CONSUMER, TransactionsTestHelper.NO_OP_HOOKS, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testClassicSendOffsetsWithGroupMetadata() throws Exception {
        TransactionsTestHelper.testSendOffsetsWithGroupMetadata(
                clusterInstance, GroupProtocol.CLASSIC, TransactionsTestHelper.NO_OP_HOOKS, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testAsyncSendOffsetsWithGroupMetadata() throws Exception {
        TransactionsTestHelper.testSendOffsetsWithGroupMetadata(
                clusterInstance, GroupProtocol.CONSUMER, TransactionsTestHelper.NO_OP_HOOKS, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testFencingOnCommit() throws Exception {
        TransactionsTestHelper.testFencingOnCommit(clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testFencingOnSendOffsets() throws Exception {
        TransactionsTestHelper.testFencingOnSendOffsets(clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testClassicOffsetMetadataInSendOffsetsToTransaction() throws Exception {
        TransactionsTestHelper.testOffsetMetadataInSendOffsetsToTransaction(
                clusterInstance, GroupProtocol.CLASSIC, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testAsyncOffsetMetadataInSendOffsetsToTransaction() throws Exception {
        TransactionsTestHelper.testOffsetMetadataInSendOffsetsToTransaction(
                clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testInitTransactionsTimeout() throws Exception {
        TransactionsTestHelper.testInitTransactionsTimeout(clusterInstance, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testSendOffsetsToTransactionTimeout() throws Exception {
        TransactionsTestHelper.testSendOffsetsToTransactionTimeout(clusterInstance, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testCommitTransactionTimeout() throws Exception {
        TransactionsTestHelper.testCommitTransactionTimeout(clusterInstance, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testAbortTransactionTimeout() throws Exception {
        TransactionsTestHelper.testAbortTransactionTimeout(clusterInstance, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testFencingOnSend() throws Exception {
        TransactionsTestHelper.testFencingOnSend(clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testFencingOnAddPartitions() throws Exception {
        TransactionsTestHelper.testFencingOnAddPartitions(clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testFencingOnTransactionExpiration() throws Exception {
        TransactionsTestHelper.testFencingOnTransactionExpiration(clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testMultipleMarkersOneLeader() throws Exception {
        TransactionsTestHelper.testMultipleMarkersOneLeader(clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest
    public void testConsecutivelyRunInitTransactions() {
        TransactionsTestHelper.testConsecutivelyRunInitTransactions(clusterInstance);
    }

    @ClusterTest
    public void testRecoveryFromEpochOverflow() throws Exception {
        TransactionsTestHelper.testRecoveryFromEpochOverflow(clusterInstance, TOPIC_CONFIG);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)})
    public void testBumpTransactionalEpochWithTV2Disabled() throws Exception {
        TransactionsTestHelper.testBumpTransactionalEpochWithTV2Disabled(
                clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    public void testBumpTransactionalEpochWithTV2Enabled() throws Exception {
        TransactionsTestHelper.testBumpTransactionalEpochWithTV2Enabled(
                clusterInstance, GroupProtocol.CONSUMER, TOPIC_CONFIG);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)})
    public void testFailureToFenceEpochWithTV1() throws Exception {
        TransactionsTestHelper.testFailureToFenceEpoch(clusterInstance, false, TOPIC_CONFIG);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    public void testFailureToFenceEpochWithTV2() throws Exception {
        TransactionsTestHelper.testFailureToFenceEpoch(clusterInstance, true, TOPIC_CONFIG);
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    public void testEmptyAbortAfterCommit() throws Exception {
        TransactionsTestHelper.testEmptyAbortAfterCommit(clusterInstance, TOPIC_CONFIG);
    }
}
