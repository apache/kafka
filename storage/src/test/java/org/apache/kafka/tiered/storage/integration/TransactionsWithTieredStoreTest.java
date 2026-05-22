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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.integration.TransactionsTestHelper.TransactionHooks;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.STORAGE_WAIT_TIMEOUT_SEC;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createTopicConfigForRemoteStorage;

/**
 * Runs transaction tests with tiered storage enabled.
 * <p>
 * This test uses {@link TransactionsTestHelper} for the shared test flow methods,
 * providing tiered-storage-specific hook implementations for verifying log offsets
 * and segment uploads.
 */
public class TransactionsWithTieredStoreTest {

    private static final String TEST_CLASS_NAME = "transactionswithtiredstoretest";
    private static final int BROKER_COUNT = 3;
    
    private static Map<String, String> baseServerProperties() {
        Map<String, String> serverProps = createServerPropsForRemoteStorage(TEST_CLASS_NAME, BROKER_COUNT, 3);
        serverProps.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false");
        serverProps.put("offsets.topic.num.partitions", "1");
        serverProps.put("transaction.state.log.num.partitions", "3");
        serverProps.put("transaction.state.log.replication.factor", "2");
        serverProps.put("transaction.state.log.min.isr", "2");
        serverProps.put("controlled.shutdown.enable", "true");
        serverProps.put("unclean.leader.election.enable", "false");
        serverProps.put("auto.leader.rebalance.enable", "false");
        serverProps.put("group.initial.rebalance.delay.ms", "0");
        serverProps.put("transaction.abort.timed.out.transaction.cleanup.interval.ms", "200");
        return serverProps;
    }

    private static List<ClusterConfig> tieredStorageClusterConfig() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.CO_KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(baseServerProperties())
                .build());
    }

    private static List<ClusterConfig> tieredStorageClusterConfigTV1() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.CO_KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(baseServerProperties())
                .setFeatures(Map.of(Feature.TRANSACTION_VERSION, (short) 1))
                .build());
    }

    private static List<ClusterConfig> tieredStorageClusterConfigTV2() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.CO_KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(baseServerProperties())
                .setFeatures(Map.of(Feature.TRANSACTION_VERSION, (short) 2))
                .build());
    }

    private static Map<String, String> topicConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        config.putAll(createTopicConfigForRemoteStorage(true, 1));
        return config;
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicBasicTransactions(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testBasicTransactions(
                clusterInstance, GroupProtocol.CLASSIC, createTieredHooks(clusterInstance), topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncBasicTransactions(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testBasicTransactions(
                clusterInstance, GroupProtocol.CONSUMER, createTieredHooks(clusterInstance), topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicDelayedFetchIncludesAbortedTransaction(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testDelayedFetchIncludesAbortedTransaction(
                clusterInstance, GroupProtocol.CLASSIC, createTieredHooks(clusterInstance), topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncDelayedFetchIncludesAbortedTransaction(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testDelayedFetchIncludesAbortedTransaction(
                clusterInstance, GroupProtocol.CONSUMER, createTieredHooks(clusterInstance), topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicSendOffsetsWithGroupMetadata(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testSendOffsetsWithGroupMetadata(
                clusterInstance, GroupProtocol.CLASSIC, createTieredHooks(clusterInstance), topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncSendOffsetsWithGroupMetadata(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testSendOffsetsWithGroupMetadata(
                clusterInstance, GroupProtocol.CONSUMER, createTieredHooks(clusterInstance), topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicReadCommittedConsumerShouldNotSeeUndecidedData(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testReadCommittedConsumerShouldNotSeeUndecidedData(
                clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncReadCommittedConsumerShouldNotSeeUndecidedData(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testReadCommittedConsumerShouldNotSeeUndecidedData(
                clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicFencingOnCommit(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnCommit(clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncFencingOnCommit(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnCommit(clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicFencingOnSendOffsets(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnSendOffsets(clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncFencingOnSendOffsets(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnSendOffsets(clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicOffsetMetadataInSendOffsetsToTransaction(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testOffsetMetadataInSendOffsetsToTransaction(
                clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncOffsetMetadataInSendOffsetsToTransaction(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testOffsetMetadataInSendOffsetsToTransaction(
                clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testInitTransactionsTimeout(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testInitTransactionsTimeout(clusterInstance, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testSendOffsetsToTransactionTimeout(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testSendOffsetsToTransactionTimeout(clusterInstance, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testCommitTransactionTimeout(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testCommitTransactionTimeout(clusterInstance, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAbortTransactionTimeout(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testAbortTransactionTimeout(clusterInstance, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicFencingOnSend(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnSend(clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncFencingOnSend(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnSend(clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicFencingOnAddPartitions(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnAddPartitions(clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncFencingOnAddPartitions(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnAddPartitions(clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicFencingOnTransactionExpiration(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnTransactionExpiration(clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncFencingOnTransactionExpiration(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFencingOnTransactionExpiration(clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testClassicMultipleMarkersOneLeader(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testMultipleMarkersOneLeader(clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testAsyncMultipleMarkersOneLeader(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testMultipleMarkersOneLeader(clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testConsecutivelyRunInitTransactions(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testConsecutivelyRunInitTransactions(clusterInstance);
    }

    @ClusterTemplate("tieredStorageClusterConfig")
    public void testRecoveryFromEpochOverflow(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testRecoveryFromEpochOverflow(clusterInstance, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV1")
    public void testClassicBumpTransactionalEpochWithTV2Disabled(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testBumpTransactionalEpochWithTV2Disabled(
                clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV1")
    public void testAsyncBumpTransactionalEpochWithTV2Disabled(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testBumpTransactionalEpochWithTV2Disabled(
                clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV2")
    public void testClassicBumpTransactionalEpochWithTV2Enabled(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testBumpTransactionalEpochWithTV2Enabled(
                clusterInstance, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV2")
    public void testAsyncBumpTransactionalEpochWithTV2Enabled(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testBumpTransactionalEpochWithTV2Enabled(
                clusterInstance, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV1")
    public void testFailureToFenceEpochWithTV1(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFailureToFenceEpoch(clusterInstance, false, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV2")
    public void testFailureToFenceEpochWithTV2(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testFailureToFenceEpoch(clusterInstance, true, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV1")
    public void testEmptyAbortAfterCommitWithTV1(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testEmptyAbortAfterCommit(clusterInstance, topicConfig());
    }

    @ClusterTemplate("tieredStorageClusterConfigTV2")
    public void testEmptyAbortAfterCommitWithTV2(ClusterInstance clusterInstance) throws Exception {
        TransactionsTestHelper.testEmptyAbortAfterCommit(clusterInstance, topicConfig());
    }

    private TransactionHooks createTieredHooks(ClusterInstance clusterInstance) {
        return new TransactionHooks() {
            @Override
            public void verifyLogStartOffsets(Map<TopicPartition, Long> expectedOffsets) throws InterruptedException {
                Map<Integer, Long> offsets = new HashMap<>();
                TestUtils.waitForCondition(() ->
                        clusterInstance.brokers().values().stream().allMatch(broker ->
                                expectedOffsets.entrySet().stream().allMatch(entry -> {
                                    long lso = broker.replicaManager().localLog(entry.getKey()).get().logStartOffset();
                                    offsets.put(broker.config().brokerId(), lso);
                                    return entry.getValue() == lso;
                                })
                        ), () -> "log start offset doesn't change to the expected position: " + expectedOffsets
                        + ", current position: " + offsets);
            }

            @Override
            public void maybeVerifyLocalLogStartOffsets(Map<TopicPartition, Long> expectedOffsets) throws InterruptedException {
                Map<Integer, Long> offsets = new HashMap<>();
                TestUtils.waitForCondition(() ->
                        clusterInstance.brokers().values().stream().allMatch(broker ->
                                expectedOffsets.entrySet().stream().allMatch(entry -> {
                                    long offset = broker.replicaManager().localLog(entry.getKey()).get().localLogStartOffset();
                                    offsets.put(broker.config().brokerId(), offset);
                                    return entry.getValue() == offset;
                                })
                        ), () -> "local log start offset doesn't change to the expected position: " + expectedOffsets
                        + ", current position: " + offsets);
            }

            @Override
            public void maybeWaitForAtLeastOneSegmentUpload(List<TopicPartition> topicPartitions) {
                for (TopicPartition topicPartition : topicPartitions) {
                    List<BrokerLocalStorage> localStorages = clusterInstance.brokers().values().stream()
                            .map(b -> new BrokerLocalStorage(b.config().brokerId(),
                                    Set.copyOf(b.config().logDirs()), STORAGE_WAIT_TIMEOUT_SEC))
                            .toList();
                    Set<Integer> assignedReplicas = getAssignedReplicaIds(clusterInstance, topicPartition);
                    localStorages.stream()
                            .filter(s -> assignedReplicas.contains(s.getBrokerId()))
                            .filter(s -> isAlive(clusterInstance, s.getBrokerId()))
                            .forEach(localStorage ->
                                    localStorage.waitForAtLeastEarliestLocalOffset(topicPartition, 1L));
                }
            }
        };
    }

    private static Set<Integer> getAssignedReplicaIds(ClusterInstance clusterInstance, TopicPartition topicPartition) {
        try (var admin = clusterInstance.admin()) {
            return admin.describeTopics(List.of(topicPartition.topic()))
                    .allTopicNames().get()
                    .get(topicPartition.topic())
                    .partitions().stream()
                    .filter(p -> p.partition() == topicPartition.partition())
                    .flatMap(p -> p.replicas().stream())
                    .map(Node::id)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            return Set.of();
        }
    }

    private static boolean isAlive(ClusterInstance clusterInstance, int brokerId) {
        return clusterInstance.aliveBrokers().containsKey(brokerId);
    }
}
