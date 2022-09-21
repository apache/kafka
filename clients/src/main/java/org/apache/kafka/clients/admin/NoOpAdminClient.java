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

package org.apache.kafka.clients.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;


public class NoOpAdminClient extends AdminClient {
    @Override
    public void close(Duration timeout) {}

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        return null;
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options) {
        return null;
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        return null;
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        return null;
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        return null;
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        return null;
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        return null;
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        return null;
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        return null;
    }

    @Deprecated
    @Override
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        return null;
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
        AlterConfigsOptions options) {
        return null;
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
        AlterReplicaLogDirsOptions options) {
        return null;
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        return null;
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas,
        DescribeReplicaLogDirsOptions options) {
        return null;
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
        CreatePartitionsOptions options) {
        return null;
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
        DeleteRecordsOptions options) {
        return null;
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        return null;
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
        return null;
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
        return null;
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        return null;
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
        DescribeConsumerGroupsOptions options) {
        return null;
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        return null;
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId,
        ListConsumerGroupOffsetsOptions options) {
        return null;
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds,
        DeleteConsumerGroupsOptions options) {
        return null;
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions options) {
        return null;
    }

    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions,
        ElectLeadersOptions options) {
        return null;
    }

    @Override
    public ElectLeadersResult electRecommendedLeaders(
        Map<TopicPartition, Integer> partitionsWithRecommendedLeaders,
        ElectLeadersOptions options) {
        return null;
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
        AlterPartitionReassignmentsOptions options) {
        return null;
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
        ListPartitionReassignmentsOptions options) {
        return null;
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId,
        RemoveMembersFromConsumerGroupOptions options) {
        return null;
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId,
        Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        return null;
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
        ListOffsetsOptions options) {
        return null;
    }

    @Override
    public MoveControllerResult moveController(MoveControllerOptions options) {
        return null;
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter,
        DescribeClientQuotasOptions options) {
        return null;
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries,
        AlterClientQuotasOptions options) {
        return null;
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users,
        DescribeUserScramCredentialsOptions options) {
        return null;
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations,
        AlterUserScramCredentialsOptions options) {
        return null;
    }

    @Override
    public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
        return null;
    }

    @Override
    public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates,
        UpdateFeaturesOptions options) {
        return null;
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        return null;
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions,
        DescribeProducersOptions options) {
        return null;
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds,
        DescribeTransactionsOptions options) {
        return null;
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        return null;
    }

    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public SkipShutdownSafetyCheckResult skipShutdownSafetyCheck(SkipShutdownSafetyCheckOptions options) {
        return null;
    }
}
