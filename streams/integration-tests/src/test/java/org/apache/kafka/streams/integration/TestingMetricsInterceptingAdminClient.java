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

package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.AbortTransactionOptions;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AddRaftVoterOptions;
import org.apache.kafka.clients.admin.AddRaftVoterResult;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClassicGroupsOptions;
import org.apache.kafka.clients.admin.DescribeClassicGroupsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesOptions;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.RemoveRaftVoterOptions;
import org.apache.kafka.clients.admin.RemoveRaftVoterResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.clients.admin.UnregisterBrokerOptions;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Wrapper for Admin client for use in Kafka Streams integration test
 */
public class TestingMetricsInterceptingAdminClient extends AdminClient {

    public final List<KafkaMetric> passedMetrics = new ArrayList<>();
    private final Admin adminDelegate;

    public TestingMetricsInterceptingAdminClient(final Map<String, Object> config) {
        adminDelegate = AdminClient.create(config);
    }

    @Override
    public void close(final Duration timeout) {
        adminDelegate.close(timeout);
    }

    @Override
    public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics, final CreateTopicsOptions options) {
        return adminDelegate.createTopics(newTopics, options);
    }

    @Override
    public DeleteTopicsResult deleteTopics(final TopicCollection topics, final DeleteTopicsOptions options) {
        return adminDelegate.deleteTopics(topics, options);
    }

    @Override
    public ListTopicsResult listTopics(final ListTopicsOptions options) {
        return adminDelegate.listTopics(options);
    }

    @Override
    public DescribeTopicsResult describeTopics(final TopicCollection topics, final DescribeTopicsOptions options) {
        return adminDelegate.describeTopics(topics, options);
    }

    @Override
    public DescribeClusterResult describeCluster(final DescribeClusterOptions options) {
        return adminDelegate.describeCluster(options);
    }

    @Override
    public DescribeAclsResult describeAcls(final AclBindingFilter filter, final DescribeAclsOptions options) {
        return adminDelegate.describeAcls(filter, options);
    }

    @Override
    public CreateAclsResult createAcls(final Collection<AclBinding> acls, final CreateAclsOptions options) {
        return adminDelegate.createAcls(acls, options);
    }

    @Override
    public DeleteAclsResult deleteAcls(final Collection<AclBindingFilter> filters, final DeleteAclsOptions options) {
        return adminDelegate.deleteAcls(filters, options);
    }

    @Override
    public DescribeConfigsResult describeConfigs(final Collection<ConfigResource> resources, final DescribeConfigsOptions options) {
        return adminDelegate.describeConfigs(resources, options);
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(final Map<ConfigResource, Collection<AlterConfigOp>> configs, final AlterConfigsOptions options) {
        return adminDelegate.incrementalAlterConfigs(configs, options);
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(final Map<TopicPartitionReplica, String> replicaAssignment, final AlterReplicaLogDirsOptions options) {
        return adminDelegate.alterReplicaLogDirs(replicaAssignment, options);
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(final Collection<Integer> brokers, final DescribeLogDirsOptions options) {
        return adminDelegate.describeLogDirs(brokers, options);
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(final Collection<TopicPartitionReplica> replicas, final DescribeReplicaLogDirsOptions options) {
        return adminDelegate.describeReplicaLogDirs(replicas, options);
    }

    @Override
    public CreatePartitionsResult createPartitions(final Map<String, NewPartitions> newPartitions, final CreatePartitionsOptions options) {
        return adminDelegate.createPartitions(newPartitions, options);
    }

    @Override
    public DeleteRecordsResult deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete, final DeleteRecordsOptions options) {
        return adminDelegate.deleteRecords(recordsToDelete, options);
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(final CreateDelegationTokenOptions options) {
        return adminDelegate.createDelegationToken(options);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(final byte[] hmac, final RenewDelegationTokenOptions options) {
        return adminDelegate.renewDelegationToken(hmac, options);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(final byte[] hmac, final ExpireDelegationTokenOptions options) {
        return adminDelegate.expireDelegationToken(hmac, options);
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(final DescribeDelegationTokenOptions options) {
        return adminDelegate.describeDelegationToken(options);
    }

    @Override
    public ListGroupsResult listGroups(final ListGroupsOptions options) {
        return adminDelegate.listGroups(options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(final Collection<String> groupIds, final DescribeConsumerGroupsOptions options) {
        return adminDelegate.describeConsumerGroups(groupIds, options);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(final ListConsumerGroupsOptions options) {
        return adminDelegate.listConsumerGroups(options);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, final ListConsumerGroupOffsetsOptions options) {
        return adminDelegate.listConsumerGroupOffsets(groupSpecs, options);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(final Collection<String> groupIds, final DeleteConsumerGroupsOptions options) {
        return adminDelegate.deleteConsumerGroups(groupIds, options);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(final String groupId, final Set<TopicPartition> partitions, final DeleteConsumerGroupOffsetsOptions options) {
        return adminDelegate.deleteConsumerGroupOffsets(groupId, partitions, options);
    }

    @Override
    public ElectLeadersResult electLeaders(final ElectionType electionType, final Set<TopicPartition> partitions, final ElectLeadersOptions options) {
        return adminDelegate.electLeaders(electionType, partitions, options);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(final Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments, final AlterPartitionReassignmentsOptions options) {
        return adminDelegate.alterPartitionReassignments(reassignments, options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(final Optional<Set<TopicPartition>> partitions, final ListPartitionReassignmentsOptions options) {
        return adminDelegate.listPartitionReassignments(partitions, options);
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(final String groupId, final RemoveMembersFromConsumerGroupOptions options) {
        return adminDelegate.removeMembersFromConsumerGroup(groupId, options);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(final String groupId, final Map<TopicPartition, OffsetAndMetadata> offsets, final AlterConsumerGroupOffsetsOptions options) {
        return adminDelegate.alterConsumerGroupOffsets(groupId, offsets, options);
    }

    @Override
    public ListOffsetsResult listOffsets(final Map<TopicPartition, OffsetSpec> topicPartitionOffsets, final ListOffsetsOptions options) {
        return adminDelegate.listOffsets(topicPartitionOffsets, options);
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(final ClientQuotaFilter filter, final DescribeClientQuotasOptions options) {
        return adminDelegate.describeClientQuotas(filter, options);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(final Collection<ClientQuotaAlteration> entries, final AlterClientQuotasOptions options) {
        return adminDelegate.alterClientQuotas(entries, options);
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(final List<String> users, final DescribeUserScramCredentialsOptions options) {
        return adminDelegate.describeUserScramCredentials(users, options);
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(final List<UserScramCredentialAlteration> alterations, final AlterUserScramCredentialsOptions options) {
        return adminDelegate.alterUserScramCredentials(alterations, options);
    }

    @Override
    public DescribeFeaturesResult describeFeatures(final DescribeFeaturesOptions options) {
        return adminDelegate.describeFeatures(options);
    }

    @Override
    public UpdateFeaturesResult updateFeatures(final Map<String, FeatureUpdate> featureUpdates, final UpdateFeaturesOptions options) {
        return adminDelegate.updateFeatures(featureUpdates, options);
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(final DescribeMetadataQuorumOptions options) {
        return adminDelegate.describeMetadataQuorum(options);
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(final int brokerId, final UnregisterBrokerOptions options) {
        return adminDelegate.unregisterBroker(brokerId, options);
    }

    @Override
    public DescribeProducersResult describeProducers(final Collection<TopicPartition> partitions, final DescribeProducersOptions options) {
        return adminDelegate.describeProducers(partitions, options);
    }

    @Override
    public DescribeTransactionsResult describeTransactions(final Collection<String> transactionalIds, final DescribeTransactionsOptions options) {
        return adminDelegate.describeTransactions(transactionalIds, options);
    }

    @Override
    public AbortTransactionResult abortTransaction(final AbortTransactionSpec spec, final AbortTransactionOptions options) {
        return adminDelegate.abortTransaction(spec, options);
    }

    @Override
    public ListTransactionsResult listTransactions(final ListTransactionsOptions options) {
        return adminDelegate.listTransactions(options);
    }

    @Override
    public FenceProducersResult fenceProducers(final Collection<String> transactionalIds, final FenceProducersOptions options) {
        return adminDelegate.fenceProducers(transactionalIds, options);
    }

    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources(final ListClientMetricsResourcesOptions options) {
        return adminDelegate.listClientMetricsResources(options);
    }

    @Override
    public Uuid clientInstanceId(final Duration timeout) {
        return adminDelegate.clientInstanceId(timeout);
    }

    @Override
    public AddRaftVoterResult addRaftVoter(final int voterId, final Uuid voterDirectoryId, final Set<RaftVoterEndpoint> endpoints, final AddRaftVoterOptions options) {
        return adminDelegate.addRaftVoter(voterId, voterDirectoryId, endpoints, options);
    }

    @Override
    public RemoveRaftVoterResult removeRaftVoter(final int voterId, final Uuid voterDirectoryId, final RemoveRaftVoterOptions options) {
        return adminDelegate.removeRaftVoter(voterId, voterDirectoryId, options);
    }

    @Override
    public DescribeShareGroupsResult describeShareGroups(final Collection<String> groupIds, final DescribeShareGroupsOptions options) {
        return adminDelegate.describeShareGroups(groupIds, options);
    }

    @Override
    public DescribeClassicGroupsResult describeClassicGroups(final Collection<String> groupIds, final DescribeClassicGroupsOptions options) {
        return adminDelegate.describeClassicGroups(groupIds, options);
    }

    @Override
    public void registerMetricForSubscription(final KafkaMetric metric) {
        passedMetrics.add(metric);
        adminDelegate.registerMetricForSubscription(metric);
    }

    @Override
    public void unregisterMetricFromSubscription(final KafkaMetric metric) {
        passedMetrics.remove(metric);
        adminDelegate.unregisterMetricFromSubscription(metric);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return adminDelegate.metrics();
    }
}
