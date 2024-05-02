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
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@code ForwardingAdmin} is the default value of {@code forwarding.admin.class} in MirrorMaker.
 * Users who wish to customize the MirrorMaker behaviour for the creation of topics and access control lists can extend this
 * class without needing to provide a whole implementation of {@code Admin}.
 * The class must have a constructor with signature {@code (Map<String, Object> config)} for configuring
 * a decorated {@link KafkaAdminClient} and any other clients needed for external resource management.
 */
public class ForwardingAdmin implements Admin {
    private final Admin delegate;

    public ForwardingAdmin(Map<String, Object> configs) {
        this.delegate = Admin.create(configs);
    }

    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        return delegate.createTopics(newTopics, options);
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options) {
        return delegate.deleteTopics(topics, options);
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        return delegate.listTopics(options);
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
        return delegate.describeTopics(topics, options);
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        return delegate.describeCluster(options);
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        return delegate.describeAcls(filter, options);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        return delegate.createAcls(acls, options);
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        return delegate.deleteAcls(filters, options);
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        return delegate.describeConfigs(resources, options);
    }

    @Deprecated
    @Override
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        return delegate.alterConfigs(configs, options);
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
        return delegate.incrementalAlterConfigs(configs, options);
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options) {
        return delegate.alterReplicaLogDirs(replicaAssignment, options);
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        return delegate.describeLogDirs(brokers, options);
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        return delegate.describeReplicaLogDirs(replicas, options);
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        return delegate.createPartitions(newPartitions, options);
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        return delegate.deleteRecords(recordsToDelete, options);
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        return delegate.createDelegationToken(options);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
        return delegate.renewDelegationToken(hmac, options);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
        return delegate.expireDelegationToken(hmac, options);
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        return delegate.describeDelegationToken(options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        return delegate.describeConsumerGroups(groupIds, options);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        return delegate.listConsumerGroups(options);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options) {
        return delegate.listConsumerGroupOffsets(groupSpecs, options);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        return delegate.deleteConsumerGroups(groupIds, options);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        return delegate.deleteConsumerGroupOffsets(groupId, partitions, options);
    }

    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions, ElectLeadersOptions options) {
        return delegate.electLeaders(electionType, partitions, options);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments, AlterPartitionReassignmentsOptions options) {
        return delegate.alterPartitionReassignments(reassignments, options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions, ListPartitionReassignmentsOptions options) {
        return delegate.listPartitionReassignments(partitions, options);
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options) {
        return delegate.removeMembersFromConsumerGroup(groupId, options);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        return delegate.alterConsumerGroupOffsets(groupId, offsets, options);
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        return delegate.listOffsets(topicPartitionOffsets, options);
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        return delegate.describeClientQuotas(filter, options);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        return delegate.alterClientQuotas(entries, options);
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
        return delegate.describeUserScramCredentials(users, options);
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options) {
        return delegate.alterUserScramCredentials(alterations, options);
    }

    @Override
    public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
        return delegate.describeFeatures(options);
    }

    @Override
    public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options) {
        return delegate.updateFeatures(featureUpdates, options);
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
        return delegate.describeMetadataQuorum(options);
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        return delegate.unregisterBroker(brokerId, options);
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions, DescribeProducersOptions options) {
        return delegate.describeProducers(partitions, options);
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        return delegate.describeTransactions(transactionalIds, options);
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        return delegate.abortTransaction(spec, options);
    }

    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        return delegate.listTransactions(options);
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        return delegate.fenceProducers(transactionalIds, options);
    }

    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions options) {
        return delegate.listClientMetricsResources(options);
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return delegate.clientInstanceId(timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }
}
