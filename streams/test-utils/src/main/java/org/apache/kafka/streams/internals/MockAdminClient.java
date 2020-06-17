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
package org.apache.kafka.streams.internals;

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
import org.apache.kafka.clients.admin.Config;
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
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Minimum admin client mock implementation needed for TTD
 */
public class MockAdminClient extends AdminClient {

    private final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
    private final Map<TopicPartition, Long> endOffsets = new HashMap<>();

    @Override
    synchronized public ListOffsetsResult listOffsets(final Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                                      final ListOffsetsOptions options) {
        final Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = new HashMap<>();

        for (final Map.Entry<TopicPartition, OffsetSpec> entry : topicPartitionOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetSpec spec = entry.getValue();
            final KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo> future = new KafkaFutureImpl<>();

            if (spec instanceof OffsetSpec.EarliestSpec)
                future.complete(new ListOffsetsResult.ListOffsetsResultInfo(beginningOffsets.get(tp), -1, Optional.empty()));
            else if (spec instanceof OffsetSpec.LatestSpec)
                future.complete(new ListOffsetsResult.ListOffsetsResultInfo(endOffsets.get(tp), -1, Optional.empty()));
            else
                throw new UnsupportedOperationException("Not implement yet");

            futures.put(tp, future);
        }

        return new ListOffsetsResult(futures);
    }

    public synchronized void updateBeginningOffsets(final Map<TopicPartition, Long> newOffsets) {
        beginningOffsets.putAll(newOffsets);
    }

    public synchronized void updateEndOffsets(final Map<TopicPartition, Long> newOffsets) {
        endOffsets.putAll(newOffsets);
    }

    // ----------- APIs below are not used by TTD --------

    @Override
    synchronized public DescribeClusterResult describeCluster(final DescribeClusterOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics, final CreateTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListTopicsResult listTopics(final ListTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeTopicsResult describeTopics(final Collection<String> topicNames, final DescribeTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteTopicsResult deleteTopics(final Collection<String> topicsToDelete, final DeleteTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreatePartitionsResult createPartitions(final Map<String, NewPartitions> newPartitions,
                                                                final CreatePartitionsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteRecordsResult deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                                          final DeleteRecordsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreateDelegationTokenResult createDelegationToken(final CreateDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public RenewDelegationTokenResult renewDelegationToken(final byte[] hmac, final RenewDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ExpireDelegationTokenResult expireDelegationToken(final byte[] hmac, final ExpireDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeDelegationTokenResult describeDelegationToken(final DescribeDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeConsumerGroupsResult describeConsumerGroups(final Collection<String> groupIds, final DescribeConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListConsumerGroupsResult listConsumerGroups(final ListConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final String groupId, final ListConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteConsumerGroupsResult deleteConsumerGroups(final Collection<String> groupIds, final DeleteConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(final String groupId,
                                                                                    final Set<TopicPartition> partitions,
                                                                                    final DeleteConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ElectLeadersResult electLeaders(
            final ElectionType electionType,
            final Set<TopicPartition> partitions,
            final ElectLeadersOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(final String groupId, final RemoveMembersFromConsumerGroupOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreateAclsResult createAcls(final Collection<AclBinding> acls, final CreateAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeAclsResult describeAcls(final AclBindingFilter filter, final DescribeAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteAclsResult deleteAcls(final Collection<AclBindingFilter> filters, final DeleteAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeConfigsResult describeConfigs(final Collection<ConfigResource> resources, final DescribeConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Deprecated
    synchronized public AlterConfigsResult alterConfigs(final Map<ConfigResource, Config> configs, final AlterConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterConfigsResult incrementalAlterConfigs(final Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                   final AlterConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterReplicaLogDirsResult alterReplicaLogDirs(
            final Map<TopicPartitionReplica, String> replicaAssignment,
            final AlterReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeLogDirsResult describeLogDirs(final Collection<Integer> brokers,
                                                              final DescribeLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeReplicaLogDirsResult describeReplicaLogDirs(final Collection<TopicPartitionReplica> replicas,
                                                                            final DescribeReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterPartitionReassignmentsResult alterPartitionReassignments(
            final Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments,
            final AlterPartitionReassignmentsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListPartitionReassignmentsResult listPartitionReassignments(
            final Optional<Set<TopicPartition>> partitions,
            final ListPartitionReassignmentsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(final String groupId,
                                                                                  final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                                  final AlterConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(final ClientQuotaFilter filter, final DescribeClientQuotasOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(final Collection<ClientQuotaAlteration> entries, final AlterClientQuotasOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    synchronized public void close(final Duration timeout) {}

    @Override
    synchronized public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException("Not implement yet");
    }
}
