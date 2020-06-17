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
import org.apache.kafka.clients.admin.ElectPreferredLeadersOptions;
import org.apache.kafka.clients.admin.ElectPreferredLeadersResult;
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
    synchronized public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetSpec> entry : topicPartitionOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetSpec spec = entry.getValue();
            KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo> future = new KafkaFutureImpl<>();

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

    public synchronized void updateBeginningOffsets(Map<TopicPartition, Long> newOffsets) {
        beginningOffsets.putAll(newOffsets);
    }

    public synchronized void updateEndOffsets(final Map<TopicPartition, Long> newOffsets) {
        endOffsets.putAll(newOffsets);
    }

    // ----------- APIs below are not used by TTD --------

    @Override
    synchronized public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListTopicsResult listTopics(ListTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteTopicsResult deleteTopics(Collection<String> topicsToDelete, DeleteTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Deprecated
    @Override
    synchronized public ElectPreferredLeadersResult electPreferredLeaders(Collection<TopicPartition> partitions, ElectPreferredLeadersOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ElectLeadersResult electLeaders(
            ElectionType electionType,
            Set<TopicPartition> partitions,
            ElectLeadersOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Deprecated
    synchronized public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterConfigsResult incrementalAlterConfigs(
            Map<ConfigResource, Collection<AlterConfigOp>> configs,
            AlterConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterReplicaLogDirsResult alterReplicaLogDirs(
            Map<TopicPartitionReplica, String> replicaAssignment,
            AlterReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers,
                                                              DescribeLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas,
                                                                            DescribeReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments,
            AlterPartitionReassignmentsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public ListPartitionReassignmentsResult listPartitionReassignments(
            Optional<Set<TopicPartition>> partitions,
            ListPartitionReassignmentsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    synchronized public void close(Duration timeout) {}

    @Override
    synchronized public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException("Not implement yet");
    }
}
