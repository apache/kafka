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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The administrative client for Kafka, which supports managing and inspecting topics, brokers, configurations and ACLs.
 *
 * The minimum broker version required is 0.10.0.0. Methods with stricter requirements will specify the minimum broker
 * version required.
 *
 * This client was introduced in 0.11.0.0 and the API is still evolving. We will try to evolve the API in a compatible
 * manner, but we reserve the right to make breaking changes in minor releases, if necessary. We will update the
 * {@code InterfaceStability} annotation and this notice once the API is considered stable.
 */
@InterfaceStability.Evolving
public abstract class AdminClient implements AutoCloseable {

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param props The configuration.
     * @return The new KafkaAdminClient.
     */
    public static AdminClient create(Properties props) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(props), null);
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param conf The configuration.
     * @return The new KafkaAdminClient.
     */
    public static AdminClient create(Map<String, Object> conf) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf), null);
    }

    /**
     * Close the AdminClient and release all associated resources.
     *
     * See {@link AdminClient#close(long, TimeUnit)}
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * Close the AdminClient and release all associated resources.
     *
     * The close operation has a grace period during which current operations will be allowed to
     * complete, specified by the given duration and time unit.
     * New operations will not be accepted during the grace period.  Once the grace period is over,
     * all operations that have not yet been completed will be aborted with a TimeoutException.
     *
     * @param duration  The duration to use for the wait time.
     * @param unit      The time unit to use for the wait time.
     */
    public abstract void close(long duration, TimeUnit unit);

    /**
     * Create a batch of new topics with the default options.
     *
     * This is a convenience method for #{@link #createTopics(Collection, CreateTopicsOptions)} with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param newTopics         The new topics to create.
     * @return                  The CreateTopicsResult.
     */
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
        return createTopics(newTopics, new CreateTopicsOptions());
    }

    /**
     * Create a batch of new topics.
     *
     * This operation is not transactional so it may succeed for some topics while fail for others.
     *
     * It may take several seconds after {@code CreateTopicsResult} returns
     * success for all the brokers to become aware that the topics have been created.
     * During this time, {@link AdminClient#listTopics()} and {@link AdminClient#describeTopics(Collection)}
     * may not return information about the new topics.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher. The validateOnly option is supported
     * from version 0.10.2.0.
     *
     * @param newTopics         The new topics to create.
     * @param options           The options to use when creating the new topics.
     * @return                  The CreateTopicsResult.
     */
    public abstract CreateTopicsResult createTopics(Collection<NewTopic> newTopics,
                                                    CreateTopicsOptions options);

    /**
     * This is a convenience method for #{@link AdminClient#deleteTopics(Collection, DeleteTopicsOptions)}
     * with default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param topics            The topic names to delete.
     * @return                  The DeleteTopicsResult.
     */
    public DeleteTopicsResult deleteTopics(Collection<String> topics) {
        return deleteTopics(topics, new DeleteTopicsOptions());
    }

    /**
     * Delete a batch of topics.
     *
     * This operation is not transactional so it may succeed for some topics while fail for others.
     *
     * It may take several seconds after the {@code DeleteTopicsResult} returns
     * success for all the brokers to become aware that the topics are gone.
     * During this time, AdminClient#listTopics and AdminClient#describeTopics
     * may continue to return information about the deleted topics.
     *
     * If delete.topic.enable is false on the brokers, deleteTopics will mark
     * the topics for deletion, but not actually delete them.  The futures will
     * return successfully in this case.
     *
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param topics            The topic names to delete.
     * @param options           The options to use when deleting the topics.
     * @return                  The DeleteTopicsResult.
     */
    public abstract DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options);

    /**
     * List the topics available in the cluster with the default options.
     *
     * This is a convenience method for #{@link AdminClient#listTopics(ListTopicsOptions)} with default options.
     * See the overload for more details.
     *
     * @return                  The ListTopicsResult.
     */
    public ListTopicsResult listTopics() {
        return listTopics(new ListTopicsOptions());
    }

    /**
     * List the topics available in the cluster.
     *
     * @param options           The options to use when listing the topics.
     * @return                  The ListTopicsResult.
     */
    public abstract ListTopicsResult listTopics(ListTopicsOptions options);

    /**
     * Describe some topics in the cluster, with the default options.
     *
     * This is a convenience method for #{@link AdminClient#describeTopics(Collection, DescribeTopicsOptions)} with
     * default options. See the overload for more details.
     *
     * @param topicNames        The names of the topics to describe.
     *
     * @return                  The DescribeTopicsResult.
     */
    public DescribeTopicsResult describeTopics(Collection<String> topicNames) {
        return describeTopics(topicNames, new DescribeTopicsOptions());
    }

    /**
     * Describe some topics in the cluster.
     *
     * @param topicNames        The names of the topics to describe.
     * @param options           The options to use when describing the topic.
     *
     * @return                  The DescribeTopicsResult.
     */
    public abstract DescribeTopicsResult describeTopics(Collection<String> topicNames,
                                                         DescribeTopicsOptions options);

    /**
     * Get information about the nodes in the cluster, using the default options.
     *
     * This is a convenience method for #{@link AdminClient#describeCluster(DescribeClusterOptions)} with default options.
     * See the overload for more details.
     *
     * @return                  The DescribeClusterResult.
     */
    public DescribeClusterResult describeCluster() {
        return describeCluster(new DescribeClusterOptions());
    }

    /**
     * Get information about the nodes in the cluster.
     *
     * @param options           The options to use when getting information about the cluster.
     * @return                  The DescribeClusterResult.
     */
    public abstract DescribeClusterResult describeCluster(DescribeClusterOptions options);

    /**
     * This is a convenience method for #{@link AdminClient#describeAcls(AclBindingFilter, DescribeAclsOptions)} with
     * default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filter            The filter to use.
     * @return                  The DeleteAclsResult.
     */
    public DescribeAclsResult describeAcls(AclBindingFilter filter) {
        return describeAcls(filter, new DescribeAclsOptions());
    }

    /**
     * Lists access control lists (ACLs) according to the supplied filter.
     *
     * Note: it may take some time for changes made by createAcls or deleteAcls to be reflected
     * in the output of describeAcls.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filter            The filter to use.
     * @param options           The options to use when listing the ACLs.
     * @return                  The DeleteAclsResult.
     */
    public abstract DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options);

    /**
     * This is a convenience method for #{@link AdminClient#createAcls(Collection, CreateAclsOptions)} with
     * default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param acls              The ACLs to create
     * @return                  The CreateAclsResult.
     */
    public CreateAclsResult createAcls(Collection<AclBinding> acls) {
        return createAcls(acls, new CreateAclsOptions());
    }

    /**
     * Creates access control lists (ACLs) which are bound to specific resources.
     *
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     *
     * If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but
     * no changes will be made.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param acls              The ACLs to create
     * @param options           The options to use when creating the ACLs.
     * @return                  The CreateAclsResult.
     */
    public abstract CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options);

    /**
     * This is a convenience method for #{@link AdminClient#deleteAcls(Collection, DeleteAclsOptions)} with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filters           The filters to use.
     * @return                  The DeleteAclsResult.
     */
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters) {
        return deleteAcls(filters, new DeleteAclsOptions());
    }

    /**
     * Deletes access control lists (ACLs) according to the supplied filters.
     *
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filters           The filters to use.
     * @param options           The options to use when deleting the ACLs.
     * @return                  The DeleteAclsResult.
     */
    public abstract DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options);


    /**
     * Get the configuration for the specified resources with the default options.
     *
     * This is a convenience method for #{@link AdminClient#describeConfigs(Collection, DescribeConfigsOptions)} with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param resources         The resources (topic and broker resource types are currently supported)
     * @return                  The DescribeConfigsResult
     */
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources) {
        return describeConfigs(resources, new DescribeConfigsOptions());
    }

    /**
     * Get the configuration for the specified resources.
     *
     * The returned configuration includes default values and the isDefault() method can be used to distinguish them
     * from user supplied values.
     *
     * The value of config entries where isSensitive() is true is always {@code null} so that sensitive information
     * is not disclosed.
     *
     * Config entries where isReadOnly() is true cannot be updated.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param resources         The resources (topic and broker resource types are currently supported)
     * @param options           The options to use when describing configs
     * @return                  The DescribeConfigsResult
     */
    public abstract DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources,
                                                           DescribeConfigsOptions options);

    /**
     * Update the configuration for the specified resources with the default options.
     *
     * This is a convenience method for #{@link AdminClient#alterConfigs(Map, AlterConfigsOptions)} with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param configs         The resources with their configs (topic is the only resource type with configs that can
     *                        be updated currently)
     * @return                The AlterConfigsResult
     */
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs) {
        return alterConfigs(configs, new AlterConfigsOptions());
    }

    /**
     * Update the configuration for the specified resources with the default options.
     *
     * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
     * a particular resource are updated atomically.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param configs         The resources with their configs (topic is the only resource type with configs that can
     *                        be updated currently)
     * @param options         The options to use when describing configs
     * @return                The AlterConfigsResult
     */
    public abstract AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options);

    /**
     * Change the log directory for the specified replicas. This API is currently only useful if it is used
     * before the replica has been created on the broker. It will support moving replicas that have already been created after
     * KIP-113 is fully implemented.
     *
     * This is a convenience method for #{@link AdminClient#alterReplicaLogDirs(Map, AlterReplicaLogDirsOptions)} with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicaAssignment  The replicas with their log directory absolute path
     * @return                   The AlterReplicaLogDirsResult
     */
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment) {
        return alterReplicaLogDirs(replicaAssignment, new AlterReplicaLogDirsOptions());
    }

    /**
     * Change the log directory for the specified replicas. This API is currently only useful if it is used
     * before the replica has been created on the broker. It will support moving replicas that have already been created after
     * KIP-113 is fully implemented.
     *
     * This operation is not transactional so it may succeed for some replicas while fail for others.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicaAssignment  The replicas with their log directory absolute path
     * @param options            The options to use when changing replica dir
     * @return                   The AlterReplicaLogDirsResult
     */
    public abstract AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options);

    /**
     * Query the information of all log directories on the given set of brokers
     *
     * This is a convenience method for #{@link AdminClient#describeLogDirs(Collection, DescribeLogDirsOptions)} with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param brokers     A list of brokers
     * @return            The DescribeLogDirsResult
     */
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers) {
        return describeLogDirs(brokers, new DescribeLogDirsOptions());
    }

    /**
     * Query the information of all log directories on the given set of brokers
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param brokers     A list of brokers
     * @param options     The options to use when querying log dir info
     * @return            The DescribeLogDirsResult
     */
    public abstract DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options);

    /**
     * Query the replica log directory information for the specified replicas.
     *
     * This is a convenience method for #{@link AdminClient#describeReplicaLogDirs(Collection, DescribeReplicaLogDirsOptions)}
     * with default options. See the overload for more details.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas      The replicas to query
     * @return              The DescribeReplicaLogDirsResult
     */
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas) {
        return describeReplicaLogDirs(replicas, new DescribeReplicaLogDirsOptions());
    }

    /**
     * Query the replica log directory information for the specified replicas.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas      The replicas to query
     * @param options       The options to use when querying replica log dir info
     * @return              The DescribeReplicaLogDirsResult
     */
    public abstract DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options);

    /**
     * <p>Increase the number of partitions of the topics given as the keys of {@code newPartitions}
     * according to the corresponding values. <strong>If partitions are increased for a topic that has a key,
     * the partition logic or ordering of the messages will be affected.</strong></p>
     *
     * <p>This is a convenience method for {@link #createPartitions(Map, CreatePartitionsOptions)} with default options.
     * See the overload for more details.</p>
     *
     * @param newPartitions The topics which should have new partitions created, and corresponding parameters
     *                      for the created partitions.
     * @return              The CreatePartitionsResult.
     */
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions) {
        return createPartitions(newPartitions, new CreatePartitionsOptions());
    }

    /**
     * <p>Increase the number of partitions of the topics given as the keys of {@code newPartitions}
     * according to the corresponding values. <strong>If partitions are increased for a topic that has a key,
     * the partition logic or ordering of the messages will be affected.</strong></p>
     *
     * <p>This operation is not transactional so it may succeed for some topics while fail for others.</p>
     *
     * <p>It may take several seconds after this method returns
     * success for all the brokers to become aware that the partitions have been created.
     * During this time, {@link AdminClient#describeTopics(Collection)}
     * may not return information about the new partitions.</p>
     *
     * <p>This operation is supported by brokers with version 1.0.0 or higher.</p>
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link CreatePartitionsResult#values() values()} method of the returned {@code CreatePartitionsResult}</p>
     * <ul>
     *     <li>{@link org.apache.kafka.common.errors.AuthorizationException}
     *     if the authenticated user is not authorized to alter the topic</li>
     *     <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *     if the request was not completed in within the given {@link CreatePartitionsOptions#timeoutMs()}.</li>
     *     <li>{@link org.apache.kafka.common.errors.ReassignmentInProgressException}
     *     if a partition reassignment is currently in progress</li>
     *     <li>{@link org.apache.kafka.common.errors.BrokerNotAvailableException}
     *     if the requested {@link NewPartitions#assignments()} contain a broker that is currently unavailable.</li>
     *     <li>{@link org.apache.kafka.common.errors.InvalidReplicationFactorException}
     *     if no {@link NewPartitions#assignments()} are given and it is impossible for the broker to assign
     *     replicas with the topics replication factor.</li>
     *     <li>Subclasses of {@link org.apache.kafka.common.KafkaException}
     *     if the request is invalid in some way.</li>
     * </ul>
     *
     * @param newPartitions The topics which should have new partitions created, and corresponding parameters
     *                      for the created partitions.
     * @param options       The options to use when creating the new paritions.
     * @return              The CreatePartitionsResult.
     */
    public abstract CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                                            CreatePartitionsOptions options);

    /**
     * Delete records whose offset is smaller than the given offset of the corresponding partition.
     *
     * This is a convenience method for {@link #deleteRecords(Map, DeleteRecordsOptions)} with default options.
     * See the overload for more details.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param recordsToDelete       The topic partitions and related offsets from which records deletion starts.
     * @return                      The DeleteRecordsResult.
     */
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) {
        return deleteRecords(recordsToDelete, new DeleteRecordsOptions());
    }

    /**
     * Delete records whose offset is smaller than the given offset of the corresponding partition.
     *
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param recordsToDelete       The topic partitions and related offsets from which records deletion starts.
     * @param options               The options to use when deleting records.
     * @return                      The DeleteRecordsResult.
     */
    public abstract DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                                      DeleteRecordsOptions options);

    /**
     * <p>Create a Delegation Token.</p>
     *
     * <p>This is a convenience method for {@link #createDelegationToken(CreateDelegationTokenOptions)} with default options.
     * See the overload for more details.</p>
     *
     * @return                      The CreateDelegationTokenResult.
     */
    public CreateDelegationTokenResult createDelegationToken() {
        return createDelegationToken(new CreateDelegationTokenOptions());
    }


    /**
     * <p>Create a Delegation Token.</p>
     *
     * <p>This operation is supported by brokers with version 1.1.0 or higher.</p>
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link CreateDelegationTokenResult#delegationToken() delegationToken()} method of the returned {@code CreateDelegationTokenResult}</p>
     * <ul>
     *     <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     *     If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     *     <li>{@link org.apache.kafka.common.errors.InvalidPrincipalTypeException}
     *     if the renewers principal type is not supported.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     *     if the delegation token feature is disabled.</li>
     *     <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *     if the request was not completed in within the given {@link CreateDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param options               The options to use when creating delegation token.
     * @return                      The DeleteRecordsResult.
     */
    public abstract CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options);


    /**
     * <p>Renew a Delegation Token.</p>
     *
     * <p>This is a convenience method for {@link #renewDelegationToken(byte[], RenewDelegationTokenOptions)} with default options.
     * See the overload for more details.</p>
     *
     *
     * @param hmac                  HMAC of the Delegation token
     * @return                      The RenewDelegationTokenResult.
     */
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac) {
        return renewDelegationToken(hmac, new RenewDelegationTokenOptions());
    }

    /**
     * <p> Renew a Delegation Token.</p>
     *
     * <p>This operation is supported by brokers with version 1.1.0 or higher.</p>
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link RenewDelegationTokenResult#expiryTimestamp() expiryTimestamp()} method of the returned {@code RenewDelegationTokenResult}</p>
     * <ul>
     *     <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     *     If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     *     if the delegation token feature is disabled.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenNotFoundException}
     *     if the delegation token is not found on server.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException}
     *     if the authenticated user is not owner/renewer of the token.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenExpiredException}
     *     if the delegation token is expired.</li>
     *     <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *     if the request was not completed in within the given {@link RenewDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param hmac                  HMAC of the Delegation token
     * @param options               The options to use when renewing delegation token.
     * @return                      The RenewDelegationTokenResult.
     */
    public abstract RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options);

    /**
     * <p>Expire a Delegation Token.</p>
     *
     * <p>This is a convenience method for {@link #expireDelegationToken(byte[], ExpireDelegationTokenOptions)} with default options.
     * This will expire the token immediately. See the overload for more details.</p>
     *
     * @param hmac                  HMAC of the Delegation token
     * @return                      The ExpireDelegationTokenResult.
     */
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac) {
        return expireDelegationToken(hmac, new ExpireDelegationTokenOptions());
    }

    /**
     * <p>Expire a Delegation Token.</p>
     *
     * <p>This operation is supported by brokers with version 1.1.0 or higher.</p>
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link ExpireDelegationTokenResult#expiryTimestamp() expiryTimestamp()} method of the returned {@code ExpireDelegationTokenResult}</p>
     * <ul>
     *     <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     *     If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     *     if the delegation token feature is disabled.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenNotFoundException}
     *     if the delegation token is not found on server.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException}
     *     if the authenticated user is not owner/renewer of the requested token.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenExpiredException}
     *     if the delegation token is expired.</li>
     *     <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *     if the request was not completed in within the given {@link ExpireDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param hmac                  HMAC of the Delegation token
     * @param options               The options to use when expiring delegation token.
     * @return                      The ExpireDelegationTokenResult.
     */
    public abstract ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options);

    /**
     *<p>Describe the Delegation Tokens.</p>
     *
     * <p>This is a convenience method for {@link #describeDelegationToken(DescribeDelegationTokenOptions)} with default options.
     * This will return all the user owned tokens and tokens where user have Describe permission. See the overload for more details.</p>
     *
     * @return                      The DescribeDelegationTokenResult.
     */
    public DescribeDelegationTokenResult describeDelegationToken() {
        return describeDelegationToken(new DescribeDelegationTokenOptions());
    }

    /**
     * <p>Describe the Delegation Tokens.</p>
     *
     * <p>This operation is supported by brokers with version 1.1.0 or higher.</p>
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link DescribeDelegationTokenResult#delegationTokens() delegationTokens()} method of the returned {@code DescribeDelegationTokenResult}</p>
     * <ul>
     *     <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     *     If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     *     <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     *     if the delegation token feature is disabled.</li>
     *     <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *     if the request was not completed in within the given {@link DescribeDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param options               The options to use when describing delegation tokens.
     * @return                      The DescribeDelegationTokenResult.
     */
    public abstract DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options);

    /**
     * Describe some group IDs in the cluster.
     *
     * @param groupIds The IDs of the groups to describe.
     * @param options  The options to use when describing the groups.
     * @return The DescribeConsumerGroupResult.
     */
    public abstract DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
                                                                        DescribeConsumerGroupsOptions options);

    /**
     * Describe some group IDs in the cluster, with the default options.
     * <p>
     * This is a convenience method for
     * #{@link AdminClient#describeConsumerGroups(Collection, DescribeConsumerGroupsOptions)} with
     * default options. See the overload for more details.
     *
     * @param groupIds The IDs of the groups to describe.
     * @return The DescribeConsumerGroupResult.
     */
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds) {
        return describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());
    }

    /**
     * List the consumer groups available in the cluster.
     *
     * @param options           The options to use when listing the consumer groups.
     * @return The ListGroupsResult.
     */
    public abstract ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options);

    /**
     * List the consumer groups available in the cluster with the default options.
     *
     * This is a convenience method for #{@link AdminClient#listConsumerGroups(ListConsumerGroupsOptions)} with default options.
     * See the overload for more details.
     *
     * @return The ListGroupsResult.
     */
    public ListConsumerGroupsResult listConsumerGroups() {
        return listConsumerGroups(new ListConsumerGroupsOptions());
    }

    /**
     * List the consumer group offsets available in the cluster.
     *
     * @param options           The options to use when listing the consumer group offsets.
     * @return The ListGroupOffsetsResult
     */
    public abstract ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options);

    /**
     * List the consumer group offsets available in the cluster with the default options.
     *
     * This is a convenience method for #{@link AdminClient#listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions)} with default options.
     *
     * @return The ListGroupOffsetsResult.
     */
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());
    }

    /**
     * Delete consumer groups from the cluster.
     *
     * @param options           The options to use when deleting a consumer group.
     * @return The DeletConsumerGroupResult.
     */
    public abstract DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options);

    /**
     * Delete consumer groups from the cluster with the default options.
     *
     * @return The DeleteConsumerGroupResult.
     */
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds) {
        return deleteConsumerGroups(groupIds, new DeleteConsumerGroupsOptions());
    }

    /**
     * Get the metrics kept by the adminClient
     *
     * @return
     */
    public abstract Map<MetricName, ? extends Metric> metrics();
}
