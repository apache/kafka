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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.LeaveGroupResponse;

/**
 * The administrative client for Kafka, which supports managing and inspecting topics, brokers, configurations and ACLs.
 * <p>
 * The minimum broker version required is 0.10.0.0. Methods with stricter requirements will specify the minimum broker
 * version required.
 */
public interface Admin extends AutoCloseable {

    /**
     * Create a new Admin with the given configuration.
     *
     * @param props The configuration.
     * @return The new KafkaAdminClient.
     */
    static Admin create(Properties props) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(props, true), null);
    }

    /**
     * Create a new Admin with the given configuration.
     *
     * @param conf The configuration.
     * @return The new KafkaAdminClient.
     */
    static Admin create(Map<String, Object> conf) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf, true), null);
    }

    /**
     * Close the Admin and release all associated resources.
     * <p>
     * See {@link #close(long, TimeUnit)}
     */
    @Override
    default void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * Close the Admin and release all associated resources.
     * <p>
     * The close operation has a grace period during which current operations will be allowed to
     * complete, specified by the given duration and time unit.
     * New operations will not be accepted during the grace period. Once the grace period is over,
     * all operations that have not yet been completed will be aborted with a {@link org.apache.kafka.common.errors.TimeoutException}.
     *
     * @param duration The duration to use for the wait time.
     * @param unit     The time unit to use for the wait time.
     * @deprecated Since 2.2. Use {@link #close(Duration)} or {@link #close()}.
     */
    @Deprecated
    default void close(long duration, TimeUnit unit) {
        close(Duration.ofMillis(unit.toMillis(duration)));
    }

    /**
     * Close the Admin client and release all associated resources.
     * <p>
     * The close operation has a grace period during which current operations will be allowed to
     * complete, specified by the given duration.
     * New operations will not be accepted during the grace period. Once the grace period is over,
     * all operations that have not yet been completed will be aborted with a {@link org.apache.kafka.common.errors.TimeoutException}.
     *
     * @param timeout The time to use for the wait time.
     */
    void close(Duration timeout);

    /**
     * Create a batch of new topics with the default options.
     * <p>
     * This is a convenience method for {@link #createTopics(Collection, CreateTopicsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param newTopics The new topics to create.
     * @return The CreateTopicsResult.
     */
    default CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
        return createTopics(newTopics, new CreateTopicsOptions());
    }

    /**
     * Create a batch of new topics.
     * <p>
     * This operation is not transactional so it may succeed for some topics while fail for others.
     * <p>
     * It may take several seconds after {@link CreateTopicsResult} returns
     * success for all the brokers to become aware that the topics have been created.
     * During this time, {@link #listTopics()} and {@link #describeTopics(Collection)}
     * may not return information about the new topics.
     * <p>
     * This operation is supported by brokers with version 0.10.1.0 or higher. The validateOnly option is supported
     * from version 0.10.2.0.
     *
     * @param newTopics The new topics to create.
     * @param options   The options to use when creating the new topics.
     * @return The CreateTopicsResult.
     */
    CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options);

    /**
     * This is a convenience method for {@link #deleteTopics(Collection, DeleteTopicsOptions)}
     * with default options. See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param topics The topic names to delete.
     * @return The DeleteTopicsResult.
     */
    default DeleteTopicsResult deleteTopics(Collection<String> topics) {
        return deleteTopics(topics, new DeleteTopicsOptions());
    }

    /**
     * Delete a batch of topics.
     * <p>
     * This operation is not transactional so it may succeed for some topics while fail for others.
     * <p>
     * It may take several seconds after the {@link DeleteTopicsResult} returns
     * success for all the brokers to become aware that the topics are gone.
     * During this time, {@link #listTopics()} and {@link #describeTopics(Collection)}
     * may continue to return information about the deleted topics.
     * <p>
     * If delete.topic.enable is false on the brokers, deleteTopics will mark
     * the topics for deletion, but not actually delete them. The futures will
     * return successfully in this case.
     * <p>
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param topics  The topic names to delete.
     * @param options The options to use when deleting the topics.
     * @return The DeleteTopicsResult.
     */
    DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options);

    /**
     * List the topics available in the cluster with the default options.
     * <p>
     * This is a convenience method for {@link #listTopics(ListTopicsOptions)} with default options.
     * See the overload for more details.
     *
     * @return The ListTopicsResult.
     */
    default ListTopicsResult listTopics() {
        return listTopics(new ListTopicsOptions());
    }

    /**
     * List the topics available in the cluster.
     *
     * @param options The options to use when listing the topics.
     * @return The ListTopicsResult.
     */
    ListTopicsResult listTopics(ListTopicsOptions options);

    /**
     * Describe some topics in the cluster, with the default options.
     * <p>
     * This is a convenience method for {@link #describeTopics(Collection, DescribeTopicsOptions)} with
     * default options. See the overload for more details.
     *
     * @param topicNames The names of the topics to describe.
     * @return The DescribeTopicsResult.
     */
    default DescribeTopicsResult describeTopics(Collection<String> topicNames) {
        return describeTopics(topicNames, new DescribeTopicsOptions());
    }

    /**
     * Describe some topics in the cluster.
     *
     * @param topicNames The names of the topics to describe.
     * @param options    The options to use when describing the topic.
     * @return The DescribeTopicsResult.
     */
    DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options);

    /**
     * Get information about the nodes in the cluster, using the default options.
     * <p>
     * This is a convenience method for {@link #describeCluster(DescribeClusterOptions)} with default options.
     * See the overload for more details.
     *
     * @return The DescribeClusterResult.
     */
    default DescribeClusterResult describeCluster() {
        return describeCluster(new DescribeClusterOptions());
    }

    /**
     * Get information about the nodes in the cluster.
     *
     * @param options The options to use when getting information about the cluster.
     * @return The DescribeClusterResult.
     */
    DescribeClusterResult describeCluster(DescribeClusterOptions options);

    /**
     * This is a convenience method for {@link #describeAcls(AclBindingFilter, DescribeAclsOptions)} with
     * default options. See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filter The filter to use.
     * @return The DeleteAclsResult.
     */
    default DescribeAclsResult describeAcls(AclBindingFilter filter) {
        return describeAcls(filter, new DescribeAclsOptions());
    }

    /**
     * Lists access control lists (ACLs) according to the supplied filter.
     * <p>
     * Note: it may take some time for changes made by {@code createAcls} or {@code deleteAcls} to be reflected
     * in the output of {@code describeAcls}.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filter  The filter to use.
     * @param options The options to use when listing the ACLs.
     * @return The DeleteAclsResult.
     */
    DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options);

    /**
     * This is a convenience method for {@link #createAcls(Collection, CreateAclsOptions)} with
     * default options. See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param acls The ACLs to create
     * @return The CreateAclsResult.
     */
    default CreateAclsResult createAcls(Collection<AclBinding> acls) {
        return createAcls(acls, new CreateAclsOptions());
    }

    /**
     * Creates access control lists (ACLs) which are bound to specific resources.
     * <p>
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     * <p>
     * If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but
     * no changes will be made.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param acls    The ACLs to create
     * @param options The options to use when creating the ACLs.
     * @return The CreateAclsResult.
     */
    CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options);

    /**
     * This is a convenience method for {@link #deleteAcls(Collection, DeleteAclsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filters The filters to use.
     * @return The DeleteAclsResult.
     */
    default DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters) {
        return deleteAcls(filters, new DeleteAclsOptions());
    }

    /**
     * Deletes access control lists (ACLs) according to the supplied filters.
     * <p>
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filters The filters to use.
     * @param options The options to use when deleting the ACLs.
     * @return The DeleteAclsResult.
     */
    DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options);


    /**
     * Get the configuration for the specified resources with the default options.
     * <p>
     * This is a convenience method for {@link #describeConfigs(Collection, DescribeConfigsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param resources The resources (topic and broker resource types are currently supported)
     * @return The DescribeConfigsResult
     */
    default DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources) {
        return describeConfigs(resources, new DescribeConfigsOptions());
    }

    /**
     * Get the configuration for the specified resources.
     * <p>
     * The returned configuration includes default values and the isDefault() method can be used to distinguish them
     * from user supplied values.
     * <p>
     * The value of config entries where isSensitive() is true is always {@code null} so that sensitive information
     * is not disclosed.
     * <p>
     * Config entries where isReadOnly() is true cannot be updated.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param resources The resources (topic and broker resource types are currently supported)
     * @param options   The options to use when describing configs
     * @return The DescribeConfigsResult
     */
    DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options);

    /**
     * Update the configuration for the specified resources with the default options.
     * <p>
     * This is a convenience method for {@link #alterConfigs(Map, AlterConfigsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param configs The resources with their configs (topic is the only resource type with configs that can
     *                be updated currently)
     * @return The AlterConfigsResult
     * @deprecated Since 2.3. Use {@link #incrementalAlterConfigs(Map)}.
     */
    @Deprecated
    default AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs) {
        return alterConfigs(configs, new AlterConfigsOptions());
    }

    /**
     * Update the configuration for the specified resources with the default options.
     * <p>
     * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
     * a particular resource are updated atomically.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param configs The resources with their configs (topic is the only resource type with configs that can
     *                be updated currently)
     * @param options The options to use when describing configs
     * @return The AlterConfigsResult
     * @deprecated Since 2.3. Use {@link #incrementalAlterConfigs(Map, AlterConfigsOptions)}.
     */
    @Deprecated
    AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options);

    /**
     * Incrementally updates the configuration for the specified resources with default options.
     * <p>
     * This is a convenience method for {@link #incrementalAlterConfigs(Map, AlterConfigsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 2.3.0 or higher.
     *
     * @param configs The resources with their configs
     * @return The AlterConfigsResult
     */
    default AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        return incrementalAlterConfigs(configs, new AlterConfigsOptions());
    }

    /**
     * Incrementally update the configuration for the specified resources.
     * <p>
     * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
     * a particular resource are updated atomically.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@link AlterConfigsResult}:
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     * if the authenticated user didn't have alter access to the cluster.</li>
     * <li>{@link org.apache.kafka.common.errors.TopicAuthorizationException}
     * if the authenticated user didn't have alter access to the Topic.</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     * if the request details are invalid. e.g., a configuration key was specified more than once for a resource</li>
     * </ul>
     * <p>
     * This operation is supported by brokers with version 2.3.0 or higher.
     *
     * @param configs The resources with their configs
     * @param options The options to use when altering configs
     * @return The AlterConfigsResult
     */
    AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource,
        Collection<AlterConfigOp>> configs, AlterConfigsOptions options);

    /**
     * Change the log directory for the specified replicas. If the replica does not exist on the broker, the result
     * shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be created in the given log directory on the
     * broker when it is created later. If the replica already exists on the broker, the replica will be moved to the given
     * log directory if it is not already there. For detailed result, inspect the returned {@link AlterReplicaLogDirsResult} instance.
     * <p>
     * This operation is not transactional so it may succeed for some replicas while fail for others.
     * <p>
     * This is a convenience method for {@link #alterReplicaLogDirs(Map, AlterReplicaLogDirsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * @param replicaAssignment     The replicas with their log directory absolute path
     * @return                      The AlterReplicaLogDirsResult
     * @throws InterruptedException Interrupted while joining I/O thread
     */
    default AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment) {
        return alterReplicaLogDirs(replicaAssignment, new AlterReplicaLogDirsOptions());
    }

    /**
     * Change the log directory for the specified replicas. If the replica does not exist on the broker, the result
     * shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be created in the given log directory on the
     * broker when it is created later. If the replica already exists on the broker, the replica will be moved to the given
     * log directory if it is not already there. For detailed result, inspect the returned {@link AlterReplicaLogDirsResult} instance.
     * <p>
     * This operation is not transactional so it may succeed for some replicas while fail for others.
     * <p>
     * This operation is supported by brokers with version 1.1.0 or higher.
     *
     * @param replicaAssignment     The replicas with their log directory absolute path
     * @param options               The options to use when changing replica dir
     * @return                      The AlterReplicaLogDirsResult
     * @throws InterruptedException Interrupted while joining I/O thread
     */
    AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
                                                  AlterReplicaLogDirsOptions options);

    /**
     * Query the information of all log directories on the given set of brokers
     * <p>
     * This is a convenience method for {@link #describeLogDirs(Collection, DescribeLogDirsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param brokers A list of brokers
     * @return The DescribeLogDirsResult
     */
    default DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers) {
        return describeLogDirs(brokers, new DescribeLogDirsOptions());
    }

    /**
     * Query the information of all log directories on the given set of brokers
     * <p>
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param brokers A list of brokers
     * @param options The options to use when querying log dir info
     * @return The DescribeLogDirsResult
     */
    DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options);

    /**
     * Query the replica log directory information for the specified replicas.
     * <p>
     * This is a convenience method for {@link #describeReplicaLogDirs(Collection, DescribeReplicaLogDirsOptions)}
     * with default options. See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas The replicas to query
     * @return The DescribeReplicaLogDirsResult
     */
    default DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas) {
        return describeReplicaLogDirs(replicas, new DescribeReplicaLogDirsOptions());
    }

    /**
     * Query the replica log directory information for the specified replicas.
     * <p>
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas The replicas to query
     * @param options  The options to use when querying replica log dir info
     * @return The DescribeReplicaLogDirsResult
     */
    DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options);

    /**
     * Increase the number of partitions of the topics given as the keys of {@code newPartitions}
     * according to the corresponding values. <strong>If partitions are increased for a topic that has a key,
     * the partition logic or ordering of the messages will be affected.</strong>
     * <p>
     * This is a convenience method for {@link #createPartitions(Map, CreatePartitionsOptions)} with default options.
     * See the overload for more details.
     *
     * @param newPartitions The topics which should have new partitions created, and corresponding parameters
     *                      for the created partitions.
     * @return The CreatePartitionsResult.
     */
    default CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions) {
        return createPartitions(newPartitions, new CreatePartitionsOptions());
    }

    /**
     * Increase the number of partitions of the topics given as the keys of {@code newPartitions}
     * according to the corresponding values. <strong>If partitions are increased for a topic that has a key,
     * the partition logic or ordering of the messages will be affected.</strong>
     * <p>
     * This operation is not transactional so it may succeed for some topics while fail for others.
     * <p>
     * It may take several seconds after this method returns
     * success for all the brokers to become aware that the partitions have been created.
     * During this time, {@link #describeTopics(Collection)}
     * may not return information about the new partitions.
     * <p>
     * This operation is supported by brokers with version 1.0.0 or higher.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link CreatePartitionsResult#values() values()} method of the returned {@link CreatePartitionsResult}
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.AuthorizationException}
     * if the authenticated user is not authorized to alter the topic</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * if the request was not completed in within the given {@link CreatePartitionsOptions#timeoutMs()}.</li>
     * <li>{@link org.apache.kafka.common.errors.ReassignmentInProgressException}
     * if a partition reassignment is currently in progress</li>
     * <li>{@link org.apache.kafka.common.errors.BrokerNotAvailableException}
     * if the requested {@link NewPartitions#assignments()} contain a broker that is currently unavailable.</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidReplicationFactorException}
     * if no {@link NewPartitions#assignments()} are given and it is impossible for the broker to assign
     * replicas with the topics replication factor.</li>
     * <li>Subclasses of {@link org.apache.kafka.common.KafkaException}
     * if the request is invalid in some way.</li>
     * </ul>
     *
     * @param newPartitions The topics which should have new partitions created, and corresponding parameters
     *                      for the created partitions.
     * @param options       The options to use when creating the new partitions.
     * @return The CreatePartitionsResult.
     */
    CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                            CreatePartitionsOptions options);

    /**
     * Delete records whose offset is smaller than the given offset of the corresponding partition.
     * <p>
     * This is a convenience method for {@link #deleteRecords(Map, DeleteRecordsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param recordsToDelete The topic partitions and related offsets from which records deletion starts.
     * @return The DeleteRecordsResult.
     */
    default DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) {
        return deleteRecords(recordsToDelete, new DeleteRecordsOptions());
    }

    /**
     * Delete records whose offset is smaller than the given offset of the corresponding partition.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param recordsToDelete The topic partitions and related offsets from which records deletion starts.
     * @param options         The options to use when deleting records.
     * @return The DeleteRecordsResult.
     */
    DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                      DeleteRecordsOptions options);

    /**
     * Create a Delegation Token.
     * <p>
     * This is a convenience method for {@link #createDelegationToken(CreateDelegationTokenOptions)} with default options.
     * See the overload for more details.
     *
     * @return The CreateDelegationTokenResult.
     */
    default CreateDelegationTokenResult createDelegationToken() {
        return createDelegationToken(new CreateDelegationTokenOptions());
    }


    /**
     * Create a Delegation Token.
     * <p>
     * This operation is supported by brokers with version 1.1.0 or higher.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link CreateDelegationTokenResult#delegationToken() delegationToken()} method of the returned {@link CreateDelegationTokenResult}
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidPrincipalTypeException}
     * if the renewers principal type is not supported.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * if the delegation token feature is disabled.</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * if the request was not completed in within the given {@link CreateDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param options The options to use when creating delegation token.
     * @return The DeleteRecordsResult.
     */
    CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options);


    /**
     * Renew a Delegation Token.
     * <p>
     * This is a convenience method for {@link #renewDelegationToken(byte[], RenewDelegationTokenOptions)} with default options.
     * See the overload for more details.
     *
     * @param hmac HMAC of the Delegation token
     * @return The RenewDelegationTokenResult.
     */
    default RenewDelegationTokenResult renewDelegationToken(byte[] hmac) {
        return renewDelegationToken(hmac, new RenewDelegationTokenOptions());
    }

    /**
     * Renew a Delegation Token.
     * <p>
     * This operation is supported by brokers with version 1.1.0 or higher.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link RenewDelegationTokenResult#expiryTimestamp() expiryTimestamp()} method of the returned {@link RenewDelegationTokenResult}
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * if the delegation token feature is disabled.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenNotFoundException}
     * if the delegation token is not found on server.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException}
     * if the authenticated user is not owner/renewer of the token.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenExpiredException}
     * if the delegation token is expired.</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * if the request was not completed in within the given {@link RenewDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param hmac    HMAC of the Delegation token
     * @param options The options to use when renewing delegation token.
     * @return The RenewDelegationTokenResult.
     */
    RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options);

    /**
     * Expire a Delegation Token.
     * <p>
     * This is a convenience method for {@link #expireDelegationToken(byte[], ExpireDelegationTokenOptions)} with default options.
     * This will expire the token immediately. See the overload for more details.
     *
     * @param hmac HMAC of the Delegation token
     * @return The ExpireDelegationTokenResult.
     */
    default ExpireDelegationTokenResult expireDelegationToken(byte[] hmac) {
        return expireDelegationToken(hmac, new ExpireDelegationTokenOptions());
    }

    /**
     * Expire a Delegation Token.
     * <p>
     * This operation is supported by brokers with version 1.1.0 or higher.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link ExpireDelegationTokenResult#expiryTimestamp() expiryTimestamp()} method of the returned {@link ExpireDelegationTokenResult}
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * if the delegation token feature is disabled.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenNotFoundException}
     * if the delegation token is not found on server.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException}
     * if the authenticated user is not owner/renewer of the requested token.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenExpiredException}
     * if the delegation token is expired.</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * if the request was not completed in within the given {@link ExpireDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param hmac    HMAC of the Delegation token
     * @param options The options to use when expiring delegation token.
     * @return The ExpireDelegationTokenResult.
     */
    ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options);

    /**
     * Describe the Delegation Tokens.
     * <p>
     * This is a convenience method for {@link #describeDelegationToken(DescribeDelegationTokenOptions)} with default options.
     * This will return all the user owned tokens and tokens where user have Describe permission. See the overload for more details.
     *
     * @return The DescribeDelegationTokenResult.
     */
    default DescribeDelegationTokenResult describeDelegationToken() {
        return describeDelegationToken(new DescribeDelegationTokenOptions());
    }

    /**
     * Describe the Delegation Tokens.
     * <p>
     * This operation is supported by brokers with version 1.1.0 or higher.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from the
     * {@link DescribeDelegationTokenResult#delegationTokens() delegationTokens()} method of the returned {@link DescribeDelegationTokenResult}
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * If the request sent on PLAINTEXT/1-way SSL channels or delegation token authenticated channels.</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * if the delegation token feature is disabled.</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * if the request was not completed in within the given {@link DescribeDelegationTokenOptions#timeoutMs()}.</li>
     * </ul>
     *
     * @param options The options to use when describing delegation tokens.
     * @return The DescribeDelegationTokenResult.
     */
    DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options);

    /**
     * Describe some group IDs in the cluster.
     *
     * @param groupIds The IDs of the groups to describe.
     * @param options  The options to use when describing the groups.
     * @return The DescribeConsumerGroupResult.
     */
    DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
                                                        DescribeConsumerGroupsOptions options);

    /**
     * Describe some group IDs in the cluster, with the default options.
     * <p>
     * This is a convenience method for {@link #describeConsumerGroups(Collection, DescribeConsumerGroupsOptions)}
     * with default options. See the overload for more details.
     *
     * @param groupIds The IDs of the groups to describe.
     * @return The DescribeConsumerGroupResult.
     */
    default DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds) {
        return describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());
    }

    /**
     * List the consumer groups available in the cluster.
     *
     * @param options The options to use when listing the consumer groups.
     * @return The ListGroupsResult.
     */
    ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options);

    /**
     * List the consumer groups available in the cluster with the default options.
     * <p>
     * This is a convenience method for {@link #listConsumerGroups(ListConsumerGroupsOptions)} with default options.
     * See the overload for more details.
     *
     * @return The ListGroupsResult.
     */
    default ListConsumerGroupsResult listConsumerGroups() {
        return listConsumerGroups(new ListConsumerGroupsOptions());
    }

    /**
     * List the consumer group offsets available in the cluster.
     *
     * @param options The options to use when listing the consumer group offsets.
     * @return The ListGroupOffsetsResult
     */
    ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options);

    /**
     * List the consumer group offsets available in the cluster with the default options.
     * <p>
     * This is a convenience method for {@link #listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions)} with default options.
     *
     * @return The ListGroupOffsetsResult.
     */
    default ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());
    }

    /**
     * Delete consumer groups from the cluster.
     *
     * @param options The options to use when deleting a consumer group.
     * @return The DeletConsumerGroupResult.
     */
    DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options);

    /**
     * Delete consumer groups from the cluster with the default options.
     *
     * @return The DeleteConsumerGroupResult.
     */
    default DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds) {
        return deleteConsumerGroups(groupIds, new DeleteConsumerGroupsOptions());
    }

    /**
     * Delete committed offsets for a set of partitions in a consumer group. This will
     * succeed at the partition level only if the group is not actively subscribed
     * to the corresponding topic.
     *
     * @param options The options to use when deleting offsets in a consumer group.
     * @return The DeleteConsumerGroupOffsetsResult.
     */
    DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId,
        Set<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions options);

    /**
     * Delete committed offsets for a set of partitions in a consumer group with the default
     * options. This will succeed at the partition level only if the group is not actively
     * subscribed to the corresponding topic.
     *
     * @return The DeleteConsumerGroupOffsetsResult.
     */
    default DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions) {
        return deleteConsumerGroupOffsets(groupId, partitions, new DeleteConsumerGroupOffsetsOptions());
    }

    /**
     * Elect the preferred replica as leader for topic partitions.
     * <p>
     * This is a convenience method for {@link #electLeaders(ElectionType, Set, ElectLeadersOptions)}
     * with preferred election type and default options.
     * <p>
     * This operation is supported by brokers with version 2.2.0 or higher.
     *
     * @param partitions The partitions for which the preferred leader should be elected.
     * @return The ElectPreferredLeadersResult.
     * @deprecated Since 2.4.0. Use {@link #electLeaders(ElectionType, Set)}.
     */
    @Deprecated
    default ElectPreferredLeadersResult electPreferredLeaders(Collection<TopicPartition> partitions) {
        return electPreferredLeaders(partitions, new ElectPreferredLeadersOptions());
    }

    /**
     * Elect the preferred replica as leader for topic partitions.
     * <p>
     * This is a convenience method for {@link #electLeaders(ElectionType, Set, ElectLeadersOptions)}
     * with preferred election type.
     * <p>
     * This operation is supported by brokers with version 2.2.0 or higher.
     *
     * @param partitions The partitions for which the preferred leader should be elected.
     * @param options    The options to use when electing the preferred leaders.
     * @return The ElectPreferredLeadersResult.
     * @deprecated Since 2.4.0. Use {@link #electLeaders(ElectionType, Set, ElectLeadersOptions)}.
     */
    @Deprecated
    default ElectPreferredLeadersResult electPreferredLeaders(Collection<TopicPartition> partitions,
                                                              ElectPreferredLeadersOptions options) {
        final ElectLeadersOptions newOptions = new ElectLeadersOptions();
        newOptions.timeoutMs(options.timeoutMs());
        final Set<TopicPartition> topicPartitions = partitions == null ? null : new HashSet<>(partitions);

        return new ElectPreferredLeadersResult(electLeaders(ElectionType.PREFERRED, topicPartitions, newOptions));
    }

    /**
     * Elect a replica as leader for topic partitions.
     * <p>
     * This is a convenience method for {@link #electLeaders(ElectionType, Set, ElectLeadersOptions)}
     * with default options.
     *
     * @param electionType The type of election to conduct.
     * @param partitions   The topics and partitions for which to conduct elections.
     * @return The ElectLeadersResult.
     */
    default ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions) {
        return electLeaders(electionType, partitions, new ElectLeadersOptions());
    }

    /**
     * Elect a replica as leader for the given {@code partitions}, or for all partitions if the argument
     * to {@code partitions} is null.
     * <p>
     * This operation is not transactional so it may succeed for some partitions while fail for others.
     * <p>
     * It may take several seconds after this method returns success for all the brokers in the cluster
     * to become aware that the partitions have new leaders. During this time,
     * {@link #describeTopics(Collection)} may not return information about the partitions'
     * new leaders.
     * <p>
     * This operation is supported by brokers with version 2.2.0 or later if preferred election is use;
     * otherwise the brokers most be 2.4.0 or higher.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the future obtained
     * from the returned {@link ElectLeadersResult}:
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     * if the authenticated user didn't have alter access to the cluster.</li>
     * <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     * if the topic or partition did not exist within the cluster.</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidTopicException}
     * if the topic was already queued for deletion.</li>
     * <li>{@link org.apache.kafka.common.errors.NotControllerException}
     * if the request was sent to a broker that was not the controller for the cluster.</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * if the request timed out before the election was complete.</li>
     * <li>{@link org.apache.kafka.common.errors.LeaderNotAvailableException}
     * if the preferred leader was not alive or not in the ISR.</li>
     * </ul>
     *
     * @param electionType The type of election to conduct.
     * @param partitions   The topics and partitions for which to conduct elections.
     * @param options      The options to use when electing the leaders.
     * @return The ElectLeadersResult.
     */
    ElectLeadersResult electLeaders(
        ElectionType electionType,
        Set<TopicPartition> partitions,
        ElectLeadersOptions options);


    /**
     * Change the reassignments for one or more partitions.
     * Providing an empty Optional (e.g via {@link Optional#empty()}) will <bold>revert</bold> the reassignment for the associated partition.
     *
     * This is a convenience method for {@link #alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)}
     * with default options.  See the overload for more details.
     */
    default AlterPartitionReassignmentsResult alterPartitionReassignments(
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {
        return alterPartitionReassignments(reassignments, new AlterPartitionReassignmentsOptions());
    }

    /**
     * Change the reassignments for one or more partitions.
     * Providing an empty Optional (e.g via {@link Optional#empty()}) will <bold>revert</bold> the reassignment for the associated partition.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code AlterPartitionReassignmentsResult}:</p>
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user didn't have alter access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     *   If the topic or partition does not exist within the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   if the request timed out before the controller could record the new assignments.</li>
     *   <li>{@link org.apache.kafka.common.errors.InvalidReplicaAssignmentException}
     *   If the specified assignment was not valid.</li>
     *   <li>{@link org.apache.kafka.common.errors.NoReassignmentInProgressException}
     *   If there was an attempt to cancel a reassignment for a partition which was not being reassigned.</li>
     * </ul>
     *
     * @param reassignments   The reassignments to add, modify, or remove. See {@link NewPartitionReassignment}.
     * @param options         The options to use.
     * @return                The result.
     */
    AlterPartitionReassignmentsResult alterPartitionReassignments(
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
        AlterPartitionReassignmentsOptions options);


    /**
     * List all of the current partition reassignments
     *
     * This is a convenience method for {@link #listPartitionReassignments(ListPartitionReassignmentsOptions)}
     * with default options. See the overload for more details.
     */
    default ListPartitionReassignmentsResult listPartitionReassignments() {
        return listPartitionReassignments(new ListPartitionReassignmentsOptions());
    }

    /**
     * List the current reassignments for the given partitions
     *
     * This is a convenience method for {@link #listPartitionReassignments(Set, ListPartitionReassignmentsOptions)}
     * with default options. See the overload for more details.
     */
    default ListPartitionReassignmentsResult listPartitionReassignments(Set<TopicPartition> partitions) {
        return listPartitionReassignments(partitions, new ListPartitionReassignmentsOptions());
    }

    /**
     * List the current reassignments for the given partitions
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code ListPartitionReassignmentsResult}:</p>
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user doesn't have alter access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     *   If a given topic or partition does not exist.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the controller could list the current reassignments.</li>
     * </ul>
     *
     * @param partitions      The topic partitions to list reassignments for.
     * @param options         The options to use.
     * @return                The result.
     */
    default ListPartitionReassignmentsResult listPartitionReassignments(
        Set<TopicPartition> partitions,
        ListPartitionReassignmentsOptions options) {
        return listPartitionReassignments(Optional.of(partitions), options);
    }

    /**
     * List all of the current partition reassignments
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code ListPartitionReassignmentsResult}:</p>
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user doesn't have alter access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     *   If a given topic or partition does not exist.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the controller could list the current reassignments.</li>
     * </ul>
     *
     * @param options         The options to use.
     * @return                The result.
     */
    default ListPartitionReassignmentsResult listPartitionReassignments(ListPartitionReassignmentsOptions options) {
        return listPartitionReassignments(Optional.empty(), options);
    }

    /**
     * @param partitions the partitions we want to get reassignment for, or an empty optional if we want to get the reassignments for all partitions in the cluster
     * @param options         The options to use.
     * @return                The result.
     */
    ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
                                                                ListPartitionReassignmentsOptions options);

    /**
     * Remove members from the consumer group by given member identities.
     * <p>
     * For possible error codes, refer to {@link LeaveGroupResponse}.
     *
     * @param groupId The ID of the group to remove member from.
     * @param options The options to carry removing members' information.
     * @return The MembershipChangeResult.
     */
    RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options);

    /**
     * <p>Alters offsets for the specified group. In order to succeed, the group must be empty.
     *
     * <p>This is a convenience method for {@link #alterConsumerGroupOffsets(String, Map, AlterConsumerGroupOffsetsOptions)} with default options.
     * See the overload for more details.
     *
     * @param groupId The group for which to alter offsets.
     * @param offsets A map of offsets by partition with associated metadata.
     * @return The AlterOffsetsResult.
     */
    default AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
        return alterConsumerGroupOffsets(groupId, offsets, new AlterConsumerGroupOffsetsOptions());
    }

    /**
     * <p>Alters offsets for the specified group. In order to succeed, the group must be empty.
     *
     * <p>This operation is not transactional so it may succeed for some partitions while fail for others.
     *
     * @param groupId The group for which to alter offsets.
     * @param offsets A map of offsets by partition with associated metadata. Partitions not specified in the map are ignored.
     * @param options The options to use when altering the offsets.
     * @return The AlterOffsetsResult.
     */
    AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options);

    /**
     * <p>List offset for the specified partitions and OffsetSpec. This operation enables to find
     * the beginning offset, end offset as well as the offset matching a timestamp in partitions.
     *
     * <p>This is a convenience method for {@link #listOffsets(Map, ListOffsetsOptions)}
     *
     * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
     * @return The ListOffsetsResult.
     */
    default ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
        return listOffsets(topicPartitionOffsets, new ListOffsetsOptions());
    }

    /**
     * <p>List offset for the specified partitions. This operation enables to find
     * the beginning offset, end offset as well as the offset matching a timestamp in partitions.
     *
     * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
     * @param options The options to use when retrieving the offsets
     * @return The ListOffsetsResult.
     */
    ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options);

    /**
     * Describes all entities matching the provided filter that have at least one client quota configuration
     * value defined.
     * <p>
     * This is a convenience method for {@link #describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)}
     * with default options. See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param filter the filter to apply to match entities
     * @return the DescribeClientQuotasResult containing the result
     */
    default DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter) {
        return describeClientQuotas(filter, new DescribeClientQuotasOptions());
    }

    /**
     * Describes all entities matching the provided filter that have at least one client quota configuration
     * value defined.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the future from the
     * returned {@link DescribeClientQuotasResult}:
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user didn't have describe access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     *   If the request details are invalid. e.g., an invalid entity type was specified.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the describe could finish.</li>
     * </ul>
     * <p>
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param filter the filter to apply to match entities
     * @param options the options to use
     * @return the DescribeClientQuotasResult containing the result
     */
    DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options);

    /**
     * Alters client quota configurations with the specified alterations.
     * <p>
     * This is a convenience method for {@link #alterClientQuotas(Collection, AlterClientQuotasOptions)}
     * with default options. See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param entries the alterations to perform
     * @return the AlterClientQuotasResult containing the result
     */
    default AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries) {
        return alterClientQuotas(entries, new AlterClientQuotasOptions());
    }

    /**
     * Alters client quota configurations with the specified alterations.
     * <p>
     * Alterations for a single entity are atomic, but across entities is not guaranteed. The resulting
     * per-entity error code should be evaluated to resolve the success or failure of all updates.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@link AlterClientQuotasResult}:
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user didn't have alter access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     *   If the request details are invalid. e.g., a configuration key was specified more than once for an entity.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the alterations could finish. It cannot be guaranteed whether the update
     *   succeed or not.</li>
     * </ul>
     * <p>
     * This operation is supported by brokers with version 2.6.0 or higher.
     *
     * @param entries the alterations to perform
     * @return the AlterClientQuotasResult containing the result
     */
    AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options);

    /**
     * Get the metrics kept by the adminClient
     */
    Map<MetricName, ? extends Metric> metrics();
}
