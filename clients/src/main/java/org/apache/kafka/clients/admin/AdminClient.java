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
     * It may take several seconds after AdminClient#createTopics returns
     * success for all the brokers to become aware that the topics have been created.
     * During this time, AdminClient#listTopics and AdminClient#describeTopics
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
     * Similar to #{@link AdminClient#deleteTopics(Collection<String>, DeleteTopicsOptions)},
     * but uses the default options.
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
     * It may take several seconds after AdminClient#deleteTopics returns
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
     * See {@link AdminClient#describeTopics(Collection<String>, DescribeTopicsOptions)}
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
     * Similar to #{@link AdminClient#describeAcls(AclBindingFilter, DescribeAclsOptions)},
     * but uses the default options.
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
     * Similar to #{@link AdminClient#createAcls(Collection<AclBinding>, CreateAclsOptions)},
     * but uses the default options.
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
     * Similar to #{@link AdminClient#deleteAcls(Collection<AclBinding>, DeleteAclsOptions)},
     * but uses the default options.
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
     * See {@link #describeConfigs(Collection, DescribeConfigsOptions)} for more details.
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
     * See {@link #alterConfigs(Map, AlterConfigsOptions)} for more details.
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
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicaAssignment  The replicas with their log directory absolute path
     * @return                   The AlterReplicaDirResult
     */
    public AlterReplicaDirResult alterReplicaDir(Map<TopicPartitionReplica, String> replicaAssignment) {
        return alterReplicaDir(replicaAssignment, new AlterReplicaDirOptions());
    }

    /**
     * Change the log directory for the specified replicas. This API is currently only useful if it is used
     * before the replica has been created on the broker. It will support moving replicas that have already been created after
     * KIP-113 is fully implemented.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicaAssignment  The replicas with their log directory absolute path
     * @param options            The options to use when changing replica dir
     * @return                   The AlterReplicaDirResult
     */
    public abstract AlterReplicaDirResult alterReplicaDir(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaDirOptions options);

    /**
     * Query the information of all log directories on the given set of brokers
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
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas      The replicas to query
     * @return              The DescribeReplicaLogDirResult
     */
    public DescribeReplicaLogDirResult describeReplicaLogDir(Collection<TopicPartitionReplica> replicas) {
        return describeReplicaLogDir(replicas, new DescribeReplicaLogDirOptions());
    }

    /**
     * Query the replica log directory information for the specified replicas.
     *
     * This operation is supported by brokers with version 1.0.0 or higher.
     *
     * @param replicas      The replicas to query
     * @param options       The options to use when querying replica log dir info
     * @return              The DescribeReplicaLogDirResult
     */
    public abstract DescribeReplicaLogDirResult describeReplicaLogDir(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirOptions options);
}
