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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * The public interface for the {@link KafkaAdminClient}, which supports managing and inspecting topics,
 * brokers, and configurations.
 *
 * @see KafkaAdminClient
 */
@InterfaceStability.Unstable
public abstract class AdminClient implements AutoCloseable {

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param props         The configuration.
     * @return              The new KafkaAdminClient.
     */
    public static AdminClient create(Properties props) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(props));
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param conf          The configuration.
     * @return              The new KafkaAdminClient.
     */
    public static AdminClient create(Map<String, Object> conf) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf));
    }

    /**
     * Close the AdminClient and release all associated resources.
     */
    public abstract void close();

    /**
     * Create a batch of new topics with the default options.
     *
     * @param newTopics         The new topics to create.
     * @return                  The CreateTopicsResults.
     */
    public CreateTopicResults createTopics(Collection<NewTopic> newTopics) {
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
     * @param newTopics         The new topics to create.
     * @param options           The options to use when creating the new topics.
     * @return                  The CreateTopicsResults.
     */
    public abstract CreateTopicResults createTopics(Collection<NewTopic> newTopics,
                                                    CreateTopicsOptions options);

    /**
     * Similar to #{@link AdminClient#deleteTopics(Collection<String>, DeleteTopicsOptions),
     * but uses the default options.
     *
     * @param topics            The topic names to delete.
     * @return                  The DeleteTopicsResults.
     */
    public DeleteTopicResults deleteTopics(Collection<String> topics) {
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
     * @param topics            The topic names to delete.
     * @param options           The options to use when deleting the topics.
     * @return                  The DeleteTopicsResults.
     */
    public abstract DeleteTopicResults deleteTopics(Collection<String> topics, DeleteTopicsOptions options);

    /**
     * List the topics available in the cluster with the default options.
     *
     * @return                  The ListTopicsResults.
     */
    public ListTopicsResults listTopics() {
        return listTopics(new ListTopicsOptions());
    }

    /**
     * List the topics available in the cluster.
     *
     * @param options           The options to use when listing the topics.
     * @return                  The ListTopicsResults.
     */
    public abstract ListTopicsResults listTopics(ListTopicsOptions options);

    /**
     * Describe some topics in the cluster, with the default options.
     *
     * See {@link AdminClient#describeTopics(Collection<String>, DescribeTopicsOptions)}
     *
     * @param topicNames        The names of the topics to describe.
     *
     * @return                  The DescribeTopicsResults.
     */
    public DescribeTopicsResults describeTopics(Collection<String> topicNames) {
        return describeTopics(topicNames, new DescribeTopicsOptions());
    }

    /**
     * Describe some topics in the cluster.
     *
     * Note that if auto.create.topics.enable is true on the brokers,
     * describeTopics(topicName, ...) may create a topic named topicName.
     * There are two workarounds: either use AdminClient#listTopics and ensure
     * that the topic is present before describing, or disable
     * auto.create.topics.enable.
     *
     * @param topicNames        The names of the topics to describe.
     * @param options           The options to use when describing the topic.
     *
     * @return                  The DescribeTopicsResults.
     */
    public abstract DescribeTopicsResults describeTopics(Collection<String> topicNames,
                                                         DescribeTopicsOptions options);

    /**
     * Get information about the nodes in the cluster, using the default options.
     *
     * @return                  The DescribeClusterResults.
     */
    public DescribeClusterResults describeCluster() {
        return describeCluster(new DescribeClusterOptions());
    }

    /**
     * Get information about the nodes in the cluster.
     *
     * @param options           The options to use when getting information about the cluster.
     * @return                  The DescribeClusterResults.
     */
    public abstract DescribeClusterResults describeCluster(DescribeClusterOptions options);

    /**
     * Get information about the api versions of nodes in the cluster with the default options.
     * See {@link AdminClient#apiVersions(Collection<Node>, ApiVersionsOptions)}
     *
     * @param nodes             The nodes to get information about, or null to get information about all nodes.
     * @return                  The ApiVersionsResults.
     */
    public ApiVersionsResults apiVersions(Collection<Node> nodes) {
        return apiVersions(nodes, new ApiVersionsOptions());
    }

    /**
     * Get information about the api versions of nodes in the cluster.
     *
     * @param nodes             The nodes to get information about, or null to get information about all nodes.
     * @param options           The options to use when getting api versions of the nodes.
     * @return                  The ApiVersionsResults.
     */
    public abstract ApiVersionsResults apiVersions(Collection<Node> nodes, ApiVersionsOptions options);

    /**
<<<<<<< HEAD
     * Similar to #{@link AdminClient#describeAcls(AclBindingFilter, DescribeAclsOptions),
     * but uses the default options.
     *
     * @param filter            The filter to use.
     * @return                  The DeleteAclsResult.
     */
    public DescribeAclsResults describeAcls(AclBindingFilter filter) {
        return describeAcls(filter, new DescribeAclsOptions());
    }

    /**
     * Lists access control lists (ACLs) according to the supplied filter.
     *
     * Note: it may take some time for changes made by createAcls or deleteAcls to be reflected
     * in the output of describeAcls.
     *
     * @param filter            The filter to use.
     * @param options           The options to use when listing the ACLs.
     * @return                  The DeleteAclsResult.
     */
    public abstract DescribeAclsResults describeAcls(AclBindingFilter filter, DescribeAclsOptions options);

    /**
     * Similar to #{@link AdminClient#createAcls(Collection<AclBinding>, CreateAclsOptions),
     * but uses the default options.
     *
     * @param acls              The ACLs to create
     * @return                  The CreateAclsResult.
     */
    public CreateAclsResults createAcls(Collection<AclBinding> acls) {
        return createAcls(acls, new CreateAclsOptions());
    }

    /**
     * Creates access control lists (ACLs) which are bound to specific resources.
     *
     * If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but
     * no changes will be made.
     *
     * @param acls              The ACLs to create
     * @param options           The options to use when creating the ACLs.
     * @return                  The CreateAclsResult.
     */
    public abstract CreateAclsResults createAcls(Collection<AclBinding> acls, CreateAclsOptions options);

    /**
     * Similar to #{@link AdminClient#deleteAcls(Collection<AclBinding>, DeleteAclsOptions),
     * but uses the default options.
     *
     * @param filters           The filters to use.
     * @return                  The DeleteAclsResult.
     */
    public DeleteAclsResults deleteAcls(Collection<AclBindingFilter> filters) {
        return deleteAcls(filters, new DeleteAclsOptions());
    }

    /**
     * Deletes access control lists (ACLs) according to the supplied filters.
     *
     * @param filters           The filters to use.
     * @param options           The options to use when deleting the ACLs.
     * @return                  The DeleteAclsResult.
     */
    public abstract DeleteAclsResults deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options);


     /**
     * Get the configuration for the specified resources with the default options.
     *
     * See {@link #describeConfigs(Collection, DescribeConfigsOptions)} for more details.
     *
     * @param resources         The resources (topic and broker resource types are currently supported)
     * @return                  The DescribeConfigsResults
     */
    public DescribeConfigsResults describeConfigs(Collection<ConfigResource> resources) {
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
     * @param resources         The resources (topic and broker resource types are currently supported)
     * @param options           The options to use when describing configs
     * @return                  The DescribeConfigsResults
     */
    public abstract DescribeConfigsResults describeConfigs(Collection<ConfigResource> resources,
                                                           DescribeConfigsOptions options);

    /**
     * Update the configuration for the specified resources with the default options.
     *
     * See {@link #alterConfigs(Map, AlterConfigsOptions)} for more details.
     *
     * @param configs         The resources with their configs (topic is the only resource type with configs that can
     *                        be updated currently)
     * @return                The AlterConfigsResults
     */
    public AlterConfigsResults alterConfigs(Map<ConfigResource, Config> configs) {
        return alterConfigs(configs, new AlterConfigsOptions());
    }

    /**
     * Update the configuration for the specified resources with the default options.
     *
     * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
     * a particular resource are updated atomically.
     *
     * @param configs         The resources with their configs (topic is the only resource type with configs that can
     *                        be updated currently)
     * @param options         The options to use when describing configs
     * @return                The AlterConfigsResults
     */
    public abstract AlterConfigsResults alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options);

}
