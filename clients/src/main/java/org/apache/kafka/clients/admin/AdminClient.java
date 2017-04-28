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
     * @param conf          The configuration.
     * @return              The new KafkaAdminClient.
     */
    public static AdminClient create(Map<String, Object> conf) {
        return KafkaAdminClient.create(new AdminClientConfig(conf));
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
     * Descripe an individual topic in the cluster, with the default options.
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
     * Descripe an individual topic in the cluster.
     *
     * Note that if auto.create.topics.enable is true on the brokers,
     * AdminClient#describeTopic(topicName) may create a topic named topicName.
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
}
