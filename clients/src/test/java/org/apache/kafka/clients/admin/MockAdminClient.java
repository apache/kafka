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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockAdminClient extends AdminClient {
    private final List<Node> brokers;
    private final Map<String, TopicMetadata> allTopics = new HashMap<>();

    private int timeoutNextRequests = 0;

    public MockAdminClient(List<Node> brokers) {
        this.brokers = brokers;
    }

    public void addTopic(boolean internal,
                         String name,
                         List<TopicPartitionInfo> partitions,
                         Map<String, String> configs) {
        if (allTopics.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Topic %s was already added.", name));
        }
        List<Node> replicas = null;
        for (TopicPartitionInfo partition : partitions) {
            if (!brokers.contains(partition.leader())) {
                throw new IllegalArgumentException("Leader broker unknown");
            }
            if (!brokers.containsAll(partition.replicas())) {
                throw new IllegalArgumentException("Unknown brokers in replica list");
            }
            if (!brokers.containsAll(partition.isr())) {
                throw new IllegalArgumentException("Unknown brokers in isr list");
            }

            if (replicas == null) {
                replicas = partition.replicas();
            } else if (!replicas.equals(partition.replicas())) {
                throw new IllegalArgumentException("All partitions need to have the same replica nodes.");
            }
        }

        allTopics.put(name, new TopicMetadata(internal, partitions, configs));
    }

    public void timeoutNextRequest(int numberOfRequest) {
        timeoutNextRequests = numberOfRequest;
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        Map<String, KafkaFuture<Void>> createTopicResult = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (final NewTopic newTopic : newTopics) {
                String topicName = newTopic.name();

                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                createTopicResult.put(topicName, future);
            }

            --timeoutNextRequests;
            return new CreateTopicsResult(createTopicResult);
        }

        for (final NewTopic newTopic : newTopics) {
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();

            String topicName = newTopic.name();
            if (allTopics.containsKey(topicName)) {
                future.completeExceptionally(new TopicExistsException(String.format("Topic %s exists already.", topicName)));
                createTopicResult.put(topicName, future);
            }
            int replicationFactor = newTopic.replicationFactor();
            List<Node> replicas = new ArrayList<>(replicationFactor);
            for (int i = 0; i < replicationFactor; ++i) {
                replicas.add(brokers.get(i));
            }

            int numberOfPartitions = newTopic.numPartitions();
            List<TopicPartitionInfo> partitions = new ArrayList<>(numberOfPartitions);
            for (int p = 0; p < numberOfPartitions; ++p) {
                partitions.add(new TopicPartitionInfo(p, brokers.get(0), replicas, Collections.<Node>emptyList()));
            }
            allTopics.put(topicName, new TopicMetadata(false, partitions, newTopic.configs()));
            future.complete(null);
            createTopicResult.put(topicName, future);
        }

        return new CreateTopicsResult(createTopicResult);
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        Map<String, TopicListing> topicListings = new HashMap<>();

        if (timeoutNextRequests > 0) {
            KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new TimeoutException());

            --timeoutNextRequests;
            return new ListTopicsResult(future);
        }

        for (Map.Entry<String, TopicMetadata> topicDescription : allTopics.entrySet()) {
            String topicName = topicDescription.getKey();
            topicListings.put(topicName, new TopicListing(topicName, topicDescription.getValue().isInternalTopic));
        }

        KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
        future.complete(topicListings);
        return new ListTopicsResult(future);
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        Map<String, KafkaFuture<TopicDescription>> topicDescriptions = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (String requestedTopic : topicNames) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                topicDescriptions.put(requestedTopic, future);
            }

            --timeoutNextRequests;
            return new DescribeTopicsResult(topicDescriptions);
        }

        for (String requestedTopic : topicNames) {
            for (Map.Entry<String, TopicMetadata> topicDescription : allTopics.entrySet()) {
                String topicName = topicDescription.getKey();
                if (topicName.equals(requestedTopic)) {
                    TopicMetadata topicMetadata = topicDescription.getValue();
                    KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                    future.complete(new TopicDescription(topicName, topicMetadata.isInternalTopic, topicMetadata.partitions));
                    topicDescriptions.put(topicName, future);
                    break;
                }
            }
            if (!topicDescriptions.containsKey(requestedTopic)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new UnknownTopicOrPartitionException(
                    String.format("Topic %s unknown.", requestedTopic)));
                topicDescriptions.put(requestedTopic, future);
            }
        }

        return new DescribeTopicsResult(topicDescriptions);
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        Map<ConfigResource, KafkaFuture<Config>> configescriptions = new HashMap<>();

        for (ConfigResource resource : resources) {
            if (resource.type() == ConfigResource.Type.TOPIC) {
                Map<String, String> configs = allTopics.get(resource.name()).configs;
                List<ConfigEntry> configEntries = new ArrayList<>();
                for (Map.Entry<String, String> entry : configs.entrySet()) {
                    configEntries.add(new ConfigEntry(entry.getKey(), entry.getValue()));
                }
                KafkaFutureImpl<Config> future = new KafkaFutureImpl<>();
                future.complete(new Config(configEntries));
                configescriptions.put(resource, future);
            } else {
                throw new UnsupportedOperationException("Not implemented yet");
            }
        }

        return new DescribeConfigsResult(configescriptions);
    }

    @Override
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close(long duration, TimeUnit unit) {}


    private final static class TopicMetadata {
        final boolean isInternalTopic;
        final List<TopicPartitionInfo> partitions;
        final Map<String, String> configs;

        TopicMetadata(boolean isInternalTopic,
                      List<TopicPartitionInfo> partitions,
                      Map<String, String> configs) {
            this.isInternalTopic = isInternalTopic;
            this.partitions = partitions;
            this.configs = configs != null ? configs : Collections.<String, String>emptyMap();
        }
    }

}
