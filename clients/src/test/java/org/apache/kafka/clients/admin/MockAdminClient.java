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

import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicIdCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MockAdminClient extends AdminClient {
    public static final String DEFAULT_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";

    public static final List<String> DEFAULT_LOG_DIRS =
        Collections.singletonList("/tmp/kafka-logs");

    private final List<Node> brokers;
    private final Map<String, TopicMetadata> allTopics = new HashMap<>();
    private final Map<String, Uuid> topicIds = new HashMap<>();
    private final Map<Uuid, String> topicNames = new HashMap<>();
    private final Map<TopicPartition, NewPartitionReassignment> reassignments =
        new HashMap<>();
    private final Map<TopicPartitionReplica, ReplicaLogDirInfo> replicaMoves =
        new HashMap<>();
    private final Map<TopicPartition, Long> beginningOffsets;
    private final Map<TopicPartition, Long> endOffsets;
    private final boolean usingRaftController;
    private final String clusterId;
    private final List<List<String>> brokerLogDirs;
    private final List<Map<String, String>> brokerConfigs;

    private Node controller;
    private int timeoutNextRequests = 0;
    private final int defaultPartitions;
    private final int defaultReplicationFactor;

    private Map<MetricName, Metric> mockMetrics = new HashMap<>();

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private String clusterId = DEFAULT_CLUSTER_ID;
        private List<Node> brokers = new ArrayList<>();
        private Node controller = null;
        private List<List<String>> brokerLogDirs = new ArrayList<>();
        private Short defaultPartitions;
        private boolean usingRaftController = false;
        private Integer defaultReplicationFactor;

        public Builder() {
            numBrokers(1);
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder brokers(List<Node> brokers) {
            numBrokers(brokers.size());
            this.brokers = brokers;
            return this;
        }

        public Builder numBrokers(int numBrokers) {
            if (brokers.size() >= numBrokers) {
                brokers = brokers.subList(0, numBrokers);
                brokerLogDirs = brokerLogDirs.subList(0, numBrokers);
            } else {
                for (int id = brokers.size(); id < numBrokers; id++) {
                    brokers.add(new Node(id, "localhost", 1000 + id));
                    brokerLogDirs.add(DEFAULT_LOG_DIRS);
                }
            }
            return this;
        }

        public Builder controller(int index) {
            this.controller = brokers.get(index);
            return this;
        }

        public Builder brokerLogDirs(List<List<String>> brokerLogDirs) {
            this.brokerLogDirs = brokerLogDirs;
            return this;
        }

        public Builder defaultReplicationFactor(int defaultReplicationFactor) {
            this.defaultReplicationFactor = defaultReplicationFactor;
            return this;
        }

        public Builder usingRaftController(boolean usingRaftController) {
            this.usingRaftController = usingRaftController;
            return this;
        }

        public Builder defaultPartitions(short numPartitions) {
            this.defaultPartitions = numPartitions;
            return this;
        }

        public MockAdminClient build() {
            return new MockAdminClient(brokers,
                controller == null ? brokers.get(0) : controller,
                clusterId,
                defaultPartitions != null ? defaultPartitions.shortValue() : 1,
                defaultReplicationFactor != null ? defaultReplicationFactor.shortValue() : Math.min(brokers.size(), 3),
                brokerLogDirs,
                usingRaftController);
        }
    }

    public MockAdminClient() {
        this(Collections.singletonList(Node.noNode()), Node.noNode());
    }

    public MockAdminClient(List<Node> brokers, Node controller) {
        this(brokers, controller, DEFAULT_CLUSTER_ID, 1, brokers.size(),
            Collections.nCopies(brokers.size(), DEFAULT_LOG_DIRS), false);
    }

    private MockAdminClient(List<Node> brokers,
                            Node controller,
                            String clusterId,
                            int defaultPartitions,
                            int defaultReplicationFactor,
                            List<List<String>> brokerLogDirs,
                            boolean usingRaftController) {
        this.brokers = brokers;
        controller(controller);
        this.clusterId = clusterId;
        this.defaultPartitions = defaultPartitions;
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.brokerLogDirs = brokerLogDirs;
        this.brokerConfigs = new ArrayList<>();
        for (int i = 0; i < brokers.size(); i++) {
            final Map<String, String> config = new HashMap<>();
            config.put("default.replication.factor", String.valueOf(defaultReplicationFactor));
            this.brokerConfigs.add(config);
        }
        this.beginningOffsets = new HashMap<>();
        this.endOffsets = new HashMap<>();
        this.usingRaftController = usingRaftController;
    }

    synchronized public void controller(Node controller) {
        if (!brokers.contains(controller))
            throw new IllegalArgumentException("The controller node must be in the list of brokers");
        this.controller = controller;
    }

    public void addTopic(boolean internal,
                         String name,
                         List<TopicPartitionInfo> partitions,
                         Map<String, String> configs) {
        addTopic(internal, name, partitions, configs, true);
    }

    synchronized public void addTopic(boolean internal,
                                      String name,
                                      List<TopicPartitionInfo> partitions,
                                      Map<String, String> configs,
                                      boolean usesTopicId) {
        if (allTopics.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Topic %s was already added.", name));
        }
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
        }
        ArrayList<String> logDirs = new ArrayList<>();
        for (TopicPartitionInfo partition : partitions) {
            if (partition.leader() != null) {
                logDirs.add(brokerLogDirs.get(partition.leader().id()).get(0));
            }
        }
        Uuid topicId;
        if (usesTopicId) {
            topicId = Uuid.randomUuid();
            topicIds.put(name, topicId);
            topicNames.put(topicId, name);
        } else {
            topicId = Uuid.ZERO_UUID;
        }
        allTopics.put(name, new TopicMetadata(topicId, internal, partitions, logDirs, configs));
    }

    synchronized public void markTopicForDeletion(final String name) {
        if (!allTopics.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Topic %s did not exist.", name));
        }

        allTopics.get(name).markedForDeletion = true;
    }

    synchronized public void timeoutNextRequest(int numberOfRequest) {
        timeoutNextRequests = numberOfRequest;
    }

    @Override
    synchronized public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        KafkaFutureImpl<Collection<Node>> nodesFuture = new KafkaFutureImpl<>();
        KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
        KafkaFutureImpl<String> brokerIdFuture = new KafkaFutureImpl<>();
        KafkaFutureImpl<Set<AclOperation>> authorizedOperationsFuture = new KafkaFutureImpl<>();

        if (timeoutNextRequests > 0) {
            nodesFuture.completeExceptionally(new TimeoutException());
            controllerFuture.completeExceptionally(new TimeoutException());
            brokerIdFuture.completeExceptionally(new TimeoutException());
            authorizedOperationsFuture.completeExceptionally(new TimeoutException());
            --timeoutNextRequests;
        } else {
            nodesFuture.complete(brokers);
            controllerFuture.complete(controller);
            brokerIdFuture.complete(clusterId);
            authorizedOperationsFuture.complete(Collections.emptySet());
        }

        return new DescribeClusterResult(nodesFuture, controllerFuture, brokerIdFuture, authorizedOperationsFuture);
    }

    @Override
    synchronized public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        Map<String, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> createTopicResult = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (final NewTopic newTopic : newTopics) {
                String topicName = newTopic.name();

                KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                createTopicResult.put(topicName, future);
            }

            --timeoutNextRequests;
            return new CreateTopicsResult(createTopicResult);
        }

        for (final NewTopic newTopic : newTopics) {
            KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> future = new KafkaFutureImpl<>();

            String topicName = newTopic.name();
            if (allTopics.containsKey(topicName)) {
                future.completeExceptionally(new TopicExistsException(String.format("Topic %s exists already.", topicName)));
                createTopicResult.put(topicName, future);
                continue;
            }
            int replicationFactor = newTopic.replicationFactor();
            if (replicationFactor == -1) {
                replicationFactor = defaultReplicationFactor;
            }
            if (replicationFactor > brokers.size()) {
                future.completeExceptionally(new InvalidReplicationFactorException(
                        String.format("Replication factor: %d is larger than brokers: %d", newTopic.replicationFactor(), brokers.size())));
                createTopicResult.put(topicName, future);
                continue;
            }

            List<Node> replicas = new ArrayList<>(replicationFactor);
            for (int i = 0; i < replicationFactor; ++i) {
                replicas.add(brokers.get(i));
            }

            int numberOfPartitions = newTopic.numPartitions();
            if (numberOfPartitions == -1) {
                numberOfPartitions = defaultPartitions;
            }
            List<TopicPartitionInfo> partitions = new ArrayList<>(numberOfPartitions);
            // Partitions start off on the first log directory of each broker, for now.
            List<String> logDirs = new ArrayList<>(numberOfPartitions);
            for (int i = 0; i < numberOfPartitions; i++) {
                partitions.add(new TopicPartitionInfo(i, brokers.get(0), replicas, Collections.emptyList()));
                logDirs.add(brokerLogDirs.get(partitions.get(i).leader().id()).get(0));
            }
            Uuid topicId = Uuid.randomUuid();
            topicIds.put(topicName, topicId);
            topicNames.put(topicId, topicName);
            allTopics.put(topicName, new TopicMetadata(topicId, false, partitions, logDirs, newTopic.configs()));
            future.complete(null);
            createTopicResult.put(topicName, future);
        }

        return new CreateTopicsResult(createTopicResult);
    }

    @Override
    synchronized public ListTopicsResult listTopics(ListTopicsOptions options) {
        Map<String, TopicListing> topicListings = new HashMap<>();

        if (timeoutNextRequests > 0) {
            KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new TimeoutException());

            --timeoutNextRequests;
            return new ListTopicsResult(future);
        }

        for (Map.Entry<String, TopicMetadata> topicDescription : allTopics.entrySet()) {
            String topicName = topicDescription.getKey();
            if (topicDescription.getValue().fetchesRemainingUntilVisible > 0) {
                topicDescription.getValue().fetchesRemainingUntilVisible--;
            } else {
                topicListings.put(topicName, new TopicListing(topicName, topicDescription.getValue().topicId, topicDescription.getValue().isInternalTopic));
            }
        }

        KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
        future.complete(topicListings);
        return new ListTopicsResult(future);
    }

    @Override
    synchronized public DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
        if (topics instanceof TopicIdCollection)
            return DescribeTopicsResult.ofTopicIds(new HashMap<>(handleDescribeTopicsUsingIds(((TopicIdCollection) topics).topicIds(), options)));
        else if (topics instanceof TopicNameCollection)
            return DescribeTopicsResult.ofTopicNames(new HashMap<>(handleDescribeTopicsByNames(((TopicNameCollection) topics).topicNames(), options)));
        else
            throw new IllegalArgumentException("The TopicCollection provided did not match any supported classes for describeTopics.");
    }

    private Map<String, KafkaFuture<TopicDescription>> handleDescribeTopicsByNames(Collection<String> topicNames, DescribeTopicsOptions options) {
        Map<String, KafkaFuture<TopicDescription>> topicDescriptions = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (String requestedTopic : topicNames) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                topicDescriptions.put(requestedTopic, future);
            }

            --timeoutNextRequests;
            return topicDescriptions;
        }

        for (String requestedTopic : topicNames) {
            for (Map.Entry<String, TopicMetadata> topicDescription : allTopics.entrySet()) {
                String topicName = topicDescription.getKey();
                Uuid topicId = topicIds.getOrDefault(topicName, Uuid.ZERO_UUID);
                if (topicName.equals(requestedTopic) && !topicDescription.getValue().markedForDeletion) {
                    if (topicDescription.getValue().fetchesRemainingUntilVisible > 0) {
                        topicDescription.getValue().fetchesRemainingUntilVisible--;
                    } else {
                        TopicMetadata topicMetadata = topicDescription.getValue();
                        KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                        future.complete(new TopicDescription(topicName, topicMetadata.isInternalTopic, topicMetadata.partitions, Collections.emptySet(), topicId));
                        topicDescriptions.put(topicName, future);
                        break;
                    }
                }
            }
            if (!topicDescriptions.containsKey(requestedTopic)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new UnknownTopicOrPartitionException("Topic " + requestedTopic + " not found."));
                topicDescriptions.put(requestedTopic, future);
            }
        }

        return topicDescriptions;
    }

    synchronized public Map<Uuid, KafkaFuture<TopicDescription>>  handleDescribeTopicsUsingIds(Collection<Uuid> topicIds, DescribeTopicsOptions options) {

        Map<Uuid, KafkaFuture<TopicDescription>> topicDescriptions = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (Uuid requestedTopicId : topicIds) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                topicDescriptions.put(requestedTopicId, future);
            }

            --timeoutNextRequests;
            return topicDescriptions;
        }

        for (Uuid requestedTopicId : topicIds) {
            for (Map.Entry<String, TopicMetadata> topicDescription : allTopics.entrySet()) {
                String topicName = topicDescription.getKey();
                Uuid topicId = this.topicIds.get(topicName);

                if (topicId != null && topicId.equals(requestedTopicId) && !topicDescription.getValue().markedForDeletion) {
                    if (topicDescription.getValue().fetchesRemainingUntilVisible > 0) {
                        topicDescription.getValue().fetchesRemainingUntilVisible--;
                    } else {
                        TopicMetadata topicMetadata = topicDescription.getValue();
                        KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                        future.complete(new TopicDescription(topicName, topicMetadata.isInternalTopic, topicMetadata.partitions, Collections.emptySet(), topicId));
                        topicDescriptions.put(requestedTopicId, future);
                        break;
                    }
                }
            }
            if (!topicDescriptions.containsKey(requestedTopicId)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new UnknownTopicIdException("Topic id" + requestedTopicId + " not found."));
                topicDescriptions.put(requestedTopicId, future);
            }
        }

        return topicDescriptions;
    }

    @Override
    synchronized public DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options) {
        DeleteTopicsResult result;
        if (topics instanceof TopicIdCollection)
            result = DeleteTopicsResult.ofTopicIds(new HashMap<>(handleDeleteTopicsUsingIds(((TopicIdCollection) topics).topicIds(), options)));
        else if (topics instanceof TopicNameCollection)
            result = DeleteTopicsResult.ofTopicNames(new HashMap<>(handleDeleteTopicsUsingNames(((TopicNameCollection) topics).topicNames(), options)));
        else
            throw new IllegalArgumentException("The TopicCollection provided did not match any supported classes for deleteTopics.");
        return result;
    }

    private Map<String, KafkaFuture<Void>> handleDeleteTopicsUsingNames(Collection<String> topicNameCollection, DeleteTopicsOptions options) {
        Map<String, KafkaFuture<Void>> deleteTopicsResult = new HashMap<>();
        Collection<String> topicNames = new ArrayList<>(topicNameCollection);

        if (timeoutNextRequests > 0) {
            for (final String topicName : topicNames) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                deleteTopicsResult.put(topicName, future);
            }

            --timeoutNextRequests;
            return deleteTopicsResult;
        }

        for (final String topicName : topicNames) {
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();

            if (allTopics.remove(topicName) == null) {
                future.completeExceptionally(new UnknownTopicOrPartitionException(String.format("Topic %s does not exist.", topicName)));
            } else {
                topicNames.remove(topicIds.remove(topicName));
                future.complete(null);
            }
            deleteTopicsResult.put(topicName, future);
        }
        return deleteTopicsResult;
    }

    private Map<Uuid, KafkaFuture<Void>> handleDeleteTopicsUsingIds(Collection<Uuid> topicIdCollection, DeleteTopicsOptions options) {
        Map<Uuid, KafkaFuture<Void>> deleteTopicsResult = new HashMap<>();
        Collection<Uuid> topicIds = new ArrayList<>(topicIdCollection);

        if (timeoutNextRequests > 0) {
            for (final Uuid topicId : topicIds) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                deleteTopicsResult.put(topicId, future);
            }

            --timeoutNextRequests;
            return deleteTopicsResult;
        }

        for (final Uuid topicId : topicIds) {
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();

            String name = topicNames.remove(topicId);
            if (name == null || allTopics.remove(name) == null) {
                future.completeExceptionally(new UnknownTopicOrPartitionException(String.format("Topic %s does not exist.", topicId)));
            } else {
                topicIds.remove(name);
                future.complete(null);
            }
            deleteTopicsResult.put(topicId, future);
        }
        return deleteTopicsResult;
    }

    @Override
    synchronized public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        Map<TopicPartition, KafkaFuture<DeletedRecords>> deletedRecordsResult = new HashMap<>();
        if (recordsToDelete.isEmpty()) {
            return new DeleteRecordsResult(deletedRecordsResult);
        } else {
            throw new UnsupportedOperationException("Not implemented yet");
        }
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

        if (timeoutNextRequests > 0) {
            Map<ConfigResource, KafkaFuture<Config>> configs = new HashMap<>();
            for (ConfigResource requestedResource : resources) {
                KafkaFutureImpl<Config> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                configs.put(requestedResource, future);
            }

            --timeoutNextRequests;
            return new DescribeConfigsResult(configs);
        }

        Map<ConfigResource, KafkaFuture<Config>> results = new HashMap<>();
        for (ConfigResource resource : resources) {
            KafkaFutureImpl<Config> future = new KafkaFutureImpl<>();
            results.put(resource, future);
            try {
                future.complete(getResourceDescription(resource));
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        }
        return new DescribeConfigsResult(results);
    }

    synchronized private Config getResourceDescription(ConfigResource resource) {
        switch (resource.type()) {
            case BROKER: {
                int brokerId = Integer.parseInt(resource.name());
                if (brokerId >= brokerConfigs.size()) {
                    throw new InvalidRequestException("Broker " + resource.name() +
                        " not found.");
                }
                return toConfigObject(brokerConfigs.get(brokerId));
            }
            case TOPIC: {
                TopicMetadata topicMetadata = allTopics.get(resource.name());
                if (topicMetadata != null && !topicMetadata.markedForDeletion) {
                    if (topicMetadata.fetchesRemainingUntilVisible > 0)
                        topicMetadata.fetchesRemainingUntilVisible = Math.max(0, topicMetadata.fetchesRemainingUntilVisible - 1);
                    else return toConfigObject(topicMetadata.configs);

                }
                throw new UnknownTopicOrPartitionException("Resource " + resource + " not found.");
            }
            default:
                throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    private static Config toConfigObject(Map<String, String> map) {
        List<ConfigEntry> configEntries = new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            configEntries.add(new ConfigEntry(entry.getKey(), entry.getValue()));
        }
        return new Config(configEntries);
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
        Map<ConfigResource, KafkaFuture<Void>> futures = new HashMap<>();
        for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry :
                configs.entrySet()) {
            ConfigResource resource = entry.getKey();
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            futures.put(resource, future);
            Throwable throwable =
                handleIncrementalResourceAlteration(resource, entry.getValue());
            if (throwable == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(throwable);
            }
        }
        return new AlterConfigsResult(futures);
    }

    synchronized private Throwable handleIncrementalResourceAlteration(
            ConfigResource resource, Collection<AlterConfigOp> ops) {
        switch (resource.type()) {
            case BROKER: {
                int brokerId;
                try {
                    brokerId = Integer.valueOf(resource.name());
                } catch (NumberFormatException e) {
                    return e;
                }
                if (brokerId >= brokerConfigs.size()) {
                    return new InvalidRequestException("no such broker as " + brokerId);
                }
                HashMap<String, String> newMap = new HashMap<>(brokerConfigs.get(brokerId));
                for (AlterConfigOp op : ops) {
                    switch (op.opType()) {
                        case SET:
                            newMap.put(op.configEntry().name(), op.configEntry().value());
                            break;
                        case DELETE:
                            newMap.remove(op.configEntry().name());
                            break;
                        default:
                            return new InvalidRequestException(
                                "Unsupported op type " + op.opType());
                    }
                }
                brokerConfigs.set(brokerId, newMap);
                return null;
            }
            case TOPIC: {
                TopicMetadata topicMetadata = allTopics.get(resource.name());
                if (topicMetadata == null) {
                    return new UnknownTopicOrPartitionException("No such topic as " +
                        resource.name());
                }
                HashMap<String, String> newMap = new HashMap<>(topicMetadata.configs);
                for (AlterConfigOp op : ops) {
                    switch (op.opType()) {
                        case SET:
                            newMap.put(op.configEntry().name(), op.configEntry().value());
                            break;
                        case DELETE:
                            newMap.remove(op.configEntry().name());
                            break;
                        default:
                            return new InvalidRequestException(
                                "Unsupported op type " + op.opType());
                    }
                }
                topicMetadata.configs = newMap;
                return null;
            }
            default:
                return new UnsupportedOperationException();
        }
    }

    @Override
    synchronized public AlterReplicaLogDirsResult alterReplicaLogDirs(
            Map<TopicPartitionReplica, String> replicaAssignment,
            AlterReplicaLogDirsOptions options) {
        Map<TopicPartitionReplica, KafkaFuture<Void>> results = new HashMap<>();
        for (Map.Entry<TopicPartitionReplica, String> entry : replicaAssignment.entrySet()) {
            TopicPartitionReplica replica = entry.getKey();
            String newLogDir = entry.getValue();
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            results.put(replica, future);
            List<String> dirs = brokerLogDirs.get(replica.brokerId());
            if (dirs == null) {
                future.completeExceptionally(
                    new ReplicaNotAvailableException("Can't find " + replica));
            } else if (!dirs.contains(newLogDir)) {
                future.completeExceptionally(
                    new KafkaStorageException("Log directory " + newLogDir + " is offline"));
            } else {
                TopicMetadata metadata = allTopics.get(replica.topic());
                if (metadata == null || metadata.partitions.size() <= replica.partition()) {
                    future.completeExceptionally(
                        new ReplicaNotAvailableException("Can't find " + replica));
                } else {
                    String currentLogDir = metadata.partitionLogDirs.get(replica.partition());
                    replicaMoves.put(replica,
                        new ReplicaLogDirInfo(currentLogDir, 0, newLogDir, 0));
                    future.complete(null);
                }
            }
        }
        return new AlterReplicaLogDirsResult(results);
    }

    @Override
    synchronized public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers,
                                                              DescribeLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public DescribeReplicaLogDirsResult describeReplicaLogDirs(
            Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> results = new HashMap<>();
        for (TopicPartitionReplica replica : replicas) {
            TopicMetadata topicMetadata = allTopics.get(replica.topic());
            if (topicMetadata != null) {
                KafkaFutureImpl<ReplicaLogDirInfo> future = new KafkaFutureImpl<>();
                results.put(replica, future);
                String currentLogDir = currentLogDir(replica);
                if (currentLogDir == null) {
                    future.complete(new ReplicaLogDirInfo(null,
                        DescribeLogDirsResponse.INVALID_OFFSET_LAG,
                        null,
                        DescribeLogDirsResponse.INVALID_OFFSET_LAG));
                } else {
                    ReplicaLogDirInfo info = replicaMoves.get(replica);
                    if (info == null) {
                        future.complete(new ReplicaLogDirInfo(currentLogDir, 0, null, 0));
                    } else {
                        future.complete(info);
                    }
                }
            }
        }
        return new DescribeReplicaLogDirsResult(results);
    }

    private synchronized String currentLogDir(TopicPartitionReplica replica) {
        TopicMetadata topicMetadata = allTopics.get(replica.topic());
        if (topicMetadata == null) {
            return null;
        }
        if (topicMetadata.partitionLogDirs.size() <= replica.partition()) {
            return null;
        }
        return topicMetadata.partitionLogDirs.get(replica.partition());
    }

    @Override
    synchronized public AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments,
            AlterPartitionReassignmentsOptions options) {
        Map<TopicPartition, KafkaFuture<Void>> futures = new HashMap<>();
        for (Map.Entry<TopicPartition, Optional<NewPartitionReassignment>> entry :
                newReassignments.entrySet()) {
            TopicPartition partition = entry.getKey();
            Optional<NewPartitionReassignment> newReassignment = entry.getValue();
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<Void>();
            futures.put(partition, future);
            TopicMetadata topicMetadata = allTopics.get(partition.topic());
            if (partition.partition() < 0 ||
                    topicMetadata == null ||
                    topicMetadata.partitions.size() <= partition.partition()) {
                future.completeExceptionally(new UnknownTopicOrPartitionException());
            } else if (newReassignment.isPresent()) {
                reassignments.put(partition, newReassignment.get());
                future.complete(null);
            } else {
                reassignments.remove(partition);
                future.complete(null);
            }
        }
        return new AlterPartitionReassignmentsResult(futures);
    }

    @Override
    synchronized public ListPartitionReassignmentsResult listPartitionReassignments(
            Optional<Set<TopicPartition>> partitions,
            ListPartitionReassignmentsOptions options) {
        Map<TopicPartition, PartitionReassignment> map = new HashMap<>();
        for (TopicPartition partition : partitions.isPresent() ?
                partitions.get() : reassignments.keySet()) {
            PartitionReassignment reassignment = findPartitionReassignment(partition);
            if (reassignment != null) {
                map.put(partition, reassignment);
            }
        }
        return new ListPartitionReassignmentsResult(KafkaFutureImpl.completedFuture(map));
    }

    synchronized private PartitionReassignment findPartitionReassignment(TopicPartition partition) {
        NewPartitionReassignment reassignment = reassignments.get(partition);
        if (reassignment == null) {
            return null;
        }
        TopicMetadata metadata = allTopics.get(partition.topic());
        if (metadata == null) {
            throw new RuntimeException("Internal MockAdminClient logic error: found " +
                "reassignment for " + partition + ", but no TopicMetadata");
        }
        TopicPartitionInfo info = metadata.partitions.get(partition.partition());
        if (info == null) {
            throw new RuntimeException("Internal MockAdminClient logic error: found " +
                "reassignment for " + partition + ", but no TopicPartitionInfo");
        }
        List<Integer> replicas = new ArrayList<>();
        List<Integer> removingReplicas = new ArrayList<>();
        List<Integer> addingReplicas = new ArrayList<>(reassignment.targetReplicas());
        for (Node node : info.replicas()) {
            replicas.add(node.id());
            if (!reassignment.targetReplicas().contains(node.id())) {
                removingReplicas.add(node.id());
            }
            addingReplicas.remove(Integer.valueOf(node.id()));
        }
        return new PartitionReassignment(replicas, addingReplicas, removingReplicas);
    }

    @Override
    synchronized public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    synchronized public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetSpec> entry : topicPartitionOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetSpec spec = entry.getValue();
            KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo> future = new KafkaFutureImpl<>();

            if (spec instanceof OffsetSpec.TimestampSpec)
                throw new UnsupportedOperationException("Not implement yet");
            else if (spec instanceof OffsetSpec.EarliestSpec)
                future.complete(new ListOffsetsResult.ListOffsetsResultInfo(beginningOffsets.get(tp), -1, Optional.empty()));
            else
                future.complete(new ListOffsetsResult.ListOffsetsResultInfo(endOffsets.get(tp), -1, Optional.empty()));

            futures.put(tp, future);
        }

        return new ListOffsetsResult(futures);
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
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        if (usingRaftController) {
            return new UnregisterBrokerResult(KafkaFuture.completedFuture(null));
        } else {
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new UnsupportedVersionException(""));
            return new UnregisterBrokerResult(future);
        }
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions, DescribeProducersOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    synchronized public void close(Duration timeout) {}

    public synchronized void updateBeginningOffsets(Map<TopicPartition, Long> newOffsets) {
        beginningOffsets.putAll(newOffsets);
    }

    public synchronized void updateEndOffsets(final Map<TopicPartition, Long> newOffsets) {
        endOffsets.putAll(newOffsets);
    }

    private final static class TopicMetadata {
        final Uuid topicId;
        final boolean isInternalTopic;
        final List<TopicPartitionInfo> partitions;
        final List<String> partitionLogDirs;
        Map<String, String> configs;
        int fetchesRemainingUntilVisible;

        public boolean markedForDeletion;

        TopicMetadata(Uuid topicId,
                      boolean isInternalTopic,
                      List<TopicPartitionInfo> partitions,
                      List<String> partitionLogDirs,
                      Map<String, String> configs) {
            this.topicId = topicId;
            this.isInternalTopic = isInternalTopic;
            this.partitions = partitions;
            this.partitionLogDirs = partitionLogDirs;
            this.configs = configs != null ? configs : Collections.emptyMap();
            this.markedForDeletion = false;
            this.fetchesRemainingUntilVisible = 0;
        }
    }

    synchronized public void setMockMetrics(MetricName name, Metric metric) {
        mockMetrics.put(name, metric);
    }

    @Override
    synchronized public Map<MetricName, ? extends Metric> metrics() {
        return mockMetrics;
    }

    synchronized public void setFetchesRemainingUntilVisible(String topicName, int fetchesRemainingUntilVisible) {
        TopicMetadata metadata = allTopics.get(topicName);
        if (metadata == null) {
            throw new RuntimeException("No such topic as " + topicName);
        }
        metadata.fetchesRemainingUntilVisible = fetchesRemainingUntilVisible;
    }

    synchronized public List<Node> brokers() {
        return new ArrayList<>(brokers);
    }

    synchronized public Node broker(int index) {
        return brokers.get(index);
    }
}
