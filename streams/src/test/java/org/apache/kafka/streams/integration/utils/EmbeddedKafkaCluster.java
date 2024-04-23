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
package org.apache.kafka.streams.integration.utils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.config.ZkConfigs;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and supplied number of Kafka brokers.
 */
public class EmbeddedKafkaCluster {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
    private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
    private static final int TOPIC_CREATION_TIMEOUT = 30000;
    private static final int TOPIC_DELETION_TIMEOUT = 30000;
    private EmbeddedZookeeper zookeeper = null;
    private final KafkaEmbedded[] brokers;

    private final Properties brokerConfig;
    private final List<Properties> brokerConfigOverrides;
    public final MockTime time;

    public EmbeddedKafkaCluster(final int numBrokers) {
        this(numBrokers, new Properties());
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig) {
        this(numBrokers, brokerConfig, System.currentTimeMillis());
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final long mockTimeMillisStart) {
        this(numBrokers, brokerConfig, Collections.emptyList(), mockTimeMillisStart);
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final List<Properties> brokerConfigOverrides) {
        this(numBrokers, brokerConfig, brokerConfigOverrides, System.currentTimeMillis());
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final List<Properties> brokerConfigOverrides,
                                final long mockTimeMillisStart) {
        this(numBrokers, brokerConfig, brokerConfigOverrides, mockTimeMillisStart, System.nanoTime());
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final List<Properties> brokerConfigOverrides,
                                final long mockTimeMillisStart,
                                final long mockTimeNanoStart) {
        if (!brokerConfigOverrides.isEmpty() && brokerConfigOverrides.size() != numBrokers) {
            throw new IllegalArgumentException("Size of brokerConfigOverrides " + brokerConfigOverrides.size()
                + " must match broker number " + numBrokers);
        }
        brokers = new KafkaEmbedded[numBrokers];
        this.brokerConfig = brokerConfig;
        time = new MockTime(mockTimeMillisStart, mockTimeNanoStart);
        this.brokerConfigOverrides = brokerConfigOverrides;
    }

    /**
     * Creates and starts a Kafka cluster.
     */
    public void start() throws IOException {
        log.debug("Initiating embedded Kafka cluster startup");
        log.debug("Starting a ZooKeeper instance");
        zookeeper = new EmbeddedZookeeper();
        log.debug("ZooKeeper instance is running at {}", zKConnectString());

        brokerConfig.put(ZkConfigs.ZK_CONNECT_CONFIG, zKConnectString());
        putIfAbsent(brokerConfig, SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:" + DEFAULT_BROKER_PORT);
        putIfAbsent(brokerConfig, KafkaConfig.DeleteTopicEnableProp(), true);
        putIfAbsent(brokerConfig, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, 2 * 1024 * 1024L);
        putIfAbsent(brokerConfig, GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 0);
        putIfAbsent(brokerConfig, GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0);
        putIfAbsent(brokerConfig, GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, (short) 1);
        putIfAbsent(brokerConfig, GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, 5);
        putIfAbsent(brokerConfig, TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, 5);
        putIfAbsent(brokerConfig, ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, true);

        for (int i = 0; i < brokers.length; i++) {
            brokerConfig.put(KafkaConfig.BrokerIdProp(), i);
            log.debug("Starting a Kafka instance on {} ...", brokerConfig.get(SocketServerConfigs.LISTENERS_CONFIG));

            final Properties effectiveConfig = new Properties();
            effectiveConfig.putAll(brokerConfig);
            if (brokerConfigOverrides != null && brokerConfigOverrides.size() > i) {
                effectiveConfig.putAll(brokerConfigOverrides.get(i));
            }
            brokers[i] = new KafkaEmbedded(effectiveConfig, time);

            log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
                brokers[i].brokerList(), brokers[i].zookeeperConnect());
        }
    }

    private void putIfAbsent(final Properties props, final String propertyKey, final Object propertyValue) {
        if (!props.containsKey(propertyKey)) {
            brokerConfig.put(propertyKey, propertyValue);
        }
    }

    /**
     * Stop the Kafka cluster.
     */
    public void stop() {
        if (brokers.length > 1) {
            // delete the topics first to avoid cascading leader elections while shutting down the brokers
            final Set<String> topics = getAllTopicsInCluster();
            if (!topics.isEmpty()) {
                try (final Admin adminClient = brokers[0].createAdminClient()) {
                    adminClient.deleteTopics(topics).all().get();
                } catch (final InterruptedException e) {
                    log.warn("Got interrupted while deleting topics in preparation for stopping embedded brokers", e);
                    throw new RuntimeException(e);
                } catch (final ExecutionException | RuntimeException e) {
                    log.warn("Couldn't delete all topics before stopping brokers", e);
                }
            }
        }
        for (final KafkaEmbedded broker : brokers) {
            broker.stopAsync();
        }
        for (final KafkaEmbedded broker : brokers) {
            broker.awaitStoppedAndPurge();
        }
        zookeeper.shutdown();
    }

    /**
     * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
     * Example: `127.0.0.1:2181`.
     * <p>
     * You can use this to e.g. tell Kafka brokers how to connect to this instance.
     */
    public String zKConnectString() {
        return "127.0.0.1:" + zookeeper.port();
    }

    /**
     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
     * <p>
     * You can use this to tell Kafka producers how to connect to this cluster.
     */
    public String bootstrapServers() {
        return brokers[0].brokerList();
    }

    /**
     * Create multiple Kafka topics each with 1 partition and a replication factor of 1.
     *
     * @param topics The name of the topics.
     */
    public void createTopics(final String... topics) throws InterruptedException {
        for (final String topic : topics) {
            createTopic(topic, 1, 1, Collections.emptyMap());
        }
    }

    /**
     * Create a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(final String topic) throws InterruptedException {
        createTopic(topic, 1, 1, Collections.emptyMap());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(final String topic, final int partitions, final int replication) throws InterruptedException {
        createTopic(topic, partitions, replication, Collections.emptyMap());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    public void createTopic(final String topic,
                            final int partitions,
                            final int replication,
                            final Map<String, String> topicConfig) throws InterruptedException {
        brokers[0].createTopic(topic, partitions, replication, topicConfig);
        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        IntegrationTestUtils.waitForTopicPartitions(brokers(), topicPartitions, TOPIC_CREATION_TIMEOUT);
    }

    /**
     * Deletes a topic returns immediately.
     *
     * @param topic the name of the topic
     */
    public void deleteTopic(final String topic) throws InterruptedException {
        deleteTopicsAndWait(-1L, topic);
    }

    /**
     * Deletes a topic and blocks for max 30 sec until the topic got deleted.
     *
     * @param topic the name of the topic
     */
    public void deleteTopicAndWait(final String topic) throws InterruptedException {
        deleteTopicsAndWait(TOPIC_DELETION_TIMEOUT, topic);
    }

    /**
     * Deletes multiple topics returns immediately.
     *
     * @param topics the name of the topics
     */
    public void deleteTopics(final String... topics) throws InterruptedException {
        deleteTopicsAndWait(-1, topics);
    }

    /**
     * Deletes multiple topics and blocks for max 30 sec until all topics got deleted.
     *
     * @param topics the name of the topics
     */
    public void deleteTopicsAndWait(final String... topics) throws InterruptedException {
        deleteTopicsAndWait(TOPIC_DELETION_TIMEOUT, topics);
    }

    /**
     * Deletes multiple topics and blocks until all topics got deleted.
     *
     * @param timeoutMs the max time to wait for the topics to be deleted (does not block if {@code <= 0})
     * @param topics the name of the topics
     */
    public void deleteTopicsAndWait(final long timeoutMs, final String... topics) throws InterruptedException {
        for (final String topic : topics) {
            try {
                brokers[0].deleteTopic(topic);
            } catch (final UnknownTopicOrPartitionException ignored) { }
        }

        if (timeoutMs > 0) {
            TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs, "Topics not deleted after " + timeoutMs + " milli seconds.");
        }
    }

    /**
     * Deletes all topics and blocks until all topics got deleted.
     *
     * @param timeoutMs the max time to wait for the topics to be deleted (does not block if {@code <= 0})
     */
    public void deleteAllTopicsAndWait(final long timeoutMs) throws InterruptedException {
        final Set<String> topics = getAllTopicsInCluster();
        for (final String topic : topics) {
            try {
                brokers[0].deleteTopic(topic);
            } catch (final UnknownTopicOrPartitionException ignored) { }
        }

        if (timeoutMs > 0) {
            TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs, "Topics not deleted after " + timeoutMs + " milli seconds.");
        }
    }

    public void waitForRemainingTopics(final long timeoutMs, final String... topics) throws InterruptedException {
        TestUtils.waitForCondition(new TopicsRemainingCondition(topics), timeoutMs, "Topics are not expected after " + timeoutMs + " milli seconds.");
    }

    private final class TopicsDeletedCondition implements TestCondition {
        final Set<String> deletedTopics = new HashSet<>();

        private TopicsDeletedCondition(final String... topics) {
            Collections.addAll(deletedTopics, topics);
        }

        private TopicsDeletedCondition(final Collection<String> topics) {
            deletedTopics.addAll(topics);
        }

        @Override
        public boolean conditionMet() {
            final Set<String> allTopics = getAllTopicsInCluster();
            return !allTopics.removeAll(deletedTopics);
        }
    }

    private final class TopicsRemainingCondition implements TestCondition {
        final Set<String> remainingTopics = new HashSet<>();

        private TopicsRemainingCondition(final String... topics) {
            Collections.addAll(remainingTopics, topics);
        }

        @Override
        public boolean conditionMet() {
            final Set<String> allTopics = getAllTopicsInCluster();
            return allTopics.equals(remainingTopics);
        }
    }

    private List<KafkaServer> brokers() {
        final List<KafkaServer> servers = new ArrayList<>();
        for (final KafkaEmbedded broker : brokers) {
            servers.add(broker.kafkaServer());
        }
        return servers;
    }

    public Properties getLogConfig(final String topic) {
        return brokers[0].kafkaServer().zkClient().getEntityConfigs(ConfigType.TOPIC, topic);
    }

    public Set<String> getAllTopicsInCluster() {
        final scala.collection.Iterator<String> topicsIterator = brokers[0].kafkaServer().zkClient().getAllTopicsInCluster(false).iterator();
        final Set<String> topics = new HashSet<>();
        while (topicsIterator.hasNext()) {
            topics.add(topicsIterator.next());
        }
        return topics;
    }
}
