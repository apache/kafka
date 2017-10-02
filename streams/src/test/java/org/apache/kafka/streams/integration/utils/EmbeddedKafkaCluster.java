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

import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedKafkaCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
    private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
    private static final int TOPIC_CREATION_TIMEOUT = 30000;
    private static final int TOPIC_DELETION_TIMEOUT = 30000;
    private EmbeddedZookeeper zookeeper = null;
    private final KafkaEmbedded[] brokers;
    private ZkUtils zkUtils = null;

    private final Properties brokerConfig;
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
        this(numBrokers, brokerConfig, mockTimeMillisStart, System.nanoTime());
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final long mockTimeMillisStart,
                                final long mockTimeNanoStart) {
        brokers = new KafkaEmbedded[numBrokers];
        this.brokerConfig = brokerConfig;
        time = new MockTime(mockTimeMillisStart, mockTimeNanoStart);

    }

    /**
     * Creates and starts a Kafka cluster.
     */
    public void start() throws IOException, InterruptedException {
        log.debug("Initiating embedded Kafka cluster startup");
        log.debug("Starting a ZooKeeper instance");
        zookeeper = new EmbeddedZookeeper();
        log.debug("ZooKeeper instance is running at {}", zKConnectString());

        zkUtils = ZkUtils.apply(
            zKConnectString(),
            30000,
            30000,
            JaasUtils.isZkSecurityEnabled());

        brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);

        for (int i = 0; i < brokers.length; i++) {
            brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), i);
            log.debug("Starting a Kafka instance on port {} ...", brokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
            brokers[i] = new KafkaEmbedded(brokerConfig, time);

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
        for (final KafkaEmbedded broker : brokers) {
            broker.stop();
        }
        zkUtils.close();
        zookeeper.shutdown();
    }

    /**
     * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
     * Example: `127.0.0.1:2181`.
     * <p>
     * You can use this to e.g. tell Kafka brokers how to connect to this instance.
     */
    public String zKConnectString() {
        return "localhost:" + zookeeper.port();
    }

    /**
     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
     * <p>
     * You can use this to tell Kafka producers how to connect to this cluster.
     */
    public String bootstrapServers() {
        return brokers[0].brokerList();
    }

    @Override
    protected void before() throws Throwable {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    /**
     * Create multiple Kafka topics each with 1 partition and a replication factor of 1.
     *
     * @param topics The name of the topics.
     */
    public void createTopics(final String... topics) throws InterruptedException {
        for (final String topic : topics) {
            createTopic(topic, 1, 1, new Properties());
        }
    }

    /**
     * Create a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(final String topic) throws InterruptedException {
        createTopic(topic, 1, 1, new Properties());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(final String topic, final int partitions, final int replication) throws InterruptedException {
        createTopic(topic, partitions, replication, new Properties());
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
                            final Properties topicConfig) throws InterruptedException {
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
     * Deletes a topic and blocks until the topic got deleted.
     *
     * @param timeoutMs the max time to wait for the topic to be deleted (does not block if {@code <= 0})
     * @param topic the name of the topic
     */
    public void deleteTopicAndWait(final long timeoutMs, final String topic) throws InterruptedException {
        deleteTopicsAndWait(timeoutMs, topic);
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
            } catch (final UnknownTopicOrPartitionException e) { }
        }

        if (timeoutMs > 0) {
            TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs, "Topics not deleted after " + timeoutMs + " milli seconds.");
        }
    }

    public void deleteAndRecreateTopics(final String... topics) throws InterruptedException {
        deleteTopicsAndWait(TOPIC_DELETION_TIMEOUT, topics);
        createTopics(topics);
    }

    public void deleteAndRecreateTopics(final long timeoutMs, final String... topics) throws InterruptedException {
        deleteTopicsAndWait(timeoutMs, topics);
        createTopics(topics);
    }

    private final class TopicsDeletedCondition implements TestCondition {
        final Set<String> deletedTopic = new HashSet<>();

        private TopicsDeletedCondition(final String... topics) {
            Collections.addAll(deletedTopic, topics);
        }

        @Override
        public boolean conditionMet() {
            final Set<String> allTopics = new HashSet<>();
            allTopics.addAll(scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics()));
            return !allTopics.removeAll(deletedTopic);
        }
    }

    public List<KafkaServer> brokers() {
        final List<KafkaServer> servers = new ArrayList<>();
        for (final KafkaEmbedded broker : brokers) {
            servers.add(broker.kafkaServer());
        }
        return servers;
    }
}
