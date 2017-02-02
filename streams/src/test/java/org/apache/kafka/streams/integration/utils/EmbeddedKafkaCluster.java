/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.TopicPartition;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedKafkaCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
    private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
    public static final int TOPIC_CREATION_TIMEOUT = 30000;
    private EmbeddedZookeeper zookeeper = null;
    private final KafkaEmbedded[] brokers;
    private final Properties brokerConfig;

    public EmbeddedKafkaCluster(final int numBrokers) {
        this(numBrokers, new Properties());
    }

    public EmbeddedKafkaCluster(final int numBrokers, final Properties brokerConfig) {
        brokers = new KafkaEmbedded[numBrokers];
        this.brokerConfig = brokerConfig;
    }

    public final MockTime time = new MockTime();

    /**
     * Creates and starts a Kafka cluster.
     */
    public void start() throws IOException, InterruptedException {
        log.debug("Initiating embedded Kafka cluster startup");
        log.debug("Starting a ZooKeeper instance");
        zookeeper = new EmbeddedZookeeper();
        log.debug("ZooKeeper instance is running at {}", zKConnectString());

        brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
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
        if (!props.containsKey(propertyKey))
            brokerConfig.put(propertyKey, propertyValue);
    }

    /**
     * Stop the Kafka cluster.
     */
    public void stop() {
        for (final KafkaEmbedded broker : brokers) {
            broker.stop();
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

    protected void before() throws Throwable {
        start();
    }

    protected void after() {
        stop();
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

    public void deleteTopic(final String topic) {
        brokers[0].deleteTopic(topic);
    }

    public List<KafkaServer> brokers() {
        final List<KafkaServer> servers = new ArrayList<>();
        for (final KafkaEmbedded broker : brokers) {
            servers.add(broker.kafkaServer());
        }
        return servers;
    }
}
