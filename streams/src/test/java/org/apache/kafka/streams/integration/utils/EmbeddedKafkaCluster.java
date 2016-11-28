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
import kafka.utils.MockTime;
import kafka.zk.EmbeddedZookeeper;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedKafkaCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
    private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
    private EmbeddedZookeeper zookeeper = null;
    private final KafkaEmbedded[] brokers;

    public EmbeddedKafkaCluster(final int numBrokers) {
        brokers = new KafkaEmbedded[numBrokers];
    }

    public MockTime time = new MockTime();

    /**
     * Creates and starts a Kafka cluster.
     */
    public void start() throws IOException, InterruptedException {
        final Properties brokerConfig = new Properties();

        log.debug("Initiating embedded Kafka cluster startup");
        log.debug("Starting a ZooKeeper instance");
        zookeeper = new EmbeddedZookeeper();
        log.debug("ZooKeeper instance is running at {}", zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
        brokerConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        brokerConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
        brokerConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
        brokerConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), false);

        for (int i = 0; i < brokers.length; i++) {
            brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), i);
            log.debug("Starting a Kafka instance on port {} ...", brokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
            brokers[i] = new KafkaEmbedded(brokerConfig, time);

            log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
                brokers[i].brokerList(), brokers[i].zookeeperConnect());
        }
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
    public void createTopic(final String topic) {
        createTopic(topic, 1, 1, new Properties());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(final String topic, final int partitions, final int replication) {
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
                            final Properties topicConfig) {
        brokers[0].createTopic(topic, partitions, replication, topicConfig);
    }

    public void deleteTopic(final String topic) {
        brokers[0].deleteTopic(topic);
    }
}
