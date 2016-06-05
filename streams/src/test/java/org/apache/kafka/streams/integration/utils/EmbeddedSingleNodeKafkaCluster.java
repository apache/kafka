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
import kafka.zk.EmbeddedZookeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import org.junit.rules.ExternalResource;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedSingleNodeKafkaCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);
    private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
    private EmbeddedZookeeper zookeeper = null;
    private KafkaEmbedded broker = null;

    /**
     * Creates and starts a Kafka cluster.
     */
    public void start() throws IOException, InterruptedException {
        Properties brokerConfig = new Properties();

        log.debug("Initiating embedded Kafka cluster startup");
        log.debug("Starting a ZooKeeper instance");
        zookeeper = new EmbeddedZookeeper();
        log.debug("ZooKeeper instance is running at {}", zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);

        log.debug("Starting a Kafka instance on port {} ...", brokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
        broker = new KafkaEmbedded(brokerConfig);

        log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
            broker.brokerList(), broker.zookeeperConnect());
    }

    /**
     * Stop the Kafka cluster.
     */
    public void stop() {
        broker.stop();
        zookeeper.shutdown();
    }

    /**
     * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
     * Example: `127.0.0.1:2181`.
     *
     * You can use this to e.g. tell Kafka brokers how to connect to this instance.
     */
    public String zKConnectString() {
        return "localhost:" + zookeeper.port();
    }

    /**
     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
     *
     * You can use this to tell Kafka producers how to connect to this cluster.
     */
    public String bootstrapServers() {
        return broker.brokerList();
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
    public void createTopic(String topic) {
        createTopic(topic, 1, 1, new Properties());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(String topic, int partitions, int replication) {
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
    public void createTopic(String topic,
                            int partitions,
                            int replication,
                            Properties topicConfig) {
        broker.createTopic(topic, partitions, replication, topicConfig);
    }
}