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

import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 * <p>
 * Requires a running ZooKeeper instance to connect to.
 */
public class KafkaEmbedded {

    private static final Logger log = LoggerFactory.getLogger(KafkaEmbedded.class);

    private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";
    private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000;
    private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000;

    private final Properties effectiveConfig;
    private final File logDir;
    public final TemporaryFolder tmpFolder;
    private final KafkaServer kafka;

    /**
     * Creates and starts an embedded Kafka broker.
     *
     * @param config Broker configuration settings.  Used to modify, for example, on which port the
     *               broker should listen to.  Note that you cannot change the `log.dirs` setting
     *               currently.
     */
    public KafkaEmbedded(final Properties config, final MockTime time) throws IOException {
        tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        logDir = tmpFolder.newFolder();
        effectiveConfig = effectiveConfigFrom(config);
        final boolean loggingEnabled = true;
        final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
        log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
            logDir, zookeeperConnect());
        kafka = TestUtils.createServer(kafkaConfig, time);
        log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
            brokerList(), zookeeperConnect());
    }


    /**
     * Creates the configuration for starting the Kafka broker by merging default values with
     * overwrites.
     *
     * @param initialConfig Broker configuration settings that override the default config.
     * @return
     * @throws IOException
     */
    private Properties effectiveConfigFrom(final Properties initialConfig) throws IOException {
        final Properties effectiveConfig = new Properties();
        effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.HostNameProp(), "localhost");
        effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), "9092");
        effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
        effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);

        effectiveConfig.putAll(initialConfig);
        effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());
        return effectiveConfig;
    }

    /**
     * This broker's `metadata.broker.list` value.  Example: `localhost:9092`.
     * <p>
     * You can use this to tell Kafka producers and consumers how to connect to this instance.
     */
    public String brokerList() {
        Object listenerConfig = effectiveConfig.get(KafkaConfig$.MODULE$.InterBrokerListenerNameProp());
        return kafka.config().hostName() + ":" + kafka.boundPort(
            new ListenerName(listenerConfig != null ? listenerConfig.toString() : "PLAINTEXT"));
    }


    /**
     * The ZooKeeper connection string aka `zookeeper.connect`.
     */
    public String zookeeperConnect() {
        return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
    }

    /**
     * Stop the broker.
     */
    public void stop() {
        log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
            brokerList(), zookeeperConnect());
        kafka.shutdown();
        kafka.awaitShutdown();
        log.debug("Removing log dir at {} ...", logDir);
        try {
            Utils.delete(logDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tmpFolder.delete();
        log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
            brokerList(), zookeeperConnect());
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
        log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
            topic, partitions, replication, topicConfig);
        try (KafkaZkClient kafkaZkClient = createZkClient()) {
            final AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
            adminZkClient.createTopic(topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
        }
    }

    private KafkaZkClient createZkClient() {
        return KafkaZkClient.apply(zookeeperConnect(), false, DEFAULT_ZK_SESSION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS, Integer.MAX_VALUE, Time.SYSTEM, "testMetricGroup", "testMetricType");
    }

    public void deleteTopic(final String topic) {
        log.debug("Deleting topic { name: {} }", topic);

        try (KafkaZkClient kafkaZkClient = createZkClient()) {
            final AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
            adminZkClient.deleteTopic(topic);
            kafkaZkClient.close();
        }
    }

    public KafkaServer kafkaServer() {
        return kafka;
    }
}
