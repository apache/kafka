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

import kafka.cluster.EndPoint;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 */
public class KafkaEmbedded {

    private static final Logger log = LoggerFactory.getLogger(KafkaEmbedded.class);

    private final Properties effectiveConfig;
    private final File logDir;
    private final File tmpFolder;
    private final BrokerServer kafka;

    /**
     * Creates and starts an embedded Kafka broker.
     *
     * @param config Broker configuration settings.  Used to modify, for example, on which port the
     *               broker should listen to.  Note that you cannot change the `log.dirs` setting
     *               currently.
     */
    @SuppressWarnings({"WeakerAccess", "this-escape"})
    public KafkaEmbedded(final Properties config) {
        tmpFolder = org.apache.kafka.test.TestUtils.tempDirectory();
        logDir = org.apache.kafka.test.TestUtils.tempDirectory(tmpFolder.toPath(), "log");
        effectiveConfig = effectiveConfigFrom(config);
        try {
            final KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                    .setNumBrokerNodes(1)
                    .build()
            );

            config.forEach((k, v) -> {
                System.out.println(k + ":" + v);
                clusterBuilder.setConfigProp((String) k, v.toString());
            });
            final KafkaClusterTestKit cluster = clusterBuilder.build();
            kafka = cluster.brokers().get(0);

        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to create test Kafka cluster", e);
        }
    }

    /**
     * Creates the configuration for starting the Kafka broker by merging default values with
     * overwrites.
     *
     * @param initialConfig Broker configuration settings that override the default config.
     */
    private Properties effectiveConfigFrom(final Properties initialConfig) {
        final Properties effectiveConfig = new Properties();
        effectiveConfig.put(KafkaConfig.BrokerIdProp(), 0);
        effectiveConfig.put(KafkaConfig.NumPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig.AutoCreateTopicsEnableProp(), true);
        effectiveConfig.put(KafkaConfig.MessageMaxBytesProp(), 1000000);
        effectiveConfig.put(KafkaConfig.ControlledShutdownEnableProp(), true);
        effectiveConfig.put(KafkaConfig.ZkSessionTimeoutMsProp(), 10000);

        effectiveConfig.putAll(initialConfig);
        effectiveConfig.setProperty(KafkaConfig.LogDirProp(), logDir.getAbsolutePath());
        return effectiveConfig;
    }

    /**
     * This broker's `metadata.broker.list` value.  Example: `localhost:9092`.
     * <p>
     * You can use this to tell Kafka producers and consumers how to connect to this instance.
     */
    @SuppressWarnings("WeakerAccess")
    public String brokerList() {
        final EndPoint endPoint = kafka.config().effectiveAdvertisedListeners().head();
        return endPoint.host() + ":" + endPoint.port();
    }

    @SuppressWarnings("WeakerAccess")
    public void stopAsync() {
        log.debug("Shutting down embedded Kafka broker at {}...",
                  brokerList());
        kafka.shutdown();
    }

    @SuppressWarnings("WeakerAccess")
    public void awaitStoppedAndPurge() {
        kafka.awaitShutdown();
        log.debug("Removing log dir at {} ...", logDir);
        try {
            Utils.delete(tmpFolder);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        log.debug("Shutdown of embedded Kafka broker at {} completed...",
            brokerList());
    }

    /**
     * Create a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(final String topic) {
        createTopic(topic, 1, 1, Collections.emptyMap());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(final String topic, final int partitions, final int replication) {
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
                            final Map<String, String> topicConfig) {
        log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
            topic, partitions, replication, topicConfig);
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        newTopic.configs(topicConfig);

        try (final Admin adminClient = createAdminClient()) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public Admin createAdminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());
        final Object listeners = effectiveConfig.get(KafkaConfig.ListenersProp());
        if (listeners != null && listeners.toString().contains("SSL")) {
            adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, effectiveConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) effectiveConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
            adminClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        return Admin.create(adminClientConfig);
    }

    @SuppressWarnings("WeakerAccess")
    public void deleteTopic(final String topic) {
        log.debug("Deleting topic { name: {} }", topic);
        try (final Admin adminClient = createAdminClient()) {
            adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("WeakerAccess")
    public BrokerServer kafkaServer() {
        return kafka;
    }
}
