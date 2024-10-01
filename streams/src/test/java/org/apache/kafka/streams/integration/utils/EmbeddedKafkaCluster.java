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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.utils.Utils.mkProperties;

/**
 * Setup an embedded Kafka KRaft cluster for integration tests (using {@link org.apache.kafka.common.test.KafkaClusterTestKit} internally) with the
 * specified number of brokers and the specified broker properties.
 * Additional Kafka client properties can also be supplied if required.
 * This class also provides various utility methods to easily create Kafka topics, produce data, consume data etc.
 */
public class EmbeddedKafkaCluster {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
    private final KafkaClusterTestKit cluster;
    private final Properties brokerConfig;
    public final MockTime time;
    public EmbeddedKafkaCluster(final int numBrokers) {
        this(numBrokers, new Properties());
    }

    public EmbeddedKafkaCluster(final int numBrokers, final Properties brokerConfig) {
        this(numBrokers, brokerConfig, Collections.emptyMap());
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final long mockTimeMillisStart) {
        this(numBrokers, brokerConfig, Collections.emptyMap(), mockTimeMillisStart, System.nanoTime());
    }
    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final Map<Integer, Map<String, String>> brokerConfigOverrides) {
        this(numBrokers, brokerConfig, brokerConfigOverrides, System.currentTimeMillis(), System.nanoTime());
    }
    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig,
                                final Map<Integer, Map<String, String>> brokerConfigOverrides,
                                final long mockTimeMillisStart,
                                final long mockTimeNanoStart) {
        addDefaultBrokerPropsIfAbsent(brokerConfig);

        if (!brokerConfigOverrides.isEmpty() && brokerConfigOverrides.size() != numBrokers) {
            throw new IllegalArgumentException("Size of brokerConfigOverrides " + brokerConfigOverrides.size()
                    + " must match broker number " + numBrokers);
        }
        try {
            final KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(
                    new TestKitNodes.Builder()
                            .setCombined(true)
                            .setNumBrokerNodes(numBrokers)
                            .setPerServerProperties(brokerConfigOverrides)
                            // Reduce number of controllers for faster startup
                            // We may make this configurable in the future if there's a use case for it
                            .setNumControllerNodes(1)
                            .build()
            );

            brokerConfig.forEach((k, v) -> clusterBuilder.setConfigProp((String) k, v));
            cluster = clusterBuilder.build();
            cluster.nonFatalFaultHandler().setIgnore(true);
        } catch (final Exception e) {
            throw new KafkaException("Failed to create test Kafka cluster", e);
        }
        this.brokerConfig = brokerConfig;
        this.time = new MockTime(mockTimeMillisStart, mockTimeNanoStart);
    }

    public void start() {
        try {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
        } catch (final Exception e) {
            throw new KafkaException("Failed to start test Kafka cluster", e);
        }

        verifyClusterReadiness();
    }

    /**
     * Perform an extended check to ensure that the primary APIs of the cluster are available, including:
     * <ul>
     *     <li>Ability to create a topic</li>
     *     <li>Ability to produce to a topic</li>
     *     <li>Ability to form a consumer group</li>
     *     <li>Ability to consume from a topic</li>
     * </ul>
     * If this method completes successfully, all resources created to verify the cluster health
     * (such as topics and consumer groups) will be cleaned up before it returns.
     * <p>
     * This provides extra guarantees compared to other cluster readiness checks such as
     * {@link KafkaClusterTestKit#waitForReadyBrokers()}, which verify that brokers have
     * completed startup and joined the cluster, but do not verify that the internal consumer
     * offsets topic has been created or that it's actually possible for users to create and
     * interact with topics.
     */
    public void verifyClusterReadiness() {
        final UUID uuid = UUID.randomUUID();
        final String consumerGroupId = "group-warmup-" + uuid;
        final Map<String, Object> consumerConfig = Collections.singletonMap(GROUP_ID_CONFIG, consumerGroupId);
        final String topic = "topic-warmup-" + uuid;

        createTopic(topic);
        final Map<String, Object> producerProps = new HashMap<>(clientDefaultConfig());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "warmup-producer");
        produce(producerProps, topic, null, "warmup message key", "warmup message value");

        try (Consumer<?, ?> consumer = createConsumerAndSubscribeTo(consumerConfig, topic)) {
            final ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(TimeUnit.MINUTES.toMillis(2)));
            if (records.isEmpty()) {
                throw new AssertionError("Failed to verify availability of group coordinator and produce/consume APIs on Kafka cluster in time");
            }
        }

        try (Admin admin = createAdminClient()) {
            admin.deleteConsumerGroups(Collections.singleton(consumerGroupId)).all().get(30, TimeUnit.SECONDS);
            admin.deleteTopics(Collections.singleton(topic)).all().get(30, TimeUnit.SECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError("Failed to clean up cluster health check resource(s)", e);
        }
    }

    /**
     * Stop the Kafka cluster.
     */
    public void stop() {
        final AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        Utils.closeQuietly(cluster, "embedded Kafka cluster", shutdownFailure);
        if (shutdownFailure.get() != null) {
            throw new KafkaException("Failed to shut down producer / embedded Kafka cluster", shutdownFailure.get());
        }
    }

    public String bootstrapServers() {
        return cluster.bootstrapServers();
    }

    public boolean sslEnabled() {
        final String listenerSecurityProtocolMap = brokerConfig.getProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG);
        if (listenerSecurityProtocolMap == null)
            return false;
        return listenerSecurityProtocolMap.contains(":SSL") || listenerSecurityProtocolMap.contains(":SASL_SSL");
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
    public void createTopic(final String topic) {
        createTopic(topic, 1);
    }

    /**
     * Create a Kafka topic with given partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     * @param partitions  The number of partitions for this topic.
     */
    public void createTopic(final String topic, final int partitions) {
        createTopic(topic, partitions, 1, Collections.emptyMap());
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
     * Create a Kafka topic with given partition, replication factor, and topic config.
     *
     * @param topic The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    public void createTopic(final String topic, final int partitions, final int replication, final Map<String, String> topicConfig) {
        if (replication > cluster.brokers().size()) {
            throw new InvalidReplicationFactorException("Insufficient brokers ("
                    + cluster.brokers().size() + ") for desired replication (" + replication + ")");
        }

        log.info("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
                topic, partitions, replication, topicConfig);
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        newTopic.configs(topicConfig);

        try (final Admin adminClient = createAdminClient()) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            TestUtils.waitForCondition(() -> adminClient.listTopics().names().get().contains(topic),
                    "Wait for topic " + topic + " to get created.");
        } catch (final TopicExistsException ignored) {
        } catch (final InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public void deleteTopics(final String... topics) {
        for (final String topic : topics) {
            deleteTopic(topic);
        }
    }


    /**
     * Delete a Kafka topic.
     *
     * @param topic the topic to delete; may not be null
     */
    public void deleteTopic(final String topic) {
        try (final Admin adminClient = createAdminClient()) {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Delete all topics except internal topics.
     */
    public void deleteAllTopics() {
        try (final Admin adminClient = createAdminClient()) {
            final Set<String> topics = adminClient.listTopics().names().get();
            adminClient.deleteTopics(topics).all().get();
        } catch (final UnknownTopicOrPartitionException ignored) {
        } catch (final ExecutionException | InterruptedException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Produce given key and value to topic partition.
     * @param topic the topic to produce to; may not be null.
     * @param partition the topic partition to produce to.
     * @param key the record key.
     * @param value the record value.
     */
    public void produce(final Map<String, Object> producerProps, final String topic, final Integer partition, final String key, final String value) {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer())) {
            final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, partition, key == null ? null : key.getBytes(), value == null ? null : value.getBytes());
            try {
                producer.send(msg).get(TimeUnit.SECONDS.toMillis(120), TimeUnit.MILLISECONDS);
                producer.flush();
            } catch (final Exception e) {
                throw new KafkaException("Could not produce message: " + msg, e);
            }
        }
    }

    public Admin createAdminClient() {
        return Admin.create(mkProperties(clientDefaultConfig()));
    }

    public Map<String, String> clientDefaultConfig() {
        final Map<String, String> props = new HashMap<>();
        props.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        if (sslEnabled()) {
            props.putIfAbsent(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG).toString());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
            props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        return props;
    }

    public KafkaConsumer<byte[], byte[]> createConsumer(final Map<String, Object> consumerProps) {
        final Map<String, Object> props = new HashMap<>(clientDefaultConfig());
        props.putAll(consumerProps);

        props.putIfAbsent(GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.putIfAbsent(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putIfAbsent(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.putIfAbsent(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        final KafkaConsumer<byte[], byte[]> consumer;
        try {
            consumer = new KafkaConsumer<>(props);
        } catch (final Throwable t) {
            throw new KafkaException("Failed to create consumer", t);
        }
        return consumer;
    }

    public KafkaConsumer<byte[], byte[]> createConsumerAndSubscribeTo(final Map<String, Object> consumerProps, final String... topics) {
        return createConsumerAndSubscribeTo(consumerProps, null, topics);
    }

    public KafkaConsumer<byte[], byte[]> createConsumerAndSubscribeTo(final Map<String, Object> consumerProps, final ConsumerRebalanceListener rebalanceListener, final String... topics) {
        final KafkaConsumer<byte[], byte[]> consumer = createConsumer(consumerProps);
        if (rebalanceListener != null) {
            consumer.subscribe(Arrays.asList(topics), rebalanceListener);
        } else {
            consumer.subscribe(Arrays.asList(topics));
        }
        return consumer;
    }

    private void addDefaultBrokerPropsIfAbsent(final Properties brokerConfig) {
        brokerConfig.putIfAbsent(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, 2 * 1024 * 1024L);
        brokerConfig.putIfAbsent(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "0");
        brokerConfig.putIfAbsent(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
        brokerConfig.putIfAbsent(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "5");
        brokerConfig.putIfAbsent(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        brokerConfig.putIfAbsent(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, "5");
        brokerConfig.putIfAbsent(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        brokerConfig.putIfAbsent(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, true);
        brokerConfig.putIfAbsent(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, true);
    }

    public void waitForRemainingTopics(final long timeoutMs, final String... topics) throws InterruptedException {
        TestUtils.waitForCondition(new TopicsRemainingCondition(topics), timeoutMs, "Topics are not expected after " + timeoutMs + " milli seconds.");
    }

    public Set<String> getAllTopicsInCluster() {
        try (final Admin adminClient = createAdminClient()) {
            return adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Properties getLogConfig(final String topic) {
        try (final Admin adminClient = createAdminClient()) {
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            final Config config = adminClient.describeConfigs(Collections.singleton(configResource)).values().get(configResource).get();
            final Properties properties = new Properties();
            for (final ConfigEntry configEntry : config.entries()) {
                if (configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                    properties.put(configEntry.name(), configEntry.value());
                }
            }
            return properties;
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
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
}
