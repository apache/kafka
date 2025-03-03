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
package org.apache.kafka.connect.util.clusters;

import kafka.server.BrokerServer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Setup an embedded Kafka KRaft cluster (using {@link org.apache.kafka.common.test.KafkaClusterTestKit} internally) with the
 * specified number of brokers and the specified broker properties. This can be used for integration tests and is
 * typically used in conjunction with {@link EmbeddedConnectCluster}. Additional Kafka client properties can also be
 * supplied if required. This class also provides various utility methods to easily create Kafka topics, produce data,
 * consume data etc.
 */
public class EmbeddedKafkaCluster {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);

    private static final long DEFAULT_PRODUCE_SEND_DURATION_MS = TimeUnit.SECONDS.toMillis(120);
    private static final long GROUP_COORDINATOR_AVAILABILITY_DURATION_MS = TimeUnit.MINUTES.toMillis(2);

    private final KafkaClusterTestKit cluster;
    private final Properties brokerConfig;
    private final Map<String, String> clientConfigs;

    private KafkaProducer<byte[], byte[]> producer;

    public EmbeddedKafkaCluster(final int numBrokers, final Properties brokerConfig) {
        this(numBrokers, brokerConfig, Collections.emptyMap());
    }

    public EmbeddedKafkaCluster(final int numBrokers,
                                   final Properties brokerConfig,
                                   final Map<String, String> clientConfigs) {
        addDefaultBrokerPropsIfAbsent(brokerConfig, numBrokers);
        try {
            KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(
                    new TestKitNodes.Builder()
                            .setCombined(true)
                            .setNumBrokerNodes(numBrokers)
                            // Reduce number of controllers for faster startup
                            // We may make this configurable in the future if there's a use case for it
                            .setNumControllerNodes(1)
                            .build()
            );

            brokerConfig.forEach((k, v) -> clusterBuilder.setConfigProp((String) k, v));
            cluster = clusterBuilder.build();
            cluster.nonFatalFaultHandler().setIgnore(true);
        } catch (Exception e) {
            throw new ConnectException("Failed to create test Kafka cluster", e);
        }
        this.brokerConfig = brokerConfig;
        this.clientConfigs = clientConfigs;
    }

    public void start() {
        try {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
        } catch (Exception e) {
            throw new ConnectException("Failed to start test Kafka cluster", e);
        }

        Map<String, Object> producerProps = new HashMap<>(clientConfigs);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        if (sslEnabled()) {
            producerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            producerProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        producer = new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());

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
     * {@link ConnectAssertions#assertExactlyNumBrokersAreUp(int, String)} and
     * {@link KafkaClusterTestKit#waitForReadyBrokers()}, which verify that brokers have
     * completed startup and joined the cluster, but do not verify that the internal consumer
     * offsets topic has been created or that it's actually possible for users to create and
     * interact with topics.
     */
    public void verifyClusterReadiness() {
        String consumerGroupId = UUID.randomUUID().toString();
        Map<String, Object> consumerConfig = Collections.singletonMap(GROUP_ID_CONFIG, consumerGroupId);
        String topic = "consumer-warmup-" + consumerGroupId;

        try {
            createTopic(topic);
            produce(topic, "warmup message key", "warmup message value");

            try (Consumer<?, ?> consumer = createConsumerAndSubscribeTo(consumerConfig, topic)) {
                ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(GROUP_COORDINATOR_AVAILABILITY_DURATION_MS));
                if (records.isEmpty()) {
                    throw new AssertionError("Failed to verify availability of group coordinator and/or consume APIs on Kafka cluster in time");
                }
            }
        } catch (Throwable e) {
            fail(
                    "The Kafka cluster used in this test was not able to start successfully in time. "
                            + "If no recent changes have altered the behavior of Kafka brokers or clients, and this error "
                            + "is not occurring frequently, it is probably the result of the testing machine being temporarily "
                            + "overloaded and can be safely ignored.",
                    e
            );
        }

        try (Admin admin = createAdminClient()) {
            admin.deleteConsumerGroups(Collections.singleton(consumerGroupId)).all().get(30, TimeUnit.SECONDS);
            admin.deleteTopics(Collections.singleton(topic)).all().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError("Failed to clean up cluster health check resource(s)", e);
        }
    }

    /**
     * Restarts the Kafka brokers. This can be called after {@link #stopOnlyBrokers()}. Note that if the Kafka brokers
     * need to be listening on the same ports as earlier, the {@link #brokerConfig} should contain the
     * {@link SocketServerConfigs#LISTENERS_CONFIG} property and it should use a fixed non-zero free port. Also note that this is
     * only possible when {@code numBrokers} is 1.
     */
    public void restartOnlyBrokers() {
        cluster.brokers().values().forEach(BrokerServer::startup);
        verifyClusterReadiness();
    }

    /**
     * Stop only the Kafka brokers (and not the KRaft controllers). This can be used to test Connect's functionality
     * when the backing Kafka cluster goes offline.
     */
    public void stopOnlyBrokers() {
        cluster.brokers().values().forEach(BrokerServer::shutdown);
        cluster.brokers().values().forEach(BrokerServer::awaitShutdown);
    }

    public void stop() {
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        Utils.closeQuietly(producer, "producer for embedded Kafka cluster", shutdownFailure);
        Utils.closeQuietly(cluster, "embedded Kafka cluster", shutdownFailure);
        if (shutdownFailure.get() != null) {
            throw new ConnectException("Failed to shut down producer / embedded Kafka cluster", shutdownFailure.get());
        }
    }

    public String bootstrapServers() {
        return cluster.bootstrapServers();
    }

    /**
     * Get the brokers that have a {@link BrokerState#RUNNING} state.
     *
     * @return the set of {@link BrokerServer} instances that are running;
     *         never null but possibly empty
     */
    public Set<BrokerServer> runningBrokers() {
        return brokersInState(BrokerState.RUNNING::equals);
    }

    /**
     * Get the brokers whose state match the given predicate.
     *
     * @return the set of {@link BrokerServer} instances with states that match the predicate;
     *         never null but possibly empty
     */
    public Set<BrokerServer> brokersInState(Predicate<BrokerState> desiredState) {
        return cluster.brokers().values().stream()
                .filter(b -> desiredState.test(b.brokerState()))
                .collect(Collectors.toSet());
    }

    public boolean sslEnabled() {
        final String listenerSecurityProtocolMap = brokerConfig.getProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG);
        if (listenerSecurityProtocolMap == null)
            return false;
        return listenerSecurityProtocolMap.contains(":SSL") || listenerSecurityProtocolMap.contains(":SASL_SSL");
    }

    /**
     * Get the topic descriptions of the named topics. The value of the map entry will be empty
     * if the topic does not exist.
     *
     * @param topicNames the names of the topics to describe
     * @return the map of optional {@link TopicDescription} keyed by the topic name
     */
    public Map<String, Optional<TopicDescription>> describeTopics(String... topicNames) {
        return describeTopics(new HashSet<>(Arrays.asList(topicNames)));
    }

    /**
     * Get the topic descriptions of the named topics. The value of the map entry will be empty
     * if the topic does not exist.
     *
     * @param topicNames the names of the topics to describe
     * @return the map of optional {@link TopicDescription} keyed by the topic name
     */
    public Map<String, Optional<TopicDescription>> describeTopics(Set<String> topicNames) {
        Map<String, Optional<TopicDescription>> results = new HashMap<>();
        log.info("Describing topics {}", topicNames);
        try (Admin admin = createAdminClient()) {
            DescribeTopicsResult result = admin.describeTopics(topicNames);
            Map<String, KafkaFuture<TopicDescription>> byName = result.topicNameValues();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : byName.entrySet()) {
                String topicName = entry.getKey();
                try {
                    TopicDescription desc = entry.getValue().get();
                    results.put(topicName, Optional.of(desc));
                    log.info("Found topic {} : {}", topicName, desc);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof UnknownTopicOrPartitionException) {
                        results.put(topicName, Optional.empty());
                        log.info("Found non-existent topic {}", topicName);
                        continue;
                    }
                    throw new AssertionError("Could not describe topic(s)" + topicNames, e);
                }
            }
        } catch (Exception e) {
            throw new AssertionError("Could not describe topic(s) " + topicNames, e);
        }
        log.info("Found topics {}", results);
        return results;
    }

    /**
     * Update the configuration for the specified resources with the default options.
     *
     * @param configs The resources with their configs (topic is the only resource type with configs that can
     *                be updated currently)
     * @return The AlterConfigsResult
     */
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        AlterConfigsResult result;
        log.info("Altering configs for topics {}", configs.keySet());
        try (Admin admin = createAdminClient()) {
            result = admin.incrementalAlterConfigs(configs);
            log.info("Altered configurations {}", result.all().get());
        } catch (Exception e) {
            throw new AssertionError("Could not alter topic configurations " + configs.keySet(), e);
        }
        return result;
    }

    /**
     * Create a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(String topic) {
        createTopic(topic, 1);
    }

    /**
     * Create a Kafka topic with given partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(String topic, int partitions) {
        createTopic(topic, partitions, 1, Collections.emptyMap());
    }

    /**
     * Create a Kafka topic with given partition, replication factor, and topic config.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(String topic, int partitions, int replication, Map<String, String> topicConfig) {
        createTopic(topic, partitions, replication, topicConfig, Collections.emptyMap());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic             The name of the topic.
     * @param partitions        The number of partitions for this topic.
     * @param replication       The replication factor for (partitions of) this topic.
     * @param topicConfig       Additional topic-level configuration settings.
     * @param adminClientConfig Additional admin client configuration settings.
     */
    public void createTopic(String topic, int partitions, int replication, Map<String, String> topicConfig, Map<String, Object> adminClientConfig) {
        if (replication > cluster.brokers().size()) {
            throw new InvalidReplicationFactorException("Insufficient brokers ("
                    + cluster.brokers().size() + ") for desired replication (" + replication + ")");
        }

        log.info("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
                topic, partitions, replication, topicConfig);
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        newTopic.configs(topicConfig);

        try (final Admin adminClient = createAdminClient(adminClientConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Delete a Kafka topic.
     *
     * @param topic the topic to delete; may not be null
     */
    public void deleteTopic(String topic) {
        try (final Admin adminClient = createAdminClient()) {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void produce(String topic, String value) {
        produce(topic, null, null, value);
    }

    public void produce(String topic, String key, String value) {
        produce(topic, null, key, value);
    }

    public void produce(String topic, Integer partition, String key, String value) {
        ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, partition, key == null ? null : key.getBytes(), value == null ? null : value.getBytes());
        try {
            producer.send(msg).get(DEFAULT_PRODUCE_SEND_DURATION_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new KafkaException("Could not produce message: " + msg, e);
        }
    }

    public Admin createAdminClient(Map<String, Object> adminClientConfig) {
        Properties props = Utils.mkProperties(clientConfigs);
        props.putAll(adminClientConfig);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        if (sslEnabled()) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        return Admin.create(props);
    }

    public Admin createAdminClient() {
        return createAdminClient(Collections.emptyMap());
    }

    /**
     * Consume at least n records in a given duration or throw an exception.
     *
     * @param n the number of expected records in this topic.
     * @param maxDuration the max duration to wait for these records (in milliseconds).
     * @param topics the topics to subscribe and consume records from.
     * @return a {@link ConsumerRecords} collection containing at least n records.
     */
    public ConsumerRecords<byte[], byte[]> consume(int n, long maxDuration, String... topics) {
        return consume(n, maxDuration, Collections.emptyMap(), topics);
    }

    /**
     * Consume at least n records in a given duration or throw an exception.
     *
     * @param n the number of expected records in this topic.
     * @param maxDuration the max duration to wait for these records (in milliseconds).
     * @param topics the topics to subscribe and consume records from.
     * @param consumerProps overrides to the default properties the consumer is constructed with;
     *                      may not be null
     * @return a {@link ConsumerRecords} collection containing at least n records.
     */
    public ConsumerRecords<byte[], byte[]> consume(int n, long maxDuration, Map<String, Object> consumerProps, String... topics) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> nextOffsets = new HashMap<>();
        int consumedRecords = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = createConsumerAndSubscribeTo(consumerProps, topics)) {
            final long startMillis = System.currentTimeMillis();
            long allowedDuration = maxDuration;
            while (allowedDuration > 0) {
                log.debug("Consuming from {} for {} millis.", Arrays.toString(topics), allowedDuration);
                ConsumerRecords<byte[], byte[]> rec = consumer.poll(Duration.ofMillis(allowedDuration));
                if (rec.isEmpty()) {
                    allowedDuration = maxDuration - (System.currentTimeMillis() - startMillis);
                    continue;
                }
                for (TopicPartition partition: rec.partitions()) {
                    final List<ConsumerRecord<byte[], byte[]>> r = rec.records(partition);
                    records.computeIfAbsent(partition, t -> new ArrayList<>()).addAll(r);
                    final ConsumerRecord<byte[], byte[]> lastRecord = r.get(r.size() - 1);
                    nextOffsets.put(partition, new OffsetAndMetadata(lastRecord.offset() + 1, lastRecord.leaderEpoch(), ""));
                    consumedRecords += r.size();
                }
                if (consumedRecords >= n) {
                    return new ConsumerRecords<>(records, nextOffsets);
                }
                allowedDuration = maxDuration - (System.currentTimeMillis() - startMillis);
            }
        }

        throw new RuntimeException("Could not find enough records. found " + consumedRecords + ", expected " + n);
    }

    /**
     * Consume all currently-available records for the specified topics in a given duration, or throw an exception.
     * @param maxDurationMs the max duration to wait for these records (in milliseconds).
     * @param topics the topics to consume from
     * @return a {@link ConsumerRecords} collection containing the records for all partitions of the given topics
     */
    public ConsumerRecords<byte[], byte[]> consumeAll(
        long maxDurationMs,
        String... topics
    ) throws TimeoutException, InterruptedException, ExecutionException {
        return consumeAll(maxDurationMs, null, null, topics);
    }

    /**
     * Consume all currently-available records for the specified topics in a given duration, or throw an exception.
     * @param maxDurationMs the max duration to wait for these records (in milliseconds).
     * @param consumerProps overrides to the default properties the consumer is constructed with; may be null
     * @param adminProps overrides to the default properties the admin used to query Kafka cluster metadata is constructed with; may be null
     * @param topics the topics to consume from
     * @return a {@link ConsumerRecords} collection containing the records for all partitions of the given topics
     */
    public ConsumerRecords<byte[], byte[]> consumeAll(
            long maxDurationMs,
            Map<String, Object> consumerProps,
            Map<String, Object> adminProps,
            String... topics
    ) throws TimeoutException, InterruptedException, ExecutionException {
        long endTimeMs = System.currentTimeMillis() + maxDurationMs;

        long remainingTimeMs;
        Set<TopicPartition> topicPartitions;
        Map<TopicPartition, Long> endOffsets;
        try (Admin admin = createAdminClient(adminProps != null ? adminProps : Collections.emptyMap())) {

            remainingTimeMs = endTimeMs - System.currentTimeMillis();
            topicPartitions = listPartitions(remainingTimeMs, admin, Arrays.asList(topics));

            remainingTimeMs = endTimeMs - System.currentTimeMillis();
            endOffsets = readEndOffsets(remainingTimeMs, admin, topicPartitions);
        }

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = topicPartitions.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        tp -> new ArrayList<>()
                ));
        Map<TopicPartition, OffsetAndMetadata> nextOffsets = new HashMap<>();
        try (Consumer<byte[], byte[]> consumer = createConsumer(consumerProps != null ? consumerProps : Collections.emptyMap())) {
            consumer.assign(topicPartitions);

            while (!endOffsets.isEmpty()) {
                Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<TopicPartition, Long> entry = it.next();
                    TopicPartition topicPartition = entry.getKey();
                    long endOffset = entry.getValue();
                    long lastConsumedOffset = consumer.position(topicPartition);
                    if (lastConsumedOffset >= endOffset) {
                        // We've reached the end offset for the topic partition; can stop polling it now
                        it.remove();
                    } else {
                        remainingTimeMs = endTimeMs - System.currentTimeMillis();
                        if (remainingTimeMs <= 0) {
                            throw new AssertionError("failed to read to end of topic(s) " + Arrays.asList(topics) + " within " + maxDurationMs + "ms");
                        }
                        // We haven't reached the end offset yet; need to keep polling
                        ConsumerRecords<byte[], byte[]> recordBatch = consumer.poll(Duration.ofMillis(remainingTimeMs));
                        recordBatch.partitions().forEach(tp -> records.get(tp)
                                .addAll(recordBatch.records(tp))
                        );
                        nextOffsets.putAll(recordBatch.nextOffsets());
                    }
                }
            }
        }

        return new ConsumerRecords<>(records, nextOffsets);
    }

    public long endOffset(TopicPartition topicPartition) throws TimeoutException, InterruptedException, ExecutionException {
        try (Admin admin = createAdminClient()) {
            Map<TopicPartition, OffsetSpec> offsets = Collections.singletonMap(
                    topicPartition, OffsetSpec.latest()
            );
            return admin.listOffsets(offsets)
                    .partitionResult(topicPartition)
                    // Hardcode duration for now; if necessary, we can add a parameter for it later
                    .get(10, TimeUnit.SECONDS)
                    .offset();
        }
    }

    /**
     * List all the known partitions for the given {@link Collection} of topics
     * @param maxDurationMs the max duration to wait for while fetching metadata from Kafka (in milliseconds).
     * @param admin the admin client to use for fetching metadata from the Kafka cluster
     * @param topics the topics whose partitions should be listed
     * @return a {@link Set} of {@link TopicPartition topic partitions} for the given topics; never null, and never empty
     */
    private Set<TopicPartition> listPartitions(
            long maxDurationMs,
            Admin admin,
            Collection<String> topics
    ) throws TimeoutException, InterruptedException, ExecutionException {
        if (topics.isEmpty()) {
            throw new AssertionError("collection of topics may not be empty");
        }
        return admin.describeTopics(topics)
                .allTopicNames().get(maxDurationMs, TimeUnit.MILLISECONDS)
                .entrySet().stream()
                .flatMap(e -> e.getValue().partitions().stream().map(p -> new TopicPartition(e.getKey(), p.partition())))
                .collect(Collectors.toSet());
    }

    /**
     * List the latest current offsets for the given {@link Collection} of {@link TopicPartition topic partitions}
     * @param maxDurationMs the max duration to wait for while fetching metadata from Kafka (in milliseconds)
     * @param admin the admin client to use for fetching metadata from the Kafka cluster
     * @param topicPartitions the topic partitions to list end offsets for
     * @return a {@link Map} containing the latest offset for each requested {@link TopicPartition topic partition}; never null, and never empty
     */
    private Map<TopicPartition, Long> readEndOffsets(
            long maxDurationMs,
            Admin admin,
            Collection<TopicPartition> topicPartitions
    ) throws TimeoutException, InterruptedException, ExecutionException {
        if (topicPartitions.isEmpty()) {
            throw new AssertionError("collection of topic partitions may not be empty");
        }
        Map<TopicPartition, OffsetSpec> offsetSpecMap = topicPartitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
        return admin.listOffsets(offsetSpecMap, new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED))
                .all().get(maxDurationMs, TimeUnit.MILLISECONDS)
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().offset()
                ));
    }

    public KafkaConsumer<byte[], byte[]> createConsumer(Map<String, Object> consumerProps) {
        Map<String, Object> props = new HashMap<>(clientConfigs);
        props.putAll(consumerProps);

        props.putIfAbsent(GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.putIfAbsent(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putIfAbsent(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.putIfAbsent(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        if (sslEnabled()) {
            props.putIfAbsent(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            props.putIfAbsent(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        KafkaConsumer<byte[], byte[]> consumer;
        try {
            consumer = new KafkaConsumer<>(props);
        } catch (Throwable t) {
            throw new ConnectException("Failed to create consumer", t);
        }
        return consumer;
    }

    public KafkaConsumer<byte[], byte[]> createConsumerAndSubscribeTo(Map<String, Object> consumerProps, String... topics) {
        return createConsumerAndSubscribeTo(consumerProps, null, topics);
    }

    public KafkaConsumer<byte[], byte[]> createConsumerAndSubscribeTo(Map<String, Object> consumerProps, ConsumerRebalanceListener rebalanceListener, String... topics) {
        KafkaConsumer<byte[], byte[]> consumer = createConsumer(consumerProps);
        if (rebalanceListener != null) {
            consumer.subscribe(Arrays.asList(topics), rebalanceListener);
        } else {
            consumer.subscribe(Arrays.asList(topics));
        }
        return consumer;
    }

    public KafkaProducer<byte[], byte[]> createProducer(Map<String, Object> producerProps) {
        Map<String, Object> props = new HashMap<>(clientConfigs);
        props.putAll(producerProps);
        props.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (sslEnabled()) {
            props.putIfAbsent(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            props.putIfAbsent(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        KafkaProducer<byte[], byte[]> producer;
        try {
            producer = new KafkaProducer<>(props);
        } catch (Throwable t) {
            throw new ConnectException("Failed to create producer", t);
        }
        return producer;
    }

    private void addDefaultBrokerPropsIfAbsent(Properties brokerConfig, int numBrokers) {
        brokerConfig.putIfAbsent(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
        brokerConfig.putIfAbsent(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(numBrokers));
        brokerConfig.putIfAbsent(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false");
    }
}
