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

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Setup an embedded Kafka cluster with specified number of brokers and specified broker properties. To be used for
 * integration tests.
 */
public class EmbeddedKafkaCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);

    private static final long DEFAULT_PRODUCE_SEND_DURATION_MS = TimeUnit.SECONDS.toMillis(120); 

    // Kafka Config
    private final KafkaServer[] brokers;
    private final Properties brokerConfig;
    private final Time time = new MockTime();

    private EmbeddedZookeeper zookeeper = null;
    private ListenerName listenerName = new ListenerName("PLAINTEXT");
    private KafkaProducer<byte[], byte[]> producer;

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig) {
        brokers = new KafkaServer[numBrokers];
        this.brokerConfig = brokerConfig;
    }

    @Override
    protected void before() throws IOException {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    private void start() throws IOException {
        zookeeper = new EmbeddedZookeeper();

        brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), 0); // pick a random port

        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.HostNameProp(), "localhost");
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) brokers.length);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), false);

        Object listenerConfig = brokerConfig.get(KafkaConfig$.MODULE$.InterBrokerListenerNameProp());
        if (listenerConfig != null) {
            listenerName = new ListenerName(listenerConfig.toString());
        }

        for (int i = 0; i < brokers.length; i++) {
            brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), i);
            brokerConfig.put(KafkaConfig$.MODULE$.LogDirProp(), createLogDir());
            brokers[i] = TestUtils.createServer(new KafkaConfig(brokerConfig, true), time);
        }

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);
    }

    private void stop() {

        try {
            producer.close();
        } catch (Exception e) {
            log.error("Could not shutdown producer ", e);
            throw new RuntimeException("Could not shutdown producer", e);
        }

        for (KafkaServer broker : brokers) {
            try {
                broker.shutdown();
            } catch (Throwable t) {
                String msg = String.format("Could not shutdown broker at %s", address(broker));
                log.error(msg, t);
                throw new RuntimeException(msg, t);
            }
        }

        for (KafkaServer broker : brokers) {
            try {
                log.info("Cleaning up kafka log dirs at {}", broker.config().logDirs());
                CoreUtils.delete(broker.config().logDirs());
            } catch (Throwable t) {
                String msg = String.format("Could not clean up log dirs for broker at %s", address(broker));
                log.error(msg, t);
                throw new RuntimeException(msg, t);
            }
        }

        try {
            zookeeper.shutdown();
        } catch (Throwable t) {
            String msg = String.format("Could not shutdown zookeeper at %s", zKConnectString());
            log.error(msg, t);
            throw new RuntimeException(msg, t);
        }
    }

    private static void putIfAbsent(final Properties props, final String propertyKey, final Object propertyValue) {
        if (!props.containsKey(propertyKey)) {
            props.put(propertyKey, propertyValue);
        }
    }

    private String createLogDir() throws IOException {
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        return tmpFolder.newFolder().getAbsolutePath();
    }

    public String bootstrapServers() {
        return Arrays.stream(brokers)
                .map(this::address)
                .collect(Collectors.joining(","));
    }

    public String address(KafkaServer server) {
        return server.config().hostName() + ":" + server.boundPort(listenerName);
    }

    public String zKConnectString() {
        return "127.0.0.1:" + zookeeper.port();
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
        createTopic(topic, partitions, 1, new HashMap<>());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    public void createTopic(String topic, int partitions, int replication, Map<String, String> topicConfig) {
        if (replication > brokers.length) {
            throw new InvalidReplicationFactorException("Insufficient brokers ("
                    + brokers.length + ") for desired replication (" + replication + ")");
        }

        log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
                topic, partitions, replication, topicConfig);
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        newTopic.configs(topicConfig);

        try (final AdminClient adminClient = createAdminClient()) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
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

    public AdminClient createAdminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        final Object listeners = brokerConfig.get(KafkaConfig$.MODULE$.ListenersProp());
        if (listeners != null && listeners.toString().contains("SSL")) {
            adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) brokerConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
            adminClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        return AdminClient.create(adminClientConfig);
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
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        int consumedRecords = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = createConsumerAndSubscribeTo(Collections.emptyMap(), topics)) {
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
                    consumedRecords += r.size();
                }
                if (consumedRecords >= n) {
                    return new ConsumerRecords<>(records);
                }
                allowedDuration = maxDuration - (System.currentTimeMillis() - startMillis);
            }
        }

        throw new RuntimeException("Could not find enough records. found " + consumedRecords + ", expected " + n);
    }

    public KafkaConsumer<byte[], byte[]> createConsumer(Map<String, Object> consumerProps) {
        Map<String, Object> props = new HashMap<>(consumerProps);

        putIfAbsent(props, GROUP_ID_CONFIG, UUID.randomUUID().toString());
        putIfAbsent(props, BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        putIfAbsent(props, ENABLE_AUTO_COMMIT_CONFIG, "false");
        putIfAbsent(props, AUTO_OFFSET_RESET_CONFIG, "earliest");
        putIfAbsent(props, KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        putIfAbsent(props, VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<byte[], byte[]> consumer;
        try {
            consumer = new KafkaConsumer<>(props);
        } catch (Throwable t) {
            throw new ConnectException("Failed to create consumer", t);
        }
        return consumer;
    }

    public KafkaConsumer<byte[], byte[]> createConsumerAndSubscribeTo(Map<String, Object> consumerProps, String... topics) {
        KafkaConsumer<byte[], byte[]> consumer = createConsumer(consumerProps);
        consumer.subscribe(Arrays.asList(topics));
        return consumer;
    }

    private static void putIfAbsent(final Map<String, Object> props, final String propertyKey, final Object propertyValue) {
        if (!props.containsKey(propertyKey)) {
            props.put(propertyKey, propertyValue);
        }
    }
}
