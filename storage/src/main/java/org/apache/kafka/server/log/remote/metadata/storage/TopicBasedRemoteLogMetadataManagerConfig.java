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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;

/**
 * This class defines the configuration of topic based {@link org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager} implementation.
 */
public final class TopicBasedRemoteLogMetadataManagerConfig {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerConfig.class.getName());

    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP = "remote.log.metadata.topic.replication.factor";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP = "remote.log.metadata.topic.num.partitions";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP = "remote.log.metadata.topic.retention.ms";
    public static final String REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP = "remote.log.metadata.publish.wait.ms";
    public static final String REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS_PROP = "remote.log.metadata.secondary.consumer.subscription.interval.ms";

    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS = 50;
    public static final long DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS = -1L;
    public static final short DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR = 3;
    public static final long DEFAULT_REMOTE_LOG_METADATA_CONSUME_WAIT_MS = 120 * 1000L;
    public static final long DEFAULT_REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS = 30 * 1000L;

    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor of remote log metadata Topic.";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_DOC = "The number of partitions for remote log metadata Topic.";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_DOC = "Remote log metadata topic log retention in milli seconds." +
            "Default: -1, that means unlimited. Users can configure this value based on their use cases. " +
            "To avoid any data loss, this value should be more than the maximum retention period of any topic enabled with " +
            "tiered storage in the cluster.";
    public static final String REMOTE_LOG_METADATA_CONSUME_WAIT_MS_DOC = "The amount of time in milli seconds to wait for the local consumer to " +
            "receive the published event.";
    public static final String REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS_DOC = "The interval amount of time in milli seconds " +
            "to subscribe with the updated subscriptions by the secondary consumer.";

    public static final String REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX = "remote.log.metadata.common.client.";
    public static final String REMOTE_LOG_METADATA_PRODUCER_PREFIX = "remote.log.metadata.producer.";
    public static final String REMOTE_LOG_METADATA_CONSUMER_PREFIX = "remote.log.metadata.consumer.";
    public static final String BROKER_ID = "broker.id";
    public static final String LOG_DIR = "log.dir";
    public static final String LOG_DIRS = "log.dirs";

    public static final String REMOTE_LOG_METADATA_TOPIC_NAME = "__remote_log_metadata";
    public static final String REMOTE_LOG_METADATA_CLIENT_PREFIX = "__remote_log_metadata_client";

    private static final ConfigDef CONFIG = new ConfigDef();

    static {
        CONFIG.define(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP,
                      SHORT,
                      DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR,
                      atLeast(1),
                      LOW,
                      REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_DOC)
                .define(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS,
                        atLeast(1),
                        LOW,
                        REMOTE_LOG_METADATA_TOPIC_PARTITIONS_DOC)
                .define(REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS,
                        LOW,
                        REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_DOC)
                .define(REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_METADATA_CONSUME_WAIT_MS,
                        atLeast(0),
                        LOW,
                        REMOTE_LOG_METADATA_CONSUME_WAIT_MS_DOC)
                .define(REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS,
                        atLeast(1),
                        LOW,
                        REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS_DOC);
    }

    private final String clientIdPrefix;
    private final int metadataTopicPartitionsCount;
    private final String bootstrapServers;
    private final String logDir;
    private final long consumeWaitMs;
    private final long metadataTopicRetentionMs;
    private final short metadataTopicReplicationFactor;
    private final long secondaryConsumerSubscriptionIntervalMs;

    private Map<String, Object> consumerProps;
    private Map<String, Object> producerProps;

    public TopicBasedRemoteLogMetadataManagerConfig(Map<String, ?> props) {
        log.info("Received props: [{}]", props);
        Objects.requireNonNull(props, "props can not be null");

        Map<String, Object> parsedConfigs = CONFIG.parse(props);

        bootstrapServers = (String) props.get(BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException(BOOTSTRAP_SERVERS_CONFIG + " config must not be null or empty.");
        }

        logDir = getLogDirectory(props);

        consumeWaitMs = (long) parsedConfigs.get(REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP);
        secondaryConsumerSubscriptionIntervalMs = (long) parsedConfigs.get(REMOTE_LOG_METADATA_SECONDARY_CONSUMER_SUBSCRIPTION_INTERVAL_MS_PROP);
        metadataTopicPartitionsCount = (int) parsedConfigs.get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP);
        metadataTopicReplicationFactor = (short) parsedConfigs.get(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP);
        metadataTopicRetentionMs = (long) parsedConfigs.get(REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP);
        if (metadataTopicRetentionMs != -1 && metadataTopicRetentionMs <= 0) {
            throw new IllegalArgumentException("Invalid metadata topic retention in millis: " + metadataTopicRetentionMs);
        }

        clientIdPrefix = REMOTE_LOG_METADATA_CLIENT_PREFIX + "_" + props.get(BROKER_ID);

        initializeProducerConsumerProperties(props);
    }

    private void initializeProducerConsumerProperties(Map<String, ?> configs) {
        Map<String, Object> commonClientConfigs = new HashMap<>();
        commonClientConfigs.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        Map<String, Object> producerOnlyConfigs = new HashMap<>();
        Map<String, Object> consumerOnlyConfigs = new HashMap<>();

        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX)) {
                commonClientConfigs.put(key.substring(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX.length()), entry.getValue());
            } else if (key.startsWith(REMOTE_LOG_METADATA_PRODUCER_PREFIX)) {
                producerOnlyConfigs.put(key.substring(REMOTE_LOG_METADATA_PRODUCER_PREFIX.length()), entry.getValue());
            } else if (key.startsWith(REMOTE_LOG_METADATA_CONSUMER_PREFIX)) {
                consumerOnlyConfigs.put(key.substring(REMOTE_LOG_METADATA_CONSUMER_PREFIX.length()), entry.getValue());
            }
        }

        Map<String, Object> allProducerConfigs = new HashMap<>(commonClientConfigs);
        allProducerConfigs.putAll(producerOnlyConfigs);
        producerProps = createProducerProps(allProducerConfigs);

        Map<String, Object> allConsumerConfigs = new HashMap<>(commonClientConfigs);
        allConsumerConfigs.putAll(consumerOnlyConfigs);
        consumerProps = createConsumerProps(allConsumerConfigs);
    }

    public String remoteLogMetadataTopicName() {
        return REMOTE_LOG_METADATA_TOPIC_NAME;
    }

    public int metadataTopicPartitionsCount() {
        return metadataTopicPartitionsCount;
    }

    public short metadataTopicReplicationFactor() {
        return metadataTopicReplicationFactor;
    }

    public long metadataTopicRetentionMs() {
        return metadataTopicRetentionMs;
    }

    public long consumeWaitMs() {
        return consumeWaitMs;
    }

    public String logDir() {
        return logDir;
    }

    public long secondaryConsumerSubscriptionIntervalMs() {
        return secondaryConsumerSubscriptionIntervalMs;
    }

    public Map<String, Object> consumerProperties() {
        return consumerProps;
    }

    public Map<String, Object> producerProperties() {
        return producerProps;
    }

    private Map<String, Object> createConsumerProps(Map<String, Object> allConsumerConfigs) {
        Map<String, Object> props = new HashMap<>(allConsumerConfigs);

        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientIdPrefix + "_consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    private Map<String, Object> createProducerProps(Map<String, Object> allProducerConfigs) {
        Map<String, Object> props = new HashMap<>(allProducerConfigs);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "_producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return Collections.unmodifiableMap(props);
    }

    @Override
    public String toString() {
        return "TopicBasedRemoteLogMetadataManagerConfig{" +
                "clientIdPrefix='" + clientIdPrefix + '\'' +
                ", metadataTopicPartitionsCount=" + metadataTopicPartitionsCount +
                ", bootstrapServers='" + bootstrapServers + '\'' +
                ", consumeWaitMs=" + consumeWaitMs +
                ", secondaryConsumerSubscriptionIntervalMs=" + secondaryConsumerSubscriptionIntervalMs +
                ", metadataTopicRetentionMillis=" + metadataTopicRetentionMs +
                ", consumerProps=" + consumerProps +
                ", producerProps=" + producerProps +
                '}';
    }

    static String getLogDirectory(Map<String, ?> props) {
        String logDirs = (String) props.get(LOG_DIRS);
        if (logDirs == null || logDirs.isEmpty()) {
            logDirs = (String) props.get(LOG_DIR);
        }
        if (logDirs == null || logDirs.isEmpty()) {
            throw new IllegalArgumentException("At least one log directory must be defined via log.dirs or log.dir.");
        }
        final List<String> dirs = Arrays.stream(logDirs.split("\\s*,\\s*"))
                .filter(v -> !v.trim().isEmpty())
                .collect(Collectors.toList());
        if (dirs.isEmpty()) {
            throw new IllegalArgumentException("At least one log directory must be defined via log.dirs or log.dir.");
        }
        if (dirs.size() > 1) {
            throw new IllegalArgumentException("Multiple log directories are not supported when remote log storage is enabled");
        }
        return dirs.get(0);
    }
}
