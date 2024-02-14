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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;

/**
 * This class defines the configuration of topic based {@link org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager} implementation.
 */
public final class TopicBasedRemoteLogMetadataManagerConfig {

    public static final String REMOTE_LOG_METADATA_TOPIC_NAME = "__remote_log_metadata";

    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP = "remote.log.metadata.topic.replication.factor";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP = "remote.log.metadata.topic.num.partitions";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP = "remote.log.metadata.topic.retention.ms";
    public static final String REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP = "remote.log.metadata.consume.wait.ms";
    public static final String REMOTE_LOG_METADATA_INITIALIZATION_RETRY_MAX_TIMEOUT_MS_PROP = "remote.log.metadata.initialization.retry.max.timeout.ms";
    public static final String REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS_PROP = "remote.log.metadata.initialization.retry.interval.ms";

    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS = 50;
    public static final long DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MS = -1L;
    public static final short DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR = 3;
    public static final long DEFAULT_REMOTE_LOG_METADATA_CONSUME_WAIT_MS = 2 * 60 * 1000L;
    public static final long DEFAULT_REMOTE_LOG_METADATA_INITIALIZATION_RETRY_MAX_TIMEOUT_MS = 2 * 60 * 1000L;
    public static final long DEFAULT_REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS = 100L;

    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor of remote log metadata topic.";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_DOC = "The number of partitions for remote log metadata topic.";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_DOC = "Retention of remote log metadata topic in milliseconds. " +
            "Default: -1, that means unlimited. Users can configure this value based on their use cases. " +
            "To avoid any data loss, this value should be more than the maximum retention period of any topic enabled with " +
            "tiered storage in the cluster.";
    public static final String REMOTE_LOG_METADATA_CONSUME_WAIT_MS_DOC = "The amount of time in milliseconds to wait for the local consumer to " +
            "receive the published event.";
    public static final String REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS_DOC = "The retry interval in milliseconds for " +
            "retrying RemoteLogMetadataManager resources initialization again.";

    public static final String REMOTE_LOG_METADATA_INITIALIZATION_RETRY_MAX_TIMEOUT_MS_DOC = "The maximum amount of time in milliseconds " +
            "for retrying RemoteLogMetadataManager resources initialization. When total retry intervals reach this timeout, initialization " +
            "is considered as failed and broker starts shutting down.";

    public static final String REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX = "remote.log.metadata.common.client.";
    public static final String REMOTE_LOG_METADATA_PRODUCER_PREFIX = "remote.log.metadata.producer.";
    public static final String REMOTE_LOG_METADATA_CONSUMER_PREFIX = "remote.log.metadata.consumer.";
    public static final String BROKER_ID = "broker.id";
    public static final String LOG_DIR = "log.dir";

    private static final String REMOTE_LOG_METADATA_CLIENT_PREFIX = "__remote_log_metadata_client";

    private static final ConfigDef CONFIG = new ConfigDef();
    static {
        CONFIG.define(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, SHORT, DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR, atLeast(1), LOW,
                      REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_DOC)
              .define(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, INT, DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS, atLeast(1), LOW,
                      REMOTE_LOG_METADATA_TOPIC_PARTITIONS_DOC)
              .define(REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP, LONG, DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MS, LOW,
                      REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_DOC)
              .define(REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP, LONG, DEFAULT_REMOTE_LOG_METADATA_CONSUME_WAIT_MS, atLeast(0), LOW,
                      REMOTE_LOG_METADATA_CONSUME_WAIT_MS_DOC)
              .define(REMOTE_LOG_METADATA_INITIALIZATION_RETRY_MAX_TIMEOUT_MS_PROP, LONG,
                      DEFAULT_REMOTE_LOG_METADATA_INITIALIZATION_RETRY_MAX_TIMEOUT_MS, atLeast(0), LOW,
                      REMOTE_LOG_METADATA_INITIALIZATION_RETRY_MAX_TIMEOUT_MS_DOC)
              .define(REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS_PROP, LONG,
                      DEFAULT_REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS, atLeast(0), LOW,
                      REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS_DOC);
    }

    private final String clientIdPrefix;
    private final int metadataTopicPartitionsCount;
    private final String logDir;
    private final long consumeWaitMs;
    private final long metadataTopicRetentionMs;
    private final short metadataTopicReplicationFactor;
    private final long initializationRetryMaxTimeoutMs;
    private final long initializationRetryIntervalMs;

    private Map<String, Object> commonProps;
    private Map<String, Object> consumerProps;
    private Map<String, Object> producerProps;

    public TopicBasedRemoteLogMetadataManagerConfig(Map<String, ?> props) {
        Objects.requireNonNull(props, "props can not be null");
        Map<String, Object> parsedConfigs = CONFIG.parse(props);
        logDir = (String) props.get(LOG_DIR);
        if (logDir == null || logDir.isEmpty()) {
            throw new IllegalArgumentException(LOG_DIR + " config must not be null or empty.");
        }
        metadataTopicPartitionsCount = (int) parsedConfigs.get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP);
        metadataTopicReplicationFactor = (short) parsedConfigs.get(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP);
        metadataTopicRetentionMs = (long) parsedConfigs.get(REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP);
        if (metadataTopicRetentionMs != -1 && metadataTopicRetentionMs <= 0) {
            throw new IllegalArgumentException("Invalid metadata topic retention in millis: " + metadataTopicRetentionMs);
        }
        consumeWaitMs = (long) parsedConfigs.get(REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP);
        initializationRetryIntervalMs = (long) parsedConfigs.get(REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS_PROP);
        initializationRetryMaxTimeoutMs = (long) parsedConfigs.get(REMOTE_LOG_METADATA_INITIALIZATION_RETRY_MAX_TIMEOUT_MS_PROP);
        clientIdPrefix = REMOTE_LOG_METADATA_CLIENT_PREFIX + "_" + props.get(BROKER_ID);
        initializeProducerConsumerProperties(props);
    }

    private void initializeProducerConsumerProperties(Map<String, ?> configs) {
        Map<String, Object> commonClientConfigs = new HashMap<>();
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
        commonProps = new HashMap<>(commonClientConfigs);
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

    public long initializationRetryMaxTimeoutMs() {
        return initializationRetryMaxTimeoutMs;
    }

    public long initializationRetryIntervalMs() {
        return initializationRetryIntervalMs;
    }

    public String logDir() {
        return logDir;
    }

    public Map<String, Object> commonProperties() {
        return commonProps;
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
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return Collections.unmodifiableMap(props);
    }

    @Override
    public String toString() {
        return "TopicBasedRemoteLogMetadataManagerConfig{" +
                "clientIdPrefix='" + clientIdPrefix + '\'' +
                ", metadataTopicPartitionsCount=" + metadataTopicPartitionsCount +
                ", consumeWaitMs=" + consumeWaitMs +
                ", metadataTopicRetentionMs=" + metadataTopicRetentionMs +
                ", metadataTopicReplicationFactor=" + metadataTopicReplicationFactor +
                ", initializationRetryMaxTimeoutMs=" + initializationRetryMaxTimeoutMs +
                ", initializationRetryIntervalMs=" + initializationRetryIntervalMs +
                ", commonProps=" + commonProps +
                ", consumerProps=" + consumerProps +
                ", producerProps=" + producerProps +
                '}';
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "remote_log_metadata_manager_" + config));
    }
}
