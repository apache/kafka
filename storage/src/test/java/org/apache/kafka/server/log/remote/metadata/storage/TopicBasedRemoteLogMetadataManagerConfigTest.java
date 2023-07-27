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
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.BROKER_ID;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_CONSUMER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_PRODUCER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP;

public class TopicBasedRemoteLogMetadataManagerConfigTest {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerConfigTest.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9091";

    @Test
    public void testValidConfig() {
        Map<String, Object> commonClientConfig = new HashMap<>();
        commonClientConfig.put(CommonClientConfigs.RETRIES_CONFIG, 10);
        commonClientConfig.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, 1000L);
        commonClientConfig.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 60000L);

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Map<String, Object> props = createValidConfigProps(commonClientConfig, producerConfig, consumerConfig);

        // Check for topic properties
        TopicBasedRemoteLogMetadataManagerConfig rlmmConfig = new TopicBasedRemoteLogMetadataManagerConfig(props);
        Assertions.assertEquals(props.get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP), rlmmConfig.metadataTopicPartitionsCount());

        // Check for common client configs.
        Assertions.assertEquals(BOOTSTRAP_SERVERS, rlmmConfig.commonProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals(BOOTSTRAP_SERVERS, rlmmConfig.producerProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals(BOOTSTRAP_SERVERS, rlmmConfig.consumerProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));

        for (Map.Entry<String, Object> entry : commonClientConfig.entrySet()) {
            log.info("Checking config: " + entry.getKey());
            Assertions.assertEquals(entry.getValue(),
                                    rlmmConfig.commonProperties().get(entry.getKey()));
            Assertions.assertEquals(entry.getValue(),
                                    rlmmConfig.producerProperties().get(entry.getKey()));
            Assertions.assertEquals(entry.getValue(),
                                    rlmmConfig.consumerProperties().get(entry.getKey()));
        }

        // Check for producer configs.
        for (Map.Entry<String, Object> entry : producerConfig.entrySet()) {
            log.info("Checking config: " + entry.getKey());
            Assertions.assertEquals(entry.getValue(),
                                    rlmmConfig.producerProperties().get(entry.getKey()));
        }

        // Check for consumer configs.
        for (Map.Entry<String, Object> entry : consumerConfig.entrySet()) {
            log.info("Checking config: " + entry.getKey());
            Assertions.assertEquals(entry.getValue(),
                                    rlmmConfig.consumerProperties().get(entry.getKey()));
        }
    }

    @Test
    public void testCommonProducerConsumerOverridesConfig() {
        Map.Entry<String, Long> overrideEntry = new AbstractMap.SimpleImmutableEntry<>(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 60000L);
        Map<String, Object> commonClientConfig = new HashMap<>();
        commonClientConfig.put(CommonClientConfigs.RETRIES_CONFIG, 10);
        commonClientConfig.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, 1000L);
        Long overrideCommonPropValue = overrideEntry.getValue();
        commonClientConfig.put(overrideEntry.getKey(), overrideCommonPropValue);

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, -1);
        Long overriddenProducerPropValue = overrideEntry.getValue() * 2;
        producerConfig.put(overrideEntry.getKey(), overriddenProducerPropValue);

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Long overriddenConsumerPropValue = overrideEntry.getValue() * 3;
        consumerConfig.put(overrideEntry.getKey(), overriddenConsumerPropValue);

        Map<String, Object> props = createValidConfigProps(commonClientConfig, producerConfig, consumerConfig);
        TopicBasedRemoteLogMetadataManagerConfig rlmmConfig = new TopicBasedRemoteLogMetadataManagerConfig(props);

        Assertions.assertEquals(overrideCommonPropValue,
                                rlmmConfig.commonProperties().get(overrideEntry.getKey()));
        Assertions.assertEquals(overriddenProducerPropValue,
                                rlmmConfig.producerProperties().get(overrideEntry.getKey()));
        Assertions.assertEquals(overriddenConsumerPropValue,
                                rlmmConfig.consumerProperties().get(overrideEntry.getKey()));
    }

    private Map<String, Object> createValidConfigProps(Map<String, Object> commonClientConfig,
                                                       Map<String, Object> producerConfig,
                                                       Map<String, Object> consumerConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(BROKER_ID, 1);
        props.put(LOG_DIR, TestUtils.tempDirectory().getAbsolutePath());

        props.put(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, (short) 3);
        props.put(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, 10);
        props.put(REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP, 60 * 60 * 1000L);

        // common client configs
        for (Map.Entry<String, Object> entry : commonClientConfig.entrySet()) {
            props.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + entry.getKey(), entry.getValue());
        }

        // producer configs
        for (Map.Entry<String, Object> entry : producerConfig.entrySet()) {
            props.put(REMOTE_LOG_METADATA_PRODUCER_PREFIX + entry.getKey(), entry.getValue());
        }

        //consumer configs
        for (Map.Entry<String, Object> entry : consumerConfig.entrySet()) {
            props.put(REMOTE_LOG_METADATA_CONSUMER_PREFIX + entry.getKey(), entry.getValue());
        }

        return props;
    }
}