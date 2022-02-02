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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.BROKER_ID;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIRS;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_CONSUMER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_PRODUCER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.getLogDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TopicBasedRemoteLogMetadataManagerConfigTest {
    private static final  Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerConfigTest.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9091";

    @Test
    public void testEmptyConfig() {
        // "bootstrap.servers" config is required, it will throw IllegalArgumentException if it does not exist.
        assertThrows(IllegalArgumentException.class, () -> new TopicBasedRemoteLogMetadataManagerConfig(Collections.emptyMap()));
    }

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
        assertEquals(props.get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP), rlmmConfig.metadataTopicPartitionsCount());

        // Check for common client configs.
        for (Map.Entry<String, Object> entry : commonClientConfig.entrySet()) {
            log.info("Checking config: " + entry.getKey());
            assertEquals(entry.getValue(),
                                    rlmmConfig.producerProperties().get(entry.getKey()));
            assertEquals(entry.getValue(),
                                    rlmmConfig.consumerProperties().get(entry.getKey()));
        }

        // Check for producer configs.
        for (Map.Entry<String, Object> entry : producerConfig.entrySet()) {
            log.info("Checking config: " + entry.getKey());
            assertEquals(entry.getValue(),
                                    rlmmConfig.producerProperties().get(entry.getKey()));
        }

        // Check for consumer configs.
        for (Map.Entry<String, Object> entry : consumerConfig.entrySet()) {
            log.info("Checking config: " + entry.getKey());
            assertEquals(entry.getValue(),
                                    rlmmConfig.consumerProperties().get(entry.getKey()));
        }
    }

    @Test
    public void testProducerConsumerOverridesConfig() {
        Map.Entry<String, Long> overrideEntry = new AbstractMap.SimpleImmutableEntry<>(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 60000L);
        Map<String, Object> commonClientConfig = new HashMap<>();
        commonClientConfig.put(CommonClientConfigs.RETRIES_CONFIG, 10);
        commonClientConfig.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, 1000L);
        commonClientConfig.put(overrideEntry.getKey(), overrideEntry.getValue());

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

        assertEquals(overriddenProducerPropValue,
                                rlmmConfig.producerProperties().get(overrideEntry.getKey()));
        assertEquals(overriddenConsumerPropValue,
                                rlmmConfig.consumerProperties().get(overrideEntry.getKey()));
    }

    private Map<String, Object> createValidConfigProps(Map<String, Object> commonClientConfig,
                                                       Map<String, Object> producerConfig,
                                                       Map<String, Object> consumerConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(BROKER_ID, 1);
        props.put(LOG_DIR, TestUtils.tempDirectory().getAbsolutePath());

        props.put(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, (short) 3);
        props.put(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, 10);
        props.put(REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP, 60 * 60 * 1000L);

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

    @Test
    public void shouldThrowErrorWhenNoLogDirectoryIsConfigured() {
        Throwable throwable =
                assertThrows(IllegalArgumentException.class, () -> getLogDirectory(Collections.emptyMap()));
        assertEquals("At least one log directory must be defined via log.dirs or log.dir.",
                throwable.getMessage());

        Map<String, Object> props = Collections.singletonMap(LOG_DIRS, "  , ");
        throwable = assertThrows(IllegalArgumentException.class, () -> getLogDirectory(props));
        assertEquals("At least one log directory must be defined via log.dirs or log.dir.",
                throwable.getMessage());
    }

    @Test
    public void shouldThrowErrorWhenMultipleLogDirectoriesAreConfigured() {
        Map<String, Object> props = Collections.singletonMap(LOG_DIRS, "/tmp/a,/tmp/b");
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> getLogDirectory(props));
        assertEquals("Multiple log directories are not supported when remote log storage is enabled",
                throwable.getMessage());
    }

    @Test
    public void testGetLogDirectory() {
        Map<String, Object> props = new HashMap<>();
        props.put(LOG_DIR, "/tmp/a");
        assertEquals("/tmp/a", getLogDirectory(props));

        props.put(LOG_DIRS, "/tmp/b");
        assertEquals("/tmp/b", getLogDirectory(props));

        props.remove(LOG_DIRS);
        assertEquals("/tmp/a", getLogDirectory(props));
    }
}