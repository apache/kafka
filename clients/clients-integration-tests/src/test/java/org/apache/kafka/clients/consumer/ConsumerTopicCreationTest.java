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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.common.test.api.Type.KRAFT;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerTopicCreationTest {
    private static final String TOPIC = "topic";
    private static final long POLL_TIMEOUT = 1000;

    @ClusterTemplate("autoCreateTopicsConfigs")
    void testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic(ClusterInstance cluster) {
        try (Consumer<byte[], byte[]> consumer = createConsumer(cluster, GroupProtocol.CONSUMER, true)) {
            consumer.subscribe(List.of(TOPIC));
            consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
            if (allowAutoCreateTopics(cluster))
                assertTrue(getAllTopics(cluster).contains(TOPIC));
            else
                assertFalse(getAllTopics(cluster).contains(TOPIC),
                    "Both " + AUTO_CREATE_TOPICS_ENABLE_CONFIG + " and " + ALLOW_AUTO_CREATE_TOPICS_CONFIG + " need to be true to create topic automatically");
        }
    }

    @ClusterTemplate("autoCreateTopicsConfigs")
    void testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic(ClusterInstance cluster) {
        try (Consumer<byte[], byte[]> consumer = createConsumer(cluster, GroupProtocol.CONSUMER, false)) {
            consumer.subscribe(List.of(TOPIC));
            consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
            assertFalse(getAllTopics(cluster).contains(TOPIC),
                "Both " + AUTO_CREATE_TOPICS_ENABLE_CONFIG + " and " + ALLOW_AUTO_CREATE_TOPICS_CONFIG + " need to be true to create topic automatically");
        }
    }

    @ClusterTemplate("autoCreateTopicsConfigs")
    void testClassicConsumerTopicCreationIfConsumerAllowToCreateTopic(ClusterInstance cluster) {
        try (Consumer<byte[], byte[]> consumer = createConsumer(cluster, GroupProtocol.CLASSIC, true)) {
            consumer.subscribe(List.of(TOPIC));
            consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
            if (allowAutoCreateTopics(cluster))
                assertTrue(getAllTopics(cluster).contains(TOPIC));
            else
                assertFalse(getAllTopics(cluster).contains(TOPIC),
                    "Both " + AUTO_CREATE_TOPICS_ENABLE_CONFIG + " and " + ALLOW_AUTO_CREATE_TOPICS_CONFIG + " need to be true to create topic automatically");
        }
    }

    @ClusterTemplate("autoCreateTopicsConfigs")
    void testClassicConsumerTopicCreationIfConsumerDisallowToCreateTopic(ClusterInstance cluster) {
        try (Consumer<byte[], byte[]> consumer = createConsumer(cluster, GroupProtocol.CLASSIC, false)) {
            consumer.subscribe(List.of(TOPIC));
            consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
            assertFalse(getAllTopics(cluster).contains(TOPIC),
                "Both " + AUTO_CREATE_TOPICS_ENABLE_CONFIG + " and " + ALLOW_AUTO_CREATE_TOPICS_CONFIG + " need to be true to create topic automatically");
        }
    }

    private boolean allowAutoCreateTopics(ClusterInstance cluster) {
        return cluster.config().serverProperties().get(AUTO_CREATE_TOPICS_ENABLE_CONFIG).equals("true");
    }

    private Set<String> getAllTopics(ClusterInstance cluster) {
        return cluster.brokers().values().iterator().next().metadataCache().getAllTopics();
    }

    private static List<ClusterConfig> autoCreateTopicsConfigs() {
        return List.of(
            ClusterConfig.defaultBuilder()
                .setTypes(Set.of(KRAFT))
                .setServerProperties(Map.of(AUTO_CREATE_TOPICS_ENABLE_CONFIG, "true"))
                .build(),
            ClusterConfig.defaultBuilder()
                .setTypes(Set.of(KRAFT))
                .setServerProperties(Map.of(AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false"))
                .build()
        );
    }

    private Consumer<byte[], byte[]> createConsumer(ClusterInstance cluster, GroupProtocol protocol, boolean allowConsumerAutoCreateTopics) {
        Map<String, Object> consumerConfig = Map.of(
            CLIENT_ID_CONFIG, "ConsumerTestConsumer",
            ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowConsumerAutoCreateTopics,
            GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT)
        );
        return cluster.consumer(consumerConfig);
    }
}
