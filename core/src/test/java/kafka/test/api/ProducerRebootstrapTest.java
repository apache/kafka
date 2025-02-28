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

package kafka.test.api;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProducerRebootstrapTest {
    private static final int BROKER_COUNT = 2;

    static List<ClusterConfig> generator() {
        Map<String, String> serverProperties = new HashMap<>();
        // Enable unclean leader election for the test topic
        serverProperties.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true");
        serverProperties.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(BROKER_COUNT));

        return Stream.of(false, true)
                .map(ProducerRebootstrapTest::getRebootstrapConfig)
                .map(rebootstrapProperties -> ProducerRebootstrapTest.buildConfig(serverProperties, rebootstrapProperties))
                .toList();
    }

    static Map<String, String> getRebootstrapConfig(boolean useRebootstrapTriggerMs) {
        Map<String, String> properties = new HashMap<>();
        if (useRebootstrapTriggerMs) {
            properties.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "5000");
        } else {
            properties.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "3600000");
            properties.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "5000");
            properties.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, "5000");
            properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, "1000");
            properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        }
        properties.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "rebootstrap");
        properties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1");
        return properties;
    }

    static ClusterConfig buildConfig(Map<String, String> serverProperties, Map<String, String> rebootstrapProperties) {
        return ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(BROKER_COUNT)
                .setProducerProperties(rebootstrapProperties)
                .setServerProperties(serverProperties).build();
    }

    @ClusterTemplate(value = "generator")
    public void testRebootstrap(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        var topic = "topic";
        try (var admin = clusterInstance.admin()) {
            admin.createTopics(List.of(new NewTopic(topic, BROKER_COUNT, (short) 2)));
        }

        var part = 0;
        var server0 = clusterInstance.brokers().get(0);
        var server1 = clusterInstance.brokers().get(1);

        // It's ok to shut the leader down, cause the reelection is small enough to the producer timeout.
        server1.shutdown();
        server1.awaitShutdown();

        try (var producer = clusterInstance.producer()) {
            // Only the server 0 is available for the producer during the bootstrap.
            var recordMetadata0 = producer.send(new ProducerRecord<>(topic, part, "key 1".getBytes(), "value 1".getBytes())).get();
            assertEquals(0, recordMetadata0.offset());

            server0.shutdown();
            server0.awaitShutdown();
            server1.startup();

            // Current server 0 is offline.
            // However, the server 1 from the bootstrap list is online.
            // Should be able to produce records.
            var recordMetadata1 = producer.send(new ProducerRecord<>(topic, part, "key 1".getBytes(), "value 1".getBytes())).get();
            assertEquals(0, recordMetadata1.offset());

            server1.shutdown();
            server1.awaitShutdown();
            server0.startup();

            // The same situation, but the server 1 has gone and server 0 is back.
            var recordMetadata2 = producer.send(new ProducerRecord<>(topic, part, "key 1".getBytes(), "value 1".getBytes())).get();
            assertEquals(1, recordMetadata2.offset());
        }
    }
}
