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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.test.TestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class AdminClientRebootstrapTest {
    private static final int BROKER_COUNT = 2;

    private static List<ClusterConfig> generator() {
        // Enable unclean leader election for the test topic
        Map<String, String> serverProperties = Map.of(
            TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true",
            GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(BROKER_COUNT)
        );

        return Stream.of(false, true)
            .map(AdminClientRebootstrapTest::getRebootstrapConfig)
            .map(rebootstrapProperties -> AdminClientRebootstrapTest.buildConfig(serverProperties, rebootstrapProperties))
            .toList();
    }

    private static Map<String, String> getRebootstrapConfig(boolean useRebootstrapTriggerMs) {
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
        return properties;
    }

    private static ClusterConfig buildConfig(Map<String, String> serverProperties, Map<String, String> rebootstrapProperties) {
        return ClusterConfig.defaultBuilder()
            .setTypes(Set.of(Type.KRAFT))
            .setBrokers(BROKER_COUNT)
            .setAdminClientProperties(rebootstrapProperties)
            .setServerProperties(serverProperties).build();
    }

    @ClusterTemplate(value = "generator")
    public void testRebootstrap(ClusterInstance clusterInstance) throws InterruptedException {
        var topic = "topic";
        var timeout = 5;
        try (var admin = clusterInstance.admin()) {
            admin.createTopics(List.of(new NewTopic(topic, BROKER_COUNT, (short) 2)));

            var server0 = clusterInstance.brokers().get(0);
            var server1 = clusterInstance.brokers().get(1);

            server1.shutdown();
            server1.awaitShutdown();

            // Only the server 0 is available for the admin client during the bootstrap.
            TestUtils.waitForCondition(() -> admin.listTopics().names().get(timeout, TimeUnit.MINUTES).contains(topic),
                "timed out waiting for topics");

            server0.shutdown();
            server0.awaitShutdown();
            server1.startup();

            // The server 0, originally cached during the bootstrap, is offline.
            // However, the server 1 from the bootstrap list is online.
            // Should be able to list topics again.
            TestUtils.waitForCondition(() -> admin.listTopics().names().get(timeout, TimeUnit.MINUTES).contains(topic),
                "timed out waiting for topics");

            server1.shutdown();
            server1.awaitShutdown();
            server0.startup();

            // The same situation, but the server 1 has gone and server 0 is back.
            TestUtils.waitForCondition(() -> admin.listTopics().names().get(timeout, TimeUnit.MINUTES).contains(topic),
                "timed out waiting for topics");
        }
    }
}
