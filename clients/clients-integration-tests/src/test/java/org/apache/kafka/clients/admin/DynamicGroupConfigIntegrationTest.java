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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicGroupConfigIntegrationTest {

    @ClusterTest(types = {Type.KRAFT})
    public void testDescribeGroupConfigSynonymsWithBrokerSynonym(ClusterInstance cluster) throws Exception {
        try (var admin = cluster.admin()) {
            var group = "synonym-test-group";
            var groupResource = new ConfigResource(ConfigResource.Type.GROUP, group);
            var brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            var brokerDefaultResource = new ConfigResource(ConfigResource.Type.BROKER, "");

            // Verify default config only.
            // Expected synonym chain: DEFAULT_CONFIG
            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                String.valueOf(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT),
                ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                List.of(
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );

            // Set per-broker dynamic config.
            // Expected synonym chain: DYNAMIC_BROKER_CONFIG -> DEFAULT_CONFIG
            alterConfig(admin, cluster, brokerResource, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, "1500");

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "1500",
                ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                List.of(
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );

            // Set dynamic default broker config; per-broker config still takes precedence.
            // Expected synonym chain: DYNAMIC_BROKER_CONFIG -> DYNAMIC_DEFAULT_BROKER_CONFIG -> DEFAULT_CONFIG
            alterConfig(admin, cluster, brokerDefaultResource, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, "2000");

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "1500",
                ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                List.of(
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );

            // Set group override; it takes precedence over all broker configs.
            // Expected synonym chain: DYNAMIC_GROUP_CONFIG -> DYNAMIC_BROKER_CONFIG
            // -> DYNAMIC_DEFAULT_BROKER_CONFIG -> DEFAULT_CONFIG
            alterConfig(admin, cluster, groupResource, GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "3000");

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "3000",
                ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG,
                List.of(
                    Map.entry(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );
        }
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testDescribeGroupConfigSynonymsWithoutBrokerSynonym(ClusterInstance cluster) throws Exception {
        try (var admin = cluster.admin()) {
            var group = "synonym-no-broker-test-group";
            var groupResource = new ConfigResource(ConfigResource.Type.GROUP, group);

            // Verify default config for a config with no broker synonym.
            // Expected synonym chain: DEFAULT_CONFIG
            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG,
                GroupConfig.SHARE_AUTO_OFFSET_RESET_DEFAULT,
                ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                List.of(
                    Map.entry(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );

            // Set group override; synonyms use group config name since there is no broker synonym.
            // Expected synonym chain: DYNAMIC_GROUP_CONFIG -> DEFAULT_CONFIG
            alterConfig(admin, cluster, groupResource, GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "earliest");

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG,
                List.of(
                    Map.entry(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG),
                    Map.entry(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );
        }
    }

    @ClusterTest(types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "2000")
        })
    public void testDescribeGroupConfigSynonymsWithStaticBrokerConfig(ClusterInstance cluster) throws Exception {
        try (var admin = cluster.admin()) {
            var group = "synonym-static-test-group";
            var groupResource = new ConfigResource(ConfigResource.Type.GROUP, group);
            var brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            var brokerDefaultResource = new ConfigResource(ConfigResource.Type.BROKER, "");

            // Verify static broker config is reflected in synonyms.
            // Expected synonym chain: STATIC_BROKER_CONFIG -> DEFAULT_CONFIG
            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "2000",
                ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG,
                List.of(
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );

            // Set group override; it takes precedence over static broker config.
            // Expected synonym chain: DYNAMIC_GROUP_CONFIG -> STATIC_BROKER_CONFIG -> DEFAULT_CONFIG
            alterConfig(admin, cluster, groupResource, GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG, "3000");

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "3000",
                ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG,
                List.of(
                    Map.entry(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );

            // Add dynamic default broker config.
            // Expected synonym chain: DYNAMIC_GROUP_CONFIG -> DYNAMIC_DEFAULT_BROKER_CONFIG
            // -> STATIC_BROKER_CONFIG -> DEFAULT_CONFIG
            alterConfig(admin, cluster, brokerDefaultResource, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, "4000");

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "3000",
                ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG,
                List.of(
                    Map.entry(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG)));

            // Add per-broker dynamic config to complete the full 5-layer synonym chain.
            // Expected synonym chain: DYNAMIC_GROUP_CONFIG -> DYNAMIC_BROKER_CONFIG -> DYNAMIC_DEFAULT_BROKER_CONFIG
            // -> STATIC_BROKER_CONFIG -> DEFAULT_CONFIG
            alterConfig(admin, cluster, brokerResource, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, "5000");

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "3000",
                ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG,
                List.of(
                    Map.entry(GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG))
            );

            // Delete group override; value falls back to per-broker dynamic config.
            // Expected synonym chain: DYNAMIC_BROKER_CONFIG -> DYNAMIC_DEFAULT_BROKER_CONFIG
            // -> STATIC_BROKER_CONFIG -> DEFAULT_CONFIG
            deleteConfig(admin, cluster, groupResource, GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG);

            assertGroupConfig(
                admin,
                groupResource,
                GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
                "5000",
                ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                List.of(
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                    Map.entry(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG)));
        }
    }

    private static void alterConfig(
        Admin admin,
        ClusterInstance cluster,
        ConfigResource resource,
        String key,
        String value
    ) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource, List.of(
            new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)
        ))).all().get();
        cluster.ensureConsistentMetadata();
    }

    private static void deleteConfig(
        Admin admin,
        ClusterInstance cluster,
        ConfigResource resource,
        String key
    ) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource, List.of(
            new AlterConfigOp(new ConfigEntry(key, ""), AlterConfigOp.OpType.DELETE)
        ))).all().get();
        cluster.ensureConsistentMetadata();
    }

    private static void assertGroupConfig(
        Admin admin,
        ConfigResource groupResource,
        String configKey,
        String expectedValue,
        ConfigEntry.ConfigSource expectedSource,
        List<Map.Entry<String, ConfigEntry.ConfigSource>> expectedSynonyms
    ) throws Exception {
        DescribeConfigsOptions options = new DescribeConfigsOptions().includeSynonyms(true);
        ConfigEntry entry = admin.describeConfigs(List.of(groupResource), options)
            .all().get().get(groupResource).get(configKey);
        assertEquals(expectedValue, entry.value());
        assertEquals(expectedSource, entry.source());
        assertEquals(expectedSynonyms, entry.synonyms().stream()
            .map(s -> Map.entry(s.name(), s.source()))
            .toList());
    }
}
