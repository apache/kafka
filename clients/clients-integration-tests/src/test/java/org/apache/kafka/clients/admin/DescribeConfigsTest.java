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
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DescribeConfigsTest {

    private static final String PREFIXED_SSL_ENABLED_PROTOCOLS_CONFIG =
        "listener.name.external." + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;

    @ClusterTest
    public void testDescribeConfigsNonexistent(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            // BROKER and BROKER_LOGGER requests are routed to the node named by the resource, so a nonexistent node id
            // can only be reported as a timeout. Use a short timeout to avoid waiting for the default API timeout.
            DescribeConfigsOptions shortTimeout = new DescribeConfigsOptions().timeoutMs(1000);

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "-1");
            ExecutionException brokerException = assertThrows(ExecutionException.class,
                () -> admin.describeConfigs(List.of(brokerResource), shortTimeout).all().get());
            assertInstanceOf(TimeoutException.class, brokerException.getCause());

            ConfigResource brokerLoggerResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "-1");
            ExecutionException brokerLoggerException = assertThrows(ExecutionException.class,
                () -> admin.describeConfigs(List.of(brokerLoggerResource), shortTimeout).all().get());
            assertInstanceOf(TimeoutException.class, brokerLoggerException.getCause());

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "none_topic");
            ExecutionException topicException = assertThrows(ExecutionException.class,
                () -> admin.describeConfigs(List.of(topicResource)).all().get());
            assertInstanceOf(UnknownTopicOrPartitionException.class, topicException.getCause());

            // a nonexistent group is not an error: the default group configs are returned
            ConfigResource groupResource = new ConfigResource(ConfigResource.Type.GROUP, "none_group");
            Config groupConfig = admin.describeConfigs(List.of(groupResource)).all().get().get(groupResource);
            assertNotEquals(0, groupConfig.entries().size());
        }
    }

    @ClusterTest(types = {Type.CO_KRAFT})
    public void testDescribeConfigsTopicWithInternalListConfig(ClusterInstance cluster) throws Exception {
        String topic = "topic";
        cluster.createTopic(topic, 1, (short) 1,
            Map.of(QuotaConfig.MIRROR_REPLICATION_THROTTLED_REPLICAS_CONFIG, "0:101"));
        try (Admin admin = cluster.admin()) {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config topicConfig = admin.describeConfigs(List.of(topicResource)).all().get().get(topicResource);
            ConfigEntry entry = topicConfig.get(QuotaConfig.MIRROR_REPLICATION_THROTTLED_REPLICAS_CONFIG);
            assertEquals("0:101", entry.value());
        }
    }

    // Cluster-wide configs are described from DynamicBrokerConfig#currentDynamicDefaultConfigs, which holds raw
    // strings. A LIST config must be returned as the raw string instead of being cast to a List.
    @ClusterTest
    public void testDescribeConfigsClusterWideListConfig(ClusterInstance cluster) throws Exception {
        ConfigResource clusterResource = new ConfigResource(ConfigResource.Type.BROKER, "");
        try (Admin admin = cluster.admin()) {
            admin.incrementalAlterConfigs(Map.of(clusterResource, List.of(new AlterConfigOp(
                new ConfigEntry(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG, "compact,delete"),
                AlterConfigOp.OpType.SET)))).all().get();
            TestUtils.waitForCondition(
                () -> describeConfig(admin, clusterResource, ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG).source()
                    == ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG + " was not applied as a cluster-wide config"
            );
            assertEquals("compact,delete",
                describeConfig(admin, clusterResource, ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG).value());
        }
    }

    // Listener-prefixed configs are not defined in the ConfigDef, so they are described from the raw strings in
    // originals(). The type still resolves to LIST through the base config, so the raw string must not be cast to a List.
    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = PREFIXED_SSL_ENABLED_PROTOCOLS_CONFIG, value = "TLSv1.2,TLSv1.3")
    })
    public void testDescribeConfigsPrefixedListenerListConfig(ClusterInstance cluster) throws Exception {
        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER,
            String.valueOf(cluster.brokerIds().iterator().next()));
        try (Admin admin = cluster.admin()) {
            ConfigEntry entry = describeConfig(admin, brokerResource, PREFIXED_SSL_ENABLED_PROTOCOLS_CONFIG);
            assertNotNull(entry);
            assertEquals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, entry.source());
            assertEquals("TLSv1.2,TLSv1.3", entry.value());
        }
    }

    private static ConfigEntry describeConfig(Admin admin, ConfigResource resource, String name) throws Exception {
        return admin.describeConfigs(List.of(resource)).all().get().get(resource).get(name);
    }
}
