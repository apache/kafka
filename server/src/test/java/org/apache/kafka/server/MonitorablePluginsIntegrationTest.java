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
package org.apache.kafka.server;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.replica.RackAwareReplicaSelector;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MonitorablePluginsIntegrationTest {
    private static int controllerId(Type type) {
        return type == Type.KRAFT ? 3000 : 0;
    }

    @ClusterTest(
        types = {Type.KRAFT, Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:ANONYMOUS"),
            @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
            @ClusterConfigProperty(key = REPLICA_SELECTOR_CLASS_CONFIG, value = "org.apache.kafka.server.MonitorablePluginsIntegrationTest$MonitorableReplicaSelector"),
            @ClusterConfigProperty(key = REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, value = "true"),
            @ClusterConfigProperty(key = REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                    value = "org.apache.kafka.server.MonitorablePluginsIntegrationTest$MonitorableNoOpRemoteLogMetadataManager"),
            @ClusterConfigProperty(key = REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                    value = "org.apache.kafka.server.MonitorablePluginsIntegrationTest$MonitorableNoOpRemoteStorageManager")
        }
    )
    public void testMonitorableServerPlugins(ClusterInstance clusterInstance) {
        assertAuthorizerMetrics(clusterInstance);
        assertReplicaSelectorMetrics(clusterInstance);
        assertRemoteLogManagerMetrics(clusterInstance);
    }

    private void assertAuthorizerMetrics(ClusterInstance clusterInstance) {
        assertMetrics(
                clusterInstance.brokers().get(0).metrics(),
                4,
                expectedTags(AUTHORIZER_CLASS_NAME_CONFIG, "StandardAuthorizer", Map.of("role", "broker")));

        assertMetrics(
                clusterInstance.controllers().get(controllerId(clusterInstance.type())).metrics(),
                4,
                expectedTags(AUTHORIZER_CLASS_NAME_CONFIG, "StandardAuthorizer", Map.of("role", "controller")));
    }

    private void assertRemoteLogManagerMetrics(ClusterInstance clusterInstance) {
        assertMetrics(
                clusterInstance.brokers().get(0).metrics(),
                MonitorableNoOpRemoteLogMetadataManager.METRICS_COUNT,
                expectedTags(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, MonitorableNoOpRemoteLogMetadataManager.class.getSimpleName()));
        assertMetrics(
                clusterInstance.brokers().get(0).metrics(),
                MonitorableNoOpRemoteStorageManager.METRICS_COUNT,
                expectedTags(REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, MonitorableNoOpRemoteStorageManager.class.getSimpleName()));
    }

    private void assertReplicaSelectorMetrics(ClusterInstance clusterInstance) {
        assertMetrics(
                clusterInstance.brokers().get(0).metrics(),
                MonitorableReplicaSelector.METRICS_COUNT,
                expectedTags(REPLICA_SELECTOR_CLASS_CONFIG, MonitorableReplicaSelector.class.getSimpleName()));
    }

    private void assertMetrics(Metrics metrics, int expected, Map<String, String> expectedTags) {
        int found = 0;
        for (MetricName metricName : metrics.metrics().keySet()) {
            if (metricName.group().equals("plugins")) {
                Map<String, String> tags = metricName.tags();
                if (expectedTags.equals(tags)) {
                    found++;
                }
            }
        }
        assertEquals(expected, found);
    }

    public static class MonitorableNoOpRemoteLogMetadataManager extends NoOpRemoteLogMetadataManager implements Monitorable {

        private static final int METRICS_COUNT = 1;

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            MetricName name = metrics.metricName("name", "description", new LinkedHashMap<>());
            metrics.addMetric(name, (Measurable) (config, now) -> 123);
        }
    }

    public static class MonitorableReplicaSelector extends RackAwareReplicaSelector implements Monitorable {

        private static final int METRICS_COUNT = 1;

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            MetricName name = metrics.metricName("name", "description", new LinkedHashMap<>());
            metrics.addMetric(name, (Measurable) (config, now) -> 123);
        }
    }

    public static class MonitorableNoOpRemoteStorageManager extends NoOpRemoteStorageManager implements Monitorable {

        private static final int METRICS_COUNT = 1;

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            MetricName name = metrics.metricName("name", "description", new LinkedHashMap<>());
            metrics.addMetric(name, (Measurable) (config, now) -> 123);
        }
    }

    private static Map<String, String> expectedTags(String config, String clazz) {
        return expectedTags(config, clazz, Map.of());
    }

    private static Map<String, String> expectedTags(String config, String clazz, Map<String, String> extraTags) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("config", config);
        tags.put("class", clazz);
        tags.putAll(extraTags);
        return tags;
    }
}
