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
package org.apache.kafka.server.quota;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.config.QuotaConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CustomQuotaCallbackTest {

    @ClusterTest(
        controllers = 3,
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(id = 3000, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "org.apache.kafka.server.quota.CustomQuotaCallbackTest$CustomQuotaCallback"),
            @ClusterConfigProperty(id = 3001, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "org.apache.kafka.server.quota.CustomQuotaCallbackTest$CustomQuotaCallback"),
            @ClusterConfigProperty(id = 3002, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "org.apache.kafka.server.quota.CustomQuotaCallbackTest$CustomQuotaCallback"),
        }
    )
    public void testCustomQuotaCallbackWithControllerServer(ClusterInstance cluster) throws InterruptedException {

        try (Admin admin = cluster.admin(Map.of())) {
            admin.createTopics(List.of(new NewTopic("topic", 1, (short) 1)));
            TestUtils.waitForCondition(
                () -> CustomQuotaCallback.COUNTERS.size() == 3 
                        && CustomQuotaCallback.COUNTERS.values().stream().allMatch(counter -> counter.get() > 0), 
                    "The CustomQuotaCallback not triggered in all controllers. "
            );
            
            // Reset the counters, and we expect the callback to be triggered again in all controllers
            CustomQuotaCallback.COUNTERS.clear();
            
            admin.deleteTopics(List.of("topic"));
            TestUtils.waitForCondition(
                () -> CustomQuotaCallback.COUNTERS.size() == 3
                        && CustomQuotaCallback.COUNTERS.values().stream().allMatch(counter -> counter.get() > 0), 
                    "The CustomQuotaCallback not triggered in all controllers. "
            );
        
        }
    }


    @ClusterTest(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(id = 3000, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "org.apache.kafka.server.quota.CustomQuotaCallbackTest$MonitorableCustomQuotaCallback"),
        }
    )
    public void testMonitorableCustomQuotaCallbackWithControllerServer(ClusterInstance cluster) {
        assertEquals(1, MonitorableCustomQuotaCallback.metricNames.size());
        MetricName metricName = MonitorableCustomQuotaCallback.metricNames.get(0);
        assertionMetric(metricName, ProcessRole.ControllerRole);
        MonitorableCustomQuotaCallback.clearMetrics();
    }

    @ClusterTest(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(id = 0, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "org.apache.kafka.server.quota.CustomQuotaCallbackTest$MonitorableCustomQuotaCallback"),
        }
    )
    public void testMonitorableCustomQuotaCallbackWithBrokerServer(ClusterInstance cluster) {
        assertEquals(1, MonitorableCustomQuotaCallback.metricNames.size());
        MetricName metricName = MonitorableCustomQuotaCallback.metricNames.get(0);
        assertionMetric(metricName, ProcessRole.BrokerRole);
        MonitorableCustomQuotaCallback.clearMetrics();
    }

    @ClusterTest(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(id = 0, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "org.apache.kafka.server.quota.CustomQuotaCallbackTest$MonitorableCustomQuotaCallback"),
        }
    )
    public void testMonitorableCustomQuotaCallbackWithCombinedMode(ClusterInstance cluster) {
        assertEquals(2, MonitorableCustomQuotaCallback.metricNames.size());
        MetricName metricName1 = MonitorableCustomQuotaCallback.metricNames.get(0);
        MetricName metricName2 = MonitorableCustomQuotaCallback.metricNames.get(1);
        if (metricName1.tags().get("role").equals(ProcessRole.ControllerRole.toString())) {
            assertionMetric(metricName1, ProcessRole.ControllerRole);
            assertionMetric(metricName2, ProcessRole.BrokerRole);
        } else {
            assertionMetric(metricName1, ProcessRole.BrokerRole);
            assertionMetric(metricName2, ProcessRole.ControllerRole);
        }
        MonitorableCustomQuotaCallback.clearMetrics();
    }

    private void assertionMetric(MetricName metricName, ProcessRole processRole) {
        assertEquals(QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, metricName.tags().get("config"));
        assertEquals(MonitorableCustomQuotaCallback.class.getSimpleName(), metricName.tags().get("class"));
        assertEquals(processRole.toString(), metricName.tags().get("role"));
        assertEquals(MonitorableCustomQuotaCallback.name, metricName.name());
        assertEquals(MonitorableCustomQuotaCallback.description, metricName.description());
    }

    public static class CustomQuotaCallback implements ClientQuotaCallback {

        public static final Map<String, AtomicInteger> COUNTERS = new ConcurrentHashMap<>();
        private String nodeId;

        @Override
        public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
            return Map.of();
        }

        @Override
        public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
            return Double.MAX_VALUE;
        }

        @Override
        public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {

        }

        @Override
        public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {

        }

        @Override
        public boolean quotaResetRequired(ClientQuotaType quotaType) {
            return true;
        }

        @Override
        public boolean updateClusterMetadata(Cluster cluster) {
            COUNTERS.computeIfAbsent(nodeId, k -> new AtomicInteger()).incrementAndGet();
            return true;
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {
            nodeId = (String) configs.get("node.id");
        }

    }

    public static class MonitorableCustomQuotaCallback extends CustomQuotaCallback implements Monitorable {

        
        public static List<MetricName> metricNames = new ArrayList<>();
        public static String name = "client quota callback";
        public static String description = "client quota callback description";

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            MetricName metricName = metrics.metricName(name, description, Map.of());
            metricNames.add(metricName);
            metrics.addMetric(metricName, (Gauge<Integer>) (config, now) -> 1);
        }
        
        public static void clearMetrics() {
            metricNames.clear();
        }
    }
}
