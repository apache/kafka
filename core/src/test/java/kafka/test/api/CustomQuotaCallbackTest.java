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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.test.junit.ClusterTestExtensions;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ClusterTestDefaults(controllers = 3, 
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(id = 3000, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "kafka.test.api.CustomQuotaCallbackTest$CustomQuotaCallback"),
        @ClusterConfigProperty(id = 3001, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "kafka.test.api.CustomQuotaCallbackTest$CustomQuotaCallback"),
        @ClusterConfigProperty(id = 3002, key = QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, value = "kafka.test.api.CustomQuotaCallbackTest$CustomQuotaCallback"),
    }
)
@ExtendWith(ClusterTestExtensions.class)
public class CustomQuotaCallbackTest {

    private final ClusterInstance cluster;

    public CustomQuotaCallbackTest(ClusterInstance clusterInstance) {
        this.cluster = clusterInstance;
    }

    @ClusterTest
    public void testCustomQuotaCallbackWithControllerServer() throws InterruptedException {
        
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
}
