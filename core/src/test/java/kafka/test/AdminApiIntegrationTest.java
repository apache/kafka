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

package kafka.test;

import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;


import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(value = ClusterTestExtensions.class)
public class AdminApiIntegrationTest {

    @ClusterTest(types = Type.KRAFT,
            serverProperties = @ClusterConfigProperty(key = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                    value = "kafka.test.AdminApiIntegrationTest$GetIdClientTelemetry"))
    public void testClientInstanceId(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        try (Admin admin = Admin.create(configs)) {
            assertNotNull(admin.clientInstanceId(Duration.ofSeconds(3)));
        }
    }


    @ClusterTest(types = Type.KRAFT, brokers = 3)
    public void testAddRaftVoter(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        String servers = clusterInstance.bootstrapServers();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        try (Admin admin = Admin.create(configs)) {
            assertThrows(ExecutionException.class, () -> {
                admin.addRaftVoter(1, Uuid.randomUuid(), Collections.emptySet()).all().get();
            });
        }
    }

    @ClusterTest(types = Type.KRAFT, brokers = 3)
    public void testRemoveRaftVoter(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        try (Admin admin = Admin.create(configs)) {
            assertThrows(ExecutionException.class, () -> {
                admin.removeRaftVoter(1, Uuid.randomUuid()).all().get();
            });
        }
    }

    @ClusterTest(types = Type.KRAFT)
    public void testMetrics(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        try (Admin admin = Admin.create(configs)) {
            admin.metrics().forEach((metricName, metric) -> {
                assertSame(metricName, metric.metricName());
            });
        }
    }

    /**
     * We should add a ClientTelemetry into plugins to test the clientInstanceId method Otherwise the
     * {@link  org.apache.kafka.common.protocol.ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS } command will not be supported
     * by the server
     **/
    public static class GetIdClientTelemetry implements ClientTelemetry, MetricsReporter {


        @Override
        public void init(List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ClientTelemetryReceiver clientReceiver() {
            return (context, payload) -> {
            };
        }
    }

}
