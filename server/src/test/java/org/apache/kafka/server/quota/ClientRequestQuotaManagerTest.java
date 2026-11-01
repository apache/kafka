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

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientRequestQuotaManagerTest extends BaseClientQuotaManagerTest {
    private final ClientQuotaManagerConfig config = new ClientQuotaManagerConfig();

    private static double millisToPercent(double millis) {
        return millis * 1000 * 1000 * ClientRequestQuotaManager.NANOS_TO_PERCENTAGE_PER_SECOND;
    }

    @Test
    public void testRequestQuotaUtilization() {
        ClientRequestQuotaManager clientRequestQuotaManager = new ClientRequestQuotaManager(config, metrics, time, "", Optional.empty());
        String user = "ANONYMOUS";
        String clientId = "test-client";
        ClientQuotaEntity.ConfigEntity userEntity = new ClientQuotaManager.UserEntity(user);
        ClientQuotaEntity.ConfigEntity clientEntity = new ClientQuotaManager.ClientIdEntity(clientId);

        try {
            clientRequestQuotaManager.updateQuota(
                    Optional.of(userEntity),
                    Optional.of(clientEntity),
                    Optional.of(Quota.upperBound(50))
            );

            maybeRecord(clientRequestQuotaManager, user, clientId, 400);
            Map<String, String> tags = Map.of("user", user, "client-id", clientId);
            KafkaMetric requestTimeMetric = metrics.metric(metrics.metricName("request-time", QuotaType.REQUEST.toString(), tags));
            KafkaMetric utilizationMetric = metrics.metric(metrics.metricName(
                    "quota-utilization",
                    QuotaType.REQUEST.toString(),
                    tags
            ));
            assertNotNull(utilizationMetric);
            assertEquals(requestTimeMetric.metricName().tags(), utilizationMetric.metricName().tags());
            double requestTime = (Double) requestTimeMetric.metricValue();
            assertEquals(requestTime / 50 * 100, (Double) utilizationMetric.metricValue(), 0.001);
            assertEquals(80, (Double) utilizationMetric.metricValue(), 0.001);
        } finally {
            clientRequestQuotaManager.shutdown();
        }
    }

    @Test
    public void testRequestQuotaUtilizationJmxAttribute() throws Exception {
        Metrics jmxMetrics = new Metrics(new MetricConfig(), List.of(new JmxReporter()), time);
        ClientRequestQuotaManager clientRequestQuotaManager = new ClientRequestQuotaManager(config, jmxMetrics, time, "", Optional.empty());
        String user = "ANONYMOUS";
        String clientId = "test-client";
        Map<String, String> tags = Map.of("user", user, "client-id", clientId);

        try {
            clientRequestQuotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity(user)),
                    Optional.of(new ClientQuotaManager.ClientIdEntity(clientId)),
                    Optional.of(Quota.upperBound(50))
            );
            maybeRecord(clientRequestQuotaManager, user, clientId, 400);

            KafkaMetric rateMetric = jmxMetrics.metric(jmxMetrics.metricName("request-time", QuotaType.REQUEST.toString(), tags));
            assertNotNull(rateMetric);
            ObjectName objectName = new ObjectName(":type=Request,user=" + user + ",client-id=" + clientId);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            assertTrue(Arrays.stream(mbeanServer.getMBeanInfo(objectName).getAttributes())
                    .anyMatch(attr -> attr.getName().equals("quota-utilization")));
            assertEquals("double", Arrays.stream(mbeanServer.getMBeanInfo(objectName).getAttributes())
                    .filter(attr -> attr.getName().equals("quota-utilization"))
                    .findFirst()
                    .orElseThrow()
                    .getType());

            double requestTime = (Double) mbeanServer.getAttribute(objectName, "request-time");
            double utilization = (Double) mbeanServer.getAttribute(objectName, "quota-utilization");
            assertEquals(requestTime / 50 * 100, utilization, 0.001);
        } finally {
            clientRequestQuotaManager.shutdown();
            jmxMetrics.close();
        }
    }

    @Test
    public void testRequestPercentageQuotaViolation() {
        ClientRequestQuotaManager clientRequestQuotaManager = new ClientRequestQuotaManager(config, metrics, time, "", Optional.empty());
        ClientQuotaEntity.ConfigEntity userEntity = new ClientQuotaManager.UserEntity("ANONYMOUS");
        ClientQuotaEntity.ConfigEntity clientEntity = new ClientQuotaManager.ClientIdEntity("test-client");

        clientRequestQuotaManager.updateQuota(
                Optional.of(userEntity),
                Optional.of(clientEntity),
                Optional.of(Quota.upperBound(1))
        );
        KafkaMetric queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", QuotaType.REQUEST.toString(), ""));

        try {
            // We have 10 seconds windows. Make sure that there is no quota violation
            // if we are under the quota
            for (int i = 0; i < 10; i++) {
                assertEquals(0, maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(4)));
                time.sleep(1000);
            }
            assertEquals(0, ((Double) queueSizeMetric.metricValue()).intValue());

            // Create a spike.
            // quota = 1% (10ms per second)
            // 4*10 + 67.1 = 107.1/10.5 = 10.2ms per second.
            // (10.2 - quota)/quota*window-size = (10.2-10)/10*10.5 seconds = 210ms
            // 10.5 seconds interval because the last window is half complete
            time.sleep(500);
            int throttleTime = maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(67.1));

            assertEquals(210, throttleTime, "Should be throttled");

            throttle(clientRequestQuotaManager, throttleTime, callback);
            assertEquals(1, ((Double) queueSizeMetric.metricValue()).intValue());
            // After a request is delayed, the callback cannot be triggered immediately
            clientRequestQuotaManager.processThrottledChannelReaperDoWork();
            assertEquals(0, numCallbacks);
            time.sleep(throttleTime);

            // Callback can only be triggered after the delay time passes
            clientRequestQuotaManager.processThrottledChannelReaperDoWork();
            assertEquals(0, ((Double) queueSizeMetric.metricValue()).intValue());
            assertEquals(1, numCallbacks);

            // Could continue to see delays until the bursty sample disappears
            for (int i = 0; i < 11; i++) {
                maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(4));
                time.sleep(1000);
            }

            assertEquals(0,
                    maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", 0), "Should be unthrottled since bursty sample has rolled over");

            // Create a very large spike which requires > one quota window to bring within quota
            assertEquals(1000, maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(500)));
            for (int i = 0; i < 10; i++) {
                time.sleep(1000);
                assertEquals(1000, maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", 0));
            }
            time.sleep(1000);
            assertEquals(0,
                    maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", 0), "Should be unthrottled since bursty sample has rolled over");

        } finally {
            clientRequestQuotaManager.shutdown();
        }
    }

}
