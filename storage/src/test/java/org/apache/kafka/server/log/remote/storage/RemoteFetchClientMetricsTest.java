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

package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoteFetchClientMetricsTest {
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(new MetricConfig(), List.of(), time);
    private final RemoteFetchClientMetrics clientMetrics = new RemoteFetchClientMetrics(metrics, 5);

    private KafkaMetric metric(String name, String clientId) {
        MetricName metricName = metrics.metricName(name, RemoteFetchClientMetrics.GROUP,
            Map.of(RemoteFetchClientMetrics.CLIENT_ID_TAG, clientId));
        return metrics.metric(metricName);
    }

    @Test
    public void testMetricsCreatedLazilyAndTaggedByClientId() {
        assertNull(metric(RemoteFetchClientMetrics.BYTES_RATE, "consumer-a"));
        assertNull(metric(RemoteFetchClientMetrics.REQUESTS_RATE, "consumer-a"));

        clientMetrics.recordBytes("consumer-a", 1024, time.milliseconds());
        clientMetrics.recordRequest("consumer-a", time.milliseconds());
        time.sleep(1000);

        KafkaMetric bytesRate = metric(RemoteFetchClientMetrics.BYTES_RATE, "consumer-a");
        KafkaMetric requestsRate = metric(RemoteFetchClientMetrics.REQUESTS_RATE, "consumer-a");
        assertNotNull(bytesRate);
        assertNotNull(requestsRate);
        assertTrue((double) bytesRate.metricValue() > 0.0);
        assertTrue((double) requestsRate.metricValue() > 0.0);

        assertNull(metric(RemoteFetchClientMetrics.BYTES_RATE, "consumer-b"));
        assertNull(metric(RemoteFetchClientMetrics.REQUESTS_RATE, "consumer-b"));
    }

    @Test
    public void testSensorRecreatedAfterExpiry() {
        clientMetrics.recordBytes("consumer-a", 100, time.milliseconds());
        var sensor = metrics.getSensor(RemoteFetchClientMetrics.BYTES_RATE + ":consumer-a");
        assertNotNull(sensor);

        metrics.removeSensor(sensor.name());
        assertNull(metric(RemoteFetchClientMetrics.BYTES_RATE, "consumer-a"));

        clientMetrics.recordBytes("consumer-a", 50, time.milliseconds());
        var recreated = metrics.getSensor(RemoteFetchClientMetrics.BYTES_RATE + ":consumer-a");
        assertNotNull(recreated);
        assertNotEquals(sensor, recreated);
        assertNotNull(metric(RemoteFetchClientMetrics.BYTES_RATE, "consumer-a"));
    }

    @Test
    public void testMetricsUnderExpectedGroup() {
        clientMetrics.recordRequest("consumer-a", time.milliseconds());
        boolean hasGroupedMetric = metrics.metrics().keySet().stream()
            .anyMatch(mn -> RemoteFetchClientMetrics.GROUP.equals(mn.group())
                && "consumer-a".equals(mn.tags().get(RemoteFetchClientMetrics.CLIENT_ID_TAG)));
        assertTrue(hasGroupedMetric);
    }
}
