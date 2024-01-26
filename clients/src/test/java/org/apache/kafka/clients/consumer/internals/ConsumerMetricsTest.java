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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetrics;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.FETCH_MANAGER_METRICS_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConsumerMetricsTest {
    private Time time = new MockTime();
    private Metrics metrics = new Metrics(time);

    @Test
    public void testRebalanceCallbackMetrics() {
        // MBean: kafka.consumer:type=consumer-coordinator-metrics,client-id={clientId}
        RebalanceCallbackMetrics rebalanceCallbackMetrics = new RebalanceCallbackMetrics(metrics);
        String groupName = "consumer-coordinator-metrics";
        assertNotNull(getMetric("partition-revoked-latency-avg", groupName));
        assertNotNull(getMetric("partition-revoked-latency-max", groupName));
        assertNotNull(getMetric("partition-assigned-latency-avg", groupName));
        assertNotNull(getMetric("partition-assigned-latency-max", groupName));
        assertNotNull(getMetric("partition-lost-latency-avg", groupName));
        assertNotNull(getMetric("partition-lost-latency-max", groupName));

        rebalanceCallbackMetrics.assignCallbackSensor.record(100);
        rebalanceCallbackMetrics.revokeCallbackSensor.record(101);
        rebalanceCallbackMetrics.loseCallbackSensor.record(102);

        assertEquals(101d, getMetric("partition-revoked-latency-avg", groupName).metricValue());
        assertEquals(101d, getMetric("partition-revoked-latency-max", groupName).metricValue());
        assertEquals(100d, getMetric("partition-assigned-latency-avg", groupName).metricValue());
        assertEquals(100d, getMetric("partition-assigned-latency-max", groupName).metricValue());
        assertEquals(102d, getMetric("partition-lost-latency-avg", groupName).metricValue());
        assertEquals(102d, getMetric("partition-lost-latency-max", groupName).metricValue());
    }

    @Test
    public void testHeartbeatMetrics() {
        // MBean: kafka.consumer:type=consumer-coordinator-metrics,client-id={clientId}
        HeartbeatMetrics heartbeatMetrics = new HeartbeatMetrics(metrics);
        String groupName = "consumer-coordinator-metrics";
        assertNotNull(getMetric("heartbeat-response-time-max", groupName));
        assertNotNull(getMetric("heartbeat-rate", groupName));
        assertNotNull(getMetric("heartbeat-total", groupName));

        heartbeatMetrics.heartbeatSensor.record(100);
        heartbeatMetrics.heartbeatSensor.record(100);
        heartbeatMetrics.heartbeatSensor.record(100);

        assertEquals(100d, getMetric("heartbeat-response-time-max", groupName).metricValue());
        assertEquals(0.1d, (double) getMetric("heartbeat-rate", groupName).metricValue(), 0.01d);
        assertEquals(3d, getMetric("heartbeat-total", groupName).metricValue());
    }

    @Test
    public void testMetricsGroupName() {
        // consumer-coordinator-metrics
        NoopCoordinatorMetrics defaultCoordinatorMetrics = new NoopCoordinatorMetrics(Optional.empty());
        assertEquals(CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX, defaultCoordinatorMetrics.groupMetricsName());
        NoopCoordinatorMetrics coordinatorMetrics = new NoopCoordinatorMetrics(Optional.of("customCoordinatorPrefix"));
        assertEquals("customCoordinatorPrefix" + COORDINATOR_METRICS_SUFFIX, coordinatorMetrics.groupMetricsName());

        // consumer-metrics
        NoopConsumerMetrics defaultConsumerMetrics = new NoopConsumerMetrics(Optional.empty());
        assertEquals(CONSUMER_METRIC_GROUP_PREFIX + CONSUMER_METRICS_SUFFIX, defaultConsumerMetrics.groupMetricsName());
        NoopConsumerMetrics consumerMetrics = new NoopConsumerMetrics(Optional.of("customConsumerPrefix"));
        assertEquals("customConsumerPrefix" + CONSUMER_METRICS_SUFFIX, consumerMetrics.groupMetricsName());

        // consumer-fetch-manager-metrics
        NoopFetchManagerMetrics defaultFetchMetrics = new NoopFetchManagerMetrics(Optional.empty());
        assertEquals(CONSUMER_METRIC_GROUP_PREFIX + FETCH_MANAGER_METRICS_SUFFIX, defaultFetchMetrics.groupMetricsName());
        NoopFetchManagerMetrics fetchMetrics = new NoopFetchManagerMetrics(Optional.of("customFetchPrefix"));
        assertEquals("customFetchPrefix" + FETCH_MANAGER_METRICS_SUFFIX, fetchMetrics.groupMetricsName());
    }

    private KafkaMetric getMetric(String metricName, String groupName) {
        return metrics.metrics().get(metrics.metricName(metricName, groupName));
    }

    static class NoopCoordinatorMetrics extends AbstractConsumerMetrics {
        public NoopCoordinatorMetrics(Optional<String> prefix) {
            super(prefix, MetricSuffix.COORDINATOR.getSuffix());
        }
    }

    static class NoopConsumerMetrics extends AbstractConsumerMetrics {
        public NoopConsumerMetrics(Optional<String> prefix) {
            super(prefix, MetricSuffix.CONSUMER.getSuffix());
        }
    }

    static class NoopFetchManagerMetrics extends AbstractConsumerMetrics {
        public NoopFetchManagerMetrics(Optional<String> prefix) {
            super(prefix, MetricSuffix.FETCH_MANAGER.getSuffix());
        }
    }
}
