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

import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.OffsetCommitMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConsumerMetricsManagerTest {
    private Time time = new MockTime();
    private Metrics metrics = new Metrics(time);

    @Test
    public void testRebalanceCallbackMetrics() {
        // MBean: kafka.consumer:type=consumer-coordinator-metrics,client-id={clientId}
        RebalanceCallbackMetricsManager rebalanceCallbackMetricsManager = new RebalanceCallbackMetricsManager(metrics);
        String groupName = "consumer-coordinator-metrics";
        assertNotNull(getMetric("partition-revoked-latency-avg", groupName));
        assertNotNull(getMetric("partition-revoked-latency-max", groupName));
        assertNotNull(getMetric("partition-assigned-latency-avg", groupName));
        assertNotNull(getMetric("partition-assigned-latency-max", groupName));
        assertNotNull(getMetric("partition-lost-latency-avg", groupName));
        assertNotNull(getMetric("partition-lost-latency-max", groupName));

        rebalanceCallbackMetricsManager.recordPartitionsAssignedLatency(100);
        rebalanceCallbackMetricsManager.recordPartitionsRevokedLatency(101);
        rebalanceCallbackMetricsManager.recordPartitionsLostLatency(102);

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
        HeartbeatMetricsManager heartbeatMetricsManager = new HeartbeatMetricsManager(metrics);
        String groupName = "consumer-coordinator-metrics";
        assertNotNull(getMetric("heartbeat-response-time-max", groupName));
        assertNotNull(getMetric("heartbeat-rate", groupName));
        assertNotNull(getMetric("heartbeat-total", groupName));

        heartbeatMetricsManager.recordHeartbeatSentMs(time.milliseconds());
        heartbeatMetricsManager.recordRequestLatency(100);
        heartbeatMetricsManager.recordRequestLatency(103);
        heartbeatMetricsManager.recordRequestLatency(102);

        assertEquals(103d, getMetric("heartbeat-response-time-max", groupName).metricValue());
        assertEquals(0.1d, (double) getMetric("heartbeat-rate", groupName).metricValue(), 0.01d);
        assertEquals(3d, getMetric("heartbeat-total", groupName).metricValue());

        // Randomly sleep 1-10 seconds
        Random rand = new Random();
        int randomSleepS = rand.nextInt(10) + 1;
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepS));
        assertEquals((double) randomSleepS, getMetric("last-heartbeat-seconds-ago", groupName).metricValue());
    }

    @Test
    public void testDefaultMetricsGroupName() {
        final String coordinatorMetricsGroupName = "consumer-coordinator-metrics";
        HeartbeatMetricsManager heartbeatMetrics = new HeartbeatMetricsManager(metrics);
        assertEquals(coordinatorMetricsGroupName, heartbeatMetrics.metricGroupName());

        OffsetCommitMetricsManager commitMetrics = new OffsetCommitMetricsManager(metrics);
        assertEquals(coordinatorMetricsGroupName, commitMetrics.metricGroupName());

        RebalanceCallbackMetricsManager rebalanceCallbackMetrics = new RebalanceCallbackMetricsManager(metrics);
        assertEquals(coordinatorMetricsGroupName, rebalanceCallbackMetrics.metricGroupName());

        final String consumerMetricsGroupName = "consumer-metrics";
        KafkaConsumerMetrics consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);
        assertEquals(consumerMetricsGroupName, consumerMetrics.metricGroupName());
    }

    private KafkaMetric getMetric(String metricName, String groupName) {
        return metrics.metrics().get(metrics.metricName(metricName, groupName));
    }
}
