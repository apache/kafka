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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

public class RebalanceCallbackMetricsManager {
    final MetricName partitionRevokeLatencyAvg;
    final MetricName partitionAssignLatencyAvg;
    final MetricName partitionLostLatencyAvg;
    final MetricName partitionRevokeLatencyMax;
    final MetricName partitionAssignLatencyMax;
    final MetricName partitionLostLatencyMax;
    private final Sensor partitionRevokeCallbackSensor;
    private final Sensor partitionAssignCallbackSensor;
    private final Sensor partitionLostCallbackSensor;

    public RebalanceCallbackMetricsManager(Metrics metrics) {
        this(metrics, CONSUMER_METRIC_GROUP_PREFIX);
    }

    public RebalanceCallbackMetricsManager(Metrics metrics, String grpMetricsPrefix) {
        final String metricGroupName = grpMetricsPrefix + COORDINATOR_METRICS_SUFFIX;
        partitionRevokeCallbackSensor = metrics.sensor("partition-revoked-latency");
        partitionRevokeLatencyAvg = metrics.metricName("partition-revoked-latency-avg",
            metricGroupName,
            "The average time taken for a partition-revoked rebalance listener callback");
        partitionRevokeCallbackSensor.add(partitionRevokeLatencyAvg, new Avg());
        partitionRevokeLatencyMax = metrics.metricName("partition-revoked-latency-max",
            metricGroupName,
            "The max time taken for a partition-revoked rebalance listener callback");
        partitionRevokeCallbackSensor.add(partitionRevokeLatencyMax, new Max());

        partitionAssignCallbackSensor = metrics.sensor("partition-assigned-latency");
        partitionAssignLatencyAvg = metrics.metricName("partition-assigned-latency-avg",
            metricGroupName,
            "The average time taken for a partition-assigned rebalance listener callback");
        partitionAssignCallbackSensor.add(partitionAssignLatencyAvg, new Avg());
        partitionAssignLatencyMax = metrics.metricName("partition-assigned-latency-max",
            metricGroupName,
            "The max time taken for a partition-assigned rebalance listener callback");
        partitionAssignCallbackSensor.add(partitionAssignLatencyMax, new Max());

        partitionLostCallbackSensor = metrics.sensor("partition-lost-latency");
        partitionLostLatencyAvg = metrics.metricName("partition-lost-latency-avg",
            metricGroupName,
            "The average time taken for a partition-lost rebalance listener callback");
        partitionLostCallbackSensor.add(partitionLostLatencyAvg, new Avg());
        partitionLostLatencyMax = metrics.metricName("partition-lost-latency-max",
            metricGroupName,
            "The max time taken for a partition-lost rebalance listener callback");
        partitionLostCallbackSensor.add(partitionLostLatencyMax, new Max());
    }

    public void recordPartitionsRevokedLatency(long latencyMs) {
        partitionRevokeCallbackSensor.record(latencyMs);
    }

    public void recordPartitionsAssignedLatency(long latencyMs) {
        partitionAssignCallbackSensor.record(latencyMs);
    }

    public void recordPartitionsLostLatency(long latencyMs) {
        partitionLostCallbackSensor.record(latencyMs);
    }
}
