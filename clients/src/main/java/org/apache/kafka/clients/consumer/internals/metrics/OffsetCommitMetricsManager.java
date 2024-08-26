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
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

public class OffsetCommitMetricsManager {
    final MetricName commitLatencyAvg;
    final MetricName commitLatencyMax;
    final MetricName commitRate;
    final MetricName commitTotal;
    private final Sensor commitSensor;

    public OffsetCommitMetricsManager(Metrics metrics) {
        final String metricGroupName = CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX;
        commitSensor = metrics.sensor("commit-latency");
        commitLatencyAvg = metrics.metricName("commit-latency-avg",
            metricGroupName,
            "The average time taken for a commit request");
        commitSensor.add(commitLatencyAvg, new Avg());
        commitLatencyMax = metrics.metricName("commit-latency-max",
            metricGroupName,
            "The max time taken for a commit request");
        commitSensor.add(commitLatencyMax, new Max());
        commitRate = metrics.metricName("commit-rate",
            metricGroupName,
            "The number of commit calls per second");
        commitTotal = metrics.metricName("commit-total",
            metricGroupName,
            "The total number of commit calls");
        commitSensor.add(new Meter(new WindowedCount(),
            commitRate,
            commitTotal));
    }

    public void recordRequestLatency(long responseLatencyMs) {
        this.commitSensor.record(responseLatencyMs);
    }
}
