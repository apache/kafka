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

public class OffsetCommitMetricsManager extends AbstractConsumerMetricsManager {
    final MetricName commitLatencyAvg;
    final MetricName commitLatencyMax;
    final MetricName commitRate;
    final MetricName commitTotal;
    private final Sensor commitSensor;

    @SuppressWarnings({"this-escape"})
    public OffsetCommitMetricsManager(Metrics metrics) {
        super(metrics, CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX);
        commitSensor = sensor("commit-latency");
        commitLatencyAvg = metricName("commit-latency-avg",
            "The average time taken for a commit request");
        commitSensor.add(commitLatencyAvg, new Avg());
        commitLatencyMax = metricName("commit-latency-max",
            "The max time taken for a commit request");
        commitSensor.add(commitLatencyMax, new Max());
        commitRate = metricName("commit-rate",
            "The number of commit calls per second");
        commitTotal = metricName("commit-total",
            "The total number of commit calls");
        commitSensor.add(new Meter(new WindowedCount(),
            commitRate,
            commitTotal));
    }

    public void recordRequestLatency(long responseLatencyMs) {
        this.commitSensor.record(responseLatencyMs);
    }
}
