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
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

public final class ShareRebalanceMetricsManager extends RebalanceMetricsManager {
    private final Sensor rebalanceSensor;
    public final MetricName rebalanceTotal;
    public final MetricName rebalanceRatePerHour;
    private long lastRebalanceEndMs = -1L;
    private long lastRebalanceStartMs = -1L;

    public ShareRebalanceMetricsManager(Metrics metrics) {
        super(CONSUMER_SHARE_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX);

        rebalanceTotal = createMetric(metrics, "rebalance-total",
                "The total number of rebalance events");
        rebalanceRatePerHour = createMetric(metrics, "rebalance-rate-per-hour",
                "The number of rebalance events per hour");

        rebalanceSensor = metrics.sensor("rebalance-latency");
        rebalanceSensor.add(rebalanceTotal, new CumulativeCount());
        rebalanceSensor.add(rebalanceRatePerHour, new Rate(TimeUnit.HOURS, new WindowedCount()));
    }

    public void recordRebalanceStarted(long nowMs) {
        lastRebalanceStartMs = nowMs;
    }

    public void recordRebalanceEnded(long nowMs) {
        lastRebalanceEndMs = nowMs;
        rebalanceSensor.record(nowMs - lastRebalanceStartMs);
    }

    public boolean rebalanceStarted() {
        return lastRebalanceStartMs > lastRebalanceEndMs;
    }
}
