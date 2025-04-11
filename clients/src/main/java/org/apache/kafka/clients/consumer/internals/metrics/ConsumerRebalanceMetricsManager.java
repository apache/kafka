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
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

public final class ConsumerRebalanceMetricsManager extends RebalanceMetricsManager {
    private final Sensor successfulRebalanceSensor;
    private final Sensor failedRebalanceSensor;

    public final MetricName rebalanceLatencyAvg;
    public final MetricName rebalanceLatencyMax;
    public final MetricName rebalanceLatencyTotal;
    public final MetricName rebalanceTotal;
    public final MetricName rebalanceRatePerHour;
    public final MetricName lastRebalanceSecondsAgo;
    public final MetricName failedRebalanceTotal;
    public final MetricName failedRebalanceRate;
    private long lastRebalanceEndMs = -1L;
    private long lastRebalanceStartMs = -1L;

    public ConsumerRebalanceMetricsManager(Metrics metrics) {
        super(CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX);

        rebalanceLatencyAvg = createMetric(metrics, "rebalance-latency-avg",
                "The average time in ms taken for a group to complete a rebalance");
        rebalanceLatencyMax = createMetric(metrics, "rebalance-latency-max",
                "The max time in ms taken for a group to complete a rebalance");
        rebalanceLatencyTotal = createMetric(metrics, "rebalance-latency-total",
                "The total number of milliseconds spent in rebalances");
        rebalanceTotal = createMetric(metrics, "rebalance-total",
                "The total number of rebalance events");
        rebalanceRatePerHour = createMetric(metrics, "rebalance-rate-per-hour",
                "The number of rebalance events per hour");
        failedRebalanceTotal = createMetric(metrics, "failed-rebalance-total",
                "The total number of failed rebalance events");
        failedRebalanceRate = createMetric(metrics, "failed-rebalance-rate-per-hour",
                "The number of failed rebalance events per hour");

        successfulRebalanceSensor = metrics.sensor("rebalance-latency");
        successfulRebalanceSensor.add(rebalanceLatencyAvg, new Avg());
        successfulRebalanceSensor.add(rebalanceLatencyMax, new Max());
        successfulRebalanceSensor.add(rebalanceLatencyTotal, new CumulativeSum());
        successfulRebalanceSensor.add(rebalanceTotal, new CumulativeCount());
        successfulRebalanceSensor.add(rebalanceRatePerHour, new Rate(TimeUnit.HOURS, new WindowedCount()));

        failedRebalanceSensor = metrics.sensor("failed-rebalance");
        failedRebalanceSensor.add(failedRebalanceTotal, new CumulativeSum());
        failedRebalanceSensor.add(failedRebalanceRate, new Rate(TimeUnit.HOURS, new WindowedCount()));

        Measurable lastRebalance = (config, now) -> {
            if (lastRebalanceEndMs == -1L)
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
        };
        lastRebalanceSecondsAgo = createMetric(metrics,
                "last-rebalance-seconds-ago",
                "The number of seconds since the last rebalance event");
        metrics.addMetric(lastRebalanceSecondsAgo, lastRebalance);
    }

    public void recordRebalanceStarted(long nowMs) {
        lastRebalanceStartMs = nowMs;
    }

    public void recordRebalanceEnded(long nowMs) {
        lastRebalanceEndMs = nowMs;
        successfulRebalanceSensor.record(nowMs - lastRebalanceStartMs);
    }

    public void maybeRecordRebalanceFailed() {
        if (lastRebalanceStartMs <= lastRebalanceEndMs)
            return;
        failedRebalanceSensor.record();
    }

    public boolean rebalanceStarted() {
        return lastRebalanceStartMs > lastRebalanceEndMs;
    }
}