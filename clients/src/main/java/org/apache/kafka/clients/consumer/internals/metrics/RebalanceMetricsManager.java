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

public class RebalanceMetricsManager {
    private final Sensor successfulRebalanceSensor;
    private final Sensor failedRebalanceSensor;
    private final String metricGroupName;

    private final MetricName rebalanceLatencyAvg;
    private final MetricName rebalanceLatencyMax;
    private final MetricName rebalanceLatencyTotal;
    private final MetricName rebalanceTotal;
    private final MetricName rebalanceRatePerHour;
    private final MetricName lastRebalanceSecondsAgo;
    private long lastRebalanceEndMs = -1l;

    public RebalanceMetricsManager(Metrics metrics) {
        metricGroupName = CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX;

        rebalanceLatencyAvg = createMetric(metrics, "rebalance-latency-avg",
            "The average time taken for a group to complete a rebalance");
        rebalanceLatencyMax = createMetric(metrics, "rebalance-latency-max",
            "The max time taken for a group to complete a rebalance");
        rebalanceLatencyTotal = createMetric(metrics, "rebalance-latency-total",
            "The total number of milliseconds spent in rebalances");
        rebalanceTotal = createMetric(metrics, "rebalance-total",
            "The total number of rebalance events");
        rebalanceRatePerHour = createMetric(metrics, "rebalance-rate-per-hour",
            "The number of rebalance events per hour");

        successfulRebalanceSensor = createRebalanceSensor(metrics, "rebalance");
        failedRebalanceSensor = createRebalanceSensor(metrics, "failed-rebalance");

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

    private Sensor createRebalanceSensor(Metrics metrics, String sensorNamePrefix) {
        Sensor sensor = metrics.sensor(sensorNamePrefix + "-latency");
        sensor.add(rebalanceLatencyAvg, new Avg());
        sensor.add(rebalanceLatencyMax, new Max());
        sensor.add(rebalanceLatencyTotal, new CumulativeSum());
        sensor.add(rebalanceTotal, new CumulativeCount());
        sensor.add(rebalanceRatePerHour, new Rate(TimeUnit.HOURS, new WindowedCount()));
        return sensor;
    }

    private MetricName createMetric(Metrics metrics, String name, String description) {
        return metrics.metricName(name, metricGroupName, description);
    }

    public void recordRebalanceStarted(long nowMs) {
        // record rebalance started
    }

    public void recordRebalanceEnded(long nowMs) {
        lastRebalanceEndMs = nowMs;
        // record rebalance nowMs - rebalanceStartMs
    }

    public void maybeRecordRebalanceFailed() {
        // if last rebalance started != -1L
        failedRebalanceSensor.record();
    }
}
