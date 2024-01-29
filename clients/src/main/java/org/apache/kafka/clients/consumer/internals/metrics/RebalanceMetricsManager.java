package org.apache.kafka.clients.consumer.internals.metrics;

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

public class RebalanceMetricsManager extends AbstractConsumerMetricsManager {
    private long lastRebalanceEndMs;
    private final Sensor successfulRebalanceSensor;
    private final Sensor failedRebalanceSensor;
    
    public RebalanceMetricsManager(Metrics metrics) {
        super(metrics, CONSUMER_METRIC_GROUP_PREFIX, MetricGroupSuffix.COORDINATOR);
        this.successfulRebalanceSensor = metrics.sensor("rebalance-latency");
        this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-avg",
            metricGroupName(),
            "The average time taken for a group to complete a successful rebalance, which may be composed of " +
                "several failed re-trials until it succeeded"), new Avg());
        this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-max",
            metricGroupName(),
            "The max time taken for a group to complete a successful rebalance, which may be composed of " +
                "several failed re-trials until it succeeded"), new Max());
        this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-total",
                metricGroupName(),
                "The total number of milliseconds this consumer has spent in successful rebalances since creation"),
            new CumulativeSum());
        this.successfulRebalanceSensor.add(
            metrics.metricName("rebalance-total",
                metricGroupName(),
                "The total number of successful rebalance events, each event is composed of " +
                    "several failed re-trials until it succeeded"),
            new CumulativeCount()
        );
        this.successfulRebalanceSensor.add(
            metrics.metricName(
                "rebalance-rate-per-hour",
                metricGroupName(),
                "The number of successful rebalance events per hour, each event is composed of " +
                    "several failed re-trials until it succeeded"),
            new Rate(TimeUnit.HOURS, new WindowedCount())
        );

        this.failedRebalanceSensor = metrics.sensor("failed-rebalance");
        this.failedRebalanceSensor.add(
            metrics.metricName("failed-rebalance-total",
                metricGroupName(),
                "The total number of failed rebalance events"),
            new CumulativeCount()
        );
        this.failedRebalanceSensor.add(
            metrics.metricName(
                "failed-rebalance-rate-per-hour",
                metricGroupName(),
                "The number of failed rebalance events per hour"),
            new Rate(TimeUnit.HOURS, new WindowedCount())
        );

        Measurable lastRebalance = (config, now) -> {
            if (lastRebalanceEndMs == -1L)
                // if no rebalance is ever triggered, we just return -1.
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
        };
        addMetric(
            "last-rebalance-seconds-ago",
            "The number of seconds since the last successful rebalance event",
            lastRebalance
        );
    }

    public void recordRebalanceCompleted(long timeMs) {
        lastRebalanceEndMs = timeMs;
    }
}
