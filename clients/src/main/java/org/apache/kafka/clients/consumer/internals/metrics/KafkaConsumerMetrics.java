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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;

public class KafkaConsumerMetrics implements AutoCloseable {
    private final Metrics metrics;
    private final MetricName lastPollMetricName;
    private final MetricName backgroundEventQueueSize;
    private final MetricName backgroundEventQueueTimeAvg;
    private final MetricName backgroundEventQueueTimeMax;
    private final MetricName backgroundEventQueueProcessingAvg;
    private final MetricName backgroundEventQueueProcessingMax;
    private final Sensor backgroundEventQueueSensor;
    private final Sensor backgroundEventQueueSizeSensor;
    private final Sensor backgroundEventProcessingSensor;
    private final Sensor timeBetweenPollSensor;
    private final Sensor pollIdleSensor;
    private final Sensor committedSensor;
    private final Sensor commitSyncSensor;
    private long lastPollMs;
    private long pollStartMs;
    private long timeSinceLastPollMs;
    private final HashMap<Uuid, Long> backgroundEventQueueMap;

    public KafkaConsumerMetrics(Metrics metrics, String metricGrpPrefix) {
        this.metrics = metrics;
        final String metricGroupName = metricGrpPrefix + CONSUMER_METRICS_SUFFIX;
        Measurable lastPoll = (mConfig, now) -> {
            if (lastPollMs == 0L)
                // if no poll is ever triggered, just return -1.
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastPollMs, TimeUnit.MILLISECONDS);
        };
        this.lastPollMetricName = metrics.metricName("last-poll-seconds-ago",
            metricGroupName, "The number of seconds since the last poll() invocation.");
        metrics.addMetric(lastPollMetricName, lastPoll);

        this.timeBetweenPollSensor = metrics.sensor("time-between-poll");
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-avg",
                metricGroupName,
                "The average delay between invocations of poll() in milliseconds."),
                new Avg());
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-max",
                metricGroupName,
                "The max delay between invocations of poll() in milliseconds."),
                new Max());

        this.pollIdleSensor = metrics.sensor("poll-idle-ratio-avg");
        this.pollIdleSensor.add(metrics.metricName("poll-idle-ratio-avg",
                metricGroupName,
                "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records."),
                new Avg());

        this.commitSyncSensor = metrics.sensor("commit-sync-time-ns-total");
        this.commitSyncSensor.add(
            metrics.metricName(
                "commit-sync-time-ns-total",
                metricGroupName,
                "The total time the consumer has spent in commitSync in nanoseconds"
            ),
            new CumulativeSum()
        );

        this.committedSensor = metrics.sensor("committed-time-ns-total");
        this.committedSensor.add(
            metrics.metricName(
                "committed-time-ns-total",
                metricGroupName,
                "The total time the consumer has spent in committed in nanoseconds"
            ),
            new CumulativeSum()
        );

        this.backgroundEventQueueSensor = metrics.sensor("background-event-queue");
        this.backgroundEventQueueTimeAvg = metrics.metricName("background-event-queue-time-avg",
            metricGroupName,
            "The average time spent in the background event queue.");
        this.backgroundEventQueueTimeMax = metrics.metricName("background-event-queue-time-max",
            metricGroupName,
            "The maximum time spent in the background event queue.");
        this.backgroundEventQueueSensor.add(this.backgroundEventQueueTimeAvg, new Avg());
        this.backgroundEventQueueSensor.add(this.backgroundEventQueueTimeMax, new Max());

        this.backgroundEventQueueSizeSensor = metrics.sensor("background-event-queue-size");
        this.backgroundEventQueueSize = metrics.metricName("background-event-queue-size",
            metricGroupName,
            "The current size of the background event queue.");
        this.backgroundEventQueueSizeSensor.add(this.backgroundEventQueueSize, new Value());

        this.backgroundEventProcessingSensor = metrics.sensor("background-event-processing");
        this.backgroundEventQueueProcessingAvg = metrics.metricName("background-event-processing-avg",
            metricGroupName,
            "The average time spent processing background events.");
        this.backgroundEventQueueProcessingMax = metrics.metricName("background-event-processing-max",
            metricGroupName,
            "The maximum time spent processing background events.");
        this.backgroundEventProcessingSensor.add(this.backgroundEventQueueProcessingAvg, new Avg());
        this.backgroundEventProcessingSensor.add(this.backgroundEventQueueProcessingMax, new Max());

        this.backgroundEventQueueMap = new HashMap<>();
    }

    public void recordPollStart(long pollStartMs) {
        this.pollStartMs = pollStartMs;
        this.timeSinceLastPollMs = lastPollMs != 0L ? pollStartMs - lastPollMs : 0;
        this.timeBetweenPollSensor.record(timeSinceLastPollMs);
        this.lastPollMs = pollStartMs;
    }

    public void recordPollEnd(long pollEndMs) {
        long pollTimeMs = pollEndMs - pollStartMs;
        double pollIdleRatio = pollTimeMs * 1.0 / (pollTimeMs + timeSinceLastPollMs);
        this.pollIdleSensor.record(pollIdleRatio);
    }

    public void recordCommitSync(long duration) {
        this.commitSyncSensor.record(duration);
    }

    public void recordCommitted(long duration) {
        this.committedSensor.record(duration);
    }

    public void recordBackgroundEventQueueSize(int size) {
        this.backgroundEventQueueSensor.record(size);
    }

    public void recordBackgroundEventQueueChange(Uuid id, long now, boolean isNewEvent) {
        if (isNewEvent) {
            backgroundEventQueueMap.put(id, now);
        } else {
            long timeSinceEventAdded = now - backgroundEventQueueMap.remove(id);
            backgroundEventQueueSensor.record(timeSinceEventAdded);
            backgroundEventQueueMap.remove(id);
        }
    }

    public void recordBackgroundEventQueueProcessingTime(long processingTime) {
        backgroundEventProcessingSensor.record(processingTime);
    }

    @Override
    public void close() {
        metrics.removeMetric(lastPollMetricName);
        metrics.removeMetric(backgroundEventQueueSize);
        metrics.removeMetric(backgroundEventQueueTimeAvg);
        metrics.removeMetric(backgroundEventQueueTimeMax);
        metrics.removeMetric(backgroundEventQueueProcessingAvg);
        metrics.removeMetric(backgroundEventQueueProcessingMax);
        metrics.removeSensor(timeBetweenPollSensor.name());
        metrics.removeSensor(pollIdleSensor.name());
        metrics.removeSensor(commitSyncSensor.name());
        metrics.removeSensor(committedSensor.name());
        metrics.removeSensor(backgroundEventQueueSensor.name());
        metrics.removeSensor(backgroundEventQueueSizeSensor.name());
        metrics.removeSensor(backgroundEventProcessingSensor.name());
    }
}
