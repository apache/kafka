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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.concurrent.TimeUnit;

public class KafkaConsumerMetrics implements AutoCloseable {
    private final MetricName lastPollMetricName;
    private final Sensor timeBetweenPollSensor;
    private final Sensor pollIdleSensor;
    private final Metrics metrics;
    private long lastPollMs;
    private long pollStartMs;
    private long timeSinceLastPollMs;

    public KafkaConsumerMetrics(Metrics metrics, String metricGrpPrefix) {
        this.metrics = metrics;
        String metricGroupName = metricGrpPrefix + "-metrics";
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

    @Override
    public void close() {
        metrics.removeMetric(lastPollMetricName);
        metrics.removeSensor(timeBetweenPollSensor.name());
        metrics.removeSensor(pollIdleSensor.name());
    }
}
