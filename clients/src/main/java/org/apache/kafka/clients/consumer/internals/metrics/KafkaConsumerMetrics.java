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

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRICS_SUFFIX;

public class KafkaConsumerMetrics extends AbstractMetricsManager {

    private final MetricName lastPollMetricName;
    private final Sensor timeBetweenPollSensor;
    private final Sensor pollIdleSensor;
    private final Sensor committedSensor;
    private final Sensor commitSyncSensor;
    private long lastPollMs;
    private long pollStartMs;
    private long timeSinceLastPollMs;

    public KafkaConsumerMetrics(Metrics metrics, String metricGrpPrefix) {
        super(metrics, metricGrpPrefix + CONSUMER_METRICS_SUFFIX);

        Measurable lastPoll = (mConfig, now) -> {
            if (lastPollMs == 0L)
                // if no poll is ever triggered, just return -1.
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastPollMs, TimeUnit.MILLISECONDS);
        };
        this.lastPollMetricName = newMetricName(
            "last-poll-seconds-ago",
            "The number of seconds since the last poll() invocation."
        );
        metrics.addMetric(lastPollMetricName, lastPoll);

        this.timeBetweenPollSensor = newSensorBuilder("time-between-poll")
            .withAvg(
                "time-between-poll-avg",
                "The average delay between invocations of poll() in milliseconds."
            )
            .withMax(
                "time-between-poll-max",
                "The max delay between invocations of poll() in milliseconds."
            )
            .sensor();

        this.pollIdleSensor = newSensorBuilder("poll-idle-ratio-avg")
            .withAvg(
                "poll-idle-ratio-avg",
                "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records."
            )
            .sensor();

        this.commitSyncSensor = newSensorBuilder("commit-sync-time-ns-total")
            .withCumulativeSum(
                "commit-sync-time-ns-total",
                "The total time the consumer has spent in commitSync in nanoseconds"
            )
            .sensor();

        this.committedSensor = newSensorBuilder("committed-time-ns-total")
            .withCumulativeSum(
                "committed-time-ns-total",
                "The total time the consumer has spent in committed in nanoseconds"
            )
            .sensor();
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
}
