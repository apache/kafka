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
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;

public class HeartbeatMetricsManager extends AbstractConsumerMetricsManager {
    // MetricName visible for testing
    final MetricName heartbeatResponseTimeMax;
    final MetricName heartbeatRate;
    final MetricName heartbeatTotal;
    final MetricName lastHeartbeatSecondsAgo;
    private final Sensor heartbeatSensor;
    private long lastHeartbeatMs = -1L;

    public HeartbeatMetricsManager(Metrics metrics) {
        this(metrics, CONSUMER_METRIC_GROUP_PREFIX);
    }

    public HeartbeatMetricsManager(Metrics metrics, String metricGroupPrefix) {
        super(metrics, metricGroupPrefix + COORDINATOR_METRICS_SUFFIX);
        heartbeatSensor = sensor("heartbeat-latency");
        heartbeatResponseTimeMax = metricName("heartbeat-response-time-max",
            "The max time taken to receive a response to a heartbeat request");
        heartbeatSensor.add(heartbeatResponseTimeMax, new Max());

        // windowed meters
        heartbeatRate = metricName("heartbeat-rate", "The number of heartbeats per second");
        heartbeatTotal = metricName("heartbeat-total", "The total number of heartbeats");
        heartbeatSensor.add(new Meter(new WindowedCount(),
            heartbeatRate,
            heartbeatTotal));

        Measurable lastHeartbeat = (config, now) -> {
            final long lastHeartbeatSend = lastHeartbeatMs;
            if (lastHeartbeatSend < 0L)
                // if no heartbeat is ever triggered, just return -1.
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastHeartbeatSend, TimeUnit.MILLISECONDS);
        };
        lastHeartbeatSecondsAgo = metricName("last-heartbeat-seconds-ago",
            "The number of seconds since the last coordinator heartbeat was sent");
        addMetric(lastHeartbeatSecondsAgo, lastHeartbeat);
    }

    public void recordHeartbeatSentMs(long timeMs) {
        lastHeartbeatMs = timeMs;
    }

    public void recordRequestLatency(long requestLatencyMs) {
        heartbeatSensor.record(requestLatencyMs);
    }
}
