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

import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

public class HeartbeatMetricsManager extends AbstractConsumerMetricsManager {

    private final Sensor heartbeatSensor;
    private long lastHeartbeatMs = -1L;

    public HeartbeatMetricsManager(Metrics metrics) {
        super(metrics, CONSUMER_METRIC_GROUP_PREFIX, MetricGroupSuffix.COORDINATOR);
        heartbeatSensor = metrics.sensor("heartbeat-latency");
        heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                        metricGroupName(),
                        "The max time taken to receive a response to a heartbeat request"),
                new Max());
        heartbeatSensor.add(createMeter(metrics,
                "heartbeat",
                "heartbeats"));

        Measurable lastHeartbeat = (config, now) -> {
            final long lastHeartbeatSend = lastHeartbeatMs;
            if (lastHeartbeatSend < 0L)
                // if no heartbeat is ever triggered, just return -1.
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastHeartbeatSend, TimeUnit.MILLISECONDS);
        };
        addMetric(
                "last-heartbeat-seconds-ago",
                "The number of seconds since the last coordinator heartbeat was sent",
                lastHeartbeat);
    }

    public void recordHeartbeatSentMs(long timeMs) {
        lastHeartbeatMs = timeMs;
    }

    public void recordRequestLatency(long requestLatencyMs) {
        heartbeatSensor.record(requestLatencyMs);
    }
}
