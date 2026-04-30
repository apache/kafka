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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;

import java.util.concurrent.atomic.AtomicInteger;

public final class ForwardingManagerMetrics implements AutoCloseable {

    private final Metrics metrics;

    private static final String METRIC_GROUP_NAME = "ForwardingManager";
    private static final String QUEUE_TIME_MS_NAME = "QueueTimeMs";
    private static final String REMOTE_TIME_MS_NAME = "RemoteTimeMs";

    /**
     * A histogram describing the amount of time in milliseconds each admin request spends in the broker's forwarding manager queue, waiting to be sent to the controller.
     * This does not include the time that the request spends waiting for a response from the controller.
     */
    private final LatencyHistogram queueTimeMsHist;

    /**
     * A histogram describing the amount of time in milliseconds each request sent by the ForwardingManager spends waiting for a response.
     * This does not include the time spent in the queue.
     */
    private final LatencyHistogram remoteTimeMsHist;

    private final MetricName queueLengthName;
    private final AtomicInteger queueLength = new AtomicInteger(0);

    public ForwardingManagerMetrics(Metrics metrics, long timeoutMs) {
        this.metrics = metrics;

        this.queueTimeMsHist = new LatencyHistogram(metrics, QUEUE_TIME_MS_NAME, METRIC_GROUP_NAME, timeoutMs);
        this.remoteTimeMsHist = new LatencyHistogram(metrics, REMOTE_TIME_MS_NAME, METRIC_GROUP_NAME, timeoutMs);

        this.queueLengthName = metrics.metricName(
            "QueueLength",
            METRIC_GROUP_NAME,
            "The current number of RPCs that are waiting in the broker's forwarding manager queue, waiting to be sent to the controller."
        );
        metrics.addMetric(queueLengthName, (Gauge<Integer>) (config, now) -> queueLength.get());
    }

    @Override
    public void close() {
        queueTimeMsHist.close();
        remoteTimeMsHist.close();
        metrics.removeMetric(queueLengthName);
    }

    public LatencyHistogram queueTimeMsHist() {
        return queueTimeMsHist;
    }

    public LatencyHistogram remoteTimeMsHist() {
        return remoteTimeMsHist;
    }

    public MetricName queueLengthName() {
        return queueLengthName;
    }

    public void incrementQueueLength() {
        queueLength.getAndIncrement();
    }

    public void decrementQueueLength() {
        queueLength.getAndDecrement();
    }

    public static final class LatencyHistogram implements AutoCloseable {
        private static final int SIZE_IN_BYTES = 4000;
        private final Metrics metrics;
        private final String name;
        private final Sensor sensor;
        private final MetricName latencyP99Name;
        private final MetricName latencyP999Name;

        private LatencyHistogram(Metrics metrics, String name, String group, long maxLatency) {
            this.metrics = metrics;
            this.name = name;
            this.sensor = metrics.sensor(name);
            this.latencyP99Name = metrics.metricName(name + ".p99", group);
            this.latencyP999Name = metrics.metricName(name + ".p999", group);

            sensor.add(new Percentiles(
                SIZE_IN_BYTES,
                maxLatency,
                BucketSizing.CONSTANT,
                new Percentile(latencyP99Name, 99),
                new Percentile(latencyP999Name, 99.9)
            ));
        }

        @Override
        public void close() {
            metrics.removeSensor(name);
            metrics.removeMetric(latencyP99Name);
            metrics.removeMetric(latencyP999Name);
        }

        public void record(long latencyMs) {
            sensor.record(latencyMs);
        }

        // visible for test
        public MetricName latencyP99Name() {
            return latencyP99Name;
        }

        // visible for test
        public MetricName latencyP999Name() {
            return latencyP999Name;
        }
    }
}