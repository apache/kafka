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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.WindowedCount;

public class ShareFetchMetricsManager {
    private final Metrics metrics;
    private final Sensor throttleTime;
    private final Sensor bytesFetched;
    private final Sensor recordsFetched;
    private final Sensor fetchLatency;
    private final Sensor sentAcknowledgements;
    private final Sensor failedAcknowledgements;

    public ShareFetchMetricsManager(Metrics metrics, ShareFetchMetricsRegistry metricsRegistry) {
        this.metrics = metrics;

        this.bytesFetched = new SensorBuilder(metrics, "bytes-fetched")
                .withAvg(metricsRegistry.fetchSizeAvg)
                .withMax(metricsRegistry.fetchSizeMax)
                .withMeter(metricsRegistry.bytesFetchedRate, metricsRegistry.bytesFetchedTotal)
                .build();
        this.recordsFetched = new SensorBuilder(metrics, "records-fetched")
                .withAvg(metricsRegistry.recordsPerRequestAvg)
                .withMax(metricsRegistry.recordsPerRequestMax)
                .withMeter(metricsRegistry.recordsFetchedRate, metricsRegistry.recordsFetchedTotal)
                .build();

        this.sentAcknowledgements = new SensorBuilder(metrics, "sent-acknowledgements")
                .withMeter(metricsRegistry.acknowledgementSendRate, metricsRegistry.acknowledgementSendTotal)
                .build();

        this.failedAcknowledgements = new SensorBuilder(metrics, "failed-acknowledgements")
                .withMeter(metricsRegistry.acknowledgementErrorRate, metricsRegistry.acknowledgementErrorTotal)
                .build();

        this.fetchLatency = new SensorBuilder(metrics, "fetch-latency")
                .withAvg(metricsRegistry.fetchLatencyAvg)
                .withMax(metricsRegistry.fetchLatencyMax)
                .withMeter(new WindowedCount(), metricsRegistry.fetchRequestRate, metricsRegistry.fetchRequestTotal)
                .build();

        this.throttleTime = new SensorBuilder(metrics, "fetch-throttle-time")
                .withAvg(metricsRegistry.fetchThrottleTimeAvg)
                .withMax(metricsRegistry.fetchThrottleTimeMax)
                .build();
    }

    public Sensor throttleTimeSensor() {
        return throttleTime;
    }

    void recordLatency(String node, long requestLatencyMs) {
        fetchLatency.record(requestLatencyMs);
        if (!node.isEmpty()) {
            String nodeTimeName = "node-" + node + ".latency";
            Sensor nodeRequestTime = metrics.getSensor(nodeTimeName);
            if (nodeRequestTime != null)
                nodeRequestTime.record(requestLatencyMs);
        }
    }

    void recordBytesFetched(int bytes) {
        bytesFetched.record(bytes);
    }

    void recordRecordsFetched(int records) {
        recordsFetched.record(records);
    }

    void recordAcknowledgementSent(int acknowledgements) {
        sentAcknowledgements.record(acknowledgements);
    }

    void recordFailedAcknowledgements(int acknowledgements) {
        failedAcknowledgements.record(acknowledgements);
    }
}
