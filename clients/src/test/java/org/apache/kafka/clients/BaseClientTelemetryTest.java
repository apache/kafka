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
package org.apache.kafka.clients;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricType;
import org.apache.kafka.common.telemetry.metrics.Metric;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

public abstract class BaseClientTelemetryTest {

    public static final String CLIENT_ID = "test-client";

    public static final Time MOCK_TIME = new MockTime();

    protected LogContext newLogContext() {
        return new LogContext("[Test=" + getClass().getSimpleName() + "] ");
    }

    protected ClientTelemetry newClientTelemetry() {
        return newClientTelemetry(MOCK_TIME);
    }

    protected ClientTelemetry newClientTelemetry(Time time) {
        return newClientTelemetry(time, CLIENT_ID);
    }

    protected ClientTelemetry newClientTelemetry(Time time, String clientId) {
        return new ClientTelemetry(newLogContext(), time, clientId);
    }

    protected ClientTelemetrySubscription newTelemetrySubscription() {
        return newTelemetrySubscription(MOCK_TIME);
    }

    protected ClientTelemetrySubscription newTelemetrySubscription(Time time) {
        return new ClientTelemetrySubscription(0,
            Uuid.randomUuid(),
            42,
            Collections.singletonList(CompressionType.NONE),
            10000,
            true,
            ClientTelemetryUtils.SELECTOR_ALL_METRICS);
    }
//
//    protected Map<MetricName, TelemetryMetric> currentTelemetryMetrics(DefaultClientTelemetry clientTelemetry,
//        TelemetrySubscription telemetrySubscription) {
//        clientTelemetry.
//        Collection<TelemetryMetric> metrics = clientTelemetry.get(telemetrySubscription);
//        return metrics.stream().collect(Collectors.toMap(TelemetryMetric::metricName, Function.identity()));
//    }

    protected MetricName newMetricName(String name) {
        return newMetricName(name, Collections.emptyMap());
    }

    protected MetricName newMetricName(String name, Map<String, String> tags) {
        return new MetricName(name, "group for " + name, "description for " + name, tags);
    }

    protected Metric newTelemetryMetric(MetricName metricName, long value) {
        return newTelemetryMetric(metricName, MetricType.sum, value);
    }

    protected Metric newTelemetryMetric(MetricName metricName, MetricType metricType, long value) {
        return new Metric(new MetricKey(metricName), metricType, value, Instant.ofEpochMilli(MOCK_TIME.milliseconds()));
    }

}
