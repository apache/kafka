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
package org.apache.kafka.clients.telemetry;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

public class BaseClientTelemetryTest {

    public static final String CLIENT_ID = "test-client";

    public static final MockTime MOCK_TIME = new MockTime();

    protected DefaultClientTelemetry newClientTelemetry() {
        return newClientTelemetry(MOCK_TIME);
    }

    protected DefaultClientTelemetry newClientTelemetry(Time time) {
        return new DefaultClientTelemetry(time, CLIENT_ID);
    }

    protected TelemetrySubscription newTelemetrySubscription() {
        return newTelemetrySubscription(MOCK_TIME);
    }

    protected TelemetrySubscription newTelemetrySubscription(Time time) {
        return new TelemetrySubscription(time.milliseconds(),
            0,
            Uuid.randomUuid(),
            42,
            Collections.singletonList(CompressionType.NONE),
            10000,
            true,
            MetricSelector.ALL);
    }

    protected Map<MetricName, TelemetryMetric> currentTelemetryMetrics(DefaultClientTelemetry clientTelemetry,
        TelemetrySubscription telemetrySubscription) {
        Collection<TelemetryMetric> metrics = clientTelemetry.getTelemetryMetrics(telemetrySubscription);
        return metrics.stream().collect(Collectors.toMap(TelemetryMetric::metricName, Function.identity()));
    }

    protected MetricName newMetricName(String name) {
        return newMetricName(name, Collections.emptyMap());
    }

    protected MetricName newMetricName(String name, Map<String, String> tags) {
        return new MetricName(name, "group for " + name, "description for " + name, tags);
    }

    protected TelemetryMetric newTelemetryMetric(MetricName metricName, long value) {
        return newTelemetryMetric(metricName, MetricType.sum, value);
    }

    protected TelemetryMetric newTelemetryMetric(MetricName metricName, MetricType metricType, long value) {
        return new TelemetryMetric(metricName, metricType, value);
    }

}
