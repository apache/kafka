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

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.common.telemetry.metrics.InvalidMetricTypeException;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.Metric;
import org.apache.kafka.common.telemetry.emitter.Emitter;

public class ClientTelemetryEmitter implements Emitter {

    private final Predicate<? super MetricKeyable> selector;

    private final List<Metric> emitted;

    public ClientTelemetryEmitter(Predicate<? super MetricKeyable> selector) {
        this.selector = selector;
        this.emitted = new ArrayList<>();
    }

    @Override
    public boolean shouldEmitMetric(MetricKeyable metricKeyable) {
        return selector.test(metricKeyable);
    }

    @Override
    public boolean emitMetric(Metric metric) {
        emitted.add(metric);
        return true;
    }

    public List<Metric> emitted() {
        return Collections.unmodifiableList(emitted);
    }

    public byte[] payload(Map<String, String> context) {
        Resource resource = resource(context);

        MetricsData.Builder builder = MetricsData.newBuilder();

        emitted.forEach(tm -> {
            io.opentelemetry.proto.metrics.v1.Metric m = convert(tm);
            ResourceMetrics rm = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addInstrumentationLibraryMetrics(
                    InstrumentationLibraryMetrics.newBuilder()
                        .addMetrics(m)
                        .build()
                ).build();

            builder.addResourceMetrics(rm);
        });

        return builder.build().toByteArray();
    }

    private Resource resource(Map<String, String> context) {
        Resource.Builder rb = Resource.newBuilder();
        context.forEach((k, v) -> rb.addAttributesBuilder()
            .setKey(k)
            .setValue(AnyValue.newBuilder().setStringValue(v)));
        return rb.build();
    }

    public static io.opentelemetry.proto.metrics.v1.Metric convert(Metric metric) {
        MetricKey metricKey = metric.key();
        Number value = metric.value();
        Instant timestamp = metric.timestamp();
        Instant startTimestamp = metric.startTimestamp();

        NumberDataPoint.Builder point;

        if (value instanceof Double)
            point = point(timestamp, (double) value);
        else if (value instanceof Long)
            point = point(timestamp, (long) value);
        else
            throw new InvalidMetricTypeException(String.format("The value of the metric %s (%s) was of unexpected class %s", metric.key(), value, value.getClass().getName()));

        switch (metric.metricType()) {
            case gauge:
                return gauge(metricKey, point);

            case sum:
                AggregationTemporality at;

                if (metric.deltaTemporality()) {
                    at = AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
                    point = point.setStartTimeUnixNano(toTimeUnixNanos(startTimestamp));
                } else {
                    at = AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
                }

                return sum(metricKey, at, point);

            default:
                throw new InvalidMetricTypeException(String.format("The metric %s (%s) was of unexpected type ", metric.key(), metric.metricType()));
        }
    }

    public static io.opentelemetry.proto.metrics.v1.Metric gauge(MetricKey metricKey, NumberDataPoint.Builder point) {
        point.addAllAttributes(asAttributes(metricKey.tags()));

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
            .setName(metricKey.name());

        metric.getGaugeBuilder()
            .addDataPoints(point);

        return metric.build();
    }

    public static NumberDataPoint.Builder point(Instant timestamp, double value) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(toTimeUnixNanos(timestamp))
            .setAsDouble(value);
    }

    public static Iterable<KeyValue> asAttributes(Map<String, String> labels) {
        return labels.entrySet().stream().map(
            entry -> KeyValue.newBuilder()
                .setKey(entry.getKey())
                .setValue(AnyValue.newBuilder().setStringValue(entry.getValue())).build()
        )::iterator;
    }

    public static io.opentelemetry.proto.metrics.v1.Metric sum(MetricKey metricKey,
        AggregationTemporality aggregationTemporalityDelta,
        NumberDataPoint.Builder point) {
        point.addAllAttributes(asAttributes(metricKey.tags()));
        final io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder().setName(metricKey.name());

        metric
            .getSumBuilder()
            .setAggregationTemporality(aggregationTemporalityDelta)
            .addDataPoints(point);

        return metric.build();
    }

    public static long toTimeUnixNanos(Instant t) {
        return TimeUnit.SECONDS.toNanos(t.getEpochSecond()) + t.getNano();
    }

}
