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
package org.apache.kafka.common.telemetry.internals;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class represents a telemetry metric that does not yet contain resource tags.
 * These additional resource tags will be added before emitting metrics by the telemetry reporter.
 */
public class SinglePointMetric implements MetricKeyable {

    private final MetricKey key;
    private final Metric.Builder metricBuilder;

    private SinglePointMetric(MetricKey key, Metric.Builder metricBuilder) {
        this.key = key;
        this.metricBuilder = metricBuilder;
    }

    @Override
    public MetricKey key() {
        return key;
    }

    public Metric.Builder builder() {
        return metricBuilder;
    }

    /*
        Methods to construct gauge metric type.
     */
    public static SinglePointMetric gauge(MetricKey metricKey, Number value, Instant timestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value);
        return gauge(metricKey, point, excludeLabels);
    }

    public static SinglePointMetric gauge(MetricKey metricKey, double value, Instant timestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value);
        return gauge(metricKey, point, excludeLabels);
    }

    /*
        Methods to construct sum metric type.
     */

    public static SinglePointMetric sum(MetricKey metricKey, double value, boolean monotonic, Instant timestamp, Set<String> excludeLabels) {
        return sum(metricKey, value, monotonic, timestamp, null, excludeLabels);
    }

    public static SinglePointMetric sum(MetricKey metricKey, double value, boolean monotonic, Instant timestamp,
        Instant startTimestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value);
        if (startTimestamp != null) {
            point.setStartTimeUnixNano(toTimeUnixNanos(startTimestamp));
        }

        return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, monotonic, point, excludeLabels);
    }

    public static SinglePointMetric deltaSum(MetricKey metricKey, double value, boolean monotonic,
        Instant timestamp, Instant startTimestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value)
            .setStartTimeUnixNano(toTimeUnixNanos(startTimestamp));

        return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, monotonic, point, excludeLabels);
    }

    /*
        Helper methods to support metric construction.
     */
    private static SinglePointMetric sum(MetricKey metricKey, AggregationTemporality aggregationTemporality,
        boolean monotonic, NumberDataPoint.Builder point, Set<String> excludeLabels) {
        point.addAllAttributes(asAttributes(metricKey.tags(), excludeLabels));

        Metric.Builder metric = Metric.newBuilder().setName(metricKey.name());
        metric
            .getSumBuilder()
            .setAggregationTemporality(aggregationTemporality)
            .setIsMonotonic(monotonic)
            .addDataPoints(point);
        return new SinglePointMetric(metricKey, metric);
    }

    private static SinglePointMetric gauge(MetricKey metricKey, NumberDataPoint.Builder point, Set<String> excludeLabels) {
        point.addAllAttributes(asAttributes(metricKey.tags(), excludeLabels));

        Metric.Builder metric = Metric.newBuilder().setName(metricKey.name());
        metric.getGaugeBuilder().addDataPoints(point);
        return new SinglePointMetric(metricKey, metric);
    }

    private static NumberDataPoint.Builder point(Instant timestamp, Number value) {
        if (value instanceof Long || value instanceof Integer) {
            return point(timestamp, value.longValue());
        }

        return point(timestamp, value.doubleValue());
    }

    private static NumberDataPoint.Builder point(Instant timestamp, long value) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(toTimeUnixNanos(timestamp))
            .setAsInt(value);
    }

    private static NumberDataPoint.Builder point(Instant timestamp, double value) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(toTimeUnixNanos(timestamp))
            .setAsDouble(value);
    }

    private static Iterable<KeyValue> asAttributes(Map<String, String> labels, Set<String> excludeLabels) {
        return labels.entrySet().stream().filter(entry -> !excludeLabels.contains(entry.getKey())).map(
            entry -> KeyValue.newBuilder()
                .setKey(entry.getKey())
                .setValue(AnyValue.newBuilder().setStringValue(entry.getValue())).build()
        )::iterator;
    }

    private static long toTimeUnixNanos(Instant t) {
        return TimeUnit.SECONDS.toNanos(t.getEpochSecond()) + t.getNano();
    }
}
