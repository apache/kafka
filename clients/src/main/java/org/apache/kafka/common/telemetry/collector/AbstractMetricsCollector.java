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
package org.apache.kafka.common.telemetry.collector;

import java.time.Instant;
import java.util.function.Supplier;
import org.apache.kafka.common.telemetry.emitter.Emitter;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.telemetry.metrics.MetricType;
import org.apache.kafka.common.telemetry.metrics.Metric;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code AbstractMetricsCollector} is a base {@link MetricsCollector} that an implementation
 * may use as a jumping off point by providing some utility methods for
 * {@link Emitter#emitMetric(Metric) emitting metrics}.
 */
public abstract class AbstractMetricsCollector implements MetricsCollector {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Time time;

    protected AbstractMetricsCollector() {
        this(Time.SYSTEM);
    }

    protected AbstractMetricsCollector(Time time) {
        this.time = time;
    }

    protected Instant now() {
        return Instant.ofEpochMilli(time.milliseconds());
    }

    protected boolean maybeEmitDouble(Emitter emitter, MetricKeyable metricKeyable, MetricType metricType, double value) {
        if (!emitter.shouldEmitMetric(metricKeyable))
            return false;

        Instant timestamp = now();
        Metric metric = new Metric(metricKeyable, metricType, value, timestamp);
        return emitter.emitMetric(metric);
    }

    protected boolean maybeEmitLong(Emitter emitter, MetricKeyable metricKeyable, MetricType metricType, long value) {
        if (!emitter.shouldEmitMetric(metricKeyable))
            return false;

        Instant timestamp = now();
        Metric metric = new Metric(metricKeyable, metricType, value, timestamp);
        return emitter.emitMetric(metric);
    }

    protected boolean maybeEmitDouble(Emitter emitter, MetricKeyable metricKeyable, MetricType metricType, Supplier<Double> valueSupplier) {
        if (!emitter.shouldEmitMetric(metricKeyable))
            return false;

        Double value = valueSupplier.get();

        if (value == null)
            return false;

        Instant timestamp = now();
        Metric metric = new Metric(metricKeyable, metricType, value, timestamp);
        return emitter.emitMetric(metric);
    }

    protected boolean maybeEmitLong(Emitter emitter, MetricKeyable metricKeyable, MetricType metricType, Supplier<Long> valueSupplier) {
        if (!emitter.shouldEmitMetric(metricKeyable))
            return false;

        Long value = valueSupplier.get();

        if (value == null)
            return false;

        Instant timestamp = now();
        Metric metric = new Metric(metricKeyable, metricType, value, timestamp);
        return emitter.emitMetric(metric);
    }

}
