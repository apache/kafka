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
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@code MetricsLedger} records the {@link MetricName}s and {@link Sensor}s that are created
 * using the internal {@link Metrics} instance in a ledger. Then, in {@link #close()}, the
 * ledger is reviewed and each of the {@link MetricName}s and {@link Sensor}s are removed from
 * the underlying {@link Metrics} instance.
 *
 * <p/>
 *
 * Because {@link Metrics} is a <code>final</code> class, we cannot extend it in a delegation
 * pattern. Instead, we mimic the subset of APIs that are needed by the callers.
 */
public class MetricsLedger implements AutoCloseable {

    private final Metrics metrics;
    private final Set<MetricName> metricNames;
    private final Set<Sensor> sensors;

    public MetricsLedger(Metrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);
        this.metricNames = new HashSet<>();
        this.sensors = new HashSet<>();
    }

    public MetricName metricName(String name, String metricGroupName, String description) {
        MetricName metricName = metrics.metricName(name, metricGroupName, description);
        metricNames.add(metricName);
        return metricName;
    }

    public MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        MetricName metricName = metrics.metricInstance(template, tags);
        metricNames.add(metricName);
        return metricName;
    }

    public void addMetricIfAbsent(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider) {
        metrics.addMetricIfAbsent(metricName, config, metricValueProvider);
        metricNames.add(metricName);
    }

    public void addMetric(MetricName metricName, Measurable measurable) {
        metrics.addMetric(metricName, measurable);
        metricNames.add(metricName);
    }

    public void removeMetric(MetricName metricName) {
        metrics.removeMetric(metricName);
        metricNames.remove(metricName);
    }

    public Sensor sensor(String name) {
        Sensor sensor = metrics.sensor(name);
        sensors.add(sensor);
        return sensor;
    }

    public Sensor getSensor(String name) {
        Sensor sensor = metrics.getSensor(name);

        if (sensor != null)
            sensors.add(sensor);

        return sensor;
    }

    public void removeSensor(String name) {
        Sensor s = getSensor(name);
        metrics.removeSensor(name);
        sensors.remove(s);
    }

    @Override
    public final void close() {
        sensors.forEach(s -> {
            metrics.removeSensor(s.name());
        });

        metricNames.forEach(metrics::removeMetric);
    }
}