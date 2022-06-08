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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

/**
 * A metrics registry used by client telemetry that provides basic utility methods to create
 * the various {@link Sensor sensors} that it exposes.
 *
 * <p/>
 *
 * A subclass will typically provide public instance variables to expose the static
 * {@link MetricName metric names} and methods to expose the
 * {@link MetricName dynamic metric names} that are based on {@link MetricNameTemplate templates}.
 * These {@link MetricName metric names} are then accessed by the rest of the client layer to
 * manipulate the corresponding {@link Sensor sensors}.
 */
public abstract class ClientTelemetryMetricsRegistry {

    protected final Metrics metrics;

    protected final Set<String> tags;

    protected ClientTelemetryMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
    }

    public Sensor gaugeSensor(MetricName mn) {
        return measurableStatSensor(mn, SimpleGauge::new);
    }

    public Sensor sumSensor(MetricName mn) {
        return measurableStatSensor(mn, CumulativeSum::new);
    }

    protected MetricName createMetricName(String name, String groupName, String description) {
        MetricNameTemplate mnt = new MetricNameTemplate(name, groupName, description, tags);
        return metrics.metricInstance(mnt);
    }

    protected static Set<String> appendTags(Set<String> existingTags, String... newTags) {
        // When creating a tag set in the Metrics class, they are kept in order of addition, hence
        // the use of the LinkedHashSet here...
        Set<String> set = new LinkedHashSet<>();

        if (existingTags != null)
            set.addAll(existingTags);

        if (newTags != null)
            Collections.addAll(set, newTags);

        return set;
    }

    private synchronized Sensor measurableStatSensor(MetricName mn, Supplier<MeasurableStat> statSupplier) {
        Sensor sensor = metrics.getSensor(mn.name());

        if (sensor == null) {
            sensor = metrics.sensor(mn.name());
            MeasurableStat stat = statSupplier.get();
            sensor.add(mn, stat);
        }

        return sensor;
    }

    public static class SimpleGauge implements MeasurableStat {

        private double value;

        @Override
        public void record(MetricConfig config, double value, long now) {
            this.value = value;
        }

        @Override
        public double measure(MetricConfig config, long now) {
            return value;
        }

        @Override
        public String toString() {
            return "SimpleGauge(value=" + value + ")";
        }
    }

}
