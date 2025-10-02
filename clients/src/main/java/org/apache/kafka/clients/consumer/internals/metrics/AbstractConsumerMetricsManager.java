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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Utility class that serves as a common abstraction point
 */
public abstract class AbstractConsumerMetricsManager implements AutoCloseable {

    private final Metrics metrics;
    private final String metricGroupName;
    private final Set<MetricName> metricNames;
    private final Set<Sensor> sensors;

    protected AbstractConsumerMetricsManager(Metrics metrics, String metricGroupName) {
        this.metrics = Objects.requireNonNull(metrics);
        this.metricGroupName = Objects.requireNonNull(metricGroupName);
        this.metricNames = new HashSet<>();
        this.sensors = new HashSet<>();
    }

    protected MetricName metricName(String name, String description) {
        MetricName metricName = metrics.metricName(name, metricGroupName, description);
        metricNames.add(metricName);
        return metricName;
    }

    protected MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        MetricName metricName = metrics.metricInstance(template, tags);
        metricNames.add(metricName);
        return metricName;
    }

    protected void addMetricIfAbsent(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider) {
        metrics.addMetricIfAbsent(metricName, config, metricValueProvider);
        metricNames.add(metricName);
    }

    protected void addMetric(MetricName metricName, Measurable measurable) {
        metrics.addMetric(metricName, measurable);
        metricNames.add(metricName);
    }

    protected void removeMetric(MetricName metricName) {
        metrics.removeMetric(metricName);
        metricNames.remove(metricName);
    }

    protected Sensor sensor(String name) {
        Sensor sensor = metrics.sensor(name);
        sensors.add(sensor);
        return sensor;
    }

    protected Sensor getSensor(String name) {
        Sensor sensor = metrics.getSensor(name);

        if (sensor != null)
            sensors.add(sensor);

        return sensor;
    }

    protected void removeSensor(String name) {
        Sensor s = getSensor(name);
        metrics.removeSensor(name);
        sensors.remove(s);
    }

    protected SensorBuilder sensorBuilder(String name) {
        return new SensorBuilder(name);
    }

    protected SensorBuilder sensorBuilder(String name, Supplier<Map<String, String>> tagsSupplier) {
        return new SensorBuilder(name, tagsSupplier);
    }

    @Override
    public final void close() {
        sensors.forEach(s -> {
            metrics.removeSensor(s.name());
        });

        metricNames.forEach(metrics::removeMetric);
    }

    /**
     * {@code SensorBuilder} takes a bit of the boilerplate out of creating {@link Sensor sensors} for recording
     * {@link Metric metrics}.
     */
    public class SensorBuilder {

        private final Sensor sensor;

        private final boolean preexisting;

        private final Map<String, String> tags;

        public SensorBuilder(String name) {
            this(name, Collections::emptyMap);
        }

        public SensorBuilder(String name, Supplier<Map<String, String>> tagsSupplier) {
            Sensor s = getSensor(name);

            if (s != null) {
                sensor = s;
                tags = Collections.emptyMap();
                preexisting = true;
            } else {
                sensor = sensor(name);
                sensors.add(sensor);
                tags = tagsSupplier.get();
                preexisting = false;
            }
        }

        public SensorBuilder withAvg(MetricNameTemplate name) {
            if (!preexisting)
                sensor.add(metricInstance(name, tags), new Avg());

            return this;
        }

        public SensorBuilder withMin(MetricNameTemplate name) {
            if (!preexisting)
                sensor.add(metricInstance(name, tags), new Min());

            return this;
        }

        public SensorBuilder withMax(MetricNameTemplate name) {
            if (!preexisting)
                sensor.add(metricInstance(name, tags), new Max());

            return this;
        }

        public SensorBuilder withValue(MetricNameTemplate name) {
            if (!preexisting)
                sensor.add(metricInstance(name, tags), new Value());

            return this;
        }

        public SensorBuilder withMeter(MetricNameTemplate rateName, MetricNameTemplate totalName) {
            if (!preexisting) {
                sensor.add(new Meter(metricInstance(rateName, tags), metricInstance(totalName, tags)));
            }

            return this;
        }

        public SensorBuilder withMeter(SampledStat sampledStat, MetricNameTemplate rateName, MetricNameTemplate totalName) {
            if (!preexisting) {
                sensor.add(new Meter(sampledStat, metricInstance(rateName, tags), metricInstance(totalName, tags)));
            }

            return this;
        }

        public Sensor build() {
            return sensor;
        }
    }
}