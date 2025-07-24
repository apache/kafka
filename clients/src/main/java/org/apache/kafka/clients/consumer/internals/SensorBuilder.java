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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * {@code SensorBuilder} takes a bit of the boilerplate out of creating {@link Sensor sensors} for recording
 * {@link Metric metrics}.
 */
public class SensorBuilder {

    private final Metrics metrics;

    private final Sensor sensor;

    private final boolean preexisting;

    private final String group;

    private final Map<String, String> tags;

    public SensorBuilder(Metrics metrics, String name, String group) {
        this(metrics, name, group, Collections::emptyMap);
    }

    public SensorBuilder(Metrics metrics, String name, String group, Supplier<Map<String, String>> tagsSupplier) {
        this.metrics = Objects.requireNonNull(metrics);
        this.group = Objects.requireNonNull(group);
        Sensor s = metrics.getSensor(Objects.requireNonNull(name));

        if (s != null) {
            sensor = s;
            tags = Collections.emptyMap();
            preexisting = true;
        } else {
            sensor = metrics.sensor(name);
            tags = tagsSupplier.get();
            preexisting = false;
        }
    }

    public SensorBuilder withAvg(MetricName name) {
        if (!preexisting)
            sensor.add(name, new Avg());

        return this;
    }

    public SensorBuilder withAvg(String name, String description) {
        if (!preexisting)
            sensor.add(newMetricName(name, description), new Avg());

        return this;
    }

    public SensorBuilder withAvg(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(newMetricName(name), new Avg());

        return this;
    }

    public SensorBuilder withMin(MetricName name) {
        if (!preexisting)
            sensor.add(name, new Min());

        return this;
    }

    public SensorBuilder withMin(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(newMetricName(name), new Min());

        return this;
    }

    public SensorBuilder withMax(MetricName name) {
        if (!preexisting)
            sensor.add(name, new Max());

        return this;
    }

    public SensorBuilder withMax(String name, String description) {
        if (!preexisting)
            sensor.add(newMetricName(name, description), new Max());

        return this;
    }

    public SensorBuilder withMax(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(newMetricName(name), new Max());

        return this;
    }

    public SensorBuilder withValue(MetricName name) {
        if (!preexisting)
            sensor.add(name, new Value());

        return this;
    }

    public SensorBuilder withValue(String name, String description) {
        if (!preexisting)
            sensor.add(newMetricName(name, description), new Value());

        return this;
    }

    public SensorBuilder withValue(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(newMetricName(name), new Value());

        return this;
    }

    public SensorBuilder withMeter(MetricNameTemplate rateName, MetricNameTemplate totalName) {
        if (!preexisting) {
            sensor.add(new Meter(newMetricName(rateName), newMetricName(totalName)));
        }

        return this;
    }

    public SensorBuilder withMeter(SampledStat sampledStat, MetricName rateName, MetricName totalName) {
        if (!preexisting) {
            sensor.add(new Meter(sampledStat, rateName, totalName));
        }

        return this;
    }

    public SensorBuilder withMeter(SampledStat sampledStat, MetricNameTemplate rateName, MetricNameTemplate totalName) {
        if (!preexisting) {
            sensor.add(new Meter(sampledStat, newMetricName(rateName), newMetricName(totalName)));
        }

        return this;
    }

    public SensorBuilder withCumulativeSum(MetricName name) {
        if (!preexisting)
            sensor.add(name, new CumulativeSum());

        return this;
    }

    public SensorBuilder withCumulativeSum(String name, String description) {
        if (!preexisting)
            sensor.add(newMetricName(name, description), new CumulativeSum());

        return this;
    }

    public SensorBuilder withCumulativeSum(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(newMetricName(name), new CumulativeSum());

        return this;
    }

    public SensorBuilder withCumulativeCount(MetricName name) {
        if (!preexisting)
            sensor.add(name, new CumulativeCount());

        return this;
    }

    public SensorBuilder withCumulativeCount(String name, String description) {
        if (!preexisting)
            sensor.add(newMetricName(name, description), new CumulativeCount());

        return this;
    }

    public SensorBuilder withCumulativeCount(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(newMetricName(name), new CumulativeCount());

        return this;
    }

    public SensorBuilder withMeasurableStat(MetricName name, MeasurableStat measurableStat) {
        if (!preexisting)
            sensor.add(name, measurableStat);

        return this;
    }

    public SensorBuilder withMeasurableStat(String name, String description, MeasurableStat measurableStat) {
        if (!preexisting)
            sensor.add(newMetricName(name, description), measurableStat);

        return this;
    }

    public SensorBuilder withMeasurableStat(MetricNameTemplate name, MeasurableStat measurableStat) {
        if (!preexisting)
            sensor.add(newMetricName(name), measurableStat);

        return this;
    }

    public Sensor sensor() {
        return sensor;
    }

    private MetricName newMetricName(String name, String description) {
        return new MetricName(name, group, description, tags);
    }

    private MetricName newMetricName(MetricNameTemplate name) {
        return metrics.metricInstance(name, tags);
    }
}
