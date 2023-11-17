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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code SensorBuilder} takes a bit of the boilerplate out of creating {@link Sensor sensors} for recording
 * {@link Metric metrics}.
 */
public class SensorBuilder {

    private final Metrics metrics;

    private final Sensor sensor;

    private final boolean preexisting;

    private final Map<String, String> tags;

    public SensorBuilder(Metrics metrics, String name) {
        this(metrics, name, Collections::emptyMap);
    }

    public SensorBuilder(Metrics metrics, String name, Supplier<Map<String, String>> tagsSupplier) {
        this.metrics = metrics;
        Sensor s = metrics.getSensor(name);

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

    SensorBuilder withAvg(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(metrics.metricInstance(name, tags), new Avg());

        return this;
    }

    SensorBuilder withMin(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(metrics.metricInstance(name, tags), new Min());

        return this;
    }

    SensorBuilder withMax(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(metrics.metricInstance(name, tags), new Max());

        return this;
    }

    SensorBuilder withValue(MetricNameTemplate name) {
        if (!preexisting)
            sensor.add(metrics.metricInstance(name, tags), new Value());

        return this;
    }

    SensorBuilder withMeter(MetricNameTemplate rateName, MetricNameTemplate totalName) {
        if (!preexisting) {
            sensor.add(new Meter(metrics.metricInstance(rateName, tags), metrics.metricInstance(totalName, tags)));
        }

        return this;
    }

    SensorBuilder withMeter(SampledStat sampledStat, MetricNameTemplate rateName, MetricNameTemplate totalName) {
        if (!preexisting) {
            sensor.add(new Meter(sampledStat, metrics.metricInstance(rateName, tags), metrics.metricInstance(totalName, tags)));
        }

        return this;
    }

    Sensor build() {
        return sensor;
    }

}
