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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.LinearHistogram;

/**
 * This class provides basic utility methods that client telemetry subclasses can leverage
 * to create the various {@link Sensor}s that it exposes.
 *
 * A subclass will typically provide public methods to expose the sensors that the class
 * manages for use by the rest of the client layer to access those sensors for manipulation.
 */

public abstract class MetricRecorder implements ClientMetricRecorder {

    protected final Metrics metrics;

    protected final Set<String> tags;

    protected final List<MetricNameTemplate> allTemplates;

    protected MetricRecorder(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<>();
    }

    protected Sensor gaugeUpdateSensor(MetricName mn) {
        return sensor(mn, new CumulativeSum());
    }

    protected Sensor gaugeUpdateSensor(MetricNameTemplate mnt, Map<String, String> tags) {
        MetricName mn = metrics.metricInstance(mnt, tags);
        return gaugeUpdateSensor(mn);
    }

    protected Sensor gaugeReplaceSensor(MetricName mn) {
        return sensor(mn, new SimpleGauge());
    }

    protected Sensor gaugeReplaceSensor(MetricNameTemplate mnt, Map<String, String> tags) {
        MetricName mn = metrics.metricInstance(mnt, tags);
        return gaugeReplaceSensor(mn);
    }

    protected Sensor histogramSensor(MetricName mn, int maxBin, int numBin) {
        return sensor(mn, new LinearHistogram(numBin, maxBin, mn));
    }

    protected Sensor histogramSensor(MetricNameTemplate mnt, Map<String, String> tags, int numBin, int maxBin) {
        MetricName mn = metrics.metricInstance(mnt, tags);
        return histogramSensor(mn, maxBin, numBin);
    }

    protected Sensor sumSensor(MetricName mn) {
        return sensor(mn, new CumulativeSum());
    }

    protected Sensor sumSensor(MetricNameTemplate mnt, Map<String, String> tags) {
        MetricName mn = metrics.metricInstance(mnt, tags);
        return sumSensor(mn);
    }

    protected MetricName createMetricName(String name, String groupName, String description) {
        MetricNameTemplate mnt = createMetricNameTemplate(name, groupName, description, tags);
        return metrics.metricInstance(mnt);
    }

    protected MetricNameTemplate createMetricNameTemplate(String name, String group, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, description, tags);
        allTemplates.add(template);
        return template;
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

    private Sensor sensor(MetricName mn, MeasurableStat measurableStat) {
        Sensor sensor = metrics.sensor(mn.name());
        sensor.add(mn, measurableStat);
        return sensor;
    }

    private Sensor sensor(MetricName mn, CompoundStat compoundStat) {
        Sensor sensor = metrics.sensor(mn.name());
        sensor.add(compoundStat);
        return sensor;
    }

    static class SimpleGauge implements MeasurableStat {

        private double value;

        @Override
        public void record(MetricConfig config, double value, long now) {
            value = value;
        }

        @Override
        public double measure(MetricConfig config, long now) {
            return value;
        }

        @Override
        public String toString() {
            return "SimpleGauge(total=" + value + ")";
        }
    }

}
