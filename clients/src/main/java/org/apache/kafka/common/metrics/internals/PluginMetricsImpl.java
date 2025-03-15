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
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PluginMetricsImpl implements PluginMetrics, Closeable {

    private static final String GROUP = "plugins";

    private final Metrics metrics;
    private final Map<String, String> tags;
    private final Set<MetricName> metricNames = ConcurrentHashMap.newKeySet();
    private final Set<String> sensors = ConcurrentHashMap.newKeySet();
    private volatile boolean closing = false;

    public PluginMetricsImpl(Metrics metrics, Map<String, String> tags) {
        this.metrics = metrics;
        this.tags = tags;
    }

    @Override
    public MetricName metricName(String name, String description, Map<String, String> tags) {
        if (closing) throw new IllegalStateException("This PluginMetrics instance is closed");
        for (String tagName : tags.keySet()) {
            if (this.tags.containsKey(tagName)) {
                throw new IllegalArgumentException("Cannot use " + tagName + " as a tag name");
            }
        }
        Map<String, String> metricsTags = new LinkedHashMap<>(this.tags);
        metricsTags.putAll(tags);
        return metrics.metricName(name, GROUP, description, metricsTags);
    }

    @Override
    public void addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider) {
        if (closing) throw new IllegalStateException("This PluginMetrics instance is closed");
        if (metricNames.contains(metricName)) {
            throw new IllegalArgumentException("Metric " + metricName + " already exists");
        }
        metrics.addMetric(metricName, metricValueProvider);
        metricNames.add(metricName);
    }

    @Override
    public void removeMetric(MetricName metricName) {
        if (closing) throw new IllegalStateException("This PluginMetrics instance is closed");
        if (metricNames.contains(metricName)) {
            metrics.removeMetric(metricName);
            metricNames.remove(metricName);
        } else {
            throw new IllegalArgumentException("Unknown metric " + metricName);
        }
    }

    @Override
    public Sensor addSensor(String name) {
        if (closing) throw new IllegalStateException("This PluginMetrics instance is closed");
        if (sensors.contains(name)) {
            throw new IllegalArgumentException("Sensor " + name + " already exists");
        }
        Sensor sensor = metrics.sensor(name);
        sensors.add(name);
        return sensor;
    }

    @Override
    public void removeSensor(String name) {
        if (closing) throw new IllegalStateException("This PluginMetrics instance is closed");
        if (sensors.contains(name)) {
            metrics.removeSensor(name);
            sensors.remove(name);
        } else {
            throw new IllegalArgumentException("Unknown sensor " + name);
        }
    }

    @Override
    public void close() throws IOException {
        closing = true;
        for (String sensor : sensors) {
            metrics.removeSensor(sensor);
        }
        for (MetricName metricName : metricNames) {
            metrics.removeMetric(metricName);
        }
    }
}
