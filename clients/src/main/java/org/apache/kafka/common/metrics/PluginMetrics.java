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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;

import java.util.Map;

/**
 * This allows plugins to register metrics and sensors.
 * Any metrics registered by the plugin are automatically removed when the plugin  closed.
 */
public interface PluginMetrics {

    /**
     * Create a {@link MetricName} with the given name, description and tags. The group will be set to "plugins"
     * Tags to uniquely identify the plugins are automatically added to the provided tags
     *
     * @param name        The name of the metric
     * @param description A human-readable description to include in the metric
     * @param tags        Additional tags for the metric
     * @throws IllegalArgumentException if any of the tag names collide with the default tags for the plugin
     */
    MetricName metricName(String name, String description, Map<String, String> tags);

    /**
     * Add a metric to monitor an object that implements {@link MetricValueProvider}. This metric won't be associated with any
     * sensor. This is a way to expose existing values as metrics.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     * @throws IllegalArgumentException if a metric with same name already exists
     */
    void addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider);

    /**
     * Remove a metric if it exists.
     *
     * @param metricName The name of the metric
     * @throws IllegalArgumentException if a metric with this name does not exist
     */
    void removeMetric(MetricName metricName);

    /**
     * Create a {@link Sensor} with the given unique name. The name must only be unique for the plugin, so different
     * plugins can use the same names.
     *
     * @param name The sensor name
     * @return The sensor
     * @throws IllegalArgumentException if a sensor with same name already exists for this plugin
     */
    Sensor addSensor(String name);

    /**
     * Remove a {@link Sensor} and its associated metrics.
     *
     * @param name The name of the sensor to be removed
     * @throws IllegalArgumentException if a sensor with this name does not exist
     */
    void removeSensor(String name);
}
