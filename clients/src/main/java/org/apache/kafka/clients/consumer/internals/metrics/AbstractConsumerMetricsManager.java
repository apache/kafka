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
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractConsumerMetricsManager implements AutoCloseable {

    private final Metrics metrics;
    private final String metricGroupName;
    private final Set<MetricName> metricNames;
    private final List<Sensor> sensors;

    protected AbstractConsumerMetricsManager(Metrics metrics, String metricGroupName) {
        this.metrics = Objects.requireNonNull(metrics);
        this.metricGroupName = Objects.requireNonNull(metricGroupName);
        this.metricNames = new HashSet<>();
        this.sensors = new ArrayList<>();
    }

    protected MetricName metricName(String name, String description) {
        MetricName metricName = metrics.metricName(name, metricGroupName, description);
        metricNames.add(metricName);
        return metricName;
    }

    protected void addMetric(MetricName metricName, Measurable measurable) {
        metrics.addMetric(metricName, measurable);
    }

    protected Sensor sensor(String name) {
        Sensor sensor = metrics.sensor(name);
        sensors.add(sensor);
        return sensor;
    }

    @Override
    public final void close() {
        metricNames.forEach(metrics::removeMetric);
        sensors.forEach(s -> metrics.removeSensor(s.name()));
    }
}