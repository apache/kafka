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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

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
        System.out.println("Metrics before:");
        metrics.metrics().keySet().forEach(System.out::println);

        metricNames.forEach(metrics::removeMetric);

        System.out.println("Metrics after:");
        metrics.metrics().keySet().forEach(System.out::println);

        System.out.println("Sensors before:");
        sensors.stream().filter(s -> metrics.getSensor(s.name()) != null).forEach(System.out::println);

        sensors.forEach(s -> metrics.removeSensor(s.name()));

        System.out.println("Sensors after:");
        sensors.stream().filter(s -> metrics.getSensor(s.name()) != null).forEach(System.out::println);
    }
}