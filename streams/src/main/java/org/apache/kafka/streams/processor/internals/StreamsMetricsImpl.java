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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StreamsMetricsImpl implements StreamsMetrics {
    private static final Logger log = LoggerFactory.getLogger(StreamsMetricsImpl.class);

    final Metrics metrics;
    final String groupName;
    final Map<String, String> tags;
    private final Map<Sensor, Sensor> parentSensors;

    public StreamsMetricsImpl(final Metrics metrics, final String groupName, final Map<String, String> tags) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");

        this.metrics = metrics;
        this.groupName = groupName;
        this.tags = tags;
        this.parentSensors = new HashMap<>();
    }

    public Metrics registry() {
        return metrics;
    }

    @Override
    public Sensor addSensor(final String name, final Sensor.RecordingLevel recordingLevel) {
        return metrics.sensor(name, recordingLevel);
    }

    @Override
    public Sensor addSensor(final String name, final Sensor.RecordingLevel recordingLevel, final Sensor... parents) {
        return metrics.sensor(name, recordingLevel, parents);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public void recordLatency(final Sensor sensor, final long startNs, final long endNs) {
        sensor.record(endNs - startNs);
    }

    @Override
    public void recordThroughput(final Sensor sensor, final long value) {
        sensor.record(value);
    }


    private String groupNameFromScope(final String scopeName) {
        return "stream-" + scopeName + "-metrics";
    }

    private String sensorName(final String operationName, final String entityName) {
        if (entityName == null) {
            return operationName;
        } else {
            return entityName + "-" + operationName;
        }
    }

    public Map<String, String> tagMap(final String... tags) {
        // extract the additional tags if there are any
        final Map<String, String> tagMap = new HashMap<>(this.tags);
        if (tags != null) {
            if ((tags.length % 2) != 0) {
                throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");
            }

            for (int i = 0; i < tags.length; i += 2)
                tagMap.put(tags[i], tags[i + 1]);
        }
        return tagMap;
    }


    private Map<String, String> constructTags(final String scopeName, final String entityName, final String... tags) {
        final String[] updatedTags = Arrays.copyOf(tags, tags.length + 2);
        updatedTags[tags.length] = scopeName + "-id";
        updatedTags[tags.length + 1] = entityName;
        return tagMap(updatedTags);
    }

    /**
     * @throws IllegalArgumentException if tags is not constructed in key-value pairs
     */
    @Override
    public Sensor addLatencyAndThroughputSensor(final String scopeName,
                                                final String entityName,
                                                final String operationName,
                                                final Sensor.RecordingLevel recordingLevel,
                                                final String... tags) {
        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(sensorName(operationName, null), recordingLevel);
        addLatencyAndThroughputMetrics(scopeName, parent, operationName, allTagMap);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(sensorName(operationName, entityName), recordingLevel, parent);
        addLatencyAndThroughputMetrics(scopeName, sensor, operationName, tagMap);

        parentSensors.put(sensor, parent);

        return sensor;
    }

    /**
     * @throws IllegalArgumentException if tags is not constructed in key-value pairs
     */
    @Override
    public Sensor addThroughputSensor(final String scopeName,
                                      final String entityName,
                                      final String operationName,
                                      final Sensor.RecordingLevel recordingLevel,
                                      final String... tags) {
        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(sensorName(operationName, null), recordingLevel);
        addThroughputMetrics(scopeName, parent, operationName, allTagMap);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(sensorName(operationName, entityName), recordingLevel, parent);
        addThroughputMetrics(scopeName, sensor, operationName, tagMap);

        parentSensors.put(sensor, parent);

        return sensor;
    }

    private void addLatencyAndThroughputMetrics(final String scopeName,
                                                final Sensor sensor,
                                                final String opName,
                                                final Map<String, String> tags) {
        maybeAddMetric(sensor, metrics.metricName(opName + "-latency-avg", groupNameFromScope(scopeName),
            "The average latency of " + opName + " operation.", tags), new Avg());
        maybeAddMetric(sensor, metrics.metricName(opName + "-latency-max", groupNameFromScope(scopeName),
            "The max latency of " + opName + " operation.", tags), new Max());
        addThroughputMetrics(scopeName, sensor, opName, tags);
    }

    private void addThroughputMetrics(final String scopeName,
                                      final Sensor sensor,
                                      final String opName,
                                      final Map<String, String> tags) {
        final MetricName rateMetricName = metrics.metricName(opName + "-rate", groupNameFromScope(scopeName),
            "The average number of occurrence of " + opName + " operation per second.", tags);
        final MetricName totalMetricName = metrics.metricName(opName + "-total", groupNameFromScope(scopeName),
            "The total number of occurrence of " + opName + " operations.", tags);
        if (!metrics.metrics().containsKey(rateMetricName) && !metrics.metrics().containsKey(totalMetricName)) {
            sensor.add(new Meter(new Count(), rateMetricName, totalMetricName));
        } else {
            log.trace("Trying to add metric twice: {} {}", rateMetricName, totalMetricName);
        }
    }

    /**
     * Register a metric on the sensor if it isn't already there.
     *
     * @param sensor The sensor on which to register the metric
     * @param name   The name of the metric
     * @param stat   The metric to track
     * @throws IllegalArgumentException if the same metric name is already in use elsewhere in the metrics
     */
    public void maybeAddMetric(final Sensor sensor, final MetricName name, final MeasurableStat stat) {
        sensor.add(name, stat);
    }

    /**
     * Helper function. Measure the latency of an action. This is equivalent to
     * startTs = time.nanoseconds()
     * action.run()
     * endTs = time.nanoseconds()
     * sensor.record(endTs - startTs)
     *
     * @param time   Time object.
     * @param action Action to run.
     * @param sensor Sensor to record value.
     */
    void measureLatencyNs(final Time time, final Runnable action, final Sensor sensor) {
        long startNs = -1;
        if (sensor.shouldRecord()) {
            startNs = time.nanoseconds();
        }
        action.run();
        if (startNs != -1) {
            recordLatency(sensor, startNs, time.nanoseconds());
        }
    }

    /**
     * Deletes a sensor and its parents, if any
     */
    @Override
    public void removeSensor(final Sensor sensor) {
        Objects.requireNonNull(sensor, "Sensor is null");
        metrics.removeSensor(sensor.name());

        final Sensor parent = parentSensors.get(sensor);
        if (parent != null) {
            metrics.removeSensor(parent.name());
        }

    }
}
