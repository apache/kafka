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
package org.apache.kafka.streams.processor.internals.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.streams.StreamsMetrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsConventions.threadLevelSensorName;

public class StreamsMetricsImpl implements StreamsMetrics {
    private final Metrics metrics;
    private final Map<String, String> tags;
    private final Map<Sensor, Sensor> parentSensors;
    private final Deque<String> ownedSensors = new LinkedList<>();
    private final Sensor skippedRecordsSensor;

    public StreamsMetricsImpl(final Metrics metrics, final String threadName) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");

        this.metrics = metrics;
        this.tags = StreamsMetricsConventions.threadLevelTags(threadName, Collections.<String, String>emptyMap());
        this.parentSensors = new HashMap<>();

        skippedRecordsSensor = metrics.sensor(threadLevelSensorName(threadName, "skipped-records"), Sensor.RecordingLevel.INFO);
        skippedRecordsSensor.add(metrics.metricName("skipped-records-rate", "stream-metrics", "The average per-second number of skipped records", tags), new Rate(TimeUnit.SECONDS, new Count()));
        skippedRecordsSensor.add(metrics.metricName("skipped-records-total", "stream-metrics", "The total number of skipped records", tags), new Total());
        ownedSensors.push(skippedRecordsSensor.name());
    }

    public final Metrics registry() {
        return metrics;
    }

    protected final Map<String, String> tags() {
        return tags;
    }

    public final Sensor skippedRecordsSensor() {
        return skippedRecordsSensor;
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
        addLatencyMetrics(scopeName, parent, operationName, allTagMap);
        addThroughputMetrics(scopeName, parent, operationName, allTagMap);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(sensorName(operationName, entityName), recordingLevel, parent);
        addLatencyMetrics(scopeName, sensor, operationName, tagMap);
        addThroughputMetrics(scopeName, sensor, operationName, tagMap);

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

    private void addLatencyMetrics(final String scopeName, final Sensor sensor, final String opName, final Map<String, String> tags) {
        sensor.add(
            metrics.metricName(
                opName + "-latency-avg",
                groupNameFromScope(scopeName),
                "The average latency of " + opName + " operation.", tags),
            new Avg()
        );
        sensor.add(
            metrics.metricName(
                opName + "-latency-max",
                groupNameFromScope(scopeName),
                "The max latency of " + opName + " operation.",
                tags
            ),
            new Max()
        );
    }

    private void addThroughputMetrics(final String scopeName, final Sensor sensor, final String opName, final Map<String, String> tags) {
        sensor.add(
            metrics.metricName(
                opName + "-rate",
                groupNameFromScope(scopeName),
                "The average number of occurrence of " + opName + " operation per second.",
                tags
            ),
            new Rate(TimeUnit.SECONDS, new Count())
        );
        sensor.add(
            metrics.metricName(
                opName + "-total",
                groupNameFromScope(scopeName),
                "The total number of occurrence of " + opName + " operations.",
                tags
            ),
            new Count()
        );
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

    public void removeOwnedSensors() {
        synchronized (ownedSensors) {
            while (!ownedSensors.isEmpty()) {
                metrics.removeSensor(ownedSensors.pop());
            }
        }
    }
}
