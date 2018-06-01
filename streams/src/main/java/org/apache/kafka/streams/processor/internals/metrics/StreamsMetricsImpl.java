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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StreamsMetricsImpl implements StreamsMetrics {
    private final Metrics metrics;
    private final Map<String, String> tags;
    private final Map<Sensor, Sensor> parentSensors;
    private final Sensor skippedRecordsSensor;
    private final String threadName;

    private final Deque<String> threadLevelSensors = new LinkedList<>();
    private final Map<String, Deque<String>> taskLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> cacheLevelSensors = new HashMap<>();

    public StreamsMetricsImpl(final Metrics metrics, final String threadName) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");
        this.threadName = threadName;

        this.metrics = metrics;


        final HashMap<String, String> tags = new LinkedHashMap<>();
        tags.put("client-id", threadName);
        this.tags = Collections.unmodifiableMap(tags);

        this.parentSensors = new HashMap<>();

        final String group = "stream-metrics";
        skippedRecordsSensor = threadLevelSensor("skipped-records", Sensor.RecordingLevel.INFO);
        skippedRecordsSensor.add(metrics.metricName("skipped-records-rate", group, "The average per-second number of skipped records", tags), new Rate(TimeUnit.SECONDS, new Count()));
        skippedRecordsSensor.add(metrics.metricName("skipped-records-total", group, "The total number of skipped records", tags), new Total());
    }

    public final Sensor threadLevelSensor(final String sensorName,
                                          final Sensor.RecordingLevel recordingLevel,
                                          final Sensor... parents) {
        synchronized (threadLevelSensors) {
            final String fullSensorName = threadName + "." + sensorName;
            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);
            threadLevelSensors.push(fullSensorName);

            return sensor;
        }
    }

    public final void removeAllThreadLevelSensors() {
        synchronized (threadLevelSensors) {
            while (!threadLevelSensors.isEmpty()) {
                metrics.removeSensor(threadLevelSensors.pop());
            }
        }
    }

    public final Sensor taskLevelSensor(final String taskName,
                                         final String sensorName,
                                         final Sensor.RecordingLevel recordingLevel,
                                         final Sensor... parents) {
        final String key = threadName + "." + taskName;
        synchronized (taskLevelSensors) {
            if (!taskLevelSensors.containsKey(key)) {
                taskLevelSensors.put(key, new LinkedList<String>());
            }

            final String fullSensorName = key + "." + sensorName;

            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            taskLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public final void removeAllTaskLevelSensors(final String taskName) {
        final String key = threadName + "." + taskName;
        synchronized (taskLevelSensors) {
            if (taskLevelSensors.containsKey(key)) {
                while (!taskLevelSensors.get(key).isEmpty()) {
                    metrics.removeSensor(taskLevelSensors.get(key).pop());
                }
                taskLevelSensors.remove(key);
            }
        }
    }

    public final Sensor cacheLevelSensor(final String taskName,
                                         final String cacheName,
                                         final String sensorName,
                                         final Sensor.RecordingLevel recordingLevel,
                                         final Sensor... parents) {
        final String key = threadName + "." + taskName + "." + cacheName;
        synchronized (cacheLevelSensors) {
            if (!cacheLevelSensors.containsKey(key)) {
                cacheLevelSensors.put(key, new LinkedList<String>());
            }

            final String fullSensorName = key + "." + sensorName;

            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            cacheLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public final void removeAllCacheLevelSensors(final String taskName, final String cacheName) {
        final String key = threadName + "." + taskName + "." + cacheName;
        synchronized (cacheLevelSensors) {
            if (cacheLevelSensors.containsKey(key)) {
                while (!cacheLevelSensors.get(key).isEmpty()) {
                    metrics.removeSensor(cacheLevelSensors.get(key).pop());
                }
                cacheLevelSensors.remove(key);
            }
        }
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

        return addLatencyAndThroughputSensor(null,
                                             scopeName,
                                             entityName,
                                             operationName,
                                             recordingLevel,
                                             tags);

    }

    public Sensor addLatencyAndThroughputSensor(final String taskName,
                                                final String scopeName,
                                                final String entityName,
                                                final String operationName,
                                                final Sensor.RecordingLevel recordingLevel,
                                                final String... tags) {

        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(sensorName(buildUniqueSensorName(operationName, taskName), null), recordingLevel);
        addLatencyMetrics(scopeName, parent, operationName, allTagMap);
        addThroughputMetrics(scopeName, parent, operationName, allTagMap);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(sensorName(buildUniqueSensorName(operationName, taskName), entityName), recordingLevel, parent);
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

        return addThroughputSensor(null,
                                   scopeName,
                                   entityName,
                                   operationName,
                                   recordingLevel,
                                   tags);

    }

    public Sensor addThroughputSensor(final String taskName,
                                      final String scopeName,
                                      final String entityName,
                                      final String operationName,
                                      final Sensor.RecordingLevel recordingLevel,
                                      final String... tags) {

        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(sensorName(buildUniqueSensorName(operationName, taskName), null), recordingLevel);
        addThroughputMetrics(scopeName, parent, operationName, allTagMap);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(sensorName(buildUniqueSensorName(operationName, taskName), entityName), recordingLevel, parent);
        addThroughputMetrics(scopeName, sensor, operationName, tagMap);

        parentSensors.put(sensor, parent);

        return sensor;

    }


    private String buildUniqueSensorName(String operationName, String taskName) {
        String task = taskName == null ? "" : taskName + ".";
        return threadName + "." + task + operationName;
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

}
