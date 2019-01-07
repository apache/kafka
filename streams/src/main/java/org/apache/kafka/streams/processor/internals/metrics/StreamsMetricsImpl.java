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
    private final Map<Sensor, Sensor> parentSensors;
    private final Sensor skippedRecordsSensor;
    private final String threadName;

    private final Deque<String> threadLevelSensors = new LinkedList<>();
    private final Map<String, Deque<String>> taskLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> nodeLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> cacheLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> storeLevelSensors = new HashMap<>();

    private static final String SENSOR_PREFIX_DELIMITER = ".";
    private static final String SENSOR_NAME_DELIMITER = ".s.";

    public static final String PROCESSOR_NODE_METRICS_GROUP = "stream-processor-node-metrics";
    public static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";

    public StreamsMetricsImpl(final Metrics metrics, final String threadName) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");
        this.threadName = threadName;

        this.metrics = metrics;

        this.parentSensors = new HashMap<>();

        final String group = "stream-metrics";
        skippedRecordsSensor = threadLevelSensor("skipped-records", Sensor.RecordingLevel.INFO);
        skippedRecordsSensor.add(new MetricName("skipped-records-rate", group, "The average per-second number of skipped records", tagMap()), new Rate(TimeUnit.SECONDS, new Count()));
        skippedRecordsSensor.add(new MetricName("skipped-records-total", group, "The total number of skipped records", tagMap()), new Total());
    }

    public final Sensor threadLevelSensor(final String sensorName,
                                          final Sensor.RecordingLevel recordingLevel,
                                          final Sensor... parents) {
        synchronized (threadLevelSensors) {
            final String fullSensorName = threadSensorPrefix() + SENSOR_NAME_DELIMITER + sensorName;
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

    private String threadSensorPrefix() {
        return "internal" + SENSOR_PREFIX_DELIMITER + threadName;
    }

    public final Sensor taskLevelSensor(final String taskName,
                                        final String sensorName,
                                        final Sensor.RecordingLevel recordingLevel,
                                        final Sensor... parents) {
        final String key = taskSensorPrefix(taskName);
        synchronized (taskLevelSensors) {
            if (!taskLevelSensors.containsKey(key)) {
                taskLevelSensors.put(key, new LinkedList<>());
            }

            final String fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            taskLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public final void removeAllTaskLevelSensors(final String taskName) {
        final String key = taskSensorPrefix(taskName);
        synchronized (taskLevelSensors) {
            final Deque<String> sensors = taskLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private String taskSensorPrefix(final String taskName) {
        return threadSensorPrefix() + SENSOR_PREFIX_DELIMITER + "task" + SENSOR_PREFIX_DELIMITER + taskName;
    }

    public Sensor nodeLevelSensor(final String taskName,
                                  final String processorNodeName,
                                  final String sensorName,
                                  final Sensor.RecordingLevel recordingLevel,
                                  final Sensor... parents) {
        final String key = nodeSensorPrefix(taskName, processorNodeName);
        synchronized (nodeLevelSensors) {
            if (!nodeLevelSensors.containsKey(key)) {
                nodeLevelSensors.put(key, new LinkedList<>());
            }

            final String fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            nodeLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public final void removeAllNodeLevelSensors(final String taskName, final String processorNodeName) {
        final String key = nodeSensorPrefix(taskName, processorNodeName);
        synchronized (nodeLevelSensors) {
            final Deque<String> sensors = nodeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private String nodeSensorPrefix(final String taskName, final String processorNodeName) {
        return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "node" + SENSOR_PREFIX_DELIMITER + processorNodeName;
    }

    public final Sensor cacheLevelSensor(final String taskName,
                                         final String cacheName,
                                         final String sensorName,
                                         final Sensor.RecordingLevel recordingLevel,
                                         final Sensor... parents) {
        final String key = cacheSensorPrefix(taskName, cacheName);
        synchronized (cacheLevelSensors) {
            if (!cacheLevelSensors.containsKey(key)) {
                cacheLevelSensors.put(key, new LinkedList<>());
            }

            final String fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            cacheLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public final void removeAllCacheLevelSensors(final String taskName, final String cacheName) {
        final String key = cacheSensorPrefix(taskName, cacheName);
        synchronized (cacheLevelSensors) {
            final Deque<String> strings = cacheLevelSensors.remove(key);
            while (strings != null && !strings.isEmpty()) {
                metrics.removeSensor(strings.pop());
            }
        }
    }

    private String cacheSensorPrefix(final String taskName, final String cacheName) {
        return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "cache" + SENSOR_PREFIX_DELIMITER + cacheName;
    }

    public final Sensor storeLevelSensor(final String taskName,
                                         final String storeName,
                                         final String sensorName,
                                         final Sensor.RecordingLevel recordingLevel,
                                         final Sensor... parents) {
        final String key = storeSensorPrefix(taskName, storeName);
        synchronized (storeLevelSensors) {
            if (!storeLevelSensors.containsKey(key)) {
                storeLevelSensors.put(key, new LinkedList<>());
            }

            final String fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            storeLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public final void removeAllStoreLevelSensors(final String taskName, final String storeName) {
        final String key = storeSensorPrefix(taskName, storeName);
        synchronized (storeLevelSensors) {
            final Deque<String> sensors = storeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private String storeSensorPrefix(final String taskName, final String storeName) {
        return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "store" + SENSOR_PREFIX_DELIMITER + storeName;
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

    public final Map<String, String> tagMap(final String... tags) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put("client-id", threadName);
        if (tags != null) {
            if ((tags.length % 2) != 0) {
                throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");
            }

            for (int i = 0; i < tags.length; i += 2) {
                tagMap.put(tags[i], tags[i + 1]);
            }
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
        final String group = groupNameFromScope(scopeName);

        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(externalParentSensorName(operationName), recordingLevel);
        addAvgMaxLatency(parent, group, allTagMap, operationName);
        addInvocationRateAndCount(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, parent);
        addAvgMaxLatency(sensor, group, tagMap, operationName);
        addInvocationRateAndCount(sensor, group, tagMap, operationName);

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
        final String group = groupNameFromScope(scopeName);

        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(externalParentSensorName(operationName), recordingLevel);
        addInvocationRateAndCount(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, parent);
        addInvocationRateAndCount(sensor, group, tagMap, operationName);

        parentSensors.put(sensor, parent);

        return sensor;

    }

    private String externalChildSensorName(final String operationName, final String entityName) {
        return "external" + SENSOR_PREFIX_DELIMITER + threadName
            + SENSOR_PREFIX_DELIMITER + "entity" + SENSOR_PREFIX_DELIMITER + entityName
            + SENSOR_NAME_DELIMITER + operationName;
    }

    private String externalParentSensorName(final String operationName) {
        return "external" + SENSOR_PREFIX_DELIMITER + threadName + SENSOR_NAME_DELIMITER + operationName;
    }


    public static void addAvgMaxLatency(final Sensor sensor,
                                        final String group,
                                        final Map<String, String> tags,
                                        final String operation) {
        sensor.add(
            new MetricName(
                operation + "-latency-avg",
                group,
                "The average latency of " + operation + " operation.",
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + "-latency-max",
                group,
                "The max latency of " + operation + " operation.",
                tags),
            new Max()
        );
    }

    public static void addInvocationRateAndCount(final Sensor sensor,
                                                 final String group,
                                                 final Map<String, String> tags,
                                                 final String operation) {
        sensor.add(
            new MetricName(
                operation + "-rate",
                group,
                "The average number of occurrence of " + operation + " operation per second.",
                tags
            ),
            new Rate(TimeUnit.SECONDS, new Count())
        );
        sensor.add(
            new MetricName(
                operation + "-total",
                group,
                "The total number of occurrence of " + operation + " operations.",
                tags
            ),
            new CumulativeCount()
        );
    }

    /**
     * Deletes a sensor and its parents, if any
     */
    @Override
    public void removeSensor(final Sensor sensor) {
        Objects.requireNonNull(sensor, "Sensor is null");
        metrics.removeSensor(sensor.name());

        final Sensor parent = parentSensors.remove(sensor);
        if (parent != null) {
            metrics.removeSensor(parent.name());
        }
    }

    /**
     * Visible for testing
     */
    Map<Sensor, Sensor> parentSensors() {
        return Collections.unmodifiableMap(parentSensors);
    }

    private static String groupNameFromScope(final String scopeName) {
        return "stream-" + scopeName + "-metrics";
    }
}
