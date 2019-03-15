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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class StreamsMetricsImpl implements StreamsMetrics {
    private final Metrics metrics;

    private final Deque<String> threadLevelSensors = new LinkedList<>();
    private final Map<String, Deque<String>> taskLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> nodeLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> cacheLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> storeLevelSensors = new HashMap<>();

    public static final String SENSOR_PREFIX_DELIMITER = ".";
    public static final String SENSOR_NAME_DELIMITER = ".s.";

    public static final String PROCESSOR_NODE_METRICS_GROUP = "stream-processor-node-metrics";
    public static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";

    public static final String EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";
    public static final String LATE_RECORD_DROP = "late-record-drop";

    public static final String INSTANCE_METRICS_GROUP = "stream-metrics";

    public static final String THREAD_CLIENT_ID = "client-id";

    public static final String TASK_ID = "task-id";

    // we have to keep the thread-level metrics as-is to be compatible, the cost is that
    // instance-level and thread-level metrics will be under the same metrics type
    public static final String THREAD_METRICS_GROUP = "stream-metrics";

    public static final String TASK_METRICS_GROUP = "stream-task-metrics";


    public StreamsMetricsImpl(final Metrics metrics) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");

        this.metrics = metrics;
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
    @Deprecated
    public void recordLatency(final Sensor sensor, final long startNs, final long endNs) {
        sensor.record(endNs - startNs);
    }

    @Override
    @Deprecated
    public void recordThroughput(final Sensor sensor, final long value) {
        sensor.record(value);
    }
    /**
     * Deletes a sensor and its parents, if any
     */
    @Override
    public void removeSensor(final Sensor sensor) {
        Objects.requireNonNull(sensor, "Sensor is null");
        metrics.removeSensor(sensor.name());
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

        // add the operation metrics with additional tags
        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Sensor sensor = metrics.sensor(externalSensorName(operationName, entityName), recordingLevel);

        addValueAvgAndMax(sensor, group, tagMap, operationName + "latency");
        addInvocationRateAndCount(sensor, group, tagMap, operationName);

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

        // add the operation metrics with additional tags
        final Map<String, String> tagMap = constructTags(scopeName, entityName, tags);
        final Sensor sensor = metrics.sensor(externalSensorName(operationName, entityName), recordingLevel);

        addInvocationRateAndCount(sensor, group, tagMap, operationName);

        return sensor;
    }

    public final void removeAllThreadLevelSensors() {
        synchronized (threadLevelSensors) {
            while (!threadLevelSensors.isEmpty()) {
                metrics.removeSensor(threadLevelSensors.pop());
            }
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

    public final void removeAllNodeLevelSensors(final String taskName, final String processorNodeName) {
        final String key = nodeSensorPrefix(taskName, processorNodeName);
        synchronized (nodeLevelSensors) {
            final Deque<String> sensors = nodeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
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

    // -------- thread level sensors ----------- //

    public static Map<String, String> threadLevelTagMap(final String threadName) {
        return Collections.singletonMap(THREAD_CLIENT_ID, threadName);
    }

    private static String threadSensorPrefix(final String threadName) {
        return "internal" + SENSOR_PREFIX_DELIMITER + threadName;
    }

    public Sensor threadLevelSensor(final String sensorName) {
        return threadLevelSensor(sensorName, Thread.currentThread().getName(), Sensor.RecordingLevel.INFO);
    }

    public Sensor threadLevelSensor(final String sensorName,
                                    final String threadName,
                                    final Sensor.RecordingLevel recordingLevel) {
        final String fullSensorName = threadSensorPrefix(threadName) + SENSOR_NAME_DELIMITER + sensorName;
        return metrics.sensor(fullSensorName, recordingLevel);
    }

    // -------- task level sensors ----------- //

    public static Map<String, String> taskLevelTagMap(final String threadName, final String taskName) {
        return mkMap(mkEntry(THREAD_CLIENT_ID, threadName), mkEntry(TASK_ID, taskName));
    }

    private String taskSensorPrefix(final String threadName, final String taskName) {
        return threadSensorPrefix(threadName) + SENSOR_PREFIX_DELIMITER + "task" + SENSOR_PREFIX_DELIMITER + taskName;
    }

    public final Sensor taskLevelSensor(final String sensorName,
                                        final String taskName,
                                        final Sensor.RecordingLevel recordingLevel) {
        final String fullSensorName = taskSensorPrefix(Thread.currentThread().getName(), taskName) + SENSOR_NAME_DELIMITER + sensorName;
        return metrics.sensor(fullSensorName, recordingLevel);
    }

    // -------- processor-node level sensors ----------- //

    public static Map<String, String> nodeLevelTagMap(final String threadName, final String taskName, final String processorNodeName) {
        return mkMap(mkEntry(THREAD_CLIENT_ID, threadName),
                     mkEntry(TASK_ID, taskName),
                     mkEntry(PROCESSOR_NODE_ID_TAG, processorNodeName));
    }

    private String nodeSensorPrefix(final String taskName, final String processorNodeName) {
        return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "node" + SENSOR_PREFIX_DELIMITER + processorNodeName;
    }

    public final Sensor nodeLevelSensor(final String sensorName,
                                        final String processorNodeName,
                                        final String taskName,
                                        final Sensor.RecordingLevel recordingLevel) {
        final String fullSensorName = nodeSensorPrefix(taskName, processorNodeName) + SENSOR_NAME_DELIMITER + sensorName;

        // get the parent task-level sensor first
        final Sensor parentSensor = taskLevelSensor(sensorName, taskName, recordingLevel);

        return metrics.sensor(fullSensorName, recordingLevel, parentSensor);
    }

    // -------- processor-node level sensors ----------- //

    public final Map<String, String> tagMap(final String... tags) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put("client-id", threadName);
        return tagMap;
    }


    private Map<String, String> constructTags(final String scopeName, final String entityName, final String... tags) {
        final String[] updatedTags = Arrays.copyOf(tags, tags.length + 2);
        updatedTags[tags.length] = scopeName + "-id";
        updatedTags[tags.length + 1] = entityName;
        return tagMap(updatedTags);
    }

    private String externalSensorName(final String operationName, final String entityName) {
        return "external" + SENSOR_PREFIX_DELIMITER + threadName
            + SENSOR_PREFIX_DELIMITER + "entity" + SENSOR_PREFIX_DELIMITER + entityName
            + SENSOR_NAME_DELIMITER + operationName;
    }

    public static void addValueAvgAndMax(final Sensor sensor,
                                         final String group,
                                         final Map<String, String> tags,
                                         final String operation) {
        sensor.add(
            new MetricName(
                operation + "-avg",
                group,
                "The average latency of " + operation + " operation.",
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + "-max",
                group,
                "The max latency of " + operation + " operation.",
                tags),
            new Max()
        );
    }

    public static void addInvocationRate(final Sensor sensor,
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
    }

    public static void addInvocationRateAndCount(final Sensor sensor,
                                                 final String group,
                                                 final Map<String, String> tags,
                                                 final String operation) {
        addInvocationRate(sensor, group, tags, operation);

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

    private static String groupNameFromScope(final String scopeName) {
        return "stream-" + scopeName + "-metrics";
    }
}
