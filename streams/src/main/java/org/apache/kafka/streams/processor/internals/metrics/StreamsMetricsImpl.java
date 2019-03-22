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
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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


    // we have to keep the thread-level metrics as-is to be compatible, the cost is that
    // instance-level and thread-level metrics will be under the same metrics type
    public static final String THREAD_METRICS_GROUP = "stream-metrics";



    // metric groups
    public static final String STREAM_STRING = "stream";
    private static final String TASK_STRING = "task";
    private static final String NODE_STRING = "node";
    private static final String STORE_STRING = "store";
    private static final String CACHE_STRING = "cache";
    public static final String BUFFER_STRING = "buffer";
    private static final String INTERNAL_STRING = "internal";

    public static final String STREAM_CLIENT_METRICS_GROUP = "stream-metrics";
    public static final String STREAM_THREAD_METRICS_GROUP = "stream-thread-metrics";
    public static final String STREAM_TASK_NODE_METRICS = "stream-task-metrics";
    public static final String STREAM_PROCESSOR_NODE_METRICS = "stream-processor-node-metrics";
    public static final String STREAM_CACHE_NODE_METRICS = "stream-record-cache-metrics";
    public static final String STREAM_BUFFER_METRICS = "stream-buffer-metrics";

    public static final String IN_MEMORY_STATE = "in-memory-state";
    public static final String IN_MEMORY_LRU = "in-memory-lru-state";
    public static final String ROCKSDB_STATE = "rocksdb-state";
    public static final String WINDOW_ROCKSDB_STATE = "rocksdb-window-state";
    public static final String SESSION_ROCKSDB_STATE = "rocksdb-session-state";

    // metric tag names
    public static final String CLIENT_ID_TAG = "client-id";
    public static final String THREAD_ID_TAG = "client-id";
    public static final String TASK_ID_TAG = "task-id";
    public static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";


    // metric names

    public static final String AVG_SUFFIX = "-avg";
    public static final String MAX_SUFFIX = "-max";
    public static final String MIN_SUFFIX = "-min";
    public static final String LATENCY_SUFFIX = "-latency";
    public static final String LATENCY_AVG_SUFFIX = "-latency-avg";
    public static final String LATENCY_MAX_SUFFIX = "-latency-max";
    public static final String RATE_SUFFIX = "-rate";
    public static final String TOTAL_SUFFIX = "-total";
    public static final String CURRENT_SUFFIX = "-current";


    // state-store level
    public static final String PUT = "put";
    public static final String PUT_IF_ABSENT = "put-if-absent";
    public static final String PUT_ALL = "put-all";
    public static final String GET = "get";
    public static final String ALL = "all";
    public static final String RANGE = "range";
    public static final String DELETE = "delete";
    public static final String FLUSH = "flush";
    public static final String RESTORE = "restore";

    public static final String SUPPRESSION_BUFFER_SIZE = "suppression-buffer-size";
    public static final String SUPPRESSION_BUFFER_COUNT = "suppression-buffer-count";
    public static final String EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";

    // task level
    public static final String COMMIT = "commit";
    public static final String PROCESS = "process";
    public static final String PUNCTUATE = "punctuate";
    public static final String RECORD_LATENESS = "record-lateness";
    public static final String ENFORCED_PROCESSING = "enforced-processing";

    // thread level
    public static final String TASK_CREATED = "task-created";
    public static final String TASK_CLOSED = "task-closed";
    public static final String POLL = "poll";

    // processor-node level
    public static final String SKIPPED_RECORDS = "skipped-records";
    public static final String DROPPRED_LATE_RECORDS = "dropped-late-records";
    public static final String SUPPRESSION_EMIT_RECORDS = "suppression-emit-records";

    // cache level
    public static final String HIT_RATIO = "hit-ratio";

    public static final String HIT_RATIO_AVG = "hitRatio-avg";
    public static final String HIT_RATIO_MIN = "hitRatio-min";
    public static final String HIT_RATIO_MAX = "hitRatio-max";


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

        addValueAvgAndMax(sensor, group, tagMap, operationName + LATENCY_SUFFIX);
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

    public static <R> R maybeMeasureLatency(final Supplier<R> action,
                                            final Time time,
                                            final Sensor sensor) {
        if (sensor.shouldRecord()) {
            final long startNs = time.nanoseconds();
            try {
                return action.get();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        } else {
            return action.get();
        }
    }

    public static void maybeMeasureLatency(final Runnable runnable,
                                           final Time time,
                                           final Sensor sensor) {
        if (sensor.shouldRecord()) {
            final long startNs = time.nanoseconds();
            try {
                runnable.run();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        } else {
            runnable.run();
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

    public final void removeAllStoreLevelSensors(final String taskName, final String storeName) {
        final String key = storeSensorPrefix(taskName, storeName);
        synchronized (storeLevelSensors) {
            final Deque<String> sensors = storeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    // -------- thread level sensors ----------- //

    public static Map<String, String> threadLevelTagMap(final String threadName) {
        return Collections.singletonMap(THREAD_ID_TAG, threadName);
    }

    private static String threadSensorPrefix(final String threadName) {
        return INTERNAL_STRING + SENSOR_PREFIX_DELIMITER + threadName;
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
        return mkMap(mkEntry(THREAD_ID_TAG, threadName), mkEntry(TASK_ID_TAG, taskName));
    }

    private String taskSensorPrefix(final String threadName, final String taskName) {
        return threadSensorPrefix(threadName) + SENSOR_PREFIX_DELIMITER + TASK_STRING + SENSOR_PREFIX_DELIMITER + taskName;
    }

    public final Sensor taskLevelSensor(final String sensorName,
                                        final String taskName,
                                        final Sensor.RecordingLevel recordingLevel) {
        final String fullSensorName = taskSensorPrefix(Thread.currentThread().getName(), taskName) + SENSOR_NAME_DELIMITER + sensorName;
        return metrics.sensor(fullSensorName, recordingLevel);
    }

    // -------- processor-node level sensors ----------- //

    public static Map<String, String> nodeLevelTagMap(final String threadName, final String taskName, final String processorNodeName) {
        return mkMap(mkEntry(THREAD_ID_TAG, threadName),
                     mkEntry(TASK_ID_TAG, taskName),
                     mkEntry(PROCESSOR_NODE_ID_TAG, processorNodeName));
    }

    private String nodeSensorPrefix(final String taskName, final String processorNodeName) {
        return taskSensorPrefix(Thread.currentThread().getName(), taskName) + SENSOR_PREFIX_DELIMITER + NODE_STRING + SENSOR_PREFIX_DELIMITER + processorNodeName;
    }

    public final Sensor nodeLevelSensor(final String sensorName,
                                        final String processorNodeName,
                                        final String taskName,
                                        final Sensor.RecordingLevel recordingLevel,
                                        final Sensor... parentSenors) {
        final String fullSensorName = nodeSensorPrefix(taskName, processorNodeName) + SENSOR_NAME_DELIMITER + sensorName;

        return metrics.sensor(fullSensorName, recordingLevel, parentSenors);
    }

    // -------- state-store level sensors ----------- //

    public static Map<String, String> storeLevelTagMap(final String taskName, final String storeScopeName, final String storeName) {
        return mkMap(mkEntry(THREAD_ID_TAG, Thread.currentThread().getName()),
            mkEntry(TASK_ID_TAG, taskName),
            mkEntry(storeScopeName, storeName));
    }

    private String storeSensorPrefix(final String taskName, final String storeName) {
        return taskSensorPrefix(Thread.currentThread().getName(), taskName) + SENSOR_PREFIX_DELIMITER + STORE_STRING + SENSOR_PREFIX_DELIMITER + storeName;
    }

    public final Sensor storeLevelSensor(final String taskName,
                                         final String storeName,
                                         final String sensorName,
                                         final Sensor.RecordingLevel recordingLevel) {
        final String fullSensorName = storeSensorPrefix(taskName, storeName) + SENSOR_NAME_DELIMITER + sensorName;

        return metrics.sensor(fullSensorName, recordingLevel);
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
                operation + AVG_SUFFIX,
                group,
                "The average value of " + operation,
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + MAX_SUFFIX,
                group,
                "The max value of " + operation,
                tags),
            new Max()
        );
    }

    public static void addCurrentValue(final Sensor sensor,
                                       final String group,
                                       final Map<String, String> tags,
                                       final String operation) {
        sensor.add(
            new MetricName(
                operation + CURRENT_SUFFIX,
                group,
                "The current value of " + operation,
                tags
            ),
            new Value()
        );
    }

    public static void addInvocationRate(final Sensor sensor,
                                         final String group,
                                         final Map<String, String> tags,
                                         final String operation) {
        sensor.add(
            new MetricName(
                operation + RATE_SUFFIX,
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
                operation + TOTAL_SUFFIX,
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
