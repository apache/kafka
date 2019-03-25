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
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsMetrics;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class StreamsMetricsImpl implements StreamsMetrics {
    private final Metrics metrics;

    // delimiters
    public static final String SENSOR_PREFIX_DELIMITER = ".";
    public static final String SENSOR_NAME_DELIMITER = ".s.";

    // metric groups
    public static final String TASK_STRING = "task";
    public static final String NODE_STRING = "node";
    public static final String STORE_STRING = "store";
    public static final String CACHE_STRING = "cache";
    public static final String BUFFER_STRING = "buffer";
    public static final String INTERNAL_STRING = "internal";
    public static final String EXTERNAL_STRING = "external";
    public static final String ENTITY_STRING = "entity";

    // metric tag names;
    // for state stores the tags are constructed dynamically and hence not listed here
    public static final String THREAD_ID_TAG = "thread-id";
    public static final String TASK_ID_TAG = "task-id";
    public static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";
    public static final String RECORD_CACHE_ID_TAG = "record-cache-id";

    // metric name prefix and suffix
    public static final String AVG_SUFFIX = "-avg";
    public static final String MAX_SUFFIX = "-max";
    public static final String MIN_SUFFIX = "-min";
    public static final String LATENCY_SUFFIX = "-latency";
    public static final String RATE_SUFFIX = "-rate";
    public static final String TOTAL_SUFFIX = "-total";
    public static final String CURRENT_SUFFIX = "-current";
    public static final String ID_SUFFIX = "-id";
    public static final String STREAM_PREFIX = "stream-";
    public static final String METRICS_SUFFIX = "-metrics";

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

    // -------- user-defined external sensors ----------- //

    private Map<String, String> constructTags(final String scopeName, final String entityName, final String... tags) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(scopeName + ID_SUFFIX, entityName);

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

    private String externalSensorName(final String operationName, final String entityName) {
        return EXTERNAL_STRING + SENSOR_PREFIX_DELIMITER + Thread.currentThread().getName()
            + SENSOR_PREFIX_DELIMITER + ENTITY_STRING + SENSOR_PREFIX_DELIMITER + entityName
            + SENSOR_NAME_DELIMITER + operationName;
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

    public static Map<String, String> storeLevelTagMap(final String taskName, final String storeScopeTag, final String storeName) {
        return mkMap(mkEntry(THREAD_ID_TAG, Thread.currentThread().getName()),
            mkEntry(TASK_ID_TAG, taskName),
            mkEntry(storeScopeTag, storeName));
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

    // -------- cache level sensors ----------- //

    public static Map<String, String> cacheLevelTagMap(final String taskName, final String cacheName) {
        return mkMap(mkEntry(THREAD_ID_TAG, Thread.currentThread().getName()),
            mkEntry(TASK_ID_TAG, taskName),
            mkEntry(RECORD_CACHE_ID_TAG, cacheName));
    }

    private String cacheSensorPrefix(final String taskName, final String cacheName) {
        return taskSensorPrefix(Thread.currentThread().getName(), taskName) + SENSOR_PREFIX_DELIMITER + CACHE_STRING + SENSOR_PREFIX_DELIMITER + cacheName;
    }

    public final Sensor cacheLevelSensor(final String taskName,
                                         final String cacheName,
                                         final String sensorName,
                                         final Sensor.RecordingLevel recordingLevel) {
        final String fullSensorName = cacheSensorPrefix(taskName, cacheName) + SENSOR_NAME_DELIMITER + sensorName;

        return metrics.sensor(fullSensorName, recordingLevel);
    }

    // -------- util functions ----------- //

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

    public static void addValueAvgMinMax(final Sensor sensor,
                                         final String group,
                                         final Map<String, String> tags,
                                         final String operation) {
        addValueAvgAndMax(sensor, group, tags, operation);

        sensor.add(
            new MetricName(
                operation + MIN_SUFFIX,
                group,
                "The min value of " + operation,
                tags),
            new Min()
        );
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

    private static void addInvocationRate(final Sensor sensor,
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

    public static String groupNameFromScope(final String scopeName) {
        return STREAM_PREFIX + scopeName + METRICS_SUFFIX;
    }
}
