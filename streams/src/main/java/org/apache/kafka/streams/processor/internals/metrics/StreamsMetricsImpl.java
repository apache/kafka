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
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.metrics.stats.WindowedSum;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;

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

    public enum Version {
        LATEST,
        FROM_100_TO_23
    }

    private final Metrics metrics;
    private final Map<Sensor, Sensor> parentSensors;
    private final String threadName;

    private final Version version;
    private final Deque<String> threadLevelSensors = new LinkedList<>();
    private final Map<String, Deque<String>> taskLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> nodeLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> cacheLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> storeLevelSensors = new HashMap<>();

    private RocksDBMetricsRecordingTrigger rocksDBMetricsRecordingTrigger;

    private static final String SENSOR_PREFIX_DELIMITER = ".";
    private static final String SENSOR_NAME_DELIMITER = ".s.";

    public static final String THREAD_ID_TAG = "thread-id";
    public static final String THREAD_ID_TAG_0100_TO_23 = "client-id";
    public static final String TASK_ID_TAG = "task-id";
    public static final String STORE_ID_TAG = "state-id";
    public static final String RECORD_CACHE_ID_TAG = "record-cache-id";

    public static final String ROLLUP_VALUE = "all";

    public static final String LATENCY_SUFFIX = "-latency";
    public static final String AVG_SUFFIX = "-avg";
    public static final String MAX_SUFFIX = "-max";
    public static final String MIN_SUFFIX = "-min";
    public static final String RATE_SUFFIX = "-rate";
    public static final String TOTAL_SUFFIX = "-total";
    public static final String RATIO_SUFFIX = "-ratio";

    public static final String AVG_VALUE_DOC = "The average value of ";
    public static final String MAX_VALUE_DOC = "The maximum value of ";
    public static final String AVG_LATENCY_DOC = "The average latency of ";
    public static final String MAX_LATENCY_DOC = "The maximum latency of ";

    public static final String GROUP_PREFIX_WO_DELIMITER = "stream";
    public static final String GROUP_PREFIX = GROUP_PREFIX_WO_DELIMITER + "-";
    public static final String GROUP_SUFFIX = "-metrics";
    public static final String STATE_LEVEL_GROUP_SUFFIX = "-state" + GROUP_SUFFIX;
    public static final String THREAD_LEVEL_GROUP = GROUP_PREFIX_WO_DELIMITER + GROUP_SUFFIX;
    public static final String TASK_LEVEL_GROUP = GROUP_PREFIX + "task" + GROUP_SUFFIX;
    public static final String STATE_LEVEL_GROUP = GROUP_PREFIX + "state" + GROUP_SUFFIX;
    public static final String CACHE_LEVEL_GROUP = GROUP_PREFIX + "record-cache" + GROUP_SUFFIX;

    public static final String PROCESSOR_NODE_METRICS_GROUP = "stream-processor-node-metrics";
    public static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";

    public static final String EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";
    public static final String LATE_RECORD_DROP = "late-record-drop";

    public StreamsMetricsImpl(final Metrics metrics, final String threadName, final String builtInMetricsVersion) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");
        Objects.requireNonNull(builtInMetricsVersion, "Built-in metrics version cannot be null");
        this.metrics = metrics;
        this.threadName = threadName;
        this.version = parseBuiltInMetricsVersion(builtInMetricsVersion);

        this.parentSensors = new HashMap<>();
    }

    private static Version parseBuiltInMetricsVersion(final String builtInMetricsVersion) {
        if (builtInMetricsVersion.equals(StreamsConfig.METRICS_LATEST)) {
            return Version.LATEST;
        } else {
            return Version.FROM_100_TO_23;
        }
    }

    public Version version() {
        return version;
    }

    public void setRocksDBMetricsRecordingTrigger(final RocksDBMetricsRecordingTrigger rocksDBMetricsRecordingTrigger) {
        this.rocksDBMetricsRecordingTrigger = rocksDBMetricsRecordingTrigger;
    }

    public RocksDBMetricsRecordingTrigger rocksDBMetricsRecordingTrigger() {
        return rocksDBMetricsRecordingTrigger;
    }

    public final Sensor threadLevelSensor(final String sensorName,
                                          final RecordingLevel recordingLevel,
                                          final Sensor... parents) {
        synchronized (threadLevelSensors) {
            final String fullSensorName = threadSensorPrefix() + SENSOR_NAME_DELIMITER + sensorName;
            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);
            threadLevelSensors.push(fullSensorName);
            return sensor;
        }
    }

    private String threadSensorPrefix() {
        return "internal" + SENSOR_PREFIX_DELIMITER + threadName;
    }

    public Map<String, String> threadLevelTagMap() {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(THREAD_ID_TAG_0100_TO_23, threadName);
        return tagMap;
    }

    public Map<String, String> threadLevelTagMap(final String... tags) {
        final Map<String, String> tagMap = threadLevelTagMap();
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

    public final void removeAllThreadLevelSensors() {
        synchronized (threadLevelSensors) {
            while (!threadLevelSensors.isEmpty()) {
                metrics.removeSensor(threadLevelSensors.pop());
            }
        }
    }

    public Map<String, String> taskLevelTagMap(final String taskName) {
        final Map<String, String> tagMap = threadLevelTagMap();
        tagMap.put(TASK_ID_TAG, taskName);
        return tagMap;
    }

    public Map<String, String> storeLevelTagMap(final String taskName, final String storeType, final String storeName) {
        final Map<String, String> tagMap = taskLevelTagMap(taskName);
        tagMap.put(storeType + "-" + STORE_ID_TAG, storeName);
        return tagMap;
    }

    public final Sensor taskLevelSensor(final String taskName,
                                        final String sensorName,
                                        final RecordingLevel recordingLevel,
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

    public Sensor cacheLevelSensor(final String taskName,
                                   final String storeName,
                                   final String sensorName,
                                   final Sensor.RecordingLevel recordingLevel,
                                   final Sensor... parents) {
        final String key = cacheSensorPrefix(taskName, storeName);
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

    public Map<String, String> cacheLevelTagMap(final String taskName, final String storeName) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(TASK_ID_TAG, taskName);
        tagMap.put(RECORD_CACHE_ID_TAG, storeName);
        if (version == Version.FROM_100_TO_23) {
            tagMap.put(THREAD_ID_TAG_0100_TO_23, Thread.currentThread().getName());
        } else {
            tagMap.put(THREAD_ID_TAG, Thread.currentThread().getName());
        }
        return tagMap;
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
        addAvgAndMaxLatencyToSensor(parent, group, allTagMap, operationName);
        addInvocationRateAndCountToSensor(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, parent);
        addAvgAndMaxLatencyToSensor(sensor, group, tagMap, operationName);
        addInvocationRateAndCountToSensor(sensor, group, tagMap, operationName);

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
        addInvocationRateAndCountToSensor(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, parent);
        addInvocationRateAndCountToSensor(sensor, group, tagMap, operationName);

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


    private static void addAvgAndMaxToSensor(final Sensor sensor,
                                            final String group,
                                            final Map<String, String> tags,
                                            final String operation,
                                            final String descriptionOfAvg,
                                            final String descriptionOfMax) {
        sensor.add(
            new MetricName(
                operation + AVG_SUFFIX,
                group,
                descriptionOfAvg,
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + MAX_SUFFIX,
                group,
                descriptionOfMax,
                tags),
            new Max()
        );
    }

    public static void addAvgAndMaxToSensor(final Sensor sensor,
                                            final String group,
                                            final Map<String, String> tags,
                                            final String operation) {
        addAvgAndMaxToSensor(
            sensor,
            group,
            tags,
            operation,
            AVG_VALUE_DOC + operation + ".",
            MAX_VALUE_DOC + operation + "."
        );
    }

    public static void addAvgAndMaxLatencyToSensor(final Sensor sensor,
                                                   final String group,
                                                   final Map<String, String> tags,
                                                   final String operation) {
        sensor.add(
            new MetricName(
                operation + "-latency-avg",
                group,
                AVG_LATENCY_DOC + operation + " operation.",
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + "-latency-max",
                group,
                MAX_LATENCY_DOC + operation + " operation.",
                tags),
            new Max()
        );
    }

    public static void addAvgAndMinAndMaxToSensor(final Sensor sensor,
                                                  final String group,
                                                  final Map<String, String> tags,
                                                  final String operation,
                                                  final String descriptionOfAvg,
                                                  final String descriptionOfMin,
                                                  final String descriptionOfMax) {
        addAvgAndMaxToSensor(sensor, group, tags, operation, descriptionOfAvg, descriptionOfMax);
        sensor.add(
            new MetricName(
                operation + MIN_SUFFIX,
                group,
                descriptionOfMin,
                tags),
            new Min()
        );
    }

    public static void addInvocationRateAndCountToSensor(final Sensor sensor,
                                                         final String group,
                                                         final Map<String, String> tags,
                                                         final String operation,
                                                         final String descriptionOfInvocation,
                                                         final String descriptionOfRate) {
        sensor.add(
            new MetricName(
                operation + TOTAL_SUFFIX,
                group,
                descriptionOfInvocation,
                tags
            ),
            new CumulativeCount()
        );
        sensor.add(
            new MetricName(
                operation + RATE_SUFFIX,
                group,
                descriptionOfRate,
                tags
            ),
            new Rate(TimeUnit.SECONDS, new WindowedCount())
        );
    }

    public static void addInvocationRateAndCountToSensor(final Sensor sensor,
                                                         final String group,
                                                         final Map<String, String> tags,
                                                         final String operation) {
        addInvocationRateAndCountToSensor(
            sensor,
            group,
            tags,
            operation,
            "The total number of " + operation,
            "The average per-second number of " + operation
        );
    }

    public static void addRateOfSumAndSumMetricsToSensor(final Sensor sensor,
                                                         final String group,
                                                         final Map<String, String> tags,
                                                         final String operation,
                                                         final String descriptionOfRate,
                                                         final String descriptionOfTotal) {
        addRateOfSumMetricToSensor(sensor, group, tags, operation, descriptionOfRate);
        addSumMetricToSensor(sensor, group, tags, operation, descriptionOfTotal);
    }

    public static void addRateOfSumMetricToSensor(final Sensor sensor,
                                                  final String group,
                                                  final Map<String, String> tags,
                                                  final String operation,
                                                  final String description) {
        sensor.add(new MetricName(operation + RATE_SUFFIX, group, description, tags),
                   new Rate(TimeUnit.SECONDS, new WindowedSum()));
    }

    public static void addSumMetricToSensor(final Sensor sensor,
                                            final String group,
                                            final Map<String, String> tags,
                                            final String operation,
                                            final String description) {
        addSumMetricToSensor(sensor, group, tags, operation, true, description);
    }

    public static void addSumMetricToSensor(final Sensor sensor,
                                            final String group,
                                            final Map<String, String> tags,
                                            final String operation,
                                            final boolean withSuffix,
                                            final String description) {
        sensor.add(
            new MetricName(
                withSuffix ? operation + TOTAL_SUFFIX : operation,
                group,
                description,
                tags
            ),
            new CumulativeSum()
        );
    }

    public static void addValueMetricToSensor(final Sensor sensor,
                                              final String group,
                                              final Map<String, String> tags,
                                              final String name,
                                              final String description) {
        sensor.add(new MetricName(name, group, description, tags), new Value());
    }

    public static void addAvgAndSumMetricsToSensor(final Sensor sensor,
                                                   final String group,
                                                   final Map<String, String> tags,
                                                   final String metricNamePrefix,
                                                   final String descriptionOfAvg,
                                                   final String descriptionOfTotal) {
        sensor.add(new MetricName(metricNamePrefix + AVG_SUFFIX, group, descriptionOfAvg, tags), new Avg());
        sensor.add(
            new MetricName(metricNamePrefix + TOTAL_SUFFIX, group, descriptionOfTotal, tags),
            new CumulativeSum()
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
