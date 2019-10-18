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
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
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

    static class ImmutableMetricValue<T> implements Gauge<T> {
        private final T value;

        public ImmutableMetricValue(final T value) {
            this.value = value;
        }

        @Override
        public T value(final MetricConfig config, final long now) {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ImmutableMetricValue<?> that = (ImmutableMetricValue<?>) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    private final Metrics metrics;
    private final Map<Sensor, Sensor> parentSensors;
    private final String clientId;

    private final Version version;
    private final Deque<MetricName> clientLevelMetrics = new LinkedList<>();
    private final Map<String, Deque<String>> threadLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> taskLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> nodeLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> cacheLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> storeLevelSensors = new HashMap<>();

    private RocksDBMetricsRecordingTrigger rocksDBMetricsRecordingTrigger;

    private static final String SENSOR_PREFIX_DELIMITER = ".";
    private static final String SENSOR_NAME_DELIMITER = ".s.";
    private static final String SENSOR_TASK_LABEL = "task";
    private static final String SENSOR_NODE_LABEL = "node";
    private static final String SENSOR_CACHE_LABEL = "cache";
    private static final String SENSOR_STORE_LABEL = "store";
    private static final String SENSOR_ENTITY_LABEL = "entity";
    private static final String SENSOR_EXTERNAL_LABEL = "external";
    private static final String SENSOR_INTERNAL_LABEL = "internal";

    public static final String CLIENT_ID_TAG = "client-id";
    public static final String THREAD_ID_TAG = "thread-id";
    public static final String THREAD_ID_TAG_0100_TO_23 = "client-id";
    public static final String TASK_ID_TAG = "task-id";
    public static final String STORE_ID_TAG = "state-id";
    public static final String BUFFER_ID_TAG = "buffer-id";
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
    public static final String CLIENT_LEVEL_GROUP = GROUP_PREFIX_WO_DELIMITER + GROUP_SUFFIX;
    public static final String THREAD_LEVEL_GROUP = GROUP_PREFIX_WO_DELIMITER + GROUP_SUFFIX;
    public static final String TASK_LEVEL_GROUP = GROUP_PREFIX + "task" + GROUP_SUFFIX;
    public static final String STATE_LEVEL_GROUP = GROUP_PREFIX + "state" + GROUP_SUFFIX;
    public static final String CACHE_LEVEL_GROUP = GROUP_PREFIX + "record-cache" + GROUP_SUFFIX;

    public static final String PROCESSOR_NODE_METRICS_GROUP = "stream-processor-node-metrics";
    public static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";

    public static final String EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";
    public static final String LATE_RECORD_DROP = "late-record-drop";

    public StreamsMetricsImpl(final Metrics metrics, final String clientId, final String builtInMetricsVersion) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");
        Objects.requireNonNull(builtInMetricsVersion, "Built-in metrics version cannot be null");
        this.metrics = metrics;
        this.clientId = clientId;
        version = parseBuiltInMetricsVersion(builtInMetricsVersion);

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

    public <T> void addClientLevelImmutableMetric(final String name,
                                                  final String description,
                                                  final RecordingLevel recordingLevel,
                                                  final T value) {
        final MetricName metricName = metrics.metricName(name, CLIENT_LEVEL_GROUP, description, clientLevelTagMap());
        final MetricConfig metricConfig = new MetricConfig().recordLevel(recordingLevel);
        synchronized (clientLevelMetrics) {
            metrics.addMetric(metricName, metricConfig, new ImmutableMetricValue<>(value));
            clientLevelMetrics.push(metricName);
        }
    }

    public <T> void addClientLevelMutableMetric(final String name,
                                                final String description,
                                                final RecordingLevel recordingLevel,
                                                final Gauge<T> valueProvider) {
        final MetricName metricName = metrics.metricName(name, CLIENT_LEVEL_GROUP, description, clientLevelTagMap());
        final MetricConfig metricConfig = new MetricConfig().recordLevel(recordingLevel);
        synchronized (clientLevelMetrics) {
            metrics.addMetric(metricName, metricConfig, valueProvider);
            clientLevelMetrics.push(metricName);
        }
    }

    public final Sensor threadLevelSensor(final String threadId,
                                          final String sensorName,
                                          final RecordingLevel recordingLevel,
                                          final Sensor... parents) {
        final String key = threadSensorPrefix(threadId);
        synchronized (threadLevelSensors) {
            threadLevelSensors.putIfAbsent(key, new LinkedList<>());
            final String fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;
            final Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);
            threadLevelSensors.get(key).push(fullSensorName);
            return sensor;
        }
    }

    private String threadSensorPrefix(final String threadId) {
        return SENSOR_INTERNAL_LABEL + SENSOR_PREFIX_DELIMITER + threadId;
    }

    public Map<String, String> clientLevelTagMap() {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(CLIENT_ID_TAG, clientId);
        return tagMap;
    }

    public Map<String, String> threadLevelTagMap(final String threadId) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(version == Version.LATEST ? THREAD_ID_TAG : THREAD_ID_TAG_0100_TO_23, threadId);
        return tagMap;
    }

    public Map<String, String> threadLevelTagMap(final String threadId, final String... tags) {
        final Map<String, String> tagMap = threadLevelTagMap(threadId);
        return addTags(tagMap, tags);
    }

    public final void removeAllClientLevelMetrics() {
        synchronized (clientLevelMetrics) {
            while (!clientLevelMetrics.isEmpty()) {
                metrics.removeMetric(clientLevelMetrics.pop());
            }
        }
    }

    public final void removeAllThreadLevelSensors(final String threadId) {
        final String key = threadSensorPrefix(threadId);
        synchronized (threadLevelSensors) {
            final Deque<String> sensors = threadLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    public Map<String, String> taskLevelTagMap(final String threadId, final String taskId) {
        final Map<String, String> tagMap = threadLevelTagMap(threadId);
        tagMap.put(TASK_ID_TAG, taskId);
        return tagMap;
    }


    public Map<String, String> nodeLevelTagMap(final String threadId,
                                               final String taskName,
                                               final String processorNodeName) {
        final Map<String, String> tagMap = taskLevelTagMap(threadId, taskName);
        tagMap.put(PROCESSOR_NODE_ID_TAG, processorNodeName);
        return tagMap;
    }

    public Map<String, String> storeLevelTagMap(final String threadId,
                                                final String taskName,
                                                final String storeType,
                                                final String storeName) {
        final Map<String, String> tagMap = taskLevelTagMap(threadId, taskName);
        tagMap.put(storeType + "-" + STORE_ID_TAG, storeName);
        return tagMap;
    }

    public Map<String, String> bufferLevelTagMap(final String threadId,
                                                 final String taskName,
                                                 final String bufferName) {
        final Map<String, String> tagMap = taskLevelTagMap(threadId, taskName);
        tagMap.put(BUFFER_ID_TAG, bufferName);
        return tagMap;
    }

    private Map<String, String> addTags(final Map<String, String> tagMap,
                                        final String... tags) {
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

    public final Sensor taskLevelSensor(final String threadId,
                                        final String taskId,
                                        final String sensorName,
                                        final RecordingLevel recordingLevel,
                                        final Sensor... parents) {
        final String key = taskSensorPrefix(threadId, taskId);
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

    public final void removeAllTaskLevelSensors(final String threadId, final String taskId) {
        final String key = taskSensorPrefix(threadId, taskId);
        synchronized (taskLevelSensors) {
            final Deque<String> sensors = taskLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private String taskSensorPrefix(final String threadId, final String taskId) {
        return threadSensorPrefix(threadId) + SENSOR_PREFIX_DELIMITER + SENSOR_TASK_LABEL + SENSOR_PREFIX_DELIMITER +
            taskId;
    }

    public Sensor nodeLevelSensor(final String threadId,
                                  final String taskId,
                                  final String processorNodeName,
                                  final String sensorName,
                                  final Sensor.RecordingLevel recordingLevel,
                                  final Sensor... parents) {
        final String key = nodeSensorPrefix(threadId, taskId, processorNodeName);
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

    public final void removeAllNodeLevelSensors(final String threadId,
                                                final String taskId,
                                                final String processorNodeName) {
        final String key = nodeSensorPrefix(threadId, taskId, processorNodeName);
        synchronized (nodeLevelSensors) {
            final Deque<String> sensors = nodeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private String nodeSensorPrefix(final String threadId, final String taskId, final String processorNodeName) {
        return taskSensorPrefix(threadId, taskId)
            + SENSOR_PREFIX_DELIMITER + SENSOR_NODE_LABEL + SENSOR_PREFIX_DELIMITER + processorNodeName;
    }

    public Sensor cacheLevelSensor(final String threadId,
                                   final String taskName,
                                   final String storeName,
                                   final String sensorName,
                                   final Sensor.RecordingLevel recordingLevel,
                                   final Sensor... parents) {
        final String key = cacheSensorPrefix(threadId, taskName, storeName);
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

    public Map<String, String> cacheLevelTagMap(final String threadId,
                                                final String taskId,
                                                final String storeName) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        if (version == Version.FROM_100_TO_23) {
            tagMap.put(THREAD_ID_TAG_0100_TO_23, threadId);
        } else {
            tagMap.put(THREAD_ID_TAG, threadId);
        }
        tagMap.put(TASK_ID_TAG, taskId);
        tagMap.put(RECORD_CACHE_ID_TAG, storeName);
        return tagMap;
    }

    public final void removeAllCacheLevelSensors(final String threadId, final String taskId, final String cacheName) {
        final String key = cacheSensorPrefix(threadId, taskId, cacheName);
        synchronized (cacheLevelSensors) {
            final Deque<String> strings = cacheLevelSensors.remove(key);
            while (strings != null && !strings.isEmpty()) {
                metrics.removeSensor(strings.pop());
            }
        }
    }

    private String cacheSensorPrefix(final String threadId, final String taskId, final String cacheName) {
        return taskSensorPrefix(threadId, taskId)
            + SENSOR_PREFIX_DELIMITER + SENSOR_CACHE_LABEL + SENSOR_PREFIX_DELIMITER + cacheName;
    }

    public final Sensor storeLevelSensor(final String threadId,
                                         final String taskId,
                                         final String storeName,
                                         final String sensorName,
                                         final Sensor.RecordingLevel recordingLevel,
                                         final Sensor... parents) {
        final String key = storeSensorPrefix(threadId, taskId, storeName);
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

    public final void removeAllStoreLevelSensors(final String threadId,
                                                 final String taskId,
                                                 final String storeName) {
        final String key = storeSensorPrefix(threadId, taskId, storeName);
        synchronized (storeLevelSensors) {
            final Deque<String> sensors = storeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private String storeSensorPrefix(final String threadId,
                                     final String taskId,
                                     final String storeName) {
        return taskSensorPrefix(threadId, taskId)
            + SENSOR_PREFIX_DELIMITER + SENSOR_STORE_LABEL + SENSOR_PREFIX_DELIMITER + storeName;
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

    private Map<String, String> constructTags(final String threadId,
                                              final String scopeName,
                                              final String entityName,
                                              final String... tags) {
        final String[] updatedTags = Arrays.copyOf(tags, tags.length + 2);
        updatedTags[tags.length] = scopeName + "-id";
        updatedTags[tags.length + 1] = entityName;
        return threadLevelTagMap(threadId, updatedTags);
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

        final String threadId = Thread.currentThread().getName();
        final Map<String, String> tagMap = constructTags(threadId, scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(threadId, scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(externalParentSensorName(threadId, operationName), recordingLevel);
        addAvgAndMaxLatencyToSensor(parent, group, allTagMap, operationName);
        addInvocationRateAndCountToSensor(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(
            externalChildSensorName(threadId, operationName, entityName),
            recordingLevel,
            parent
        );
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

        final String threadId = Thread.currentThread().getName();
        final Map<String, String> tagMap = constructTags(threadId, scopeName, entityName, tags);
        final Map<String, String> allTagMap = constructTags(threadId, scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        final Sensor parent = metrics.sensor(
            externalParentSensorName(threadId, operationName),
            recordingLevel
        );
        addInvocationRateAndCountToSensor(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        final Sensor sensor = metrics.sensor(
            externalChildSensorName(threadId, operationName, entityName),
            recordingLevel,
            parent
        );
        addInvocationRateAndCountToSensor(sensor, group, tagMap, operationName);

        parentSensors.put(sensor, parent);

        return sensor;

    }

    private String externalChildSensorName(final String threadId, final String operationName, final String entityName) {
        return SENSOR_EXTERNAL_LABEL + SENSOR_PREFIX_DELIMITER + threadId
            + SENSOR_PREFIX_DELIMITER + SENSOR_ENTITY_LABEL + SENSOR_PREFIX_DELIMITER + entityName
            + SENSOR_NAME_DELIMITER + operationName;
    }

    private String externalParentSensorName(final String threadId, final String operationName) {
        return SENSOR_EXTERNAL_LABEL + SENSOR_PREFIX_DELIMITER + threadId + SENSOR_NAME_DELIMITER + operationName;
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
