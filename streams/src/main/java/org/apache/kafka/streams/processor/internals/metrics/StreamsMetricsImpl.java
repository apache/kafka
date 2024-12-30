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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class StreamsMetricsImpl implements StreamsMetrics {

    public enum Version {
        LATEST
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
    private final String processId;

    private final Version version;
    private final Deque<MetricName> clientLevelMetrics = new LinkedList<>();
    private final Deque<String> clientLevelSensors = new LinkedList<>();
    private final Map<String, Deque<MetricName>> threadLevelMetrics = new HashMap<>();
    private final Map<String, Deque<String>> threadLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> taskLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> nodeLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> topicLevelSensors = new HashMap<>();
    private final Map<String, Deque<String>> cacheLevelSensors = new HashMap<>();
    private final ConcurrentMap<String, Deque<String>> storeLevelSensors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Deque<MetricName>> storeLevelMetrics = new ConcurrentHashMap<>();

    private final RocksDBMetricsRecordingTrigger rocksDBMetricsRecordingTrigger;

    private static final String SENSOR_PREFIX_DELIMITER = ".";
    private static final String SENSOR_NAME_DELIMITER = ".s.";
    private static final String SENSOR_TASK_LABEL = "task";
    private static final String SENSOR_NODE_LABEL = "node";
    private static final String SENSOR_TOPIC_LABEL = "topic";
    private static final String SENSOR_CACHE_LABEL = "cache";
    private static final String SENSOR_STORE_LABEL = "store";
    private static final String SENSOR_ENTITY_LABEL = "entity";
    private static final String SENSOR_EXTERNAL_LABEL = "external";
    private static final String SENSOR_INTERNAL_LABEL = "internal";

    public static final String CLIENT_ID_TAG = "client-id";
    public static final String PROCESS_ID_TAG = "process-id";
    public static final String THREAD_ID_TAG = "thread-id";
    public static final String TASK_ID_TAG = "task-id";
    public static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";
    public static final String TOPIC_NAME_TAG = "topic";
    public static final String STORE_ID_TAG = "state-id";
    public static final String RECORD_CACHE_ID_TAG = "record-cache-id";

    public static final String ROLLUP_VALUE = "all";

    public static final String LATENCY_SUFFIX = "-latency";
    public static final String RECORDS_SUFFIX = "-records";
    public static final String AVG_SUFFIX = "-avg";
    public static final String MAX_SUFFIX = "-max";
    public static final String MIN_SUFFIX = "-min";
    public static final String RATE_SUFFIX = "-rate";
    public static final String TOTAL_SUFFIX = "-total";
    public static final String RATIO_SUFFIX = "-ratio";

    public static final String GROUP_PREFIX_WO_DELIMITER = "stream";
    public static final String GROUP_PREFIX = GROUP_PREFIX_WO_DELIMITER + "-";
    public static final String GROUP_SUFFIX = "-metrics";
    public static final String CLIENT_LEVEL_GROUP = GROUP_PREFIX_WO_DELIMITER + GROUP_SUFFIX;
    public static final String THREAD_LEVEL_GROUP = GROUP_PREFIX + "thread" + GROUP_SUFFIX;
    public static final String TASK_LEVEL_GROUP = GROUP_PREFIX + "task" + GROUP_SUFFIX;
    public static final String PROCESSOR_NODE_LEVEL_GROUP = GROUP_PREFIX + "processor-node" + GROUP_SUFFIX;
    public static final String TOPIC_LEVEL_GROUP = GROUP_PREFIX + "topic" + GROUP_SUFFIX;
    public static final String STATE_STORE_LEVEL_GROUP = GROUP_PREFIX + "state" + GROUP_SUFFIX;
    public static final String CACHE_LEVEL_GROUP = GROUP_PREFIX + "record-cache" + GROUP_SUFFIX;

    public static final String OPERATIONS = " operations";
    public static final String TOTAL_DESCRIPTION = "The total number of ";
    public static final String RATE_DESCRIPTION = "The average per-second number of ";
    public static final String RATIO_DESCRIPTION = "The fraction of time the thread spent on ";
    public static final String AVG_LATENCY_DESCRIPTION = "The average latency of ";
    public static final String MAX_LATENCY_DESCRIPTION = "The maximum latency of ";
    public static final String LATENCY_DESCRIPTION_SUFFIX = " in milliseconds";
    public static final String RATE_DESCRIPTION_PREFIX = "The average number of ";
    public static final String RATE_DESCRIPTION_SUFFIX = " per second";

    public static final String RECORD_E2E_LATENCY = "record-e2e-latency";
    public static final String RECORD_E2E_LATENCY_DESCRIPTION_SUFFIX =
        "end-to-end latency of a record, measuring by comparing the record timestamp with the "
            + "system time when it has been fully processed by the node";
    public static final String RECORD_E2E_LATENCY_AVG_DESCRIPTION = "The average " + RECORD_E2E_LATENCY_DESCRIPTION_SUFFIX;
    public static final String RECORD_E2E_LATENCY_MIN_DESCRIPTION = "The minimum " + RECORD_E2E_LATENCY_DESCRIPTION_SUFFIX;
    public static final String RECORD_E2E_LATENCY_MAX_DESCRIPTION = "The maximum " + RECORD_E2E_LATENCY_DESCRIPTION_SUFFIX;

    public StreamsMetricsImpl(final Metrics metrics,
                              final String clientId,
                              final String processId,
                              final Time time) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");
        this.metrics = metrics;
        this.clientId = clientId;
        this.processId = processId;
        version = Version.LATEST;
        rocksDBMetricsRecordingTrigger = new RocksDBMetricsRecordingTrigger(time);

        this.parentSensors = new HashMap<>();
    }

    public Version version() {
        return version;
    }

    public Metrics metricsRegistry() {
        return metrics;
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

    public <T> void addThreadLevelImmutableMetric(final String name,
        final String description,
        final String threadId,
        final T value) {
        final MetricName metricName = metrics.metricName(
            name, THREAD_LEVEL_GROUP, description, threadLevelTagMap(threadId));
        synchronized (threadLevelMetrics) {
            threadLevelMetrics.computeIfAbsent(
                threadSensorPrefix(threadId),
                tid -> new LinkedList<>()
            ).add(metricName);
            metrics.addMetric(metricName, new ImmutableMetricValue<>(value));
        }
    }

    public <T> void addThreadLevelMutableMetric(final String name,
                                                final String description,
                                                final String threadId,
                                                final Gauge<T> valueProvider) {
        final MetricName metricName = metrics.metricName(
            name, THREAD_LEVEL_GROUP, description, threadLevelTagMap(threadId));
        synchronized (threadLevelMetrics) {
            threadLevelMetrics.computeIfAbsent(
                threadSensorPrefix(threadId),
                tid -> new LinkedList<>()
            ).add(metricName);
            metrics.addMetric(metricName, valueProvider);
        }
    }

    public final Sensor clientLevelSensor(final String sensorName,
                                          final RecordingLevel recordingLevel,
                                          final Sensor... parents) {
        synchronized (clientLevelSensors) {
            final String fullSensorName = CLIENT_LEVEL_GROUP + SENSOR_NAME_DELIMITER + sensorName;
            final Sensor sensor = metrics.getSensor(fullSensorName);
            if (sensor == null) {
                clientLevelSensors.push(fullSensorName);
                return metrics.sensor(fullSensorName, recordingLevel, parents);
            }
            return sensor;
        }
    }

    public final Sensor threadLevelSensor(final String threadId,
                                          final String sensorSuffix,
                                          final RecordingLevel recordingLevel,
                                          final Sensor... parents) {
        final String sensorPrefix = threadSensorPrefix(threadId);
        synchronized (threadLevelSensors) {
            return getSensors(threadLevelSensors, sensorSuffix, sensorPrefix, recordingLevel, parents);
        }
    }

    private String threadSensorPrefix(final String threadId) {
        return SENSOR_INTERNAL_LABEL + SENSOR_PREFIX_DELIMITER + threadId;
    }

    public Map<String, String> clientLevelTagMap() {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(CLIENT_ID_TAG, clientId);
        tagMap.put(PROCESS_ID_TAG, processId);
        return tagMap;
    }

    public Map<String, String> threadLevelTagMap(final String threadId) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(THREAD_ID_TAG, threadId);
        return tagMap;
    }

    public final void removeAllClientLevelSensorsAndMetrics() {
        removeAllClientLevelSensors();
        removeAllClientLevelMetrics();
    }

    private void removeAllClientLevelMetrics() {
        synchronized (clientLevelMetrics) {
            while (!clientLevelMetrics.isEmpty()) {
                metrics.removeMetric(clientLevelMetrics.pop());
            }
        }
    }

    private void removeAllClientLevelSensors() {
        synchronized (clientLevelSensors) {
            while (!clientLevelSensors.isEmpty()) {
                metrics.removeSensor(clientLevelSensors.pop());
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

    public final void removeAllThreadLevelMetrics(final String threadId) {
        synchronized (threadLevelMetrics) {
            final Deque<MetricName> names = threadLevelMetrics.remove(threadSensorPrefix(threadId));
            while (names != null && !names.isEmpty()) {
                metrics.removeMetric(names.pop());
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

    public Map<String, String> topicLevelTagMap(final String threadId,
                                                final String taskName,
                                                final String processorNodeName,
                                                final String topicName) {
        final Map<String, String> tagMap = nodeLevelTagMap(threadId, taskName, processorNodeName);
        tagMap.put(TOPIC_NAME_TAG, topicName);
        return tagMap;
    }

    public Map<String, String> storeLevelTagMap(final String taskName,
                                                final String storeType,
                                                final String storeName) {
        final Map<String, String> tagMap = taskLevelTagMap(Thread.currentThread().getName(), taskName);
        tagMap.put(storeType + "-" + STORE_ID_TAG, storeName);
        return tagMap;
    }

    public final Sensor taskLevelSensor(final String threadId,
                                        final String taskId,
                                        final String sensorSuffix,
                                        final RecordingLevel recordingLevel,
                                        final Sensor... parents) {
        final String sensorPrefix = taskSensorPrefix(threadId, taskId);
        synchronized (taskLevelSensors) {
            return getSensors(taskLevelSensors, sensorSuffix, sensorPrefix, recordingLevel, parents);
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
                                  final String sensorSuffix,
                                  final Sensor.RecordingLevel recordingLevel,
                                  final Sensor... parents) {
        final String sensorPrefix = nodeSensorPrefix(threadId, taskId, processorNodeName);
        synchronized (nodeLevelSensors) {
            return getSensors(nodeLevelSensors, sensorSuffix, sensorPrefix, recordingLevel, parents);
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

    public Sensor topicLevelSensor(final String threadId,
                                   final String taskId,
                                   final String processorNodeName,
                                   final String topicName,
                                   final String sensorSuffix,
                                   final Sensor.RecordingLevel recordingLevel,
                                   final Sensor... parents) {
        final String sensorPrefix = topicSensorPrefix(threadId, taskId, processorNodeName, topicName);
        synchronized (topicLevelSensors) {
            return getSensors(topicLevelSensors, sensorSuffix, sensorPrefix, recordingLevel, parents);
        }
    }

    public final void removeAllTopicLevelSensors(final String threadId,
                                                 final String taskId,
                                                 final String processorNodeName,
                                                 final String topicName) {
        final String key = topicSensorPrefix(threadId, taskId, processorNodeName, topicName);
        synchronized (topicLevelSensors) {
            final Deque<String> sensors = topicLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private String topicSensorPrefix(final String threadId,
                                     final String taskId,
                                     final String processorNodeName,
                                     final String topicName) {
        return nodeSensorPrefix(threadId, taskId, processorNodeName)
            + SENSOR_PREFIX_DELIMITER + SENSOR_TOPIC_LABEL + SENSOR_PREFIX_DELIMITER + topicName;
    }

    public Sensor cacheLevelSensor(final String threadId,
                                   final String taskName,
                                   final String storeName,
                                   final String ratioName,
                                   final Sensor.RecordingLevel recordingLevel,
                                   final Sensor... parents) {
        // use ratio name as sensor suffix
        final String sensorPrefix = cacheSensorPrefix(threadId, taskName, storeName);
        synchronized (cacheLevelSensors) {
            return getSensors(cacheLevelSensors, ratioName, sensorPrefix, recordingLevel, parents);
        }
    }

    public Map<String, String> cacheLevelTagMap(final String threadId,
                                                final String taskId,
                                                final String storeName) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(THREAD_ID_TAG, threadId);
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

    public final Sensor storeLevelSensor(final String taskId,
                                         final String storeName,
                                         final String sensorSuffix,
                                         final RecordingLevel recordingLevel,
                                         final Sensor... parents) {
        final String sensorPrefix = storeSensorPrefix(Thread.currentThread().getName(), taskId, storeName);
            // since the keys in the map storeLevelSensors contain the name of the current thread and threads only
            // access keys in which their name is contained, the value in the maps do not need to be thread safe
            // and we can use a LinkedList here.
            // TODO: In future, we could use thread local maps since each thread will exclusively access the set of keys
            //  that contain its name. Similar is true for the other metric levels. Thread-level metrics need some
            //  special attention, since they are created before the thread is constructed. The creation of those
            //  metrics could be moved into the run() method of the thread.
        return getSensors(storeLevelSensors, sensorSuffix, sensorPrefix, recordingLevel, parents);
    }

    public <T> void addStoreLevelMutableMetric(final String taskId,
                                               final String metricsScope,
                                               final String storeName,
                                               final String name,
                                               final String description,
                                               final RecordingLevel recordingLevel,
                                               final Gauge<T> valueProvider) {
        final MetricName metricName = metrics.metricName(
            name,
            STATE_STORE_LEVEL_GROUP,
            description,
            storeLevelTagMap(taskId, metricsScope, storeName)
        );
        if (metrics.metric(metricName) == null) {
            metrics.addMetricIfAbsent(metricName, new MetricConfig().recordLevel(recordingLevel), valueProvider);
            final String key = storeSensorPrefix(Thread.currentThread().getName(), taskId, storeName);
            storeLevelMetrics.computeIfAbsent(key, ignored -> new LinkedList<>()).push(metricName);
        }
    }

    public final void removeAllStoreLevelSensorsAndMetrics(final String taskId,
                                                           final String storeName) {
        final String threadId = Thread.currentThread().getName();
        removeAllStoreLevelSensors(threadId, taskId, storeName);
        removeAllStoreLevelMetrics(threadId, taskId, storeName);
    }

    private void removeAllStoreLevelSensors(final String threadId,
                                            final String taskId,
                                            final String storeName) {
        final String key = storeSensorPrefix(threadId, taskId, storeName);
        final Deque<String> sensors = storeLevelSensors.remove(key);
        while (sensors != null && !sensors.isEmpty()) {
            metrics.removeSensor(sensors.pop());
        }
    }

    private void removeAllStoreLevelMetrics(final String threadId,
                                            final String taskId,
                                            final String storeName) {
        final String key = storeSensorPrefix(threadId, taskId, storeName);
        final Deque<MetricName> metricNames = storeLevelMetrics.remove(key);
        while (metricNames != null && !metricNames.isEmpty()) {
            metrics.removeMetric(metricNames.pop());
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

    private Map<String, String> customizedTags(final String threadId,
                                               final String scopeName,
                                               final String entityName,
                                               final String... tags) {
        final Map<String, String> tagMap = threadLevelTagMap(threadId);
        tagMap.put(scopeName + "-id", entityName);
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

    private Sensor customInvocationRateAndCountSensor(final String threadId,
                                                      final String groupName,
                                                      final String entityName,
                                                      final String operationName,
                                                      final Map<String, String> tags,
                                                      final Sensor.RecordingLevel recordingLevel) {
        final Sensor sensor = metrics.sensor(externalChildSensorName(threadId, operationName, entityName), recordingLevel);
        addInvocationRateAndCountToSensor(
            sensor,
            groupName,
            tags,
            operationName,
            RATE_DESCRIPTION_PREFIX + operationName + OPERATIONS + RATE_DESCRIPTION_SUFFIX,
            TOTAL_DESCRIPTION + operationName + OPERATIONS
        );
        return sensor;
    }

    @Override
    public Sensor addLatencyRateTotalSensor(final String scopeName,
                                            final String entityName,
                                            final String operationName,
                                            final Sensor.RecordingLevel recordingLevel,
                                            final String... tags) {
        final String threadId = Thread.currentThread().getName();
        final String group = groupNameFromScope(scopeName);
        final Map<String, String> tagMap = customizedTags(threadId, scopeName, entityName, tags);
        final Sensor sensor =
            customInvocationRateAndCountSensor(threadId, group, entityName, operationName, tagMap, recordingLevel);
        addAvgAndMaxToSensor(
            sensor,
            group,
            tagMap,
            operationName + LATENCY_SUFFIX,
            AVG_LATENCY_DESCRIPTION + operationName,
            MAX_LATENCY_DESCRIPTION + operationName
        );

        return sensor;
    }

    @Override
    public Sensor addRateTotalSensor(final String scopeName,
                                     final String entityName,
                                     final String operationName,
                                     final Sensor.RecordingLevel recordingLevel,
                                     final String... tags) {
        final String threadId = Thread.currentThread().getName();
        final Map<String, String> tagMap = customizedTags(threadId, scopeName, entityName, tags);
        return customInvocationRateAndCountSensor(
            threadId,
            groupNameFromScope(scopeName),
            entityName,
            operationName,
            tagMap,
            recordingLevel
        );
    }

    private String externalChildSensorName(final String threadId, final String operationName, final String entityName) {
        return SENSOR_EXTERNAL_LABEL + SENSOR_PREFIX_DELIMITER + threadId
            + SENSOR_PREFIX_DELIMITER + SENSOR_ENTITY_LABEL + SENSOR_PREFIX_DELIMITER + entityName
            + SENSOR_NAME_DELIMITER + operationName;
    }

    public static void addAvgAndMaxToSensor(final Sensor sensor,
                                            final String group,
                                            final Map<String, String> tags,
                                            final String gaugeName,
                                            final String descriptionOfAvg,
                                            final String descriptionOfMax) {
        sensor.add(
            new MetricName(
                gaugeName + AVG_SUFFIX,
                group,
                descriptionOfAvg,
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                gaugeName + MAX_SUFFIX,
                group,
                descriptionOfMax,
                tags),
            new Max()
        );
    }

    public static void addMinAndMaxToSensor(final Sensor sensor,
                                            final String group,
                                            final Map<String, String> tags,
                                            final String operation,
                                            final String descriptionOfMin,
                                            final String descriptionOfMax) {
        sensor.add(
            new MetricName(
                operation + MIN_SUFFIX,
                group,
                descriptionOfMin,
                tags),
            new Min()
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

    public static void addAvgAndMaxLatencyToSensor(final Sensor sensor,
                                                   final String group,
                                                   final Map<String, String> tags,
                                                   final String operation) {
        sensor.add(
            new MetricName(
                operation + "-latency-avg",
                group,
                AVG_LATENCY_DESCRIPTION + operation + " operation.",
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + "-latency-max",
                group,
                MAX_LATENCY_DESCRIPTION + operation + " operation.",
                tags),
            new Max()
        );
    }

    public static void addAvgAndMinAndMaxToSensor(final Sensor sensor,
                                                  final String group,
                                                  final Map<String, String> tags,
                                                  final String gaugeName,
                                                  final String descriptionOfAvg,
                                                  final String descriptionOfMin,
                                                  final String descriptionOfMax) {
        addAvgAndMaxToSensor(sensor, group, tags, gaugeName, descriptionOfAvg, descriptionOfMax);
        sensor.add(
            new MetricName(
                gaugeName + MIN_SUFFIX,
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
                                                         final String descriptionOfRate,
                                                         final String descriptionOfCount) {
        addInvocationRateToSensor(sensor, group, tags, operation, descriptionOfRate);
        sensor.add(
            new MetricName(
                operation + TOTAL_SUFFIX,
                group,
                descriptionOfCount,
                tags
            ),
            new CumulativeCount()
        );
    }

    public static void addInvocationRateToSensor(final Sensor sensor,
                                                 final String group,
                                                 final Map<String, String> tags,
                                                 final String operation,
                                                 final String descriptionOfRate) {
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

    public static void addTotalCountAndSumMetricsToSensor(final Sensor sensor,
                                                          final String group,
                                                          final Map<String, String> tags,
                                                          final String countMetricNamePrefix,
                                                          final String sumMetricNamePrefix,
                                                          final String descriptionOfCount,
                                                          final String descriptionOfTotal) {
        sensor.add(
            new MetricName(countMetricNamePrefix + TOTAL_SUFFIX, group, descriptionOfCount, tags),
            new CumulativeCount()
        );
        sensor.add(
            new MetricName(sumMetricNamePrefix + TOTAL_SUFFIX, group, descriptionOfTotal, tags),
            new CumulativeSum()
        );
    }

    public static void maybeRecordSensor(final double value,
                                         final Time time,
                                         final Sensor sensor) {
        if (sensor.shouldRecord() && sensor.hasMetrics()) {
            sensor.record(value, time.milliseconds());
        }
    }

    public static void maybeMeasureLatency(final Runnable actionToMeasure,
                                           final Time time,
                                           final Sensor sensor) {
        if (sensor.shouldRecord() && sensor.hasMetrics()) {
            final long startNs = time.nanoseconds();
            try {
                actionToMeasure.run();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        } else {
            actionToMeasure.run();
        }
    }

    public static <T> T maybeMeasureLatency(final Supplier<T> actionToMeasure,
                                            final Time time,
                                            final Sensor sensor) {
        if (sensor.shouldRecord() && sensor.hasMetrics()) {
            final long startNs = time.nanoseconds();
            try {
                return actionToMeasure.get();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        } else {
            return actionToMeasure.get();
        }
    }

    private Sensor getSensors(final Map<String, Deque<String>> sensors,
                              final String sensorSuffix,
                              final String sensorPrefix,
                              final RecordingLevel recordingLevel,
                              final Sensor... parents) {
        final String fullSensorName = sensorPrefix + SENSOR_NAME_DELIMITER + sensorSuffix;
        final Sensor sensor = metrics.getSensor(fullSensorName);
        if (sensor == null) {
            sensors.computeIfAbsent(sensorPrefix, ignored -> new LinkedList<>()).push(fullSensorName);
            return metrics.sensor(fullSensorName, recordingLevel, parents);
        }
        return sensor;
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
