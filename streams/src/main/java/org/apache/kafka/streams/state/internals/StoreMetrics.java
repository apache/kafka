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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ID_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;

public class StoreMetrics {

    private final StreamsMetricsImpl metrics;
    private final Map<String, String> tagMap;
    private final String metricsGroupName;
    private final List<Sensor> sensors;
    private final String storeName;
    private final String taskName;

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

    public static final String IN_MEMORY_STATE = "in-memory-state";
    public static final String IN_MEMORY_LRU = "in-memory-lru-state";
    public static final String WINDOW_IN_MEMORY = "in-memory-window-state";
    public static final String ROCKSDB_STATE = "rocksdb-state";
    public static final String WINDOW_ROCKSDB_STATE = "rocksdb-window-state";
    public static final String SESSION_ROCKSDB_STATE = "rocksdb-session-state";

    public StoreMetrics(final ProcessorContext context,
                        final String storeScopeName,
                        final String storeName,
                        final StreamsMetricsImpl metrics) {
        this.metrics = metrics;
        this.sensors = new ArrayList<>();
        this.storeName = storeName;
        this.taskName = context.taskId().toString();
        this.metricsGroupName = StreamsMetricsImpl.groupNameFromScope(storeScopeName);

        this.tagMap = StreamsMetricsImpl.storeLevelTagMap(taskName, storeScopeName + ID_SUFFIX, storeName);
    }

    // this function is not thread-safe, assuming its caller is single-threaded
    public Sensor addOperationLatencySensor(final String operation) {
        final Sensor sensor = metrics.storeLevelSensor(taskName, storeName, operation, Sensor.RecordingLevel.DEBUG);
        StreamsMetricsImpl.addValueAvgAndMax(sensor, metricsGroupName, tagMap, operation + LATENCY_SUFFIX);
        StreamsMetricsImpl.addInvocationRateAndCount(sensor, metricsGroupName, tagMap, operation);

        sensors.add(sensor);

        return sensor;
    }

    public Sensor addBufferSizeSensor() {
        final Sensor sensor = metrics.storeLevelSensor(taskName, storeName, SUPPRESSION_BUFFER_SIZE, Sensor.RecordingLevel.DEBUG);
        StreamsMetricsImpl.addValueAvgAndMax(sensor, metricsGroupName, tagMap, SUPPRESSION_BUFFER_SIZE);
        StreamsMetricsImpl.addCurrentValue(sensor, metricsGroupName, tagMap, SUPPRESSION_BUFFER_SIZE);

        sensors.add(sensor);
        return sensor;
    }

    public Sensor addBufferCountSensor() {
        final Sensor sensor = metrics.storeLevelSensor(taskName, storeName, SUPPRESSION_BUFFER_COUNT, Sensor.RecordingLevel.DEBUG);
        StreamsMetricsImpl.addValueAvgAndMax(sensor, metricsGroupName, tagMap, SUPPRESSION_BUFFER_COUNT);
        StreamsMetricsImpl.addCurrentValue(sensor, metricsGroupName, tagMap, SUPPRESSION_BUFFER_COUNT);

        sensors.add(sensor);
        return sensor;
    }

    public Sensor addExpiredRecordSensor() {
        final Sensor sensor = metrics.storeLevelSensor(taskName, storeName, EXPIRED_WINDOW_RECORD_DROP, Sensor.RecordingLevel.DEBUG);
        StreamsMetricsImpl.addInvocationRateAndCount(sensor, metricsGroupName, tagMap, EXPIRED_WINDOW_RECORD_DROP);

        sensors.add(sensor);
        return sensor;
    }

    public void clear() {
        for (final Sensor sensor : sensors) {
            metrics.removeSensor(sensor);
        }
    }
}
