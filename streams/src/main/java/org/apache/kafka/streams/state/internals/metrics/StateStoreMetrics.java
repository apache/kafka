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
package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_AVG_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_MAX_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_MIN_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMinAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateToSensor;

public class StateStoreMetrics {
    private StateStoreMetrics() {}

    private static final String AVG_DESCRIPTION_PREFIX = "The average ";
    private static final String MAX_DESCRIPTION_PREFIX = "The maximum ";
    private static final String LATENCY_DESCRIPTION = "latency of ";
    private static final String AVG_LATENCY_DESCRIPTION_PREFIX = AVG_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
    private static final String MAX_LATENCY_DESCRIPTION_PREFIX = MAX_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
    private static final String RATE_DESCRIPTION_PREFIX = "The average number of ";
    private static final String RATE_DESCRIPTION_SUFFIX = " per second";
    private static final String BUFFERED_RECORDS = "buffered records";

    private static final String PUT = "put";
    private static final String PUT_DESCRIPTION = "calls to put";
    private static final String PUT_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PUT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String PUT_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PUT_DESCRIPTION;
    private static final String PUT_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PUT_DESCRIPTION;

    private static final String PUT_IF_ABSENT = "put-if-absent";
    private static final String PUT_IF_ABSENT_DESCRIPTION = "calls to put-if-absent";
    private static final String PUT_IF_ABSENT_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String PUT_IF_ABSENT_AVG_LATENCY_DESCRIPTION =
        AVG_LATENCY_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION;
    private static final String PUT_IF_ABSENT_MAX_LATENCY_DESCRIPTION =
        MAX_LATENCY_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION;

    private static final String PUT_ALL = "put-all";
    private static final String PUT_ALL_DESCRIPTION = "calls to put-all";
    private static final String PUT_ALL_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String PUT_ALL_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION;
    private static final String PUT_ALL_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION;

    private static final String GET = "get";
    private static final String GET_DESCRIPTION = "calls to get";
    private static final String GET_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + GET_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String GET_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + GET_DESCRIPTION;
    private static final String GET_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + GET_DESCRIPTION;

    private static final String FETCH = "fetch";
    private static final String FETCH_DESCRIPTION = "calls to fetch";
    private static final String FETCH_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + FETCH_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String FETCH_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + FETCH_DESCRIPTION;
    private static final String FETCH_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + FETCH_DESCRIPTION;

    private static final String ALL = "all";
    private static final String ALL_DESCRIPTION = "calls to all";
    private static final String ALL_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + ALL_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String ALL_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + ALL_DESCRIPTION;
    private static final String ALL_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + ALL_DESCRIPTION;

    private static final String RANGE = "range";
    private static final String RANGE_DESCRIPTION = "calls to range";
    private static final String RANGE_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + RANGE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String RANGE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + RANGE_DESCRIPTION;
    private static final String RANGE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + RANGE_DESCRIPTION;

    private static final String PREFIX_SCAN = "prefix-scan";
    private static final String PREFIX_SCAN_DESCRIPTION = "calls to prefix-scan";
    private static final String PREFIX_SCAN_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PREFIX_SCAN_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String PREFIX_SCAN_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PREFIX_SCAN_DESCRIPTION;
    private static final String PREFIX_SCAN_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PREFIX_SCAN_DESCRIPTION;

    private static final String FLUSH = "flush";
    private static final String FLUSH_DESCRIPTION = "calls to flush";
    private static final String FLUSH_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String FLUSH_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION;
    private static final String FLUSH_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION;

    private static final String DELETE = "delete";
    private static final String DELETE_DESCRIPTION = "calls to delete";
    private static final String DELETE_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + DELETE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String DELETE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + DELETE_DESCRIPTION;
    private static final String DELETE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + DELETE_DESCRIPTION;

    private static final String REMOVE = "remove";
    private static final String REMOVE_DESCRIPTION = "calls to remove";
    private static final String REMOVE_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String REMOVE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION;
    private static final String REMOVE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION;

    private static final String RESTORE = "restore";
    private static final String RESTORE_DESCRIPTION = "restorations";
    private static final String RESTORE_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String RESTORE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION;
    private static final String RESTORE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION;

    private static final String SUPPRESSION_BUFFER_COUNT = "suppression-buffer-count";
    private static final String SUPPRESSION_BUFFER_COUNT_DESCRIPTION = "count of " + BUFFERED_RECORDS;
    private static final String SUPPRESSION_BUFFER_COUNT_AVG_DESCRIPTION =
        AVG_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_COUNT_DESCRIPTION;
    private static final String SUPPRESSION_BUFFER_COUNT_MAX_DESCRIPTION =
        MAX_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_COUNT_DESCRIPTION;

    private static final String SUPPRESSION_BUFFER_SIZE = "suppression-buffer-size";
    private static final String SUPPRESSION_BUFFER_SIZE_DESCRIPTION = "size of " + BUFFERED_RECORDS;
    private static final String SUPPRESSION_BUFFER_SIZE_AVG_DESCRIPTION =
        AVG_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_SIZE_DESCRIPTION;
    private static final String SUPPRESSION_BUFFER_SIZE_MAX_DESCRIPTION =
        MAX_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_SIZE_DESCRIPTION;

    private static final String EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";
    private static final String EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION = "dropped records due to an expired window";
    private static final String EXPIRED_WINDOW_RECORD_DROP_TOTAL_DESCRIPTION =
        TOTAL_DESCRIPTION + EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION;
    private static final String EXPIRED_WINDOW_RECORD_DROP_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    public static Sensor putSensor(final String taskId,
                                   final String storeType,
                                   final String storeName,
                                   final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            PUT,
            PUT_RATE_DESCRIPTION,
            PUT_AVG_LATENCY_DESCRIPTION,
            PUT_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor putIfAbsentSensor(final String taskId,
                                           final String storeType,
                                           final String storeName,
                                           final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            PUT_IF_ABSENT,
            PUT_IF_ABSENT_RATE_DESCRIPTION,
            PUT_IF_ABSENT_AVG_LATENCY_DESCRIPTION,
            PUT_IF_ABSENT_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor putAllSensor(final String taskId,
                                      final String storeType,
                                      final String storeName,
                                      final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            PUT_ALL,
            PUT_ALL_RATE_DESCRIPTION,
            PUT_ALL_AVG_LATENCY_DESCRIPTION,
            PUT_ALL_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor getSensor(final String taskId,
                                   final String storeType,
                                   final String storeName,
                                   final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            GET,
            GET_RATE_DESCRIPTION,
            GET_AVG_LATENCY_DESCRIPTION,
            GET_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor fetchSensor(final String taskId,
                                     final String storeType,
                                     final String storeName,
                                     final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            FETCH,
            FETCH_RATE_DESCRIPTION,
            FETCH_AVG_LATENCY_DESCRIPTION,
            FETCH_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor allSensor(final String taskId,
                                   final String storeType,
                                   final String storeName,
                                   final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            ALL,
            ALL_RATE_DESCRIPTION,
            ALL_AVG_LATENCY_DESCRIPTION,
            ALL_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor rangeSensor(final String taskId,
                                     final String storeType,
                                     final String storeName,
                                     final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            RANGE,
            RANGE_RATE_DESCRIPTION,
            RANGE_AVG_LATENCY_DESCRIPTION,
            RANGE_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor prefixScanSensor(final String taskId,
                                          final String storeType,
                                          final String storeName,
                                          final StreamsMetricsImpl streamsMetrics) {

        final String latencyMetricName = PREFIX_SCAN + LATENCY_SUFFIX;
        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(taskId, storeType, storeName);

        final Sensor sensor = streamsMetrics.storeLevelSensor(taskId, storeName, PREFIX_SCAN, RecordingLevel.DEBUG);
        addInvocationRateToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            tagMap,
            PREFIX_SCAN,
            PREFIX_SCAN_RATE_DESCRIPTION
        );
        addAvgAndMaxToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            tagMap,
            latencyMetricName,
            PREFIX_SCAN_AVG_LATENCY_DESCRIPTION,
            PREFIX_SCAN_MAX_LATENCY_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor flushSensor(final String taskId,
                                     final String storeType,
                                     final String storeName,
                                     final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            FLUSH,
            FLUSH_RATE_DESCRIPTION,
            FLUSH_AVG_LATENCY_DESCRIPTION,
            FLUSH_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor deleteSensor(final String taskId,
                                      final String storeType,
                                      final String storeName,
                                      final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            DELETE,
            DELETE_RATE_DESCRIPTION,
            DELETE_AVG_LATENCY_DESCRIPTION,
            DELETE_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor removeSensor(final String taskId,
                                      final String storeType,
                                      final String storeName,
                                      final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId,
            storeType,
            storeName,
            REMOVE,
            REMOVE_RATE_DESCRIPTION,
            REMOVE_AVG_LATENCY_DESCRIPTION,
            REMOVE_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor restoreSensor(final String taskId,
                                       final String storeType,
                                       final String storeName,
                                       final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
            taskId, storeType,
            storeName,
            RESTORE,
            RESTORE_RATE_DESCRIPTION,
            RESTORE_AVG_LATENCY_DESCRIPTION,
            RESTORE_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor expiredWindowRecordDropSensor(final String taskId,
                                                       final String storeType,
                                                       final String storeName,
                                                       final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.storeLevelSensor(
            taskId,
            storeName,
            EXPIRED_WINDOW_RECORD_DROP,
            RecordingLevel.INFO
        );
        addInvocationRateAndCountToSensor(
            sensor,
            "stream-" + storeType + "-metrics",
            streamsMetrics.storeLevelTagMap(taskId, storeType, storeName),
            EXPIRED_WINDOW_RECORD_DROP,
            EXPIRED_WINDOW_RECORD_DROP_RATE_DESCRIPTION,
            EXPIRED_WINDOW_RECORD_DROP_TOTAL_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor suppressionBufferCountSensor(final String taskId,
                                                      final String storeType,
                                                      final String storeName,
                                                      final StreamsMetricsImpl streamsMetrics) {
        return sizeOrCountSensor(
            taskId,
            storeType,
            storeName,
            SUPPRESSION_BUFFER_COUNT,
            SUPPRESSION_BUFFER_COUNT_AVG_DESCRIPTION,
            SUPPRESSION_BUFFER_COUNT_MAX_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor suppressionBufferSizeSensor(final String taskId,
                                                     final String storeType,
                                                     final String storeName,
                                                     final StreamsMetricsImpl streamsMetrics) {
        return sizeOrCountSensor(
            taskId,
            storeType,
            storeName,
            SUPPRESSION_BUFFER_SIZE,
            SUPPRESSION_BUFFER_SIZE_AVG_DESCRIPTION,
            SUPPRESSION_BUFFER_SIZE_MAX_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor e2ELatencySensor(final String taskId,
                                          final String storeType,
                                          final String storeName,
                                          final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.storeLevelSensor(taskId, storeName, RECORD_E2E_LATENCY, RecordingLevel.TRACE);
        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(taskId, storeType, storeName);
        addAvgAndMinAndMaxToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            tagMap,
            RECORD_E2E_LATENCY,
            RECORD_E2E_LATENCY_AVG_DESCRIPTION,
            RECORD_E2E_LATENCY_MIN_DESCRIPTION,
            RECORD_E2E_LATENCY_MAX_DESCRIPTION
        );
        return sensor;
    }

    private static Sensor sizeOrCountSensor(final String taskId,
                                            final String storeType,
                                            final String storeName,
                                            final String metricName,
                                            final String descriptionOfAvg,
                                            final String descriptionOfMax,
                                            final RecordingLevel recordingLevel,
                                            final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.storeLevelSensor(taskId, storeName, metricName, recordingLevel);
        final String group;
        final Map<String, String> tagMap;
        group = STATE_STORE_LEVEL_GROUP;
        tagMap = streamsMetrics.storeLevelTagMap(taskId, storeType, storeName);
        addAvgAndMaxToSensor(sensor, group, tagMap, metricName, descriptionOfAvg, descriptionOfMax);
        return sensor;
    }

    private static Sensor throughputAndLatencySensor(final String taskId,
                                                     final String storeType,
                                                     final String storeName,
                                                     final String metricName,
                                                     final String descriptionOfRate,
                                                     final String descriptionOfAvg,
                                                     final String descriptionOfMax,
                                                     final RecordingLevel recordingLevel,
                                                     final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor;
        final String latencyMetricName = metricName + LATENCY_SUFFIX;
        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(taskId, storeType, storeName);
        sensor = streamsMetrics.storeLevelSensor(taskId, storeName, metricName, recordingLevel);
        addInvocationRateToSensor(sensor, STATE_STORE_LEVEL_GROUP, tagMap, metricName, descriptionOfRate);
        addAvgAndMaxToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            tagMap,
            latencyMetricName,
            descriptionOfAvg,
            descriptionOfMax
        );
        return sensor;
    }
}
