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

import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class StateStoreMetricsTest {

    private static final String TASK_ID = "test-task";
    private static final String STORE_NAME = "test-store";
    private static final String STORE_TYPE = "test-type";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String BUFFER_NAME = "test-buffer";

    private final Sensor expectedSensor = mock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final Map<String, String> storeTagMap = Collections.singletonMap("hello", "world");

    @Test
    public void shouldGetPutSensor() {
        final String metricName = "put";
        final String descriptionOfRate = "The average number of calls to put per second";
        final String descriptionOfAvg = "The average latency of calls to put";
        final String descriptionOfMax = "The maximum latency of calls to put";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.putSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetPutIfAbsentSensor() {
        final String metricName = "put-if-absent";
        final String descriptionOfRate = "The average number of calls to put-if-absent per second";
        final String descriptionOfAvg = "The average latency of calls to put-if-absent";
        final String descriptionOfMax = "The maximum latency of calls to put-if-absent";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.putIfAbsentSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetPutAllSensor() {
        final String metricName = "put-all";
        final String descriptionOfRate = "The average number of calls to put-all per second";
        final String descriptionOfAvg = "The average latency of calls to put-all";
        final String descriptionOfMax = "The maximum latency of calls to put-all";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.putAllSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetFetchSensor() {
        final String metricName = "fetch";
        final String descriptionOfRate = "The average number of calls to fetch per second";
        final String descriptionOfAvg = "The average latency of calls to fetch";
        final String descriptionOfMax = "The maximum latency of calls to fetch";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.fetchSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetGetSensor() {
        final String metricName = "get";
        final String descriptionOfRate = "The average number of calls to get per second";
        final String descriptionOfAvg = "The average latency of calls to get";
        final String descriptionOfMax = "The maximum latency of calls to get";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.getSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetAllSensor() {
        final String metricName = "all";
        final String descriptionOfRate = "The average number of calls to all per second";
        final String descriptionOfAvg = "The average latency of calls to all";
        final String descriptionOfMax = "The maximum latency of calls to all";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.allSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetRangeSensor() {
        final String metricName = "range";
        final String descriptionOfRate = "The average number of calls to range per second";
        final String descriptionOfAvg = "The average latency of calls to range";
        final String descriptionOfMax = "The maximum latency of calls to range";
        setupStreamsMetrics(
            metricName
        );

        getAndVerifySensor(
            () -> StateStoreMetrics.rangeSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetFlushSensor() {
        final String metricName = "flush";
        final String descriptionOfRate = "The average number of calls to flush per second";
        final String descriptionOfAvg = "The average latency of calls to flush";
        final String descriptionOfMax = "The maximum latency of calls to flush";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.flushSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetRemoveSensor() {
        final String metricName = "remove";
        final String descriptionOfRate = "The average number of calls to remove per second";
        final String descriptionOfAvg = "The average latency of calls to remove";
        final String descriptionOfMax = "The maximum latency of calls to remove";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.removeSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetDeleteSensor() {
        final String metricName = "delete";
        final String descriptionOfRate = "The average number of calls to delete per second";
        final String descriptionOfAvg = "The average latency of calls to delete";
        final String descriptionOfMax = "The maximum latency of calls to delete";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.deleteSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetRestoreSensor() {
        final String metricName = "restore";
        final String descriptionOfRate = "The average number of restorations per second";
        final String descriptionOfAvg = "The average latency of restorations";
        final String descriptionOfMax = "The maximum latency of restorations";
        setupStreamsMetrics(metricName);

        getAndVerifySensor(
            () -> StateStoreMetrics.restoreSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax,
            descriptionOfRate
        );
    }

    @Test
    public void shouldGetPrefixScanSensor() {
        final String metricName = "prefix-scan";
        final String descriptionOfRate = "The average number of calls to prefix-scan per second";
        final String descriptionOfAvg = "The average latency of calls to prefix-scan";
        final String descriptionOfMax = "The maximum latency of calls to prefix-scan";
        when(streamsMetrics.storeLevelSensor(TASK_ID, STORE_NAME, metricName, RecordingLevel.DEBUG))
            .thenReturn(expectedSensor);
        when(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, STORE_NAME)).thenReturn(storeTagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = StateStoreMetrics.prefixScanSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics);

            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateToSensor(
                    expectedSensor,
                    STORE_LEVEL_GROUP,
                    storeTagMap,
                    metricName,
                    descriptionOfRate
                )
            );
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    STORE_LEVEL_GROUP,
                    storeTagMap,
                    latencyMetricName(metricName),
                    descriptionOfAvg,
                    descriptionOfMax
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetSuppressionBufferCountSensor() {
        final String metricName = "suppression-buffer-count";
        final String descriptionOfAvg = "The average count of buffered records";
        final String descriptionOfMax = "The maximum count of buffered records";
        setupStreamsMetricsForSuppressionBufferSensor(metricName);

        verifySensorSuppressionBufferSensor(
            () -> StateStoreMetrics.suppressionBufferCountSensor(TASK_ID, STORE_TYPE, BUFFER_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax
        );
    }

    @Test
    public void shouldGetSuppressionBufferSizeSensor() {
        final String metricName = "suppression-buffer-size";
        final String descriptionOfAvg = "The average size of buffered records";
        final String descriptionOfMax = "The maximum size of buffered records";
        setupStreamsMetricsForSuppressionBufferSensor(metricName);

        verifySensorSuppressionBufferSensor(
            () -> StateStoreMetrics.suppressionBufferSizeSensor(TASK_ID, STORE_TYPE, BUFFER_NAME, streamsMetrics),
            metricName,
            descriptionOfAvg,
            descriptionOfMax
        );
    }

    @Test
    public void shouldGetExpiredWindowRecordDropSensor() {
        final String metricName = "expired-window-record-drop";
        final String descriptionOfRate = "The average number of dropped records due to an expired window per second";
        final String descriptionOfCount = "The total number of dropped records due to an expired window";
        when(streamsMetrics.storeLevelSensor(TASK_ID, STORE_NAME, metricName, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, STORE_NAME)).thenReturn(storeTagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor =
                StateStoreMetrics.expiredWindowRecordDropSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                    expectedSensor,
                    "stream-" + STORE_TYPE + "-metrics",
                    storeTagMap,
                    metricName,
                    descriptionOfRate,
                    descriptionOfCount
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    @Test
    public void shouldGetRecordE2ELatencySensor() {
        final String metricName = "record-e2e-latency";
        final String e2eLatencyDescription =
            "end-to-end latency of a record, measuring by comparing the record timestamp with the "
                + "system time when it has been fully processed by the node";
        final String descriptionOfAvg = "The average " + e2eLatencyDescription;
        final String descriptionOfMin = "The minimum " + e2eLatencyDescription;
        final String descriptionOfMax = "The maximum " + e2eLatencyDescription;
        when(streamsMetrics.storeLevelSensor(TASK_ID, STORE_NAME, metricName, RecordingLevel.TRACE))
            .thenReturn(expectedSensor);
        when(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, STORE_NAME)).thenReturn(storeTagMap);

        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor =
                StateStoreMetrics.e2ELatencySensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics);
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMinAndMaxToSensor(
                    expectedSensor,
                    STORE_LEVEL_GROUP,
                    storeTagMap,
                    metricName,
                    descriptionOfAvg,
                    descriptionOfMin,
                    descriptionOfMax
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    private void setupStreamsMetrics(final String metricName) {
        when(streamsMetrics.storeLevelSensor(TASK_ID, STORE_NAME, metricName, RecordingLevel.DEBUG))
            .thenReturn(expectedSensor);
        when(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, STORE_NAME)).thenReturn(storeTagMap);
    }

    private void getAndVerifySensor(final Supplier<Sensor> sensorSupplier,
                                    final String metricName,
                                    final String descriptionOfAvg,
                                    final String descriptionOfMax,
                                    final String descriptionOfRate) {
        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = sensorSupplier.get();
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addInvocationRateToSensor(
                    expectedSensor,
                    STORE_LEVEL_GROUP,
                    storeTagMap,
                    metricName,
                    descriptionOfRate
                )
            );
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    STORE_LEVEL_GROUP,
                    storeTagMap,
                    latencyMetricName(metricName),
                    descriptionOfAvg,
                    descriptionOfMax
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }

    private String latencyMetricName(final String metricName) {
        return metricName + StreamsMetricsImpl.LATENCY_SUFFIX;
    }

    private void setupStreamsMetricsForSuppressionBufferSensor(final String metricName) {
        when(streamsMetrics.storeLevelSensor(
            TASK_ID,
            BUFFER_NAME,
            metricName,
            RecordingLevel.DEBUG
        )).thenReturn(expectedSensor);
        when(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, BUFFER_NAME)).thenReturn(storeTagMap);
    }

    private void verifySensorSuppressionBufferSensor(final Supplier<Sensor> sensorSupplier,
                                                     final String metricName,
                                                     final String descriptionOfAvg,
                                                     final String descriptionOfMax) {
        try (final MockedStatic<StreamsMetricsImpl> streamsMetricsStaticMock = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = sensorSupplier.get();
            streamsMetricsStaticMock.verify(
                () -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                    expectedSensor,
                    STORE_LEVEL_GROUP,
                    storeTagMap,
                    metricName,
                    descriptionOfAvg,
                    descriptionOfMax
                )
            );
            assertThat(sensor, is(expectedSensor));
        }
    }
}
