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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({StreamsMetricsImpl.class, Sensor.class})
public class StateStoreMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String STORE_NAME = "test-store";
    private static final String STORE_TYPE = "test-type";
    private static final String STORE_LEVEL_GROUP_FROM_0100_TO_24 = "stream-" + STORE_TYPE + "-state-metrics";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String BUFFER_NAME = "test-buffer";
    private static final String BUFFER_LEVEL_GROUP_FROM_0100_TO_24 = "stream-buffer-metrics";

    private final Sensor expectedSensor = createMock(Sensor.class);
    private final Sensor parentSensor = createMock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Map<String, String> storeTagMap = Collections.singletonMap("hello", "world");
    private final Map<String, String> bufferTagMap = Collections.singletonMap("hi", "galaxy");
    private final Map<String, String> allTagMap = Collections.singletonMap("hello", "universe");
    private String storeLevelGroup;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {Version.LATEST},
            {Version.FROM_0100_TO_24}
        });
    }

    @Parameter
    public Version builtInMetricsVersion;

    @Before
    public void setUp() {
        storeLevelGroup =
            builtInMetricsVersion == Version.FROM_0100_TO_24 ? STORE_LEVEL_GROUP_FROM_0100_TO_24 : STORE_LEVEL_GROUP;
        expect(streamsMetrics.version()).andReturn(builtInMetricsVersion).anyTimes();
        mockStatic(StreamsMetricsImpl.class);
    }

    @Test
    public void shouldGetPutSensor() {
        final String metricName = "put";
        final String descriptionOfRate = "The average number of calls to put per second";
        final String descriptionOfCount = "The total number of calls to put";
        final String descriptionOfAvg = "The average latency of calls to put";
        final String descriptionOfMax = "The maximum latency of calls to put";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.putSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetPutIfAbsentSensor() {
        final String metricName = "put-if-absent";
        final String descriptionOfRate = "The average number of calls to put-if-absent per second";
        final String descriptionOfCount = "The total number of calls to put-if-absent";
        final String descriptionOfAvg = "The average latency of calls to put-if-absent";
        final String descriptionOfMax = "The maximum latency of calls to put-if-absent";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.putIfAbsentSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetPutAllSensor() {
        final String metricName = "put-all";
        final String descriptionOfRate = "The average number of calls to put-all per second";
        final String descriptionOfCount = "The total number of calls to put-all";
        final String descriptionOfAvg = "The average latency of calls to put-all";
        final String descriptionOfMax = "The maximum latency of calls to put-all";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.putAllSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetFetchSensor() {
        final String metricName = "fetch";
        final String descriptionOfRate = "The average number of calls to fetch per second";
        final String descriptionOfCount = "The total number of calls to fetch";
        final String descriptionOfAvg = "The average latency of calls to fetch";
        final String descriptionOfMax = "The maximum latency of calls to fetch";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.fetchSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetGetSensor() {
        final String metricName = "get";
        final String descriptionOfRate = "The average number of calls to get per second";
        final String descriptionOfCount = "The total number of calls to get";
        final String descriptionOfAvg = "The average latency of calls to get";
        final String descriptionOfMax = "The maximum latency of calls to get";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.getSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetAllSensor() {
        final String metricName = "all";
        final String descriptionOfRate = "The average number of calls to all per second";
        final String descriptionOfCount = "The total number of calls to all";
        final String descriptionOfAvg = "The average latency of calls to all";
        final String descriptionOfMax = "The maximum latency of calls to all";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.allSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetRangeSensor() {
        final String metricName = "range";
        final String descriptionOfRate = "The average number of calls to range per second";
        final String descriptionOfCount = "The total number of calls to range";
        final String descriptionOfAvg = "The average latency of calls to range";
        final String descriptionOfMax = "The maximum latency of calls to range";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.rangeSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetFlushSensor() {
        final String metricName = "flush";
        final String descriptionOfRate = "The average number of calls to flush per second";
        final String descriptionOfCount = "The total number of calls to flush";
        final String descriptionOfAvg = "The average latency of calls to flush";
        final String descriptionOfMax = "The maximum latency of calls to flush";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.flushSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetRemoveSensor() {
        final String metricName = "remove";
        final String descriptionOfRate = "The average number of calls to remove per second";
        final String descriptionOfCount = "The total number of calls to remove";
        final String descriptionOfAvg = "The average latency of calls to remove";
        final String descriptionOfMax = "The maximum latency of calls to remove";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.removeSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetDeleteSensor() {
        final String metricName = "delete";
        final String descriptionOfRate = "The average number of calls to delete per second";
        final String descriptionOfCount = "The total number of calls to delete";
        final String descriptionOfAvg = "The average latency of calls to delete";
        final String descriptionOfMax = "The maximum latency of calls to delete";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.deleteSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetRestoreSensor() {
        final String metricName = "restore";
        final String descriptionOfRate = "The average number of restorations per second";
        final String descriptionOfCount = "The total number of restorations";
        final String descriptionOfAvg = "The average latency of restorations";
        final String descriptionOfMax = "The maximum latency of restorations";
        shouldGetSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.restoreSensor(THREAD_ID, TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetSuppressionBufferCountSensor() {
        final String metricName = "suppression-buffer-count";
        final String descriptionOfCurrentValue = "The current count of buffered records";
        final String descriptionOfAvg = "The average count of buffered records";
        final String descriptionOfMax = "The maximum count of buffered records";
        shouldGetSuppressionBufferSensor(
            metricName,
            descriptionOfCurrentValue,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.suppressionBufferCountSensor(THREAD_ID, TASK_ID, STORE_TYPE, BUFFER_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetSuppressionBufferSizeSensor() {
        final String metricName = "suppression-buffer-size";
        final String descriptionOfCurrentValue = "The current size of buffered records";
        final String descriptionOfAvg = "The average size of buffered records";
        final String descriptionOfMax = "The maximum size of buffered records";
        shouldGetSuppressionBufferSensor(
            metricName,
            descriptionOfCurrentValue,
            descriptionOfAvg,
            descriptionOfMax,
            () -> StateStoreMetrics.suppressionBufferSizeSensor(THREAD_ID, TASK_ID, STORE_TYPE, BUFFER_NAME, streamsMetrics)
        );
    }

    @Test
    public void shouldGetExpiredWindowRecordDropSensor() {
        final String metricName = "expired-window-record-drop";
        final String descriptionOfRate = "The average number of dropped records due to an expired window per second";
        final String descriptionOfCount = "The total number of dropped records due to an expired window";
        expect(streamsMetrics.storeLevelSensor(TASK_ID, STORE_NAME, metricName, RecordingLevel.INFO))
            .andReturn(expectedSensor);
        expect(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, STORE_NAME)).andReturn(storeTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            "stream-" + STORE_TYPE + "-metrics",
            storeTagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor =
            StateStoreMetrics.expiredWindowRecordDropSensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
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

        expect(streamsMetrics.storeLevelSensor(TASK_ID, STORE_NAME, metricName, RecordingLevel.TRACE))
            .andReturn(expectedSensor);
        expect(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, STORE_NAME)).andReturn(storeTagMap);
        StreamsMetricsImpl.addAvgAndMinAndMaxToSensor(
            expectedSensor,
            STORE_LEVEL_GROUP,
            storeTagMap,
            metricName,
            descriptionOfAvg,
            descriptionOfMin,
            descriptionOfMax
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor =
            StateStoreMetrics.e2ELatencySensor(TASK_ID, STORE_TYPE, STORE_NAME, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    private void shouldGetSensor(final String metricName,
                                 final String descriptionOfRate,
                                 final String descriptionOfCount,
                                 final String descriptionOfAvg,
                                 final String descriptionOfMax,
                                 final Supplier<Sensor> sensorSupplier) {
        if (builtInMetricsVersion == Version.FROM_0100_TO_24) {
            setUpParentSensor(metricName, descriptionOfRate, descriptionOfCount, descriptionOfAvg, descriptionOfMax);
            expect(streamsMetrics.storeLevelSensor(
                TASK_ID,
                STORE_NAME,
                metricName,
                RecordingLevel.DEBUG,
                parentSensor
            )).andReturn(expectedSensor);
            StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedSensor,
                storeLevelGroup,
                storeTagMap,
                metricName,
                descriptionOfRate,
                descriptionOfCount
            );
        } else {
            expect(streamsMetrics.storeLevelSensor(
                TASK_ID,
                STORE_NAME,
                metricName,
                RecordingLevel.DEBUG
            )).andReturn(expectedSensor);
            StreamsMetricsImpl.addInvocationRateToSensor(
                expectedSensor,
                storeLevelGroup,
                storeTagMap,
                metricName,
                descriptionOfRate
            );
        }
        expect(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, STORE_NAME)).andReturn(storeTagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            storeLevelGroup,
            storeTagMap,
            latencyMetricName(metricName),
            descriptionOfAvg,
            descriptionOfMax
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = sensorSupplier.get();

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }
    private void setUpParentSensor(final String metricName,
                                   final String descriptionOfRate,
                                   final String descriptionOfCount,
                                   final String descriptionOfAvg,
                                   final String descriptionOfMax) {
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricName, RecordingLevel.DEBUG))
            .andReturn(parentSensor);
        expect(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, StreamsMetricsImpl.ROLLUP_VALUE))
            .andReturn(allTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            parentSensor,
            storeLevelGroup,
            allTagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            parentSensor,
            storeLevelGroup,
            allTagMap,
            latencyMetricName(metricName),
            descriptionOfAvg,
            descriptionOfMax
        );
    }

    private String latencyMetricName(final String metricName) {
        return metricName + StreamsMetricsImpl.LATENCY_SUFFIX;
    }

    private void shouldGetSuppressionBufferSensor(final String metricName,
                                                  final String descriptionOfCurrentValue,
                                                  final String descriptionOfAvg,
                                                  final String descriptionOfMax,
                                                  final Supplier<Sensor> sensorSupplier) {
        final String currentMetricName = metricName + "-current";
        final String group;
        final Map<String, String> tagMap;
        expect(streamsMetrics.storeLevelSensor(TASK_ID, BUFFER_NAME, metricName, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        if (builtInMetricsVersion == Version.FROM_0100_TO_24) {
            group = BUFFER_LEVEL_GROUP_FROM_0100_TO_24;
            tagMap = bufferTagMap;
            expect(streamsMetrics.bufferLevelTagMap(THREAD_ID, TASK_ID, BUFFER_NAME)).andReturn(tagMap);
            StreamsMetricsImpl.addValueMetricToSensor(
                expectedSensor,
                group,
                bufferTagMap,
                currentMetricName,
                descriptionOfCurrentValue
            );
        } else {
            group = STORE_LEVEL_GROUP;
            tagMap = storeTagMap;
            expect(streamsMetrics.storeLevelTagMap(TASK_ID, STORE_TYPE, BUFFER_NAME)).andReturn(tagMap);
        }
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            group,
            tagMap,
            metricName,
            descriptionOfAvg,
            descriptionOfMax
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = sensorSupplier.get();

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }
}