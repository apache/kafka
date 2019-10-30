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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
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
import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({StreamsMetricsImpl.class, Sensor.class})
public class ProcessorNodeMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String PROCESSOR_NODE_ID = "test-processor";

    private final Sensor expectedSensor = mock(Sensor.class);
    private final Sensor expectedParentSensor = mock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");
    private final Map<String, String> parentTagMap = Collections.singletonMap("hi", "universe");

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {Version.LATEST},
            {Version.FROM_0100_TO_24}
        });
    }

    @Parameter
    public Version builtInMetricsVersion;

    @Before
    public void setUp() {
        expect(streamsMetrics.version()).andReturn(builtInMetricsVersion).anyTimes();
        mockStatic(StreamsMetricsImpl.class);
    }

    @Test
    public void shouldGetSuppressionEmitSensor() {
        final String metricName = "suppression-emit";
        final String descriptionOfCount = "The total number of emitted records from the suppression buffer";
        final String descriptionOfRate = "The average number of emitted records from the suppression buffer per second";
        expect(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricName, RecordingLevel.DEBUG))
            .andReturn(expectedSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );

        verifySensor(
            () -> ProcessorNodeMetrics.suppressionEmitSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    @Test
    public void shouldGetProcessSensor() {
        final String metricName = "process";
        final String descriptionOfCount = "The total number of calls to process";
        final String descriptionOfRate = "The average number of calls to process per second";
        final String descriptionOfAvgLatency = "The average latency of calls to process";
        final String descriptionOfMaxLatency = "The maximum latency of calls to process";
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        if (builtInMetricsVersion == Version.FROM_0100_TO_24) {
            setUpThroughputAndLatencyParentSensor(
                metricName,
                descriptionOfRate,
                descriptionOfCount,
                descriptionOfAvgLatency,
                descriptionOfMaxLatency
            );
            expect(streamsMetrics.nodeLevelSensor(
                THREAD_ID,
                TASK_ID,
                PROCESSOR_NODE_ID,
                metricName,
                RecordingLevel.DEBUG,
                expectedParentSensor
            )).andReturn(expectedSensor);
            StreamsMetricsImpl.addAvgAndMaxToSensor(
                expectedSensor,
                StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
                tagMap,
                metricName + StreamsMetricsImpl.LATENCY_SUFFIX,
                descriptionOfAvgLatency,
                descriptionOfMaxLatency
            );
        } else {
            expect(streamsMetrics.nodeLevelSensor(
                THREAD_ID,
                TASK_ID,
                PROCESSOR_NODE_ID,
                metricName,
                RecordingLevel.DEBUG
            )).andReturn(expectedSensor);
        }

        verifySensor(() -> ProcessorNodeMetrics.processSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    @Test
    public void shouldGetPunctuateSensor() {
        final String metricName = "punctuate";
        final String descriptionOfCount = "The total number of calls to punctuate";
        final String descriptionOfRate = "The average number of calls to punctuate per second";
        final String descriptionOfAvgLatency = "The average latency of calls to punctuate";
        final String descriptionOfMaxLatency = "The maximum latency of calls to punctuate";
        shouldGetThroughputAndLatencySensorWithParentOrEmptySensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency,
            () -> ProcessorNodeMetrics.punctuateSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics)
        );
    }

    @Test
    public void shouldGetCreateSensor() {
        final String metricName = "create";
        final String descriptionOfCount = "The total number of processor nodes created";
        final String descriptionOfRate = "The average number of processor nodes created per second";
        final String descriptionOfAvgLatency = "The average latency of creations of processor nodes";
        final String descriptionOfMaxLatency = "The maximum latency of creations of processor nodes";
        shouldGetThroughputAndLatencySensorWithParentOrEmptySensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency,
            () -> ProcessorNodeMetrics.createSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics)
        );
    }

    @Test
    public void shouldGetDestroySensor() {
        final String metricName = "destroy";
        final String descriptionOfCount = "The total number of processor nodes destroyed";
        final String descriptionOfRate = "The average number of processor nodes destroyed per second";
        final String descriptionOfAvgLatency = "The average latency of destructions of processor nodes";
        final String descriptionOfMaxLatency = "The maximum latency of destructions of processor nodes";
        shouldGetThroughputAndLatencySensorWithParentOrEmptySensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency,
            () -> ProcessorNodeMetrics.destroySensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics)
        );
    }

    @Test
    public void shouldGetForwardSensor() {
        final String metricName = "forward";
        final String descriptionOfCount = "The total number of calls to forward";
        final String descriptionOfRate = "The average number of calls to forward per second";
        if (builtInMetricsVersion == Version.FROM_0100_TO_24) {
            setUpThroughputParentSensor(
                metricName,
                descriptionOfRate,
                descriptionOfCount
            );
            setUpThroughputSensor(
                metricName,
                descriptionOfRate,
                descriptionOfCount,
                RecordingLevel.DEBUG,
                expectedParentSensor
            );
        } else {
            expect(streamsMetrics.nodeLevelSensor(
                THREAD_ID,
                TASK_ID,
                PROCESSOR_NODE_ID,
                metricName,
                RecordingLevel.DEBUG
            )).andReturn(expectedSensor);
        }

        verifySensor(() -> ProcessorNodeMetrics.forwardSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    @Test
    public void shouldGetLateRecordDropSensor() {
        final String metricName = "late-record-drop";
        final String descriptionOfCount = "The total number of dropped late records";
        final String descriptionOfRate = "The average number of dropped late records per second";
        if (builtInMetricsVersion == Version.FROM_0100_TO_24) {
            setUpThroughputSensor(
                metricName,
                descriptionOfRate,
                descriptionOfCount,
                RecordingLevel.INFO
            );
        } else {
            expect(streamsMetrics.nodeLevelSensor(
                THREAD_ID,
                TASK_ID,
                PROCESSOR_NODE_ID,
                metricName,
                RecordingLevel.INFO
            )).andReturn(expectedSensor);
        }

        verifySensor(() -> ProcessorNodeMetrics.lateRecordDropSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics));
    }

    private void shouldGetThroughputAndLatencySensorWithParentOrEmptySensor(final String metricName,
                                                                            final String descriptionOfRate,
                                                                            final String descriptionOfCount,
                                                                            final String descriptionOfAvgLatency,
                                                                            final String descriptionOfMaxLatency,
                                                                            final Supplier<Sensor> sensorSupplier) {
        if (builtInMetricsVersion == Version.FROM_0100_TO_24) {
            setUpThroughputAndLatencySensorWithParent(
                metricName,
                descriptionOfRate,
                descriptionOfCount,
                descriptionOfAvgLatency,
                descriptionOfMaxLatency
            );
        } else {
            expect(streamsMetrics.nodeLevelSensor(
                THREAD_ID,
                TASK_ID,
                PROCESSOR_NODE_ID,
                metricName,
                RecordingLevel.DEBUG
            )).andReturn(expectedSensor);
        }

        verifySensor(sensorSupplier);
    }

    private void setUpThroughputAndLatencySensorWithParent(final String metricName,
                                                           final String descriptionOfRate,
                                                           final String descriptionOfCount,
                                                           final String descriptionOfAvgLatency,
                                                           final String descriptionOfMaxLatency) {
        setUpThroughputAndLatencyParentSensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency
        );
        setUpThroughputAndLatencySensor(
            metricName,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency,
            expectedParentSensor
        );
    }

    private void setUpThroughputAndLatencyParentSensor(final String metricName,
                                                       final String descriptionOfRate,
                                                       final String descriptionOfCount,
                                                       final String descriptionOfAvg,
                                                       final String descriptionOfMax) {
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricName, RecordingLevel.DEBUG))
            .andReturn(expectedParentSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, StreamsMetricsImpl.ROLLUP_VALUE))
            .andReturn(parentTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedParentSensor,
            StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
            parentTagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedParentSensor,
            StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
            parentTagMap,
            metricName + StreamsMetricsImpl.LATENCY_SUFFIX,
            descriptionOfAvg,
            descriptionOfMax
        );
    }

    private void setUpThroughputParentSensor(final String metricName,
                                             final String descriptionOfRate,
                                             final String descriptionOfCount) {
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, metricName, RecordingLevel.DEBUG))
            .andReturn(expectedParentSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, StreamsMetricsImpl.ROLLUP_VALUE))
            .andReturn(parentTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedParentSensor,
            StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
            parentTagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
    }

    private void setUpThroughputAndLatencySensor(final String metricName,
                                                 final String descriptionOfRate,
                                                 final String descriptionOfCount,
                                                 final String descriptionOfAvgLatency,
                                                 final String descriptionOfMaxLatency,
                                                 final Sensor... parentSensors) {
        setUpThroughputSensor(metricName, descriptionOfRate, descriptionOfCount, RecordingLevel.DEBUG, parentSensors);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricName + StreamsMetricsImpl.LATENCY_SUFFIX,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency
        );
    }

    private void setUpThroughputSensor(final String metricName,
                                       final String descriptionOfRate,
                                       final String descriptionOfCount,
                                       final RecordingLevel recordingLevel,
                                       final Sensor... parentSensors) {
        expect(streamsMetrics.nodeLevelSensor(
            THREAD_ID,
            TASK_ID,
            PROCESSOR_NODE_ID,
            metricName,
            recordingLevel,
            parentSensors
        )).andReturn(expectedSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
    }

    private void verifySensor(final Supplier<Sensor> sensorSupplier) {
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = sensorSupplier.get();

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }
}