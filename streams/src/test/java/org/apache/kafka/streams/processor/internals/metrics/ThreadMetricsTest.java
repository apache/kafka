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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
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
public class ThreadMetricsTest {

    private static final String THREAD_ID = "thread-id";
    private static final String THREAD_LEVEL_GROUP_0100_TO_24 = "stream-metrics";
    private static final String THREAD_LEVEL_GROUP = "stream-thread-metrics";
    private static final String TASK_LEVEL_GROUP = "stream-task-metrics";

    private final Sensor expectedSensor = mock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {Version.LATEST, THREAD_LEVEL_GROUP},
            {Version.FROM_0100_TO_24, THREAD_LEVEL_GROUP_0100_TO_24}
        });
    }

    @Parameter
    public Version builtInMetricsVersion;

    @Parameter(1)
    public String threadLevelGroup;

    @Before
    public void setUp() {
        expect(streamsMetrics.version()).andReturn(builtInMetricsVersion).anyTimes();
        mockStatic(StreamsMetricsImpl.class);
    }

    @Test
    public void shouldGetProcessRatioSensor() {
        final String operation = "process-ratio";
        final String ratioDescription = "The fraction of time the thread spent on processing active tasks";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addValueMetricToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            ratioDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.processRatioSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetProcessRecordsSensor() {
        final String operation = "process-records";
        final String avgDescription = "The average number of records processed within an iteration";
        final String maxDescription = "The maximum number of records processed within an iteration";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            avgDescription,
            maxDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.processRecordsSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetProcessLatencySensor() {
        final String operationLatency = "process" + LATENCY_SUFFIX;
        final String avgLatencyDescription = "The average process latency";
        final String maxLatencyDescription = "The maximum process latency";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operationLatency, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operationLatency,
            avgLatencyDescription,
            maxLatencyDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics, expectedSensor);

        final Sensor sensor = ThreadMetrics.processLatencySensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetProcessRateSensor() {
        final String operation = "process";
        final String operationRate = "process" + RATE_SUFFIX;
        final String totalDescription = "The total number of calls to process";
        final String rateDescription = "The average per-second number of calls to process";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operationRate, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addRateOfSumAndSumMetricsToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics, expectedSensor);

        final Sensor sensor = ThreadMetrics.processRateSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetPollRatioSensor() {
        final String operation = "poll-ratio";
        final String ratioDescription = "The fraction of time the thread spent on polling records from consumer";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addValueMetricToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            ratioDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.pollRatioSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetPollRecordsSensor() {
        final String operation = "poll-records";
        final String avgDescription = "The average number of records polled from consumer within an iteration";
        final String maxDescription = "The maximum number of records polled from consumer within an iteration";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            avgDescription,
            maxDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.pollRecordsSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetPollSensor() {
        final String operation = "poll";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of calls to poll";
        final String rateDescription = "The average per-second number of calls to poll";
        final String avgLatencyDescription = "The average poll latency";
        final String maxLatencyDescription = "The maximum poll latency";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operationLatency,
            avgLatencyDescription,
            maxLatencyDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.pollSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetCommitSensor() {
        final String operation = "commit";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of calls to commit";
        final String rateDescription = "The average per-second number of calls to commit";
        final String avgLatencyDescription = "The average commit latency";
        final String maxLatencyDescription = "The maximum commit latency";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operationLatency,
            avgLatencyDescription,
            maxLatencyDescription);
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.commitSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetCommitRatioSensor() {
        final String operation = "commit-ratio";
        final String ratioDescription = "The fraction of time the thread spent on committing all tasks";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addValueMetricToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            ratioDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.commitRatioSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetCommitOverTasksSensor() {
        final String operation = "commit";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription =
            "The total number of calls to commit over all tasks assigned to one stream thread";
        final String rateDescription =
            "The average per-second number of calls to commit over all tasks assigned to one stream thread";
        final String avgLatencyDescription =
            "The average commit latency over all tasks assigned to one stream thread";
        final String maxLatencyDescription =
            "The maximum commit latency over all tasks assigned to one stream thread";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(THREAD_ID, ROLLUP_VALUE)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.commitOverTasksSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetPunctuateSensor() {
        final String operation = "punctuate";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of calls to punctuate";
        final String rateDescription = "The average per-second number of calls to punctuate";
        final String avgLatencyDescription = "The average punctuate latency";
        final String maxLatencyDescription = "The maximum punctuate latency";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operationLatency,
            avgLatencyDescription,
            maxLatencyDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.punctuateSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetPunctuateRatioSensor() {
        final String operation = "punctuate-ratio";
        final String ratioDescription = "The fraction of time the thread spent on punctuating active tasks";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addValueMetricToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            ratioDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.punctuateRatioSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetSkipRecordSensor() {
        final String operation = "skipped-records";
        final String totalDescription = "The total number of skipped records";
        final String rateDescription = "The average per-second number of skipped records";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO))
            .andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.skipRecordSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetCreateTaskSensor() {
        final String operation = "task-created";
        final String totalDescription = "The total number of newly created tasks";
        final String rateDescription = "The average per-second number of newly created tasks";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);

        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );

        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.createTaskSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetCloseTaskSensor() {
        final String operation = "task-closed";
        final String totalDescription = "The total number of closed tasks";
        final String rateDescription = "The average per-second number of closed tasks";
        expect(streamsMetrics.threadLevelSensor(THREAD_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.threadLevelTagMap(THREAD_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            threadLevelGroup,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );

        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ThreadMetrics.closeTaskSensor(THREAD_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }
}
