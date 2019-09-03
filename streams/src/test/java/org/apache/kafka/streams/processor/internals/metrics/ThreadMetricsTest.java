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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verify;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StreamsMetricsImpl.class)
public class ThreadMetricsTest {

    private static final String THREAD_LEVEL_GROUP = "stream-metrics";
    private static final String TASK_LEVEL_GROUP = "stream-task-metrics";

    private final Metrics dummyMetrics = new Metrics();
    private final Sensor dummySensor = dummyMetrics.sensor("dummy");
    private final StreamsMetricsImpl streamsMetrics = createStrictMock(StreamsMetricsImpl.class);
    private final Map<String, String> dummyTagMap = Collections.singletonMap("hello", "world");

    @Test
    public void shouldGetCreateTaskSensor() {
        final String operation = "task-created";
        final String totalDescription = "The total number of newly created tasks";
        final String rateDescription = "The average per-second number of newly created tasks";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.createTaskSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }

    @Test
    public void shouldGetCloseTaskSensor() {
        final String operation = "task-closed";
        final String totalDescription = "The total number of closed tasks";
        final String rateDescription = "The average per-second number of closed tasks";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.closeTaskSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }

    @Test
    public void shouldGetCommitSensor() {
        final String operation = "commit";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of commit calls";
        final String rateDescription = "The average per-second number of commit calls";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.commitSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }

    @Test
    public void shouldGetPollSensor() {
        final String operation = "poll";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of poll calls";
        final String rateDescription = "The average per-second number of poll calls";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.pollSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }

    @Test
    public void shouldGetProcessSensor() {
        final String operation = "process";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of process calls";
        final String rateDescription = "The average per-second number of process calls";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.processSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }

    @Test
    public void shouldGetPunctuateSensor() {
        final String operation = "punctuate";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of punctuate calls";
        final String rateDescription = "The average per-second number of punctuate calls";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operationLatency);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.punctuateSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }

    @Test
    public void shouldGetSkipRecordSensor() {
        final String operation = "skipped-records";
        final String totalDescription = "The total number of skipped records";
        final String rateDescription = "The average per-second number of skipped records";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.INFO)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap()).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, THREAD_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.skipRecordSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }

    @Test
    public void shouldGetCommitOverTasksSensor() {
        final String operation = "commit";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of commit calls over all tasks";
        final String rateDescription = "The average per-second number of commit calls over all tasks";
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.threadLevelSensor(operation, RecordingLevel.DEBUG)).andReturn(dummySensor);
        expect(streamsMetrics.threadLevelTagMap(TASK_ID_TAG, ROLLUP_VALUE)).andReturn(dummyTagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            dummySensor, TASK_LEVEL_GROUP, dummyTagMap, operation, totalDescription, rateDescription);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            dummySensor, TASK_LEVEL_GROUP, dummyTagMap, operationLatency);

        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = ThreadMetrics.commitOverTasksSensor(streamsMetrics);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(dummySensor));
    }
}
