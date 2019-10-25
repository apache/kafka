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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
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
    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");

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
        expect(streamsMetrics.version()).andReturn(builtInMetricsVersion).anyTimes();
        mockStatic(StreamsMetricsImpl.class);
    }

    @Test
    public void shouldGetSuppressionEmitSensor() {
        final String metricName = "suppression-emit";
        final String descriptionOfTotal = "The total number of emitted records from the suppression buffer";
        final String descriptionOfRate = "The average number of emitted records from the suppression buffer per second";
        expect(streamsMetrics.nodeLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, metricName, RecordingLevel.DEBUG))
            .andReturn(expectedSensor);
        expect(streamsMetrics.nodeLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricName,
            descriptionOfRate,
            descriptionOfTotal
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor =
            ProcessorNodeMetrics.suppressionEmitSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }
}