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

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RebalanceListenerMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final Sensor expectedSensor = mock(Sensor.class);
    private final Map<String, String> tagMap = Map.of("thread-id", THREAD_ID);

    @Test
    public void shouldGetTasksRevokedSensor() {
        when(streamsMetrics.threadLevelSensor(THREAD_ID, "tasks-revoked" + LATENCY_SUFFIX, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (MockedStatic<StreamsMetricsImpl> streamsMetricsStatic = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = RebalanceListenerMetrics.tasksRevokedSensor(THREAD_ID, streamsMetrics);

            assertEquals(expectedSensor, sensor);
            verify(streamsMetrics).threadLevelSensor(THREAD_ID, "tasks-revoked" + LATENCY_SUFFIX, RecordingLevel.INFO);
            verify(streamsMetrics).threadLevelTagMap(THREAD_ID);

            streamsMetricsStatic.verify(() -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                expectedSensor,
                THREAD_LEVEL_GROUP,
                tagMap,
                "tasks-revoked" + LATENCY_SUFFIX,
                "The average time taken for tasks-revoked rebalance listener callback",
                "The max time taken for tasks-revoked rebalance listener callback"
            ));
        }
    }

    @Test
    public void shouldGetTasksAssignedSensor() {
        when(streamsMetrics.threadLevelSensor(THREAD_ID, "tasks-assigned" + LATENCY_SUFFIX, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (MockedStatic<StreamsMetricsImpl> streamsMetricsStatic = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = RebalanceListenerMetrics.tasksAssignedSensor(THREAD_ID, streamsMetrics);

            assertEquals(expectedSensor, sensor);
            verify(streamsMetrics).threadLevelSensor(THREAD_ID, "tasks-assigned" + LATENCY_SUFFIX, RecordingLevel.INFO);
            verify(streamsMetrics).threadLevelTagMap(THREAD_ID);

            streamsMetricsStatic.verify(() -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                expectedSensor,
                THREAD_LEVEL_GROUP,
                tagMap,
                "tasks-assigned" + LATENCY_SUFFIX,
                "The average time taken for tasks-assigned rebalance listener callback",
                "The max time taken for tasks-assigned rebalance listener callback"
            ));
        }
    }

    @Test
    public void shouldGetTasksLostSensor() {
        when(streamsMetrics.threadLevelSensor(THREAD_ID, "tasks-lost" + LATENCY_SUFFIX, RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.threadLevelTagMap(THREAD_ID)).thenReturn(tagMap);

        try (MockedStatic<StreamsMetricsImpl> streamsMetricsStatic = mockStatic(StreamsMetricsImpl.class)) {
            final Sensor sensor = RebalanceListenerMetrics.tasksLostSensor(THREAD_ID, streamsMetrics);

            assertEquals(expectedSensor, sensor);
            verify(streamsMetrics).threadLevelSensor(THREAD_ID, "tasks-lost" + LATENCY_SUFFIX, RecordingLevel.INFO);
            verify(streamsMetrics).threadLevelTagMap(THREAD_ID);

            streamsMetricsStatic.verify(() -> StreamsMetricsImpl.addAvgAndMaxToSensor(
                expectedSensor,
                THREAD_LEVEL_GROUP,
                tagMap,
                "tasks-lost" + LATENCY_SUFFIX,
                "The average time taken for tasks-lost rebalance listener callback",
                "The max time taken for tasks-lost rebalance listener callback"
            ));
        }
    }
}