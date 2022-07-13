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

import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOPIC_LEVEL_GROUP;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TopicMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String PROCESSOR_NODE_ID = "test-processor";
    private static final String TOPIC = "topic";

    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");

    private final Sensor expectedSensor = mock(Sensor.class);
    private static final MockedStatic<StreamsMetricsImpl> STREAMS_METRICS_STATIC_MOCK = mockStatic(StreamsMetricsImpl.class);
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);

    @AfterClass
    public static void cleanUp() {
        STREAMS_METRICS_STATIC_MOCK.close();
    }

    @Test
    public void shouldGetRecordsAndBytesConsumedSensor() {
        final String recordsMetricNamePrefix = "records-consumed";
        final String bytesMetricNamePrefix = "bytes-consumed";
        final String descriptionOfRecordsTotal = "The total number of records consumed from this topic";
        final String descriptionOfBytesTotal = "The total number of bytes consumed from this topic";

        when(streamsMetrics.topicLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC, "consumed", RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.topicLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC, "consumed", RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.topicLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC)).thenReturn(tagMap);

        verifySensor(
            () -> TopicMetrics.consumedSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC, streamsMetrics)
        );

        STREAMS_METRICS_STATIC_MOCK.verify(
            () -> StreamsMetricsImpl.addTotalCountAndSumMetricsToSensor(
                expectedSensor,
                TOPIC_LEVEL_GROUP,
                tagMap,
                recordsMetricNamePrefix,
                bytesMetricNamePrefix,
                descriptionOfRecordsTotal,
                descriptionOfBytesTotal
            )
        );
    }

    @Test
    public void shouldGetRecordsAndBytesProducedSensor() {
        final String recordsMetricNamePrefix = "records-produced";
        final String bytesMetricNamePrefix = "bytes-produced";
        final String descriptionOfRecordsTotal = "The total number of records produced to this topic";
        final String descriptionOfBytesTotal = "The total number of bytes produced to this topic";

        when(streamsMetrics.topicLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC, "produced", RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.topicLevelSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC, "produced", RecordingLevel.INFO))
            .thenReturn(expectedSensor);
        when(streamsMetrics.topicLevelTagMap(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC)).thenReturn(tagMap);

        verifySensor(() -> TopicMetrics.producedSensor(THREAD_ID, TASK_ID, PROCESSOR_NODE_ID, TOPIC, streamsMetrics));

        STREAMS_METRICS_STATIC_MOCK.verify(
            () -> StreamsMetricsImpl.addTotalCountAndSumMetricsToSensor(
                expectedSensor,
                TOPIC_LEVEL_GROUP,
                tagMap,
                recordsMetricNamePrefix,
                bytesMetricNamePrefix,
                descriptionOfRecordsTotal,
                descriptionOfBytesTotal
            )
        );
    }

    private void verifySensor(final Supplier<Sensor> sensorSupplier) {
        final Sensor sensor = sensorSupplier.get();
        assertThat(sensor, is(expectedSensor));
    }

}
