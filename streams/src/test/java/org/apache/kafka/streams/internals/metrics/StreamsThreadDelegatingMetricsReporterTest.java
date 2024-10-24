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

package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamsThreadDelegatingMetricsReporterTest {

    private MockConsumer<?, ?> mockConsumer;
    private StreamsThreadDelegatingMetricsReporter streamsThreadDelegatingMetricsReporter;

    private KafkaMetric kafkaMetricOneHasThreadIdTag;
    private KafkaMetric kafkaMetricTwoHasThreadIdTag;
    private KafkaMetric kafkaMetricThreeHasThreadIdTag;
    private KafkaMetric kafkaMetricWithoutThreadIdTag;
    private final Object lock = new Object();
    private final MetricConfig metricConfig = new MetricConfig();


    @BeforeEach
    void setUp() {
        final Map<String, String> threadIdTagMap = new HashMap<>();
        final String threadId = "abcxyz-StreamThread-1";
        threadIdTagMap.put("thread-id", threadId);

        final Map<String, String> threadIdWithStateUpdaterTagMap = new HashMap<>();
        final String stateUpdaterId = "deftuv-StateUpdater-1";
        threadIdWithStateUpdaterTagMap.put("thread-id", stateUpdaterId);

        final Map<String, String> noThreadIdTagMap = new HashMap<>();
        noThreadIdTagMap.put("client-id", "foo");

        mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        streamsThreadDelegatingMetricsReporter = new StreamsThreadDelegatingMetricsReporter(mockConsumer, threadId, stateUpdaterId);

        final MetricName metricNameOne = new MetricName("metric-one", "test-group-one", "foo bar baz", threadIdTagMap);
        final MetricName metricNameTwo = new MetricName("metric-two", "test-group-two", "description two", threadIdWithStateUpdaterTagMap);
        final MetricName metricNameThree = new MetricName("metric-three", "test-group-three", "description three", threadIdTagMap);
        final MetricName metricNameFour = new MetricName("metric-four", "test-group-three", "description three", noThreadIdTagMap);

        kafkaMetricOneHasThreadIdTag = new KafkaMetric(lock, metricNameOne, (Measurable) (m, now) -> 1.0, metricConfig, Time.SYSTEM);
        kafkaMetricTwoHasThreadIdTag = new KafkaMetric(lock, metricNameTwo, (Measurable) (m, now) -> 2.0, metricConfig, Time.SYSTEM);
        kafkaMetricThreeHasThreadIdTag = new KafkaMetric(lock, metricNameThree, (Measurable) (m, now) -> 3.0, metricConfig, Time.SYSTEM);
        kafkaMetricWithoutThreadIdTag = new KafkaMetric(lock, metricNameFour, (Measurable) (m, now) -> 4.0, metricConfig, Time.SYSTEM);
    }

    @AfterEach
    void tearDown() {
        mockConsumer.close();
    }


    @Test
    @DisplayName("Init method should register metrics it receives as parameters")
    void shouldInitMetrics() {
        final List<KafkaMetric> allMetrics = Arrays.asList(kafkaMetricOneHasThreadIdTag, kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
        final List<KafkaMetric> expectedMetrics = Arrays.asList(kafkaMetricOneHasThreadIdTag, kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
        streamsThreadDelegatingMetricsReporter.init(allMetrics);
        assertEquals(expectedMetrics, mockConsumer.addedMetrics());
    }

    @Test
    @DisplayName("Should register metrics with thread-id in tag map")
    void shouldRegisterMetrics() {
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricOneHasThreadIdTag);
        assertEquals(kafkaMetricOneHasThreadIdTag, mockConsumer.addedMetrics().get(0));
    }

    @Test
    @DisplayName("Should remove metrics")
    void shouldRemoveMetrics() {
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricOneHasThreadIdTag);
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricTwoHasThreadIdTag);
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricThreeHasThreadIdTag);
        List<KafkaMetric> expected = Arrays.asList(kafkaMetricOneHasThreadIdTag, kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
        assertEquals(expected, mockConsumer.addedMetrics());
        streamsThreadDelegatingMetricsReporter.metricRemoval(kafkaMetricOneHasThreadIdTag);
        expected = Arrays.asList(kafkaMetricTwoHasThreadIdTag, kafkaMetricThreeHasThreadIdTag);
        assertEquals(expected, mockConsumer.addedMetrics());
    }

    @Test
    @DisplayName("Should not register metrics without thread-id tag")
    void shouldNotRegisterMetricsWithoutThreadIdTag() {
        streamsThreadDelegatingMetricsReporter.metricChange(kafkaMetricWithoutThreadIdTag);
        assertEquals(0, mockConsumer.addedMetrics().size());
    }
}