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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamsClientMetricsDelegatingReporterTest {

    private MockAdminClient mockAdminClient;
    private StreamsClientMetricsDelegatingReporter streamsClientMetricsDelegatingReporter;

    private KafkaMetric streamClientMetricOne;
    private KafkaMetric streamClientMetricTwo;
    private KafkaMetric streamClientMetricThree;
    private KafkaMetric kafkaMetricWithThreadIdTag;
    private final Object lock = new Object();
    private final MetricConfig metricConfig = new MetricConfig();


    @BeforeEach
    public void setup() {
        mockAdminClient = new MockAdminClient();
        streamsClientMetricsDelegatingReporter = new StreamsClientMetricsDelegatingReporter(mockAdminClient, "adminClientId");

        final Map<String, String> threadIdTagMap = new HashMap<>();
        final String threadId = "abcxyz-StreamThread-1";
        threadIdTagMap.put("thread-id", threadId);

        final MetricName metricNameOne = new MetricName("metricOne", "stream-metrics", "description for metric one", new HashMap<>());
        final MetricName metricNameTwo = new MetricName("metricTwo", "stream-metrics", "description for metric two", new HashMap<>());
        final MetricName metricNameThree = new MetricName("metricThree", "stream-metrics", "description for metric three", new HashMap<>());
        final MetricName metricNameFour = new MetricName("metricThree", "thread-metrics", "description for metric three", threadIdTagMap);

        streamClientMetricOne = new KafkaMetric(lock, metricNameOne, (Measurable) (m, now) -> 1.0, metricConfig, Time.SYSTEM);
        streamClientMetricTwo = new KafkaMetric(lock, metricNameTwo, (Measurable) (m, now) -> 2.0, metricConfig, Time.SYSTEM);
        streamClientMetricThree = new KafkaMetric(lock, metricNameThree, (Measurable) (m, now) -> 3.0, metricConfig, Time.SYSTEM);
        kafkaMetricWithThreadIdTag = new KafkaMetric(lock, metricNameFour, (Measurable) (m, now) -> 4.0, metricConfig, Time.SYSTEM);
    }

    @AfterEach
    public void tearDown() {
        mockAdminClient.close();
    }

    @Test
    public void shouldInitMetrics() {
        final List<KafkaMetric> metrics = Arrays.asList(streamClientMetricOne, streamClientMetricTwo, streamClientMetricThree, kafkaMetricWithThreadIdTag);
        streamsClientMetricsDelegatingReporter.init(metrics);
        final List<KafkaMetric> expectedMetrics = Arrays.asList(streamClientMetricOne, streamClientMetricTwo, streamClientMetricThree);
        assertEquals(expectedMetrics, mockAdminClient.addedMetrics(),
            "Should register metrics from init method");
    }

    @Test
    public void shouldRegisterCorrectMetrics() {
        streamsClientMetricsDelegatingReporter.metricChange(kafkaMetricWithThreadIdTag);
        assertEquals(0, mockAdminClient.addedMetrics().size());

        streamsClientMetricsDelegatingReporter.metricChange(streamClientMetricOne);
        assertEquals(1, mockAdminClient.addedMetrics().size(),
            "Should register client instance metrics only");
    }

    @Test
    public void metricRemoval() {
        streamsClientMetricsDelegatingReporter.metricChange(streamClientMetricOne);
        streamsClientMetricsDelegatingReporter.metricChange(streamClientMetricTwo);
        streamsClientMetricsDelegatingReporter.metricChange(streamClientMetricThree);
        assertEquals(3, mockAdminClient.addedMetrics().size());

        streamsClientMetricsDelegatingReporter.metricRemoval(streamClientMetricOne);
        assertEquals(2, mockAdminClient.addedMetrics().size(),
            "Should remove client instance metrics");
    }
}