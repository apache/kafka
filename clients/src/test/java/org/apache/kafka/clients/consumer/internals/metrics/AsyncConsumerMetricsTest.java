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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AsyncConsumerMetricsTest {
    private static final long METRIC_VALUE = 123L;

    private final Metrics metrics = new Metrics();
    private AsyncConsumerMetrics consumerMetrics;

    public static Stream<String> groupNameProvider() {
        return Stream.of(
            CONSUMER_METRIC_GROUP,
            CONSUMER_SHARE_METRIC_GROUP
        );
    }

    @AfterEach
    public void tearDown() {
        if (consumerMetrics != null) {
            consumerMetrics.close();
        }
        metrics.close();
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldMetricNames(String groupName) {
        // create
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        Set<MetricName> expectedMetrics = Set.of(
            metrics.metricName("time-between-network-thread-poll-avg", groupName),
            metrics.metricName("time-between-network-thread-poll-max", groupName),
            metrics.metricName("application-event-queue-size", groupName),
            metrics.metricName("application-event-queue-time-avg", groupName),
            metrics.metricName("application-event-queue-time-max", groupName),
            metrics.metricName("application-event-queue-processing-time-avg", groupName),
            metrics.metricName("application-event-queue-processing-time-max", groupName),
            metrics.metricName("unsent-requests-queue-size", groupName),
            metrics.metricName("unsent-requests-queue-time-avg", groupName),
            metrics.metricName("unsent-requests-queue-time-max", groupName),
            metrics.metricName("background-event-queue-size", groupName),
            metrics.metricName("background-event-queue-time-avg", groupName),
            metrics.metricName("background-event-queue-time-max", groupName),
            metrics.metricName("background-event-queue-processing-time-avg", groupName),
            metrics.metricName("background-event-queue-processing-time-max", groupName)
        );
        expectedMetrics.forEach(
            metricName -> assertTrue(
                metrics.metrics().containsKey(metricName),
                "Missing metric: " + metricName
            )
        );

        // close
        consumerMetrics.close();
        expectedMetrics.forEach(
            metricName -> assertFalse(
                metrics.metrics().containsKey(metricName),
                "Metric present after close: " + metricName
            )
        );
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordTimeBetweenNetworkThreadPoll(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordTimeBetweenNetworkThreadPoll(METRIC_VALUE);

        // Then:
        assertMetricValue("time-between-network-thread-poll-avg", groupName);
        assertMetricValue("time-between-network-thread-poll-max", groupName);
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordApplicationEventQueueSize(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordApplicationEventQueueSize(10);

        // Then:
        assertEquals(
            (double) 10,
            metrics.metric(
                metrics.metricName(
                    "application-event-queue-size",
                    groupName
                )
            ).metricValue()
        );
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordApplicationEventQueueTime(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordApplicationEventQueueTime(METRIC_VALUE);

        // Then:
        assertMetricValue("application-event-queue-time-avg", groupName);
        assertMetricValue("application-event-queue-time-max", groupName);
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordApplicationEventQueueProcessingTime(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordApplicationEventQueueProcessingTime(METRIC_VALUE);

        // Then:
        assertMetricValue("application-event-queue-processing-time-avg", groupName);
        assertMetricValue("application-event-queue-processing-time-max", groupName);
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordUnsentRequestsQueueSize(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordUnsentRequestsQueueSize(10, 100);

        // Then:
        assertEquals(
            (double) 10,
            metrics.metric(
                metrics.metricName(
                    "unsent-requests-queue-size",
                    groupName
                )
            ).metricValue()
        );
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordUnsentRequestsQueueTime(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordUnsentRequestsQueueTime(METRIC_VALUE);

        // Then:
        assertMetricValue("unsent-requests-queue-time-avg", groupName);
        assertMetricValue("unsent-requests-queue-time-max", groupName);
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordBackgroundEventQueueSize(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordBackgroundEventQueueSize(10);

        // Then:
        assertEquals(
            (double) 10,
            metrics.metric(
                metrics.metricName(
                    "background-event-queue-size",
                    groupName
                )
            ).metricValue()
        );
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordBackgroundEventQueueTime(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordBackgroundEventQueueTime(METRIC_VALUE);

        // Then:
        assertMetricValue("background-event-queue-time-avg", groupName);
        assertMetricValue("background-event-queue-time-max", groupName);
    }

    @ParameterizedTest
    @MethodSource("groupNameProvider")
    public void shouldRecordBackgroundEventQueueProcessingTime(String groupName) {
        consumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
        // When:
        consumerMetrics.recordBackgroundEventQueueProcessingTime(METRIC_VALUE);

        // Then:
        assertMetricValue("background-event-queue-processing-time-avg", groupName);
        assertMetricValue("background-event-queue-processing-time-max", groupName);
    }

    private void assertMetricValue(final String name, final String groupName) {
        assertEquals(
            (double) METRIC_VALUE,
            metrics.metric(
                metrics.metricName(
                    name,
                    groupName
                )
            ).metricValue()
        );
    }
}
