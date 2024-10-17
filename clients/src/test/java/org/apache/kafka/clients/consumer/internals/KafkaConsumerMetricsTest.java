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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConsumerMetricsTest {
    private static final long METRIC_VALUE = 123L;
    private static final String CONSUMER_GROUP_PREFIX = "consumer";
    private static final String CONSUMER_METRIC_GROUP = "consumer-metrics";
    private static final String COMMIT_SYNC_TIME_TOTAL = "commit-sync-time-ns-total";
    private static final String COMMITTED_TIME_TOTAL = "committed-time-ns-total";

    private final Metrics metrics = new Metrics();
    private KafkaConsumerMetrics consumerMetrics;

    @AfterEach
    public void tearDown() {
        if (consumerMetrics != null) {
            consumerMetrics.close();
        }
        metrics.close();
    }

    @ParameterizedTest
    @EnumSource(GroupProtocol.class)
    public void shouldMetricNames(GroupProtocol groupProtocol) {
        // create
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, groupProtocol);
        HashSet<MetricName> expectedMetrics = new HashSet<>(Arrays.asList(
                metrics.metricName("last-poll-seconds-ago", CONSUMER_METRIC_GROUP),
                metrics.metricName("time-between-poll-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("time-between-poll-max", CONSUMER_METRIC_GROUP),
                metrics.metricName("poll-idle-ratio-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("commit-sync-time-ns-total", CONSUMER_METRIC_GROUP),
                metrics.metricName("committed-time-ns-total", CONSUMER_METRIC_GROUP)
        ));
        expectedMetrics.forEach(metricName -> assertTrue(metrics.metrics().containsKey(metricName), "Missing metric: " + metricName));

        HashSet<MetricName> expectedConsumerMetrics = new HashSet<>(Arrays.asList(
                metrics.metricName("time-between-network-thread-poll-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("time-between-network-thread-poll-max", CONSUMER_METRIC_GROUP),
                metrics.metricName("application-event-queue-size", CONSUMER_METRIC_GROUP),
                metrics.metricName("application-event-queue-time-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("application-event-queue-time-max", CONSUMER_METRIC_GROUP),
                metrics.metricName("application-event-queue-processing-time-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("application-event-queue-processing-time-max", CONSUMER_METRIC_GROUP),
                metrics.metricName("unsent-requests-queue-size", CONSUMER_METRIC_GROUP),
                metrics.metricName("unsent-requests-queue-time-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("unsent-requests-queue-time-max", CONSUMER_METRIC_GROUP),
                metrics.metricName("background-event-queue-size", CONSUMER_METRIC_GROUP),
                metrics.metricName("background-event-queue-time-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("background-event-queue-time-max", CONSUMER_METRIC_GROUP),
                metrics.metricName("background-event-queue-processing-time-avg", CONSUMER_METRIC_GROUP),
                metrics.metricName("background-event-queue-processing-time-max", CONSUMER_METRIC_GROUP)
        ));
        if (groupProtocol == GroupProtocol.CONSUMER) {
            expectedConsumerMetrics.forEach(metricName -> assertTrue(metrics.metrics().containsKey(metricName), "Missing metric: " + metricName));
        }

        // close
        consumerMetrics.close();
        expectedMetrics.forEach(metricName -> assertFalse(metrics.metrics().containsKey(metricName), "Missing metric: " + metricName));
        expectedConsumerMetrics.forEach(metricName -> assertFalse(metrics.metrics().containsKey(metricName), "Missing metric: " + metricName));
    }

    @ParameterizedTest
    @EnumSource(GroupProtocol.class)
    public void shouldRecordCommitSyncTime(GroupProtocol groupProtocol) {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, groupProtocol);
        // When:
        consumerMetrics.recordCommitSync(METRIC_VALUE);

        // Then:
        assertMetricValue(COMMIT_SYNC_TIME_TOTAL);
    }

    @ParameterizedTest
    @EnumSource(GroupProtocol.class)
    public void shouldRecordCommittedTime(GroupProtocol groupProtocol) {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, groupProtocol);
        // When:
        consumerMetrics.recordCommitted(METRIC_VALUE);

        // Then:
        assertMetricValue(COMMITTED_TIME_TOTAL);
    }

    @Test
    public void shouldRecordTimeBetweenNetworkThreadPoll() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordTimeBetweenNetworkThreadPoll(METRIC_VALUE);

        // Then:
        assertMetricValue("time-between-network-thread-poll-avg");
        assertMetricValue("time-between-network-thread-poll-max");
    }

    @Test
    public void shouldRecordApplicationEventQueueSize() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordApplicationEventQueueSize(10);

        // Then:
        assertEquals(
                metrics.metric(metrics.metricName("application-event-queue-size", CONSUMER_METRIC_GROUP)).metricValue(),
                (double) 10
        );
    }

    @Test
    public void shouldRecordApplicationEventQueueTime() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordApplicationEventQueueTime(METRIC_VALUE);

        // Then:
        assertMetricValue("application-event-queue-time-avg");
        assertMetricValue("application-event-queue-time-max");
    }

    @Test
    public void shouldRecordApplicationEventQueueProcessingTime() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordApplicationEventQueueProcessingTime(METRIC_VALUE);

        // Then:
        assertMetricValue("application-event-queue-processing-time-avg");
        assertMetricValue("application-event-queue-processing-time-max");
    }

    @Test
    public void shouldRecordUnsentRequestsQueueSize() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordUnsentRequestsQueueSize(10);

        // Then:
        assertEquals(
                metrics.metric(metrics.metricName("unsent-requests-queue-size", CONSUMER_METRIC_GROUP)).metricValue(),
                (double) 10
        );
    }

    @Test
    public void shouldRecordUnsentRequestsQueueTime() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordUnsentRequestsQueueTime(METRIC_VALUE);

        // Then:
        assertMetricValue("unsent-requests-queue-time-avg");
        assertMetricValue("unsent-requests-queue-time-max");
    }

    @Test
    public void shouldRecordBackgroundEventQueueSize() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordBackgroundEventQueueSize(10);

        // Then:
        assertEquals(
                metrics.metric(metrics.metricName("background-event-queue-size", CONSUMER_METRIC_GROUP)).metricValue(),
                (double) 10
        );
    }

    @Test
    public void shouldRecordBackgroundEventQueueTime() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordBackgroundEventQueueTime(METRIC_VALUE);

        // Then:
        assertMetricValue("background-event-queue-time-avg");
        assertMetricValue("background-event-queue-time-max");
    }

    @Test
    public void shouldRecordBackgroundEventQueueProcessingTime() {
        consumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX, GroupProtocol.CONSUMER);
        // When:
        consumerMetrics.recordBackgroundEventQueueProcessingTime(METRIC_VALUE);

        // Then:
        assertMetricValue("background-event-queue-processing-time-avg");
        assertMetricValue("background-event-queue-processing-time-avg");
    }

    private void assertMetricValue(final String name) {
        assertEquals(
            metrics.metric(metrics.metricName(name, CONSUMER_METRIC_GROUP)).metricValue(),
            (double) METRIC_VALUE
        );
    }
}