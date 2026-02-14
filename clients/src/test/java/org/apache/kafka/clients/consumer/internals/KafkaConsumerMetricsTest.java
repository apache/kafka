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

import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConsumerMetricsTest {
    private static final long METRIC_VALUE = 123L;
    private static final String CONSUMER_METRIC_GROUP = "consumer-metrics";
    private static final String COMMIT_SYNC_TIME_TOTAL = "commit-sync-time-ns-total";
    private static final String COMMITTED_TIME_TOTAL = "committed-time-ns-total";

    private final Metrics metrics = new Metrics();
    private final KafkaConsumerMetrics consumerMetrics
        = new KafkaConsumerMetrics(metrics);

    @Test
    public void shouldRecordCommitSyncTime() {
        // When:
        consumerMetrics.recordCommitSync(METRIC_VALUE);

        // Then:
        assertMetricValue(COMMIT_SYNC_TIME_TOTAL);
    }

    @Test
    public void shouldRecordCommittedTime() {
        // When:
        consumerMetrics.recordCommitted(METRIC_VALUE);

        // Then:
        assertMetricValue(COMMITTED_TIME_TOTAL);
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        // When:
        consumerMetrics.close();

        // Then:
        assertMetricRemoved(COMMIT_SYNC_TIME_TOTAL);
        assertMetricRemoved(COMMITTED_TIME_TOTAL);
    }

    @Test
    public void checkMetricsAfterCreation() {
        Set<MetricName> expectedMetrics = Set.of(
            metrics.metricName("last-poll-seconds-ago", CONSUMER_METRIC_GROUP),
            metrics.metricName("time-between-poll-avg", CONSUMER_METRIC_GROUP),
            metrics.metricName("time-between-poll-max", CONSUMER_METRIC_GROUP),
            metrics.metricName("poll-idle-ratio-avg", CONSUMER_METRIC_GROUP),
            metrics.metricName("commit-sync-time-ns-total", CONSUMER_METRIC_GROUP),
            metrics.metricName("committed-time-ns-total", CONSUMER_METRIC_GROUP)
        );
        expectedMetrics.forEach(
            metricName -> assertTrue(
                metrics.metrics().containsKey(metricName),
                "Missing metric: " + metricName
            )
        );
        consumerMetrics.close();
        expectedMetrics.forEach(
                metricName -> assertFalse(
                        metrics.metrics().containsKey(metricName),
                        "Metric present after close: " + metricName
                )
        );
    }

    private void assertMetricRemoved(final String name) {
        assertNull(metrics.metric(metrics.metricName(name, CONSUMER_METRIC_GROUP)));
    }

    private void assertMetricValue(final String name) {
        assertEquals(
            (double) METRIC_VALUE,
            metrics.metric(metrics.metricName(name, CONSUMER_METRIC_GROUP)).metricValue()
        );
    }
}