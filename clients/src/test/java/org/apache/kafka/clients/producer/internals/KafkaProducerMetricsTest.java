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

package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaProducerMetricsTest {
    private static final long METRIC_VALUE = 123L;
    private static final String FLUSH_TIME_TOTAL = "flush-time-ns-total";
    private static final String TXN_INIT_TIME_TOTAL = "txn-init-time-ns-total";
    private static final String TXN_BEGIN_TIME_TOTAL = "txn-begin-time-ns-total";
    private static final String TXN_COMMIT_TIME_TOTAL = "txn-commit-time-ns-total";
    private static final String TXN_ABORT_TIME_TOTAL = "txn-abort-time-ns-total";
    private static final String TXN_SEND_OFFSETS_TIME_TOTAL = "txn-send-offsets-time-ns-total";

    private final Metrics metrics = new Metrics();
    private final KafkaProducerMetrics producerMetrics = new KafkaProducerMetrics(metrics);

    @Test
    public void shouldRecordFlushTime() {
        // When:
        producerMetrics.recordFlush(METRIC_VALUE);

        // Then:
        assertMetricValue(FLUSH_TIME_TOTAL);
    }

    @Test
    public void shouldRecordInitTime() {
        // When:
        producerMetrics.recordInit(METRIC_VALUE);

        // Then:
        assertMetricValue(TXN_INIT_TIME_TOTAL);
    }

    @Test
    public void shouldRecordTxBeginTime() {
        // When:
        producerMetrics.recordBeginTxn(METRIC_VALUE);

        // Then:
        assertMetricValue(TXN_BEGIN_TIME_TOTAL);
    }

    @Test
    public void shouldRecordTxCommitTime() {
        // When:
        producerMetrics.recordCommitTxn(METRIC_VALUE);

        // Then:
        assertMetricValue(TXN_COMMIT_TIME_TOTAL);
    }

    @Test
    public void shouldRecordTxAbortTime() {
        // When:
        producerMetrics.recordAbortTxn(METRIC_VALUE);

        // Then:
        assertMetricValue(TXN_ABORT_TIME_TOTAL);
    }

    @Test
    public void shouldRecordSendOffsetsTime() {
        // When:
        producerMetrics.recordSendOffsets(METRIC_VALUE);

        // Then:
        assertMetricValue(TXN_SEND_OFFSETS_TIME_TOTAL);
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        // When:
        producerMetrics.close();

        // Then:
        assertMetricRemoved(FLUSH_TIME_TOTAL);
        assertMetricRemoved(TXN_INIT_TIME_TOTAL);
        assertMetricRemoved(TXN_BEGIN_TIME_TOTAL);
        assertMetricRemoved(TXN_COMMIT_TIME_TOTAL);
        assertMetricRemoved(TXN_ABORT_TIME_TOTAL);
        assertMetricRemoved(TXN_SEND_OFFSETS_TIME_TOTAL);
    }

    private void assertMetricRemoved(final String name) {
        assertNull(metrics.metric(metrics.metricName(name, KafkaProducerMetrics.GROUP)));
    }

    private void assertMetricValue(final String name) {
        assertEquals(
            metrics.metric(metrics.metricName(name, KafkaProducerMetrics.GROUP)).metricValue(),
            (double) METRIC_VALUE
        );
    }
}