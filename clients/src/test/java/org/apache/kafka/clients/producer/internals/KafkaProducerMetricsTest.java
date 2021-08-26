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

class KafkaProducerMetricsTest {
  private static final long METRIC_VALUE = 123L;

  private final Metrics metrics = new Metrics();
  private final KafkaProducerMetrics producerMetrics = new KafkaProducerMetrics(metrics);

  @Test
  public void shouldRecordFlushTime() {
    // When:
    producerMetrics.recordFlush(METRIC_VALUE);

    // Then:
    assertMetricValue("flush-time-total");
  }

  @Test
  public void shouldRecordInitTime() {
    // When:
    producerMetrics.recordInit(METRIC_VALUE);

    // Then:
    assertMetricValue("txn-init-time-total");
  }

  @Test
  public void shouldRecordTxBeginTime() {
    // When:
    producerMetrics.recordBeginTxn(METRIC_VALUE);

    // Then:
    assertMetricValue("txn-begin-time-total");
  }

  @Test
  public void shouldRecordTxCommitTime() {
    // When:
    producerMetrics.recordCommitTxn(METRIC_VALUE);

    // Then:
    assertMetricValue("txn-commit-time-total");
  }

  @Test
  public void shouldRecordTxAbortTime() {
    // When:
    producerMetrics.recordAbortTxn(METRIC_VALUE);

    // Then:
    assertMetricValue("txn-abort-time-total");
  }

  @Test
  public void shouldRecordSendOffsetsTime() {
    // When:
    producerMetrics.recordSendOffsets(METRIC_VALUE);

    // Then:
    assertMetricValue("txn-send-offsets-time-total");
  }

  private void assertMetricValue(final String name) {
    assertEquals(
        metrics.metric(metrics.metricName(name, KafkaProducerMetrics.GROUP)).metricValue(),
        (double) METRIC_VALUE
    );
  }
}