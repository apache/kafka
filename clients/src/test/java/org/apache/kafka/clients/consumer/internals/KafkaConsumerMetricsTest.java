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

import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaConsumerMetricsTest {
  private static final long METRIC_VALUE = 123L;
  private static final String CONSUMER_GROUP_PREFIX = "consumer";
  private static final String CONSUMER_METRIC_GROUP = "consumer-metrics";

  private final Metrics metrics = new Metrics();
  private final KafkaConsumerMetrics consumerMetrics
      = new KafkaConsumerMetrics(metrics, CONSUMER_GROUP_PREFIX);

  @Test
  public void shouldRecordCommitSyncTime() {
    // When:
    consumerMetrics.recordCommitSync(METRIC_VALUE);

    // Then:
    assertMetricValue("commit-sync-time-total");
  }

  @Test
  public void shouldRecordCommittedTime() {
    // When:
    consumerMetrics.recordCommitted(METRIC_VALUE);

    // Then:
    assertMetricValue("committed-time-total");
  }

  private void assertMetricValue(final String name) {
    assertEquals(
        metrics.metric(metrics.metricName(name, CONSUMER_METRIC_GROUP)).metricValue(),
        (double) METRIC_VALUE
    );
  }
}