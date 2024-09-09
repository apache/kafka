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
package org.apache.kafka.server.log.remote.quota;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.quota.QuotaType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RLMQuotaManagerTest {
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time);
    private static final QuotaType QUOTA_TYPE = QuotaType.RLM_FETCH;
    private static final String DESCRIPTION = "Tracking byte rate";

    @Test
    public void testQuotaExceeded() {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(50, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        assertEquals(0L, quotaManager.getThrottleTimeMs());
        quotaManager.record(500);
        // Move clock by 1 sec, quota is violated
        moveClock(1);
        assertEquals(9_000L, quotaManager.getThrottleTimeMs());

        // Move clock by another 8 secs, quota is still violated for the window
        moveClock(8);
        assertEquals(1_000L, quotaManager.getThrottleTimeMs());

        // Move clock by 1 sec, quota is no more violated
        moveClock(1);
        assertEquals(0L, quotaManager.getThrottleTimeMs());
    }

    @Test
    public void testQuotaUpdate() {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(50, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        assertFalse(quotaManager.getThrottleTimeMs() > 0);
        quotaManager.record(51);
        assertTrue(quotaManager.getThrottleTimeMs() > 0);

        Map<MetricName, KafkaMetric> fetchQuotaMetrics = metrics.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("byte-rate") && entry.getKey().group().equals(QUOTA_TYPE.toString()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<MetricName, KafkaMetric> nonQuotaMetrics = metrics.metrics().entrySet().stream()
            .filter(entry -> !entry.getKey().name().equals("byte-rate") || !entry.getKey().group().equals(QUOTA_TYPE.toString()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(1, fetchQuotaMetrics.size());
        assertFalse(nonQuotaMetrics.isEmpty());

        Map<MetricName, MetricConfig> configForQuotaMetricsBeforeUpdate = extractMetricConfig(fetchQuotaMetrics);
        Map<MetricName, MetricConfig> configForNonQuotaMetricsBeforeUpdate = extractMetricConfig(nonQuotaMetrics);

        // Update quota to 60, quota is no more violated
        Quota quota60Bytes = new Quota(60, true);
        quotaManager.updateQuota(quota60Bytes);
        assertFalse(quotaManager.getThrottleTimeMs() > 0);

        // Verify quota metrics were updated
        Map<MetricName, MetricConfig> configForQuotaMetricsAfterFirstUpdate = extractMetricConfig(fetchQuotaMetrics);
        assertNotEquals(configForQuotaMetricsBeforeUpdate, configForQuotaMetricsAfterFirstUpdate);
        fetchQuotaMetrics.values().forEach(metric -> assertEquals(quota60Bytes, metric.config().quota()));
        // Verify non quota metrics are unchanged
        assertEquals(configForNonQuotaMetricsBeforeUpdate, extractMetricConfig(nonQuotaMetrics));

        // Update quota to 40, quota is violated again
        Quota quota40Bytes = new Quota(40, true);
        quotaManager.updateQuota(quota40Bytes);
        assertTrue(quotaManager.getThrottleTimeMs() > 0);

        // Verify quota metrics were updated
        assertNotEquals(configForQuotaMetricsAfterFirstUpdate, extractMetricConfig(fetchQuotaMetrics));
        fetchQuotaMetrics.values().forEach(metric -> assertEquals(quota40Bytes, metric.config().quota()));
        // Verify non quota metrics are unchanged
        assertEquals(configForNonQuotaMetricsBeforeUpdate, extractMetricConfig(nonQuotaMetrics));
    }

    private void moveClock(int secs) {
        time.setCurrentTimeMs(time.milliseconds() + secs * 1000L);
    }

    private Map<MetricName, MetricConfig> extractMetricConfig(Map<MetricName, KafkaMetric> metrics) {
        return metrics.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().config()));
    }
}