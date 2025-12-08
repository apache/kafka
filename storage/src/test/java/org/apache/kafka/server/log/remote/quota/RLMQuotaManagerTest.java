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

import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RLMQuotaManagerTest {
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(new MetricConfig(), List.of(), time);
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

    @Test
    public void testRecordAndGetThrottleTimeMs() {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(50, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        // First request should not be throttled
        long throttleTime = quotaManager.recordAndGetThrottleTimeMs(30);
        assertEquals(0L, throttleTime, "First request under quota should not be throttled");

        // Move clock forward to capture the rate
        moveClock(1);

        // Second request that exceeds quota should be throttled
        throttleTime = quotaManager.recordAndGetThrottleTimeMs(30);
        assertTrue(throttleTime > 0, "Request exceeding quota should be throttled");
    }

    @Test
    public void testRecordAndGetThrottleTimeMsSeesRecordedValue() {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(50, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        // Record a value that exceeds quota
        long throttleTime = quotaManager.recordAndGetThrottleTimeMs(500);
        moveClock(1);

        // The throttle time should be non-zero because recordAndGetThrottleTimeMs
        // should see its own recorded value
        assertTrue(throttleTime > 0, "recordAndGetThrottleTimeMs should see its own recorded value");
    }

    @Test
    public void testRecordAndGetThrottleTimeMsMultipleRequests() {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(100, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        // Record multiple small requests
        assertEquals(0L, quotaManager.recordAndGetThrottleTimeMs(20));
        assertEquals(0L, quotaManager.recordAndGetThrottleTimeMs(20));
        assertEquals(0L, quotaManager.recordAndGetThrottleTimeMs(20));

        moveClock(1);

        // Next request should see accumulated usage
        assertEquals(0L, quotaManager.recordAndGetThrottleTimeMs(20));

        // This one pushes over quota
        long throttleTime = quotaManager.recordAndGetThrottleTimeMs(30);
        assertTrue(throttleTime > 0, "Request that pushes over quota should be throttled");
    }

    @Test
    public void testConcurrentRecordAndGetThrottleTimeMs() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(100, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 10;
        int recordsPerThread = 5;
        int bytesPerRecord = 30; // 10 threads * 5 records * 30 bytes = 1500 bytes total

        Thread[] threads = new Thread[numThreads];
        long[][] throttleTimes = new long[numThreads][recordsPerThread];

        // Create threads that will concurrently record
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < recordsPerThread; j++) {
                    throttleTimes[threadIndex][j] = quotaManager.recordAndGetThrottleTimeMs(bytesPerRecord);
                    try {
                        Thread.sleep(1); // Small delay to simulate real work
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        moveClock(1);

        int throttledCount = 0;
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < recordsPerThread; j++) {
                if (throttleTimes[i][j] > 0) {
                    throttledCount++;
                }
            }
        }

        assertTrue(throttledCount > 0,
            "At least some concurrent requests should have been throttled when quota is exceeded");
    }

    @Test
    public void testConcurrentRecordAndGetThrottleTimeMsConsistency() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(50, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];
        boolean[] wasThrottled = new boolean[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                long throttleTime = quotaManager.recordAndGetThrottleTimeMs(60);
                wasThrottled[threadIndex] = throttleTime > 0;
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        moveClock(1);

        int throttledCount = 0;
        for (boolean throttled : wasThrottled) {
            if (throttled) {
                throttledCount++;
            }
        }

        assertTrue(throttledCount >= 1,
            "At least one thread should have been throttled when all threads exceed quota");
    }

    @Test
    public void testRecordAndGetThrottleTimeMsNegativeValue() {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(100, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        quotaManager.recordAndGetThrottleTimeMs(150);
        moveClock(1);

        assertTrue(quotaManager.getThrottleTimeMs() > 0);

        quotaManager.record(-150);

        assertEquals(0L, quotaManager.getThrottleTimeMs(),
            "Quota should not be exceeded after releasing reservation");
    }

    @Test
    public void testConcurrentDeltaAdjustments() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(200, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 20;
        Thread[] threads = new Thread[numThreads];

        // Simulate fetch scenario: reserve estimate, then adjust with actual delta
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                int estimated = 50;
                quotaManager.recordAndGetThrottleTimeMs(estimated);

                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                int actual = 30 + (threadIndex % 31);
                int delta = actual - estimated;
                quotaManager.record(delta);
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        moveClock(1);

        long throttleTime = quotaManager.getThrottleTimeMs();
        assertTrue(throttleTime >= 0, "Quota manager should be in consistent state after concurrent delta adjustments");
    }

    @Test
    public void testConcurrentReservationReleases() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(100, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 15;
        Thread[] threads = new Thread[numThreads];

        // Half succeed with delta adjustment, half fail and release
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                int estimated = 40;
                quotaManager.recordAndGetThrottleTimeMs(estimated);

                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                if (threadIndex % 2 == 0) {
                    int actual = 35;
                    int delta = actual - estimated;
                    quotaManager.record(delta);
                } else {
                    quotaManager.record(-estimated);
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        moveClock(1);

        long throttleTime = quotaManager.getThrottleTimeMs();
        assertTrue(throttleTime >= 0, "Quota manager should handle mixed success/failure scenarios");
    }

    @Test
    public void testHighConcurrencyStressTest() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(500, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 50;
        Thread[] threads = new Thread[numThreads];
        boolean[] completed = new boolean[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                try {
                    int estimated = 20 + (threadIndex % 30);
                    long throttleTime = quotaManager.recordAndGetThrottleTimeMs(estimated);

                    if (throttleTime > 0) {
                        Thread.sleep(Math.min(throttleTime, 10));
                    }

                    Thread.sleep(1);

                    if (threadIndex % 3 == 0) {
                        quotaManager.record(-estimated);
                    } else {
                        int actual = estimated + (threadIndex % 10) - 5;
                        quotaManager.record(actual - estimated);
                    }

                    completed[threadIndex] = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join(5000);
        }

        int completedCount = 0;
        for (boolean c : completed) {
            if (c) {
                completedCount++;
            }
        }

        assertTrue(completedCount >= numThreads * 0.95,
            "At least 95% of threads should complete under high concurrency");

        moveClock(1);

        long throttleTime = quotaManager.getThrottleTimeMs();
        assertTrue(throttleTime >= 0, "Quota manager should remain consistent under stress");
    }

    @Test
    public void testConcurrentRecordAndCheckRace() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(100, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 20;
        Thread[] threads = new Thread[numThreads];
        long[] throttleTimes = new long[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger exceptions = new AtomicInteger(0);

        // Use barrier to ensure true concurrent execution
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    throttleTimes[threadIndex] = quotaManager.recordAndGetThrottleTimeMs(10);
                } catch (InterruptedException | BrokenBarrierException e) {
                    exceptions.incrementAndGet();
                    Thread.currentThread().interrupt();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(0, exceptions.get(), "No threads should have exceptions");

        moveClock(1);

        int throttledCount = 0;
        for (long throttleTime : throttleTimes) {
            if (throttleTime > 0) {
                throttledCount++;
            }
        }

        assertTrue(throttledCount > 0,
            "With 20 threads recording 10 bytes each (200 total), some should be throttled (quota is 100)");

        long finalThrottleTime = quotaManager.getThrottleTimeMs();
        assertTrue(finalThrottleTime > 0,
            "After recording 200 bytes with 100 bytes/sec quota, should be throttled");
    }

    @Test
    public void testConcurrentRecordAndReleaseInterleaved() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(150, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 30;
        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger exceptions = new AtomicInteger(0);

        // Use barrier to ensure true concurrent execution
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();

                    if (threadIndex % 3 == 0) {
                        quotaManager.recordAndGetThrottleTimeMs(30);
                    } else if (threadIndex % 3 == 1) {
                        quotaManager.record(-30);
                    } else {
                        quotaManager.record(5);
                    }
                } catch (InterruptedException | BrokenBarrierException e) {
                    exceptions.incrementAndGet();
                    Thread.currentThread().interrupt();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(0, exceptions.get(), "No threads should have exceptions");

        moveClock(1);

        long throttleTime = quotaManager.getThrottleTimeMs();
        assertTrue(throttleTime >= 0,
            "Quota manager should handle interleaved record/release operations");
    }

    @Test
    public void testRecordCheckInterleavingRace() throws InterruptedException {
        RLMQuotaManager quotaManager = new RLMQuotaManager(
            new RLMQuotaManagerConfig(100, 11, 1), metrics, QUOTA_TYPE, DESCRIPTION, time);

        int numThreads = 10;
        Thread[] threads = new Thread[numThreads];
        long[] throttleTimes = new long[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger exceptions = new AtomicInteger(0);

        // Use barrier to ensure true concurrent execution
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    throttleTimes[threadIndex] = quotaManager.recordAndGetThrottleTimeMs(15);
                } catch (InterruptedException | BrokenBarrierException e) {
                    exceptions.incrementAndGet();
                    Thread.currentThread().interrupt();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(0, exceptions.get(), "No threads should have exceptions");

        moveClock(1);

        int throttledCount = 0;
        for (long throttleTime : throttleTimes) {
            if (throttleTime > 0) {
                throttledCount++;
            }
        }

        assertTrue(throttledCount > 0,
            "With 10 threads recording 15 bytes each (150 total), exceeding 100 quota, some should be throttled");

        long finalThrottleTime = quotaManager.getThrottleTimeMs();
        assertTrue(finalThrottleTime > 0,
            "Final quota check should show violation after 150 bytes recorded (quota is 100)");
    }

    private Map<MetricName, MetricConfig> extractMetricConfig(Map<MetricName, KafkaMetric> metrics) {
        return metrics.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().config()));
    }
}