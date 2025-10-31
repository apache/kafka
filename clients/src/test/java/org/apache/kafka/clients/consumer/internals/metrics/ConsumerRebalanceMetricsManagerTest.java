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

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;


class ConsumerRebalanceMetricsManagerTest {

    private Time time;
    private Metrics metrics;
    private SubscriptionState subscriptionState;
    private ConsumerRebalanceMetricsManager metricsManager;
    private MetricConfig metricConfig;
    private long windowSizeMs;
    private int numSamples;

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        // Use MetricConfig with its default values
        windowSizeMs = 30000; // 30 seconds - default value
        numSamples = 2; // default value
        metricConfig = new MetricConfig()
                .samples(numSamples)
                .timeWindow(windowSizeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        metrics = new Metrics(metricConfig, time);
        subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testAssignedPartitionCountMetric() {
        assertNotNull(metrics.metric(metricsManager.assignedPartitionsCount), "Metric assigned-partitions has not been registered as expected");

        // Check for manually assigned partitions
        subscriptionState.assignFromUser(Set.of(new TopicPartition("topic", 0), new TopicPartition("topic", 1)));
        assertEquals(2.0d, metrics.metric(metricsManager.assignedPartitionsCount).metricValue());
        subscriptionState.assignFromUser(Set.of());
        assertEquals(0.0d, metrics.metric(metricsManager.assignedPartitionsCount).metricValue());

        subscriptionState.unsubscribe();
        assertEquals(0.0d, metrics.metric(metricsManager.assignedPartitionsCount).metricValue());

        // Check for automatically assigned partitions
        subscriptionState.subscribe(Set.of("topic"), Optional.empty());
        subscriptionState.assignFromSubscribed(Set.of(new TopicPartition("topic", 0)));
        assertEquals(1.0d, metrics.metric(metricsManager.assignedPartitionsCount).metricValue());
    }

    @Test
    public void testRebalanceTimingMetrics() {

        // Verify timing metrics are registered
        assertNotNull(metrics.metric(metricsManager.rebalanceLatencyAvg));
        assertNotNull(metrics.metric(metricsManager.rebalanceLatencyMax));
        assertNotNull(metrics.metric(metricsManager.rebalanceLatencyTotal));
        assertNotNull(metrics.metric(metricsManager.rebalanceTotal));

        // Record first rebalance (10ms duration)
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(10);
        metricsManager.recordRebalanceEnded(time.milliseconds());

        // Verify metrics after first rebalance
        assertEquals(10.0d, metrics.metric(metricsManager.rebalanceLatencyAvg).metricValue());
        assertEquals(10.0d, metrics.metric(metricsManager.rebalanceLatencyMax).metricValue());
        assertEquals(10.0d, metrics.metric(metricsManager.rebalanceLatencyTotal).metricValue());
        assertEquals(1.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());

        // Record second rebalance (30ms duration)
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(30);
        metricsManager.recordRebalanceEnded(time.milliseconds());

        // Verify metrics after second rebalance
        assertEquals(20.0d, metrics.metric(metricsManager.rebalanceLatencyAvg).metricValue(),
                "Average latency should be (10 + 30) / 2 = 20ms");
        assertEquals(30.0d, metrics.metric(metricsManager.rebalanceLatencyMax).metricValue(),
                "Max latency should be max(10, 30) = 30ms");
        assertEquals(40.0d, metrics.metric(metricsManager.rebalanceLatencyTotal).metricValue(),
                "Total latency should be 10 + 30 = 40ms");
        assertEquals(2.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());

        // Record third rebalance (50ms duration)
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(50);
        metricsManager.recordRebalanceEnded(time.milliseconds());

        // Verify metrics after third rebalance
        assertEquals(30.0d, metrics.metric(metricsManager.rebalanceLatencyAvg).metricValue(),
                "Average latency should be (10 + 30 + 50) / 3 = 30ms");
        assertEquals(50.0d, metrics.metric(metricsManager.rebalanceLatencyMax).metricValue(),
                "Max latency should be max(10, 30, 50) = 50ms");
        assertEquals(90.0d, metrics.metric(metricsManager.rebalanceLatencyTotal).metricValue(),
                "Total latency should be 10 + 30 + 50 = 90ms");
        assertEquals(3.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
    }

    @Test
    public void testRebalanceRateMetric() {

        // Verify rate metric is registered
        assertNotNull(metrics.metric(metricsManager.rebalanceRatePerHour));

        // Record 3 rebalances within 30ms total (3 x 10ms)
        int rebalanceCount = 3;
        long startTime = time.milliseconds();
        for (int i = 0; i < rebalanceCount; i++) {
            metricsManager.recordRebalanceStarted(time.milliseconds());
            time.sleep(10);
            metricsManager.recordRebalanceEnded(time.milliseconds());
        }
        long endTime = time.milliseconds();
        long actualElapsedMs = endTime - startTime;

        double ratePerHour = (Double) metrics.metric(metricsManager.rebalanceRatePerHour).metricValue();
        
        // The Rate metric calculation:
        // - Uses elapsed time from the oldest sample
        // - Ensures minimum window size of (numSamples - 1) * windowSizeMs
        // - With default config: minWindow = (2-1) * 30000 = 30000ms
        long minWindowMs = (numSamples - 1) * windowSizeMs; // (2-1) * 30000 = 30000ms
        
        // Since actualElapsedMs (30ms) is much less than minWindowMs (30000ms), 
        // the rate calculation will use minWindowMs as the window
        // Rate per hour = count / (windowMs / 1000) * 3600
        double expectedRatePerHour = (double) rebalanceCount / (minWindowMs / 1000.0) * 3600.0;
        
        assertEquals(expectedRatePerHour, ratePerHour, 1.0,
                String.format("With %d rebalances in %dms, min window %dms: expecting %.1f rebalances/hour", 
                        rebalanceCount, actualElapsedMs, minWindowMs, expectedRatePerHour));
    }

    @Test
    public void testFailedRebalanceMetrics() {

        // Verify failed rebalance metrics are registered
        assertNotNull(metrics.metric(metricsManager.failedRebalanceTotal));
        assertNotNull(metrics.metric(metricsManager.failedRebalanceRate));

        assertEquals(0.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue(),
                "Initially, there should be no failed rebalances");

        // Start a rebalance but don't complete it
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(10);
        
        metricsManager.maybeRecordRebalanceFailed();
        assertEquals(1.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue(),
                "Failed rebalance count should increment to 1 after recording failure");

        // Complete a successful rebalance
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(10);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        
        metricsManager.maybeRecordRebalanceFailed();
        assertEquals(1.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue(),
                "Failed count should not increment after successful rebalance completes");

        // Start another rebalance, don't complete it, then record failure
        time.sleep(10);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), "Rebalance should be in progress");
        time.sleep(10);
        // Don't call recordRebalanceEnded() to simulate an incomplete rebalance
        metricsManager.maybeRecordRebalanceFailed();
        assertEquals(2.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue());

        double failedRate = (Double) metrics.metric(metricsManager.failedRebalanceRate).metricValue();
        
        // Calculate expected failed rate based on Rate metric behavior
        // We had 2 failures over ~40ms, but minimum window is (numSamples - 1) * windowSizeMs
        long minWindowMs = (numSamples - 1) * windowSizeMs; // (2-1) * 30000 = 30000ms
        double expectedFailedRatePerHour = 2.0 / (minWindowMs / 1000.0) * 3600.0;
        
        assertEquals(expectedFailedRatePerHour, failedRate, 1.0, 
                String.format("With 2 failures, min window %dms: expecting %.1f failures/hour", 
                        minWindowMs, expectedFailedRatePerHour));
    }

    @Test
    public void testLastRebalanceSecondsAgoMetric() {

        // Verify metric is registered
        assertNotNull(metrics.metric(metricsManager.lastRebalanceSecondsAgo));

        assertEquals(-1.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue(),
                "Should return -1 when no rebalance has occurred");

        // Complete a rebalance
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(10);
        metricsManager.recordRebalanceEnded(time.milliseconds());

        assertEquals(0.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue(),
                "Should return 0 immediately after rebalance completes");

        // Advance time by 5 seconds
        time.sleep(5000);
        assertEquals(5.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue());

        // Advance time by another 10 seconds
        time.sleep(10000);
        assertEquals(15.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue());

        // Complete another rebalance
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(20);
        metricsManager.recordRebalanceEnded(time.milliseconds());

        assertEquals(0.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue(),
                "Should reset to 0 after a new rebalance completes");
    }

    @Test
    public void testRebalanceStartedFlag() {

        assertFalse(metricsManager.rebalanceStarted(), 
                "Initially, no rebalance should be in progress");

        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), 
                "Rebalance should be marked as started after recordRebalanceStarted()");

        time.sleep(10);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        assertFalse(metricsManager.rebalanceStarted(), 
                "Rebalance should not be in progress after recordRebalanceEnded()");

        // Start another rebalance - advance time first
        time.sleep(100);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), 
                "New rebalance should be marked as started");
    }

    @Test
    public void testMultipleConsecutiveFailures() {

        // Record multiple consecutive failures
        for (int i = 0; i < 5; i++) {
            metricsManager.recordRebalanceStarted(time.milliseconds());
            time.sleep(10);
            metricsManager.maybeRecordRebalanceFailed();
        }

        assertEquals(5.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue(),
                "Should have recorded 5 consecutive failed rebalances");
        
        assertEquals(0.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue(),
                "Successful rebalance count should remain 0 when only failures occur");
    }

    @Test
    public void testMixedSuccessAndFailureScenarios() {

        // Success -> Failure -> Success -> Failure pattern
        // First success
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(20);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        assertEquals(1.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
        
        // First failure
        time.sleep(10);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), "First failure rebalance should be in progress");
        time.sleep(30);
        metricsManager.maybeRecordRebalanceFailed();
        
        double failedAfterFirst = (Double) metrics.metric(metricsManager.failedRebalanceTotal).metricValue();
        assertEquals(1.0d, failedAfterFirst, "Should have recorded one failed rebalance after first failure");
        
        // Second success
        time.sleep(10);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(40);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        assertEquals(2.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
        
        // Second failure
        time.sleep(10);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), "Second failure rebalance should be in progress");
        time.sleep(50);
        metricsManager.maybeRecordRebalanceFailed();

        assertEquals(2.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue(),
                "Should have 2 successful rebalances in mixed scenario");
        assertEquals(2.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue(),
                "Should have 2 failed rebalances in mixed scenario");
        
        assertEquals(30.0d, metrics.metric(metricsManager.rebalanceLatencyAvg).metricValue(),
                "Average latency should only include successful rebalances: (20 + 40) / 2 = 30ms");
        assertEquals(40.0d, metrics.metric(metricsManager.rebalanceLatencyMax).metricValue(),
                "Max latency should be 40ms from successful rebalances only");
        assertEquals(60.0d, metrics.metric(metricsManager.rebalanceLatencyTotal).metricValue(),
                "Total latency should only include successful rebalances: 20 + 40 = 60ms");
    }
}
