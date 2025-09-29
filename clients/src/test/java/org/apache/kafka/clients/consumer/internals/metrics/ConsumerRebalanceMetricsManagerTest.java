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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;


class ConsumerRebalanceMetricsManagerTest {

    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @Test
    public void testAssignedPartitionCountMetric() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager consumerRebalanceMetricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        assertNotNull(metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount), "Metric assigned-partitions has not been registered as expected");

        // Check for manually assigned partitions
        subscriptionState.assignFromUser(Set.of(new TopicPartition("topic", 0), new TopicPartition("topic", 1)));
        assertEquals(2.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());
        subscriptionState.assignFromUser(Set.of());
        assertEquals(0.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());

        subscriptionState.unsubscribe();
        assertEquals(0.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());

        // Check for automatically assigned partitions
        subscriptionState.subscribe(Set.of("topic"), Optional.empty());
        subscriptionState.assignFromSubscribed(Set.of(new TopicPartition("topic", 0)));
        assertEquals(1.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());
    }

    @Test
    public void testRebalanceTimingMetrics() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

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
        assertEquals(20.0d, metrics.metric(metricsManager.rebalanceLatencyAvg).metricValue()); // (10 + 30) / 2
        assertEquals(30.0d, metrics.metric(metricsManager.rebalanceLatencyMax).metricValue()); // max(10, 30)
        assertEquals(40.0d, metrics.metric(metricsManager.rebalanceLatencyTotal).metricValue()); // 10 + 30
        assertEquals(2.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());

        // Record third rebalance (50ms duration)
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(50);
        metricsManager.recordRebalanceEnded(time.milliseconds());

        // Verify metrics after third rebalance
        assertEquals(30.0d, metrics.metric(metricsManager.rebalanceLatencyAvg).metricValue()); // (10 + 30 + 50) / 3
        assertEquals(50.0d, metrics.metric(metricsManager.rebalanceLatencyMax).metricValue()); // max(10, 30, 50)
        assertEquals(90.0d, metrics.metric(metricsManager.rebalanceLatencyTotal).metricValue()); // 10 + 30 + 50
        assertEquals(3.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
    }

    @Test
    public void testRebalanceRateMetric() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        // Verify rate metric is registered
        assertNotNull(metrics.metric(metricsManager.rebalanceRatePerHour));

        // Record 3 rebalances within 30ms total (3 x 10ms)
        for (int i = 0; i < 3; i++) {
            metricsManager.recordRebalanceStarted(time.milliseconds());
            time.sleep(10);
            metricsManager.recordRebalanceEnded(time.milliseconds());
        }

        // The rate metric uses a Rate stat with WindowedCount
        // Based on the ShareRebalanceMetricsManagerTest, we expect 360.0 for this scenario
        double ratePerHour = (Double) metrics.metric(metricsManager.rebalanceRatePerHour).metricValue();
        assertEquals(360.0d, ratePerHour, 1.0, "Should be approximately 360 rebalances per hour");
    }

    @Test
    public void testFailedRebalanceMetrics() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        // Verify failed rebalance metrics are registered
        assertNotNull(metrics.metric(metricsManager.failedRebalanceTotal));
        assertNotNull(metrics.metric(metricsManager.failedRebalanceRate));

        // Initially, no failed rebalances
        assertEquals(0.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue());

        // Start a rebalance but don't complete it
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(10);
        
        // Record a failure
        metricsManager.maybeRecordRebalanceFailed();
        assertEquals(1.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue());

        // Complete a successful rebalance
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(10);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        
        // Try to record failure after successful rebalance (should not increment)
        metricsManager.maybeRecordRebalanceFailed();
        assertEquals(1.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue());

        // Start another rebalance, don't complete it, then record failure
        time.sleep(10); // Advance time before starting next rebalance
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), "Rebalance should be in progress");
        time.sleep(10);
        // Don't call recordRebalanceEnded, so the rebalance is still in progress
        metricsManager.maybeRecordRebalanceFailed();
        assertEquals(2.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue());

        // Verify failed rebalance rate
        // We recorded 2 failures: first at ~10ms, second at ~40ms (total elapsed ~50ms)
        // With minimum window of 30 seconds: 2 failures / 30 seconds = 240 failures/hour
        double failedRate = (Double) metrics.metric(metricsManager.failedRebalanceRate).metricValue();
        assertEquals(240.0d, failedRate, 1.0, "Should be approximately 240 failed rebalances per hour");
    }

    @Test
    public void testLastRebalanceSecondsAgoMetric() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        // Verify metric is registered
        assertNotNull(metrics.metric(metricsManager.lastRebalanceSecondsAgo));

        // Initially, no rebalance has occurred
        assertEquals(-1.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue());

        // Complete a rebalance
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(10);
        metricsManager.recordRebalanceEnded(time.milliseconds());

        // Immediately after rebalance, should be 0 seconds
        assertEquals(0.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue());

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

        // Should reset to 0
        assertEquals(0.0d, metrics.metric(metricsManager.lastRebalanceSecondsAgo).metricValue());
    }

    @Test
    public void testRebalanceStartedFlag() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        // Initially, no rebalance in progress
        assertFalse(metricsManager.rebalanceStarted());

        // Start rebalance
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted());

        // End rebalance
        time.sleep(10);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        assertFalse(metricsManager.rebalanceStarted());

        // Start another rebalance - advance time first
        time.sleep(100);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted());
    }

    @Test
    public void testMultipleConsecutiveFailures() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        // Record multiple consecutive failures
        for (int i = 0; i < 5; i++) {
            metricsManager.recordRebalanceStarted(time.milliseconds());
            time.sleep(10);
            metricsManager.maybeRecordRebalanceFailed();
        }

        assertEquals(5.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue());
        
        // Successful rebalances should still be 0
        assertEquals(0.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
    }

    @Test
    public void testMixedSuccessAndFailureScenarios() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager metricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        // Success -> Failure -> Success -> Failure pattern
        // First success
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(20);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        assertEquals(1.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
        
        // First failure - advance time to ensure new timestamps
        time.sleep(10);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), "First failure rebalance should be in progress");
        time.sleep(30);
        metricsManager.maybeRecordRebalanceFailed();
        
        // Check failure was recorded
        double failedAfterFirst = (Double) metrics.metric(metricsManager.failedRebalanceTotal).metricValue();
        assertEquals(1.0d, failedAfterFirst, "Should have one failed rebalance");
        
        // Second success - advance time to ensure new timestamps
        time.sleep(10);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        time.sleep(40);
        metricsManager.recordRebalanceEnded(time.milliseconds());
        assertEquals(2.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
        
        // Second failure - advance time to ensure new timestamps
        time.sleep(10);
        metricsManager.recordRebalanceStarted(time.milliseconds());
        assertTrue(metricsManager.rebalanceStarted(), "Second failure rebalance should be in progress");
        time.sleep(50);
        metricsManager.maybeRecordRebalanceFailed();

        // Verify final counts
        assertEquals(2.0d, metrics.metric(metricsManager.rebalanceTotal).metricValue());
        assertEquals(2.0d, metrics.metric(metricsManager.failedRebalanceTotal).metricValue());
        
        // Verify timing metrics (only successful rebalances contribute)
        assertEquals(30.0d, metrics.metric(metricsManager.rebalanceLatencyAvg).metricValue()); // (20 + 40) / 2
        assertEquals(40.0d, metrics.metric(metricsManager.rebalanceLatencyMax).metricValue());
        assertEquals(60.0d, metrics.metric(metricsManager.rebalanceLatencyTotal).metricValue()); // 20 + 40
    }
}
