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

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareFetchEvent;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.Double.NaN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class NetworkThreadMetricsManagerTest {
    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final long shortTimeToSleep = 100;
    private final NetworkThreadMetricsManager networkThreadMetricsManager = new NetworkThreadMetricsManager(metrics);

    @Test
    public void testTimeBetweenNetworkThreadPollMetrics() {
        // Assert the existence of metrics
        assertNotNull(metrics.metric(networkThreadMetricsManager.pollTimeMax));
        assertNotNull(metrics.metric(networkThreadMetricsManager.pollTimeAvg));

        // Record poll time and sleep
        long currentTimeMs = time.milliseconds();
        networkThreadMetricsManager.updatePollTime(currentTimeMs);
        time.sleep(shortTimeToSleep);

        // Record poll time
        currentTimeMs = time.milliseconds();
        networkThreadMetricsManager.updatePollTime(currentTimeMs);

        // Randomly sleep 1-10 seconds
        Random rand = new Random();
        int randomSleepTime = rand.nextInt(10) + 1;
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepTime));

        // Record poll time
        currentTimeMs = time.milliseconds();
        networkThreadMetricsManager.updatePollTime(currentTimeMs);

        long totalTimeBetweenPolls = randomSleepTime * 1000 + shortTimeToSleep;

        assertEquals(metrics.metric(networkThreadMetricsManager.pollTimeAvg).metricValue(), totalTimeBetweenPolls / 2d);
        assertEquals(metrics.metric(networkThreadMetricsManager.pollTimeMax).metricValue(), randomSleepTime * 1000d);
    }

    @Test
    public void testApplicationEventQueueSizeMetric() {
        // Assert the existence of metrics
        assertNotNull(metrics.metric(networkThreadMetricsManager.applicationEventQueueSize));

        // Record application event queue size
        networkThreadMetricsManager.recordApplicationEventQueueSize(10);
        networkThreadMetricsManager.recordApplicationEventQueueSize(20);
        networkThreadMetricsManager.recordApplicationEventQueueSize(30);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.applicationEventQueueSize).metricValue(), 30d);
    }

    @Test
    public void testApplicationEventQueueChangeMetrics() {
        PollEvent pollEvent = new PollEvent(0);
        ShareFetchEvent shareFetchEvent = new ShareFetchEvent(null);

        // Assert the existence of metrics
        assertNotNull(metrics.metric(networkThreadMetricsManager.appEventTimeAvg));
        assertNotNull(metrics.metric(networkThreadMetricsManager.appEventTimeMax));

        // Record application event queue change
        networkThreadMetricsManager.recordApplicationEventQueueChange(pollEvent, time.milliseconds(), true);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.appEventTimeAvg).metricValue(), NaN);
        assertEquals(metrics.metric(networkThreadMetricsManager.appEventTimeMax).metricValue(), NaN);

        // Randomly sleep 1-10 seconds
        Random rand = new Random();
        int randomSleepTime = rand.nextInt(10) + 1;
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepTime));

        // Record application event queue change
        networkThreadMetricsManager.recordApplicationEventQueueChange(pollEvent, time.milliseconds(), false);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.appEventTimeAvg).metricValue(), (double) randomSleepTime * 1000);
        assertEquals(metrics.metric(networkThreadMetricsManager.appEventTimeMax).metricValue(), (double) randomSleepTime * 1000);

        // Record application event queue change
        networkThreadMetricsManager.recordApplicationEventQueueChange(shareFetchEvent, time.milliseconds(), true);
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepTime) + shortTimeToSleep);

        // Record application event queue change
        networkThreadMetricsManager.recordApplicationEventQueueChange(shareFetchEvent, time.milliseconds(), false);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.appEventTimeAvg).metricValue(), (double) randomSleepTime * 1000 + shortTimeToSleep / 2d);
        assertEquals(metrics.metric(networkThreadMetricsManager.appEventTimeMax).metricValue(), (double) randomSleepTime * 1000 + shortTimeToSleep);
    }

    @Test
    public void testUnsentRequestMetrics() {
        NetworkClientDelegate.UnsentRequest request1 = new NetworkClientDelegate.UnsentRequest(mock(FetchRequest.Builder.class), null);
        NetworkClientDelegate.UnsentRequest request2 = new NetworkClientDelegate.UnsentRequest(mock(FetchRequest.Builder.class), null);

        // Assert the existence of metrics
        assertNotNull(metrics.metric(networkThreadMetricsManager.unsentRequestsQueueSize));
        assertNotNull(metrics.metric(networkThreadMetricsManager.unsentRequestTimeAvg));
        assertNotNull(metrics.metric(networkThreadMetricsManager.unsentRequestTimeMax));

        // Record adding an unsent request
        long now = time.milliseconds();
        networkThreadMetricsManager.recordUnsentRequestsQueueChange(request1, now, true);
        networkThreadMetricsManager.recordUnsentRequestQueueSize(1);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestsQueueSize).metricValue(), 1d);
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestTimeAvg).metricValue(), NaN);
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestTimeMax).metricValue(), NaN);

        // Randomly sleep 1-10 seconds
        Random rand = new Random();
        int randomSleepTime1 = rand.nextInt(10) + 1;
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepTime1));

        // Record sending an unsent request
        networkThreadMetricsManager.recordUnsentRequestsQueueChange(request1, time.milliseconds(), false);
        networkThreadMetricsManager.recordUnsentRequestQueueSize(0);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestsQueueSize).metricValue(), 0d);
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestTimeAvg).metricValue(), (double) randomSleepTime1 * 1000);
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestTimeMax).metricValue(), (double) randomSleepTime1 * 1000);

        // Record adding an unsent request
        now = time.milliseconds();
        networkThreadMetricsManager.recordUnsentRequestsQueueChange(request2, now, true);
        networkThreadMetricsManager.recordUnsentRequestQueueSize(1);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestsQueueSize).metricValue(), 1d);

        // Randomly sleep 1-10 seconds
        int randomSleepTime2 = rand.nextInt(10) + 1;
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepTime2));

        // Record sending unsent requests
        now = time.milliseconds();
        networkThreadMetricsManager.recordUnsentRequestsQueueChange(request2, now, false);
        networkThreadMetricsManager.recordUnsentRequestQueueSize(0);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestsQueueSize).metricValue(), 0d);
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestTimeAvg).metricValue(), (double) (randomSleepTime1 + randomSleepTime2) * 1000 / 2);
        assertEquals(metrics.metric(networkThreadMetricsManager.unsentRequestTimeMax).metricValue(), (double) (Math.max(randomSleepTime1, randomSleepTime2)) * 1000);
    }
}
