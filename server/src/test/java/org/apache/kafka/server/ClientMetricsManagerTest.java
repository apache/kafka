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
package org.apache.kafka.server;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest.Builder;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;
import org.apache.kafka.server.metrics.ClientMetricsInstance;
import org.apache.kafka.server.metrics.ClientMetricsReceiverPlugin;
import org.apache.kafka.server.metrics.ClientMetricsTestUtils;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsManagerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMetricsManagerTest.class);

    private MockTime time;
    private Metrics kafkaMetrics;
    private ClientMetricsReceiverPlugin clientMetricsReceiverPlugin;
    private ClientMetricsManager clientMetricsManager;

    @AfterAll
    public static void ensureNoThreadLeak() throws InterruptedException {
        TestUtils.waitForCondition(
                () -> Thread.getAllStackTraces().keySet().stream()
                    .map(Thread::getName)
                    .noneMatch(t -> t.contains(ClientMetricsManager.CLIENT_METRICS_REAPER_THREAD_NAME) || t.contains(SystemTimer.SYSTEM_TIMER_THREAD_PREFIX)),
                "Thread leak detected"
        );

    }

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        kafkaMetrics = new Metrics();
        clientMetricsReceiverPlugin = new ClientMetricsReceiverPlugin();
        clientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time, 100, kafkaMetrics);
    }

    @AfterEach
    public void tearDown() throws Exception {
        clientMetricsManager.close();
        kafkaMetrics.close();
    }

    @Test
    public void testUpdateSubscription() throws Exception {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());

        assertEquals(0, clientMetricsManager.subscriptionUpdateVersion());
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());

        assertEquals(1, clientMetricsManager.subscriptions().size());
        assertNotNull(clientMetricsManager.subscriptionInfo("sub-1"));

        ClientMetricsManager.SubscriptionInfo subscriptionInfo = clientMetricsManager.subscriptionInfo("sub-1");
        Set<String> metrics = subscriptionInfo.metrics();

        // Validate metrics.
        assertEquals(ClientMetricsTestUtils.DEFAULT_METRICS.split(",").length, metrics.size());
        Arrays.stream(ClientMetricsTestUtils.DEFAULT_METRICS.split(",")).forEach(metric ->
            assertTrue(metrics.contains(metric)));
        // Validate push interval.
        assertEquals(ClientMetricsTestUtils.defaultProperties().getProperty(ClientMetricsConfigs.PUSH_INTERVAL_MS),
            String.valueOf(subscriptionInfo.intervalMs()));

        // Validate match patterns.
        assertEquals(ClientMetricsTestUtils.DEFAULT_CLIENT_MATCH_PATTERNS.size(),
            subscriptionInfo.matchPattern().size());
        ClientMetricsTestUtils.DEFAULT_CLIENT_MATCH_PATTERNS.forEach(pattern -> {
            String[] split = pattern.split("=");
            assertTrue(subscriptionInfo.matchPattern().containsKey(split[0]));
            assertEquals(split[1], subscriptionInfo.matchPattern().get(split[0]).pattern());
        });
        assertEquals(1, clientMetricsManager.subscriptionUpdateVersion());
        // Validate metrics should have instance count metric, 2 unknown subscription count metrics
        // and kafka metrics count registered i.e. 4 metrics.
        assertEquals(4, kafkaMetrics.metrics().size());
        // Metrics should not have any instance while updating the subscriptions.
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
    }

    @Test
    public void testUpdateSubscriptionWithEmptyProperties() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());
        assertEquals(0, clientMetricsManager.subscriptionUpdateVersion());
        clientMetricsManager.updateSubscription("sub-1", new Properties());
        // No subscription should be added as the properties are empty.
        assertEquals(0, clientMetricsManager.subscriptions().size());
        assertEquals(0, clientMetricsManager.subscriptionUpdateVersion());
    }

    @Test
    public void testUpdateSubscriptionWithNullProperties() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());
        assertEquals(0, clientMetricsManager.subscriptionUpdateVersion());
        // Properties shouldn't be passed as null.
        assertThrows(NullPointerException.class, () ->
            clientMetricsManager.updateSubscription("sub-1", null));
        assertEquals(0, clientMetricsManager.subscriptions().size());
        assertEquals(0, clientMetricsManager.subscriptionUpdateVersion());
    }

    @Test
    public void testUpdateSubscriptionWithInvalidMetricsProperties() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());

        Properties properties = new Properties();
        properties.put("random", "random");
        assertThrows(InvalidRequestException.class, () -> clientMetricsManager.updateSubscription("sub-1", properties));
    }

    @Test
    public void testUpdateSubscriptionWithPropertiesDeletion() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());
        assertEquals(0, clientMetricsManager.subscriptionUpdateVersion());

        Properties properties = new Properties();
        properties.put("interval.ms", "100");
        clientMetricsManager.updateSubscription("sub-1", properties);
        assertEquals(1, clientMetricsManager.subscriptions().size());
        assertNotNull(clientMetricsManager.subscriptionInfo("sub-1"));
        assertEquals(1, clientMetricsManager.subscriptionUpdateVersion());

        clientMetricsManager.updateSubscription("sub-1", new Properties());
        // Subscription should be removed as all properties are removed.
        assertEquals(0, clientMetricsManager.subscriptions().size());
        assertEquals(2, clientMetricsManager.subscriptionUpdateVersion());
    }

    @Test
    public void testGetTelemetry() throws Exception {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertNotNull(response.data().clientInstanceId());
        assertTrue(response.data().subscriptionId() != 0);

        assertEquals(ClientMetricsTestUtils.DEFAULT_METRICS.split(",").length, response.data().requestedMetrics().size());
        Arrays.stream(ClientMetricsTestUtils.DEFAULT_METRICS.split(",")).forEach(metric ->
            assertTrue(response.data().requestedMetrics().contains(metric)));

        assertEquals(4, response.data().acceptedCompressionTypes().size());
        // validate compression types order.
        assertEquals(CompressionType.ZSTD.id, response.data().acceptedCompressionTypes().get(0));
        assertEquals(CompressionType.LZ4.id, response.data().acceptedCompressionTypes().get(1));
        assertEquals(CompressionType.GZIP.id, response.data().acceptedCompressionTypes().get(2));
        assertEquals(CompressionType.SNAPPY.id, response.data().acceptedCompressionTypes().get(3));
        assertEquals(ClientMetricsTestUtils.DEFAULT_PUSH_INTERVAL_MS, response.data().pushIntervalMs());
        assertTrue(response.data().deltaTemporality());
        assertEquals(100, response.data().telemetryMaxBytes());
        assertEquals(Errors.NONE, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(response.data().clientInstanceId());
        assertNotNull(instance);
        assertEquals(Errors.NONE, instance.lastKnownError());
        // Validate metrics should have instance count metric, kafka metrics count and 4 sensors with 10 metrics
        // registered i.e. 12 metrics.
        assertEquals(12, kafkaMetrics.metrics().size());
        // Metrics should only have one instance.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-rate").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-rate").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-rate").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
    }

    @Test
    public void testGetTelemetryWithoutSubscription() throws UnknownHostException {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertNotNull(response.data().clientInstanceId());
        assertTrue(response.data().subscriptionId() != 0);
        assertTrue(response.data().requestedMetrics().isEmpty());
        assertEquals(4, response.data().acceptedCompressionTypes().size());
        assertEquals(ClientMetricsConfigs.DEFAULT_INTERVAL_MS, response.data().pushIntervalMs());
        assertTrue(response.data().deltaTemporality());
        assertEquals(100, response.data().telemetryMaxBytes());
        assertEquals(Errors.NONE, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(response.data().clientInstanceId());
        assertNotNull(instance);
        assertEquals(Errors.NONE, instance.lastKnownError());
    }

    @Test
    public void testGetTelemetryAfterPushIntervalTime() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertNotNull(response.data().clientInstanceId());
        assertEquals(Errors.NONE, response.error());

        time.sleep(ClientMetricsConfigs.DEFAULT_INTERVAL_MS);

        request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(response.data().clientInstanceId()), true).build();

        response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());
        assertNotNull(response.data().clientInstanceId());
        assertEquals(Errors.NONE, response.error());
    }

    @Test
    public void testGetTelemetryAllMetricSubscribedSubscription() throws UnknownHostException {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        Properties properties = new Properties();
        properties.put("metrics", ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS_CONFIG);
        clientMetricsManager.updateSubscription("sub-2", properties);

        assertEquals(2, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertNotNull(response.data().clientInstanceId());
        assertTrue(response.data().subscriptionId() != 0);

        assertEquals(1, response.data().requestedMetrics().size());
        assertTrue(response.data().requestedMetrics().contains(ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS_CONFIG));

        assertEquals(4, response.data().acceptedCompressionTypes().size());
        assertEquals(ClientMetricsTestUtils.DEFAULT_PUSH_INTERVAL_MS, response.data().pushIntervalMs());
        assertTrue(response.data().deltaTemporality());
        assertEquals(100, response.data().telemetryMaxBytes());
        assertEquals(Errors.NONE, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(response.data().clientInstanceId());
        assertNotNull(instance);
        assertEquals(Errors.NONE, instance.lastKnownError());
    }

    @Test
    public void testGetTelemetrySameClientImmediateRetryFail() throws Exception {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        Uuid clientInstanceId = response.data().clientInstanceId();
        assertNotNull(clientInstanceId);
        assertEquals(Errors.NONE, response.error());

        request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId), true).build();
        response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, response.error());
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
        // Should register 1 throttle metric.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue() > 0);
    }

    @Test
    public void testGetTelemetrySameClientImmediateRetryAfterPushFail() throws Exception {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        Uuid clientInstanceId = response.data().clientInstanceId();
        assertNotNull(clientInstanceId);
        assertEquals(Errors.NONE, response.error());

        // Create new client metrics manager which simulates a new server as it will not have any
        // last request information but request should succeed as subscription id should match
        // the one with new client instance.
        try (
                Metrics kafkaMetrics = new Metrics();
                ClientMetricsManager newClientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time, kafkaMetrics)
        ) {

            PushTelemetryRequest pushRequest = new Builder(
                    new PushTelemetryRequestData()
                            .setClientInstanceId(response.data().clientInstanceId())
                            .setSubscriptionId(response.data().subscriptionId())
                            .setCompressionType(CompressionType.NONE.id)
                            .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

            PushTelemetryResponse pushResponse = newClientMetricsManager.processPushTelemetryRequest(
                    pushRequest, ClientMetricsTestUtils.requestContext());

            assertEquals(Errors.NONE, pushResponse.error());

            request = new GetTelemetrySubscriptionsRequest.Builder(
                    new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId), true).build();

            response = newClientMetricsManager.processGetTelemetrySubscriptionRequest(
                    request, ClientMetricsTestUtils.requestContext());

            assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, response.error());
            // Should register 1 throttle metric.
            assertEquals((double) 1, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
            assertTrue((double) getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue() > 0);
        }
    }

    @Test
    public void testGetTelemetryUpdateSubscription() throws UnknownHostException {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        Uuid clientInstanceId = response.data().clientInstanceId();
        int subscriptionId = response.data().subscriptionId();
        assertNotNull(clientInstanceId);
        assertTrue(subscriptionId != 0);
        assertEquals(Errors.NONE, response.error());

        // Update subscription
        Properties properties = new Properties();
        properties.put("metrics", ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS_CONFIG);
        clientMetricsManager.updateSubscription("sub-2", properties);
        assertEquals(2, clientMetricsManager.subscriptions().size());

        request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId), true).build();

        response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        // No throttle error as the subscription has changed.
        assertEquals(Errors.NONE, response.error());
        // Subscription id updated in next request
        assertTrue(subscriptionId != response.data().subscriptionId());
    }

    @Test
    public void testGetTelemetryConcurrentRequestNewClientInstance() throws Exception {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(Uuid.randomUuid()), true).build();

        CountDownLatch lock = new CountDownLatch(2);
        List<GetTelemetrySubscriptionsResponse> responses = Collections.synchronizedList(new ArrayList<>());

        Thread thread = new Thread(() -> {
            try {
                GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
                    request, ClientMetricsTestUtils.requestContext());

                responses.add(response);
            } catch (UnknownHostException e) {
                LOG.error("Error processing request", e);
            } finally {
                lock.countDown();
            }
        });

        Thread thread1 = new Thread(() -> {
            try {
                GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
                    request, ClientMetricsTestUtils.requestContext());

                responses.add(response);
            } catch (UnknownHostException e) {
                LOG.error("Error processing request", e);
            } finally {
                lock.countDown();
            }
        });

        thread.start();
        thread1.start();

        assertTrue(lock.await(2000, TimeUnit.MILLISECONDS));
        assertEquals(2, responses.size());

        int throttlingErrorCount = 0;
        for (GetTelemetrySubscriptionsResponse response : responses) {
            if (response.error() == Errors.THROTTLING_QUOTA_EXCEEDED) {
                throttlingErrorCount++;
            } else {
                // As subscription is updated hence 1 request shall fail with unknown subscription id.
                assertEquals(Errors.NONE, response.error());
            }
        }
        // 1 request should fail with throttling error.
        assertEquals(1, throttlingErrorCount);
        // Should register 1 throttle metric.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue() > 0);
    }

    @Test
    public void testGetTelemetryConcurrentRequestAfterSubscriptionUpdate() throws Exception {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(Uuid.randomUuid()), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        CountDownLatch lock = new CountDownLatch(2);
        List<GetTelemetrySubscriptionsResponse> responses = Collections.synchronizedList(new ArrayList<>());

        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        Thread thread = new Thread(() -> {
            try {
                GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
                    request, ClientMetricsTestUtils.requestContext());

                responses.add(response);
            } catch (UnknownHostException e) {
                LOG.error("Error processing request", e);
            } finally {
                lock.countDown();
            }
        });

        Thread thread1 = new Thread(() -> {
            try {
                GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
                    request, ClientMetricsTestUtils.requestContext());

                responses.add(response);
            } catch (UnknownHostException e) {
                LOG.error("Error processing request", e);
            } finally {
                lock.countDown();
            }
        });

        thread.start();
        thread1.start();

        assertTrue(lock.await(2000, TimeUnit.MILLISECONDS));
        assertEquals(2, responses.size());

        int throttlingErrorCount = 0;
        for (GetTelemetrySubscriptionsResponse response : responses) {
            if (response.error() == Errors.THROTTLING_QUOTA_EXCEEDED) {
                throttlingErrorCount++;
            } else {
                // As subscription is updated hence 1 request shall fail with unknown subscription id.
                assertEquals(Errors.NONE, response.error());
            }
        }
        // 1 request should fail with throttling error.
        assertEquals(1, throttlingErrorCount);
        // Should register 1 throttle metric.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue() > 0);
    }

    @Test
    public void testPushTelemetry() throws Exception {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setCompressionType(CompressionType.NONE.id)
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
        // Validate metrics should have instance count metric, kafka metrics count and 4 sensors with 10 metrics
        // registered i.e. 12 metrics.
        assertEquals(12, kafkaMetrics.metrics().size());
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
        // Should have 1 successful export and 0 error metrics.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-rate").metricValue() > 0);
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-rate").metricValue());
        // Should not have default NaN value, must register actual export time.
        assertNotEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
        assertNotEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
    }

    @Test
    public void testPushTelemetryOnNewServer() throws Exception {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        // Create new client metrics manager which simulates a new server as it will not have any
        // client instance information but request should succeed as subscription id should match
        // the one with new client instance.
        try (
                Metrics kafkaMetrics = new Metrics();
                ClientMetricsManager newClientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time, kafkaMetrics)
        ) {

            PushTelemetryRequest request = new PushTelemetryRequest.Builder(
                    new PushTelemetryRequestData()
                            .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                            .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                            .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

            PushTelemetryResponse response = newClientMetricsManager.processPushTelemetryRequest(
                    request, ClientMetricsTestUtils.requestContext());

            assertEquals(Errors.NONE, response.error());
            // Validate metrics should have instance count metric, kafka metrics count and 4 sensors with 10 metrics
            // registered i.e. 12 metrics.
            assertEquals(12, kafkaMetrics.metrics().size());
            assertEquals((double) 1, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
            assertEquals((double) 0, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-count").metricValue());
            assertEquals((double) 0, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
            // Should have 1 successful export and 0 error metrics.
            assertEquals((double) 1, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
            assertEquals((double) 0, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
            // Should not have default NaN value, must register actual export time metrics.
            assertNotEquals(Double.NaN, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
            assertNotEquals(Double.NaN, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
        }
    }

    @Test
    public void testPushTelemetryAfterPushIntervalTime() throws UnknownHostException {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        PushTelemetryRequest request = new Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setCompressionType(CompressionType.NONE.id)
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());

        time.sleep(ClientMetricsTestUtils.DEFAULT_PUSH_INTERVAL_MS);

        response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());
    }

    @Test
    public void testPushTelemetryClientInstanceIdInvalid() throws UnknownHostException {
        // Null client instance id
        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData().setClientInstanceId(null), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.INVALID_REQUEST, response.error());

        // Zero client instance id
        request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData().setClientInstanceId(Uuid.ZERO_UUID), true).build();

        response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.INVALID_REQUEST, response.error());
    }

    @Test
    public void testPushTelemetryThrottleError() throws Exception {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());
        // Immediate push request should succeed.
        assertEquals(Errors.NONE, response.error());

        response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());
        // Second push request should fail with throttle error.
        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        assertFalse(instance.terminating());
        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, instance.lastKnownError());
        // Should have 1 throttle error metrics.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue() > 0);
        // Should have 1 successful export metrics as well.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
        assertNotEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
        assertNotEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
    }

    @Test
    public void testPushTelemetryTerminatingFlag() throws Exception {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());

        // Push telemetry with terminating flag set to true.
        request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8)))
                .setTerminating(true), true).build();

        response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        assertTrue(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
        // Should have 2 successful export metrics.
        assertEquals((double) 2, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-rate").metricValue() > 0);
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
        assertNotEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
        assertNotEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
    }

    @Test
    public void testPushTelemetryNextRequestPostTerminatingFlag() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setTerminating(true), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());
        assertTrue(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());

        request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setTerminating(true), true).build();

        response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.INVALID_REQUEST, response.error());
        assertTrue(instance.terminating());
        assertEquals(Errors.INVALID_REQUEST, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetrySubscriptionIdInvalid() throws Exception {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8)))
                .setSubscriptionId(1234), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.UNKNOWN_SUBSCRIPTION_ID, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.UNKNOWN_SUBSCRIPTION_ID, instance.lastKnownError());
        // Should have 1 unknown subscription request metric and no successful export.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());

    }

    @Test
    public void testPushTelemetryCompressionTypeInvalid() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setCompressionType((byte) 100), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryNullMetricsData() throws Exception {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setMetrics(null), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        // Should not report any error though no metrics will be exported.
        assertEquals(Errors.NONE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
        // Metrics should not report any export.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
    }

    @Test
    public void testPushTelemetryMetricsTooLarge() throws Exception {
        try (
                Metrics kafkaMetrics = new Metrics();
                ClientMetricsManager clientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 1, time, kafkaMetrics)
        ) {

            GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
                    new GetTelemetrySubscriptionsRequestData(), true).build();

            GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
                    subscriptionsRequest, ClientMetricsTestUtils.requestContext());

            ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
            assertNotNull(instance);

            byte[] metrics = "ab".getBytes(StandardCharsets.UTF_8);
            assertEquals(2, metrics.length);

            PushTelemetryRequest request = new PushTelemetryRequest.Builder(
                    new PushTelemetryRequestData()
                            .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                            .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                            .setMetrics(ByteBuffer.wrap(metrics)), true).build();

            // Set the max bytes 1 to force the error.
            PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
                    request, ClientMetricsTestUtils.requestContext());

            // Should not report any error though no metrics will be exported.
            assertEquals(Errors.TELEMETRY_TOO_LARGE, response.error());
            assertFalse(instance.terminating());
            assertEquals(Errors.TELEMETRY_TOO_LARGE, instance.lastKnownError());
        }
    }

    @Test
    public void testPushTelemetryConcurrentRequestNewClientInstance() throws Exception {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setCompressionType(CompressionType.NONE.id)
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

        CountDownLatch lock = new CountDownLatch(2);
        List<PushTelemetryResponse> responses = Collections.synchronizedList(new ArrayList<>());

        try (
                Metrics kafkaMetrics = new Metrics();
                ClientMetricsManager newClientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time, kafkaMetrics);
        ) {

            Thread thread = new Thread(() -> {
                try {
                    PushTelemetryResponse response = newClientMetricsManager.processPushTelemetryRequest(
                            request, ClientMetricsTestUtils.requestContext());
                    responses.add(response);
                } catch (UnknownHostException e) {
                    LOG.error("Error processing request", e);
                } finally {
                    lock.countDown();
                }
            });

            Thread thread1 = new Thread(() -> {
                try {
                    PushTelemetryResponse response = newClientMetricsManager.processPushTelemetryRequest(
                            request, ClientMetricsTestUtils.requestContext());
                    responses.add(response);
                } catch (UnknownHostException e) {
                    LOG.error("Error processing request", e);
                } finally {
                    lock.countDown();
                }
            });

            thread.start();
            thread1.start();

            assertTrue(lock.await(2000, TimeUnit.MILLISECONDS));
            assertEquals(2, responses.size());

            int throttlingErrorCount = 0;
            for (PushTelemetryResponse response : responses) {
                if (response.error() == Errors.THROTTLING_QUOTA_EXCEEDED) {
                    throttlingErrorCount++;
                } else {
                    assertEquals(Errors.NONE, response.error());
                }
            }
            // 1 request should fail with throttling error.
            assertEquals(1, throttlingErrorCount);
            // Metrics should report 1 export and 1 throttle error.
            assertEquals((double) 1, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
            assertTrue((double) getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue() > 0);
            assertEquals((double) 1, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
            assertEquals((double) 0, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
            assertNotEquals(Double.NaN, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
            assertNotEquals(Double.NaN, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
        }
    }

    @Test
    public void testPushTelemetryConcurrentRequestAfterSubscriptionUpdate() throws Exception {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setCompressionType(CompressionType.NONE.id)
                .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        CountDownLatch lock = new CountDownLatch(2);
        List<PushTelemetryResponse> responses = Collections.synchronizedList(new ArrayList<>());

        Thread thread = new Thread(() -> {
            try {
                PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
                    request, ClientMetricsTestUtils.requestContext());
                responses.add(response);
            } catch (UnknownHostException e) {
                LOG.error("Error processing request", e);
            } finally {
                lock.countDown();
            }
        });

        Thread thread1 = new Thread(() -> {
            try {
                PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
                    request, ClientMetricsTestUtils.requestContext());
                responses.add(response);
            } catch (UnknownHostException e) {
                LOG.error("Error processing request", e);
            } finally {
                lock.countDown();
            }
        });

        thread.start();
        thread1.start();

        assertTrue(lock.await(2000, TimeUnit.MILLISECONDS));
        assertEquals(2, responses.size());

        int throttlingErrorCount = 0;
        for (PushTelemetryResponse response : responses) {
            if (response.error() == Errors.THROTTLING_QUOTA_EXCEEDED) {
                throttlingErrorCount++;
            } else {
                // As subscription is updated hence 1 request shall fail with unknown subscription id.
                assertEquals(Errors.UNKNOWN_SUBSCRIPTION_ID, response.error());
            }
        }
        // 1 request should fail with throttling error.
        assertEquals(1, throttlingErrorCount);
        // Metrics should report 1 subscription, 1 throttle error and no successful export.
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-rate").metricValue() > 0);
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
        assertTrue((double) getMetric(ClientMetricsManager.ClientMetricsStats.THROTTLE + "-rate").metricValue() > 0);
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
        assertEquals(Double.NaN, getMetric(ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
        // Validate client connection id map should contain 2 entries. 1 for GET request and 1 for PUSH request as we did
        // generate random connection id. The throttled request should not be added to the map.
        assertEquals(2, clientMetricsManager.clientConnectionIdMap().size());
    }

    @Test
    public void testPushTelemetryPluginException() throws Exception {
        ClientMetricsReceiverPlugin receiverPlugin = Mockito.mock(ClientMetricsReceiverPlugin.class);
        Mockito.doThrow(new RuntimeException("test exception")).when(receiverPlugin).exportMetrics(Mockito.any(), Mockito.any());

        try (
                Metrics kafkaMetrics = new Metrics();
                ClientMetricsManager clientMetricsManager = new ClientMetricsManager(receiverPlugin, 100, time, 100, kafkaMetrics)
        ) {

            clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
            assertEquals(1, clientMetricsManager.subscriptions().size());

            GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
                    new GetTelemetrySubscriptionsRequestData(), true).build();

            GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
                    subscriptionsRequest, ClientMetricsTestUtils.requestContext());

            ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
            assertNotNull(instance);

            PushTelemetryRequest request = new Builder(
                    new PushTelemetryRequestData()
                            .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                            .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                            .setCompressionType(CompressionType.NONE.id)
                            .setMetrics(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8))), true).build();

            PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
                    request, ClientMetricsTestUtils.requestContext());

            assertEquals(Errors.INVALID_RECORD, response.error());
            assertFalse(instance.terminating());
            assertEquals(Errors.INVALID_RECORD, instance.lastKnownError());
            // Metrics should report 1 plugin export error and 0 successful export.
            assertEquals((double) 0, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.UNKNOWN_SUBSCRIPTION_REQUEST + "-count").metricValue());
            assertEquals((double) 0, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.THROTTLE + "-count").metricValue());
            assertEquals((double) 0, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT + "-count").metricValue());
            assertEquals((double) 1, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-count").metricValue());
            assertTrue((double) getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_ERROR + "-rate").metricValue() > 0);
            // Should have default NaN value, must not register export time.
            assertEquals(Double.NaN, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-avg").metricValue());
            assertEquals(Double.NaN, getMetric(kafkaMetrics, ClientMetricsManager.ClientMetricsStats.PLUGIN_EXPORT_TIME + "-max").metricValue());
        }
    }

    @Test
    public void testCacheEviction() throws Exception {
        Properties properties = new Properties();
        properties.put("metrics", ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS_CONFIG);
        properties.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "100");
        clientMetricsManager.updateSubscription("sub-1", properties);

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());
        assertEquals(Errors.NONE, response.error());

        // Metrics for instance should exist.
        assertEquals(12, kafkaMetrics.metrics().size());
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());

        assertNotNull(clientMetricsManager.clientInstance(response.data().clientInstanceId()));
        assertEquals(1, clientMetricsManager.clientConnectionIdMap().size());
        assertEquals(1, clientMetricsManager.expirationTimer().size());
        // Cache expiry should occur after 100 * 3 = 300 ms, wait for the eviction to happen.
        // Force clocks to advance by 300 ms.
        clientMetricsManager.expirationTimer().advanceClock(300);
        assertTimeoutPreemptively(Duration.ofMillis(300), () -> {
            // Validate that cache eviction happens and client instance is removed from cache.
            while (clientMetricsManager.expirationTimer().size() != 0 ||
                clientMetricsManager.clientInstance(response.data().clientInstanceId()) != null) {
                // Wait for cache eviction to happen.
                Thread.sleep(50);
            }
        });
        // Metrics for instance should be removed. Should have instance count metric, 2 unknown
        // subscription count metrics and kafka metrics count registered i.e. 4 metrics.
        assertEquals(4, kafkaMetrics.metrics().size());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
        // Validate client connection id map should be empty.
        assertTrue(clientMetricsManager.clientConnectionIdMap().isEmpty());
    }

    @Test
    public void testCacheEvictionWithMultipleClients() throws Exception {
        Properties properties = new Properties();
        properties.put("metrics", ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS_CONFIG);
        properties.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "100");
        clientMetricsManager.updateSubscription("sub-1", properties);

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response1 = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());
        assertEquals(Errors.NONE, response1.error());

        GetTelemetrySubscriptionsResponse response2 = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());
        assertEquals(Errors.NONE, response2.error());

        // Metrics for both instances should exist.
        assertEquals(20, kafkaMetrics.metrics().size());
        assertEquals((double) 2, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());

        assertNotNull(clientMetricsManager.clientInstance(response1.data().clientInstanceId()));
        assertNotNull(clientMetricsManager.clientInstance(response2.data().clientInstanceId()));
        assertEquals(2, clientMetricsManager.clientConnectionIdMap().size());
        assertEquals(2, clientMetricsManager.expirationTimer().size());
        // Cache expiry should occur after 100 * 3 = 300 ms, wait for the eviction to happen.
        // Force clocks to advance by 300 ms.
        clientMetricsManager.expirationTimer().advanceClock(300);
        assertTimeoutPreemptively(Duration.ofMillis(300), () -> {
            // Validate that cache eviction happens and client instance is removed from cache.
            while (clientMetricsManager.expirationTimer().size() != 0 ||
                clientMetricsManager.clientInstance(response1.data().clientInstanceId()) != null ||
                clientMetricsManager.clientInstance(response2.data().clientInstanceId()) != null) {
                // Wait for cache eviction to happen.
                Thread.sleep(50);
            }
        });
        // Metrics for both instances should be removed. Should have instance count metric, 2 unknown
        // subscription count metrics and kafka metrics count registered i.e. 4 metrics.
        assertEquals(4, kafkaMetrics.metrics().size());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
        // Validate client connection id map should be empty.
        assertTrue(clientMetricsManager.clientConnectionIdMap().isEmpty());
    }

    @Test
    public void testCacheExpirationTaskCancelledOnInstanceUpdate() throws Exception {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContextWithConnectionId("conn-1"));
        assertEquals(Errors.NONE, response.error());
        Uuid clientInstanceId = response.data().clientInstanceId();
        int subscriptionId = response.data().subscriptionId();

        // Validate timer task in instance should not be null.
        ClientMetricsInstance instance = clientMetricsManager.clientInstance(response.data().clientInstanceId());
        assertNotNull(instance);
        assertNotNull(instance.expirationTimerTask());

        // Update subscription
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId), true).build();

        response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContextWithConnectionId("conn-1"));
        assertEquals(Errors.NONE, response.error());
        assertTrue(subscriptionId != response.data().subscriptionId());

        // Validate timer task in old instance should be null, as new client is created in cache and
        // old client instance is removed from cache and respective timer task is cancelled.
        assertNull(instance.expirationTimerTask());
        // Validate new client instance is created in cache with updated expiration timer task.
        instance = clientMetricsManager.clientInstance(response.data().clientInstanceId());
        assertNotNull(instance);
        assertNotNull(instance.expirationTimerTask());
        assertEquals(1, clientMetricsManager.expirationTimer().size());
        // Metrics size should remain same on instance update.
        assertEquals(12, kafkaMetrics.metrics().size());
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
        // Validate client connection id map contains new connection id.
        assertEquals(1, clientMetricsManager.clientConnectionIdMap().size());
    }

    @Test
    public void testRemoveConnection() throws Exception {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContextWithConnectionId("conn-1"));
        assertEquals(Errors.NONE, response.error());
        Uuid clientInstanceId = response.data().clientInstanceId();

        // Validate instance and metrics exists.
        ClientMetricsInstance instance = clientMetricsManager.clientInstance(clientInstanceId);
        assertNotNull(instance);
        assertEquals(1, clientMetricsManager.clientConnectionIdMap().size());
        assertEquals(clientInstanceId, clientMetricsManager.clientConnectionIdMap().get("conn-1"));
        assertEquals(12, kafkaMetrics.metrics().size());
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());

        clientMetricsManager.connectionDisconnectListener().onDisconnect("conn-1");
        assertNull(clientMetricsManager.clientInstance(clientInstanceId));
        assertTrue(clientMetricsManager.clientConnectionIdMap().isEmpty());
        // Instance metrics should get removed.
        assertEquals(4, kafkaMetrics.metrics().size());
        assertEquals((double) 0, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
    }

    @Test
    public void testRemoveConnectionUnknownConnectionId() throws Exception {
        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContextWithConnectionId("conn-1"));
        assertEquals(Errors.NONE, response.error());
        Uuid clientInstanceId = response.data().clientInstanceId();

        // Validate instance and metrics exists.
        ClientMetricsInstance instance = clientMetricsManager.clientInstance(clientInstanceId);
        assertNotNull(instance);
        assertEquals(1, clientMetricsManager.clientConnectionIdMap().size());
        assertEquals(clientInstanceId, clientMetricsManager.clientConnectionIdMap().get("conn-1"));
        assertEquals(12, kafkaMetrics.metrics().size());
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());

        clientMetricsManager.connectionDisconnectListener().onDisconnect("conn-2");
        assertNotNull(clientMetricsManager.clientInstance(clientInstanceId));
        assertEquals(1, clientMetricsManager.clientConnectionIdMap().size());
        // Metrics size should remain same.
        assertEquals(12, kafkaMetrics.metrics().size());
        assertEquals((double) 1, getMetric(ClientMetricsManager.ClientMetricsStats.INSTANCE_COUNT).metricValue());
    }

    private KafkaMetric getMetric(String name) throws Exception {
        return getMetric(kafkaMetrics, name);
    }

    private KafkaMetric getMetric(Metrics kafkaMetrics, String name) throws Exception {
        Optional<Entry<MetricName, KafkaMetric>> metric = kafkaMetrics.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals(name))
            .findFirst();
        if (!metric.isPresent())
            throw new Exception(String.format("Could not find metric called %s", name));

        return metric.get().getValue();
    }
}
