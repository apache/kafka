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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsManagerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMetricsManagerTest.class);

    private MockTime time;
    private ClientMetricsReceiverPlugin clientMetricsReceiverPlugin;
    private ClientMetricsManager clientMetricsManager;

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        clientMetricsReceiverPlugin = new ClientMetricsReceiverPlugin();
        clientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time);
    }

    @Test
    public void testUpdateSubscription() {
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
    public void testGetTelemetry() throws UnknownHostException {
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
    public void testGetTelemetrySameClientImmediateRetryFail() throws UnknownHostException {
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
    }

    @Test
    public void testGetTelemetrySameClientImmediateRetryAfterPushFail() throws UnknownHostException {
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

        ClientMetricsManager newClientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time);

        PushTelemetryRequest pushRequest = new Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(response.data().clientInstanceId())
                .setSubscriptionId(response.data().subscriptionId())
                .setCompressionType(CompressionType.NONE.id)
                .setMetrics("test-data".getBytes(StandardCharsets.UTF_8)), true).build();

        PushTelemetryResponse pushResponse = newClientMetricsManager.processPushTelemetryRequest(
            pushRequest, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, pushResponse.error());

        request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId), true).build();

        response = newClientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, response.error());
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
    public void testGetTelemetryConcurrentRequestNewClientInstance() throws InterruptedException {
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
    }

    @Test
    public void testGetTelemetryConcurrentRequestAfterSubscriptionUpdate()
        throws InterruptedException, UnknownHostException {
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
    }

    @Test
    public void testPushTelemetry() throws UnknownHostException {
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
                .setMetrics("test-data".getBytes(StandardCharsets.UTF_8)), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryOnNewServer() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        // Create new client metrics manager which simulates a new server as it will not have any
        // client instance information but request should succeed as subscription id should match
        // the one with new client instance.

        ClientMetricsManager newClientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId()), true).build();

        PushTelemetryResponse response = newClientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());
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
                .setMetrics("test-data".getBytes(StandardCharsets.UTF_8)), true).build();

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
    public void testPushTelemetryThrottleError() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId()), true).build();

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
    }

    @Test
    public void testPushTelemetryTerminatingFlag() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId()), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());

        // Push telemetry with terminating flag set to true.
        request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setTerminating(true), true).build();

        response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.NONE, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        assertTrue(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
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
    public void testPushTelemetrySubscriptionIdInvalid() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData(), true).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, ClientMetricsTestUtils.requestContext());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(1234), true).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        assertEquals(Errors.UNKNOWN_SUBSCRIPTION_ID, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.UNKNOWN_SUBSCRIPTION_ID, instance.lastKnownError());
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
    public void testPushTelemetryNullMetricsData() throws UnknownHostException {
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
    }

    @Test
    public void testPushTelemetryMetricsTooLarge() throws UnknownHostException {
        clientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 1, time);

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
                .setMetrics(metrics), true).build();

        // Set the max bytes 1 to force the error.
        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, ClientMetricsTestUtils.requestContext());

        // Should not report any error though no metrics will be exported.
        assertEquals(Errors.TELEMETRY_TOO_LARGE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.TELEMETRY_TOO_LARGE, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryConcurrentRequestNewClientInstance() throws UnknownHostException, InterruptedException {
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
                .setMetrics("test-data".getBytes(StandardCharsets.UTF_8)), true).build();

        CountDownLatch lock = new CountDownLatch(2);
        List<PushTelemetryResponse> responses = Collections.synchronizedList(new ArrayList<>());

        ClientMetricsManager newClientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, 100, time);

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
                // As subscription is updates hence 1 request shall fail with unknown subscription id.
                assertEquals(Errors.NONE, response.error());
            }
        }
        // 1 request should fail with throttling error.
        assertEquals(1, throttlingErrorCount);
    }

    @Test
    public void testPushTelemetryConcurrentRequestAfterSubscriptionUpdate() throws UnknownHostException, InterruptedException {
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
                .setMetrics("test-data".getBytes(StandardCharsets.UTF_8)), true).build();

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
    }
}
