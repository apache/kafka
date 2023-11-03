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
package kafka.server;

import kafka.metrics.ClientMetricsConfigs;
import kafka.metrics.ClientMetricsInstance;
import kafka.metrics.ClientMetricsTestUtils;
import kafka.server.ClientMetricsManager.SubscriptionInfo;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest.Builder;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsManagerTest {

    private ClientMetricsManager clientMetricsManager;

    @BeforeEach
    public void setUp() {
        clientMetricsManager = new ClientMetricsManager();
    }

    @Test
    public void testUpdateSubscription() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());

        long previousEpoch = clientMetricsManager.lastSubscriptionUpdateEpoch();
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());

        assertEquals(1, clientMetricsManager.subscriptions().size());
        assertNotNull(clientMetricsManager.subscriptionInfo("sub-1"));

        SubscriptionInfo subscriptionInfo = clientMetricsManager.subscriptionInfo("sub-1");
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
        assertTrue(clientMetricsManager.lastSubscriptionUpdateEpoch() > previousEpoch);
    }

    @Test
    public void testUpdateSubscriptionWithEmptyProperties() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());
        long previousEpoch = clientMetricsManager.lastSubscriptionUpdateEpoch();
        clientMetricsManager.updateSubscription("sub-1", new Properties());
        // No subscription should be added as the properties are empty.
        assertEquals(0, clientMetricsManager.subscriptions().size());
        assertEquals(previousEpoch, clientMetricsManager.lastSubscriptionUpdateEpoch());
    }

    @Test
    public void testUpdateSubscriptionWithNullProperties() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());
        long previousEpoch = clientMetricsManager.lastSubscriptionUpdateEpoch();
        // Properties shouldn't be passed as null.
        assertThrows(NullPointerException.class, () ->
            clientMetricsManager.updateSubscription("sub-1", null));
        assertEquals(0, clientMetricsManager.subscriptions().size());
        assertEquals(previousEpoch, clientMetricsManager.lastSubscriptionUpdateEpoch());
    }

    @Test
    public void testUpdateSubscriptionWithInvalidMetricsProperties() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());

        long previousEpoch = clientMetricsManager.lastSubscriptionUpdateEpoch();
        Properties properties = new Properties();
        properties.put("random", "random");
        clientMetricsManager.updateSubscription("sub-1", properties);

        // Undefined keys are ignored.
        assertEquals(1, clientMetricsManager.subscriptions().size());
        assertNotNull(clientMetricsManager.subscriptionInfo("sub-1"));
        // Config def default properties.
        assertTrue(clientMetricsManager.subscriptionInfo("sub-1").metrics().isEmpty());
        assertEquals(ClientMetricsConfigs.DEFAULT_INTERVAL_MS, clientMetricsManager.subscriptionInfo("sub-1").intervalMs());
        assertTrue(clientMetricsManager.subscriptionInfo("sub-1").matchPattern().isEmpty());
        assertTrue(clientMetricsManager.lastSubscriptionUpdateEpoch() > previousEpoch);
    }

    @Test
    public void testUpdateSubscriptionWithPropertiesDeletion() {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());

        long previousEpoch = clientMetricsManager.lastSubscriptionUpdateEpoch();
        Properties properties = new Properties();
        properties.put("interval.ms", "100");
        clientMetricsManager.updateSubscription("sub-1", properties);
        assertEquals(1, clientMetricsManager.subscriptions().size());
        assertNotNull(clientMetricsManager.subscriptionInfo("sub-1"));
        assertTrue(clientMetricsManager.lastSubscriptionUpdateEpoch() > previousEpoch);

        previousEpoch = clientMetricsManager.lastSubscriptionUpdateEpoch();
        clientMetricsManager.updateSubscription("sub-1", new Properties());
        // Subscription should be removed as all properties are removed.
        assertEquals(0, clientMetricsManager.subscriptions().size());
        assertTrue(clientMetricsManager.lastSubscriptionUpdateEpoch() > previousEpoch);
    }

    @Test
    public void testGetTelemetry() throws UnknownHostException {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertNotNull(response.data().clientInstanceId());
        assertTrue(response.data().subscriptionId() != 0);

        assertEquals(ClientMetricsTestUtils.DEFAULT_METRICS.split(",").length, response.data().requestedMetrics().size());
        Arrays.stream(ClientMetricsTestUtils.DEFAULT_METRICS.split(",")).forEach(metric ->
            assertTrue(response.data().requestedMetrics().contains(metric)));

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
    public void testGetTelemetryWithoutSubscription() throws UnknownHostException {
        assertTrue(clientMetricsManager.subscriptions().isEmpty());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, 1024 * 1024, ClientMetricsTestUtils.requestContext(), 0);

        assertNotNull(response.data().clientInstanceId());
        assertTrue(response.data().subscriptionId() != 0);
        assertTrue(response.data().requestedMetrics().isEmpty());
        assertEquals(4, response.data().acceptedCompressionTypes().size());
        assertEquals(ClientMetricsConfigs.DEFAULT_INTERVAL_MS, response.data().pushIntervalMs());
        assertTrue(response.data().deltaTemporality());
        assertEquals(1024 * 1024, response.data().telemetryMaxBytes());
        assertEquals(Errors.NONE, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(response.data().clientInstanceId());
        assertNotNull(instance);
        assertEquals(Errors.NONE, instance.lastKnownError());
    }

    @Test
    public void testGetTelemetryAllMetricSubscribedSubscription() throws UnknownHostException {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        Properties properties = new Properties();
        properties.put("metrics", ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS);
        clientMetricsManager.updateSubscription("sub-2", properties);

        assertEquals(2, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertNotNull(response.data().clientInstanceId());
        assertTrue(response.data().subscriptionId() != 0);

        assertEquals(1, response.data().requestedMetrics().size());
        assertTrue(response.data().requestedMetrics().contains(ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS));

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
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        Uuid clientInstanceId = response.data().clientInstanceId();
        assertNotNull(clientInstanceId);
        assertEquals(Errors.NONE, response.error());

        request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId)).build();
        response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, response.error());
    }

    @Test
    public void testGetTelemetryUpdateSubscription() throws Exception {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        Uuid clientInstanceId = response.data().clientInstanceId();
        int subscriptionId = response.data().subscriptionId();
        assertNotNull(clientInstanceId);
        assertTrue(subscriptionId != 0);
        assertEquals(Errors.NONE, response.error());

        // Wait for subscription time to be greater from last client instance time.
        Thread.sleep(10);

        // Update subscription
        Properties properties = new Properties();
        properties.put("metrics", ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS);
        clientMetricsManager.updateSubscription("sub-2", properties);
        assertEquals(2, clientMetricsManager.subscriptions().size());

        request = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(clientInstanceId)).build();
        response = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        // No throttle error as the subscription has changed.
        assertEquals(Errors.NONE, response.error());
        // Subscription id updated in next request
        assertTrue(subscriptionId != response.data().subscriptionId());
    }

    @Test
    public void testPushTelemetry() throws UnknownHostException {
        clientMetricsManager.updateSubscription("sub-1", ClientMetricsTestUtils.defaultProperties());
        assertEquals(1, clientMetricsManager.subscriptions().size());

        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        // Set last request time to 0 to force a push.
        instance.lastRequestEpoch(0);

        PushTelemetryRequest request = new Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setCompressionType(CompressionType.NONE.id)
                .setMetrics("test-data".getBytes(StandardCharsets.UTF_8))).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.NONE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryOnNewServer() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        // Create new client metrics manager which simulates a new server as it will not have any
        // client instance information but request should succeed as subscription id should match
        // the one with new client instance.

        ClientMetricsManager newClientMetricsManager = new ClientMetricsManager();

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())).build();

        PushTelemetryResponse response = newClientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.NONE, response.error());
    }

    @Test
    public void testPushTelemetryClientInstanceIdInvalid() throws UnknownHostException {
        // Null client instance id
        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(null)).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.INVALID_REQUEST, response.error());

        // Zero client instance id
        request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(Uuid.ZERO_UUID)).build();

        response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.INVALID_REQUEST, response.error());
    }

    @Test
    public void testPushTelemetryThrottleError() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        // Immediately push telemetry with terminating flag set to true.
        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        assertFalse(instance.terminating());
        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryTerminatingFlag() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        // Immediately push telemetry with terminating flag set to true.
        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setTerminating(true)).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.NONE, response.error());

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        assertTrue(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryNextRequestPostTerminatingFlag() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setTerminating(true)).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.NONE, response.error());
        assertTrue(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());

        // Set last request time to 0 to force a push.
        instance.lastRequestEpoch(0);

        request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setTerminating(true)).build();

        response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.INVALID_REQUEST, response.error());
        assertTrue(instance.terminating());
        assertEquals(Errors.INVALID_REQUEST, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetrySubscriptionIdInvalid() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        // Set last request time to 0 to force a push.
        instance.lastRequestEpoch(0);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(1234)).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.UNKNOWN_SUBSCRIPTION_ID, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.UNKNOWN_SUBSCRIPTION_ID, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryCompressionTypeInvalid() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        // Set last request time to 0 to force a push.
        instance.lastRequestEpoch(0);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setCompressionType((byte) 100)).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryNullMetricsData() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 100, ClientMetricsTestUtils.requestContext(), 0);

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        // Set last request time to 0 to force a push.
        instance.lastRequestEpoch(0);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setMetrics(null)).build();

        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 100, ClientMetricsTestUtils.requestContext(), 0);

        // Should not report any error though no metrics will be exported.
        assertEquals(Errors.NONE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.NONE, instance.lastKnownError());
    }

    @Test
    public void testPushTelemetryMetricsTooLarge() throws UnknownHostException {
        GetTelemetrySubscriptionsRequest subscriptionsRequest = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData()).build();

        GetTelemetrySubscriptionsResponse subscriptionsResponse = clientMetricsManager.processGetTelemetrySubscriptionRequest(
            subscriptionsRequest, 1, ClientMetricsTestUtils.requestContext(), 0);

        ClientMetricsInstance instance = clientMetricsManager.clientInstance(subscriptionsResponse.data().clientInstanceId());
        assertNotNull(instance);
        // Set last request time to 0 to force a push.
        instance.lastRequestEpoch(0);

        byte[] metrics = "ab".getBytes(StandardCharsets.UTF_8);
        assertEquals(2, metrics.length);

        PushTelemetryRequest request = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setClientInstanceId(subscriptionsResponse.data().clientInstanceId())
                .setSubscriptionId(subscriptionsResponse.data().subscriptionId())
                .setMetrics(metrics)).build();

        // Set the max bytes 1 to force the error.
        PushTelemetryResponse response = clientMetricsManager.processPushTelemetryRequest(
            request, 1, ClientMetricsTestUtils.requestContext(), 0);

        // Should not report any error though no metrics will be exported.
        assertEquals(Errors.TELEMETRY_TOO_LARGE, response.error());
        assertFalse(instance.terminating());
        assertEquals(Errors.TELEMETRY_TOO_LARGE, instance.lastKnownError());
    }
}
