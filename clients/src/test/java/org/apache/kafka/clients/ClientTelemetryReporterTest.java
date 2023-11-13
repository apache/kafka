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
package org.apache.kafka.clients;

import io.opentelemetry.proto.common.v1.KeyValue;

import org.apache.kafka.clients.ClientTelemetryReporter.ClientTelemetrySubscription;
import org.apache.kafka.clients.ClientTelemetryReporter.DefaultClientTelemetrySender;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.telemetry.ClientTelemetryState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTelemetryReporterTest {

    private ClientTelemetryReporter clientTelemetryReporter;
    private Map<String, Object> configs;
    private MetricsContext metricsContext;
    private ClientTelemetrySubscription subscription;

    @BeforeEach
    public void setUp() {
        clientTelemetryReporter = new ClientTelemetryReporter();
        configs = new HashMap<>();
        metricsContext = new KafkaMetricsContext("test");
        subscription = new ClientTelemetrySubscription(Uuid.randomUuid(), 1234, 20000,
            Collections.emptyList(), true, null);
    }

    @Test
    public void testInitTelemetryReporter() {
        configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, "test-client");
        configs.put(CommonClientConfigs.CLIENT_RACK_CONFIG, "rack");

        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);
        assertEquals(1, clientTelemetryReporter.collectors().size());
        assertNotNull(clientTelemetryReporter.telemetryProvider().resource());
        assertEquals(1, clientTelemetryReporter.telemetryProvider().resource().getAttributesCount());
        assertEquals(ClientTelemetryProvider.CLIENT_RACK, clientTelemetryReporter.telemetryProvider().resource().getAttributes(0).getKey());
        assertEquals("rack", clientTelemetryReporter.telemetryProvider().resource().getAttributes(0).getValue().getStringValue());
    }

    @Test
    public void testInitTelemetryReporterNoCollector() {
        // Skip client id config which skips the collector initialization.
        configs.put(CommonClientConfigs.CLIENT_RACK_CONFIG, "rack");

        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);
        assertTrue(clientTelemetryReporter.collectors().isEmpty());
    }

    @Test
    public void testProducerLabels() {
        configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, "test-client");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "group-instance-id");
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");
        configs.put(CommonClientConfigs.CLIENT_RACK_CONFIG, "rack");

        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(new KafkaMetricsContext(KafkaProducer.JMX_PREFIX));
        assertEquals(1, clientTelemetryReporter.collectors().size());
        assertNotNull(clientTelemetryReporter.telemetryProvider().resource());

        List<KeyValue> attributes = clientTelemetryReporter.telemetryProvider().resource().getAttributesList();
        assertEquals(2, attributes.size());
        attributes.forEach(attribute -> {
            if (attribute.getKey().equals(ClientTelemetryProvider.CLIENT_RACK)) {
                assertEquals("rack", attribute.getValue().getStringValue());
            } else if (attribute.getKey().equals(ClientTelemetryProvider.TRANSACTIONAL_ID)) {
                assertEquals("transaction-id", attribute.getValue().getStringValue());
            }
        });
    }

    @Test
    public void testConsumerLabels() {
        configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, "test-client");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "group-instance-id");
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");
        configs.put(CommonClientConfigs.CLIENT_RACK_CONFIG, "rack");

        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(new KafkaMetricsContext(ConsumerUtils.CONSUMER_JMX_PREFIX));
        assertEquals(1, clientTelemetryReporter.collectors().size());
        assertNotNull(clientTelemetryReporter.telemetryProvider().resource());

        List<KeyValue> attributes = clientTelemetryReporter.telemetryProvider().resource().getAttributesList();
        assertEquals(3, attributes.size());
        attributes.forEach(attribute -> {
            if (attribute.getKey().equals(ClientTelemetryProvider.CLIENT_RACK)) {
                assertEquals("rack", attribute.getValue().getStringValue());
            } else if (attribute.getKey().equals(ClientTelemetryProvider.GROUP_ID)) {
                assertEquals("group-id", attribute.getValue().getStringValue());
            } else if (attribute.getKey().equals(ClientTelemetryProvider.GROUP_INSTANCE_ID)) {
                assertEquals("group-instance-id", attribute.getValue().getStringValue());
            }
        });
    }

    @Test
    public void testTelemetryReporterClose() throws Exception {
        clientTelemetryReporter.close();
        assertEquals(ClientTelemetryState.TERMINATED, ((DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());
    }

    @Test
    public void testTelemetryReporterCloseMultipleTimesNoException() {
        clientTelemetryReporter.close();
        clientTelemetryReporter.close();
        assertEquals(ClientTelemetryState.TERMINATED, ((DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());
    }

    @Test
    public void testTelemetrySenderTimeToNextUpdate() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        // subscription needed state
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(0, telemetrySender.timeToNextUpdate(100));
        // subscription needed state with local request time
        telemetrySender.updateSubscriptionResult(subscription, System.currentTimeMillis());
        assertEquals(20000, telemetrySender.timeToNextUpdate(100), 200);
        // subscription in progress state
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertEquals(100, telemetrySender.timeToNextUpdate(100));
        // push needed state
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        long time = telemetrySender.timeToNextUpdate(100);
        assertTrue(time > 0 && time >= 0.5 * time && time <= 1.5 * time);
        // push in progress state
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));
        assertEquals(100, telemetrySender.timeToNextUpdate(100));
        // terminating push needed state
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED));
        assertEquals(0, telemetrySender.timeToNextUpdate(100));
        // terminating push in progress state
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS));
        assertEquals(100, telemetrySender.timeToNextUpdate(100));
        // terminated state
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATED));
        assertEquals(Long.MAX_VALUE, telemetrySender.timeToNextUpdate(100));
    }

    @Test
    public void testCreateRequestSubscriptionNeeded() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertTrue(requestOptional.isPresent());
        assertTrue(requestOptional.get().build() instanceof GetTelemetrySubscriptionsRequest);
        GetTelemetrySubscriptionsRequest request = (GetTelemetrySubscriptionsRequest) requestOptional.get().build();

        GetTelemetrySubscriptionsRequest expectedResult = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(Uuid.ZERO_UUID)).build();

        assertEquals(expectedResult.data(), request.data());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS, telemetrySender.state());
    }

    @Test
    public void testCreateRequestSubscriptionNeededAfterExistingSubscription() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, System.currentTimeMillis());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertTrue(requestOptional.isPresent());
        assertTrue(requestOptional.get().build() instanceof GetTelemetrySubscriptionsRequest);
        GetTelemetrySubscriptionsRequest request = (GetTelemetrySubscriptionsRequest) requestOptional.get().build();

        GetTelemetrySubscriptionsRequest expectedResult = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(subscription.clientInstanceId())).build();

        assertEquals(expectedResult.data(), request.data());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS, telemetrySender.state());
    }

    @Test
    public void testCreateRequestPushNeeded() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        // create request to move state to subscription in progress
        telemetrySender.updateSubscriptionResult(subscription, System.currentTimeMillis());
        telemetrySender.createRequest();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertTrue(requestOptional.isPresent());
        assertTrue(requestOptional.get().build() instanceof PushTelemetryRequest);
        PushTelemetryRequest request = (PushTelemetryRequest) requestOptional.get().build();

        PushTelemetryRequest expectedResult = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData().setClientInstanceId(subscription.clientInstanceId())
                .setSubscriptionId(subscription.subscriptionId())).build();

        assertEquals(expectedResult.data(), request.data());
        assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());
    }

    @Test
    public void testCreateRequestPushNeededWithoutSubscription() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        // create request to move state to subscription in progress
        telemetrySender.createRequest();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertFalse(requestOptional.isPresent());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
    }

    @Test
    public void testCreateRequestInvalidState() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertFalse(telemetrySender.createRequest().isPresent());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));
        assertFalse(telemetrySender.createRequest().isPresent());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED));
        assertFalse(telemetrySender.createRequest().isPresent());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS));
        assertFalse(telemetrySender.createRequest().isPresent());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATED));
        assertFalse(telemetrySender.createRequest().isPresent());
    }

    @Test
    public void testHandleResponseGetSubscriptions() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        Uuid clientInstanceId = Uuid.randomUuid();
        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData()
                .setClientInstanceId(clientInstanceId)
                .setSubscriptionId(5678)
                .setAcceptedCompressionTypes(Collections.singletonList(CompressionType.GZIP.id))
                .setPushIntervalMs(20000)
                .setRequestedMetrics(Collections.singletonList("")));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.PUSH_NEEDED, telemetrySender.state());

        ClientTelemetrySubscription subscription = telemetrySender.subscription();
        assertNotNull(subscription);
        assertEquals(clientInstanceId, subscription.clientInstanceId());
        assertEquals(5678, subscription.subscriptionId());
        assertEquals(Collections.singletonList(CompressionType.GZIP), subscription.acceptedCompressionTypes());
        assertEquals(20000, subscription.pushIntervalMs());
        assertEquals(ClientTelemetryUtils.SELECTOR_ALL_METRICS, subscription.selector());
    }

    @Test
    public void testHandleResponseGetSubscriptionsWithoutMetrics() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        Uuid clientInstanceId = Uuid.randomUuid();
        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData()
                .setClientInstanceId(clientInstanceId)
                .setSubscriptionId(5678)
                .setAcceptedCompressionTypes(Collections.singletonList(CompressionType.GZIP.id))
                .setPushIntervalMs(20000));

        telemetrySender.handleResponse(response);
        // Again subscription should be required.
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());

        ClientTelemetrySubscription subscription = telemetrySender.subscription();
        assertNotNull(subscription);
        assertEquals(clientInstanceId, subscription.clientInstanceId());
        assertEquals(5678, subscription.subscriptionId());
        assertEquals(Collections.singletonList(CompressionType.GZIP), subscription.acceptedCompressionTypes());
        assertEquals(20000, subscription.pushIntervalMs());
        assertEquals(ClientTelemetryUtils.SELECTOR_NO_METRICS, subscription.selector());
    }

    @Test
    public void testHandleResponseGetTelemetryErrorResponse() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        // throttling quota exceeded
        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(300000, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());

        // invalid request error
        response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.INVALID_REQUEST.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());

        // unknown error
        telemetrySender.enabled(true);
        response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
    }

    @Test
    public void testHandleResponsePushTelemetry() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, System.currentTimeMillis());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        PushTelemetryResponse response = new PushTelemetryResponse(new PushTelemetryResponseData());

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.PUSH_NEEDED, telemetrySender.state());
        assertEquals(subscription.pushIntervalMs(), telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
    }

    @Test
    public void testHandleResponsePushTelemetryErrorResponse() {
        DefaultClientTelemetrySender telemetrySender = (DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, System.currentTimeMillis());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // unknown subscription id
        PushTelemetryResponse response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.UNKNOWN_SUBSCRIPTION_ID.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(0, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());

        // unsupported compression type
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.UNSUPPORTED_COMPRESSION_TYPE.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(0, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());

        // telemetry too large
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.TELEMETRY_TOO_LARGE.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(20000, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());

        // throttling quota exceeded
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(20000, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());

        // invalid request error
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.INVALID_REQUEST.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());

        // invalid record
        telemetrySender.enabled(true);
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.INVALID_RECORD.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());

        // unknown error
        telemetrySender.enabled(true);
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
    }

    @AfterEach
    public void tearDown() {
        clientTelemetryReporter.close();
    }
}
