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
package org.apache.kafka.common.telemetry.internals;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.telemetry.ClientTelemetryState;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.opentelemetry.proto.common.v1.KeyValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class ClientTelemetryReporterTest {

    private MockTime time;
    private ClientTelemetryReporter clientTelemetryReporter;
    private Map<String, Object> configs;
    private MetricsContext metricsContext;
    private Uuid uuid;
    private ClientTelemetryReporter.ClientTelemetrySubscription subscription;

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        clientTelemetryReporter = new ClientTelemetryReporter(time);
        configs = new HashMap<>();
        metricsContext = new KafkaMetricsContext("test");
        uuid = Uuid.randomUuid();
        subscription = new ClientTelemetryReporter.ClientTelemetrySubscription(uuid, 1234, 20000,
            Collections.emptyList(), true, null);
    }

    @Test
    public void testInitTelemetryReporter() {
        configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, "test-client");
        configs.put(CommonClientConfigs.CLIENT_RACK_CONFIG, "rack");

        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);
        assertNotNull(clientTelemetryReporter.metricsCollector());
        assertNotNull(clientTelemetryReporter.telemetryProvider().resource());
        assertEquals(1, clientTelemetryReporter.telemetryProvider().resource().getAttributesCount());
        assertEquals(
            ClientTelemetryProvider.CLIENT_RACK, clientTelemetryReporter.telemetryProvider().resource().getAttributes(0).getKey());
        assertEquals("rack", clientTelemetryReporter.telemetryProvider().resource().getAttributes(0).getValue().getStringValue());
    }

    @Test
    public void testInitTelemetryReporterNoCollector() {
        // Remove namespace config which skips the collector initialization.
        MetricsContext metricsContext = Collections::emptyMap;

        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);
        assertNull(clientTelemetryReporter.metricsCollector());
    }

    @Test
    public void testProducerLabels() {
        configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, "test-client");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "group-instance-id");
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");
        configs.put(CommonClientConfigs.CLIENT_RACK_CONFIG, "rack");

        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(new KafkaMetricsContext("kafka.producer"));
        assertNotNull(clientTelemetryReporter.metricsCollector());
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
        clientTelemetryReporter.contextChange(new KafkaMetricsContext("kafka.consumer"));
        assertNotNull(clientTelemetryReporter.metricsCollector());
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
    public void testTelemetryReporterClose() {
        clientTelemetryReporter.close();
        assertEquals(ClientTelemetryState.TERMINATED, ((ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());
    }

    @Test
    public void testTelemetryReporterCloseMultipleTimesNoException() {
        clientTelemetryReporter.close();
        clientTelemetryReporter.close();
        assertEquals(ClientTelemetryState.TERMINATED, ((ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());
    }

    @Test
    public void testUpdateMetricsLabels() {
        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);
        assertTrue(clientTelemetryReporter.telemetryProvider().resource().getAttributesList().isEmpty());

        clientTelemetryReporter.updateMetricsLabels(Collections.singletonMap("key1", "value1"));
        assertEquals(1, clientTelemetryReporter.telemetryProvider().resource().getAttributesList().size());
        assertEquals("key1", clientTelemetryReporter.telemetryProvider().resource().getAttributesList().get(0).getKey());
        assertEquals("value1", clientTelemetryReporter.telemetryProvider().resource().getAttributesList().get(0).getValue().getStringValue());

        clientTelemetryReporter.updateMetricsLabels(Collections.singletonMap("key2", "value2"));
        assertEquals(2, clientTelemetryReporter.telemetryProvider().resource().getAttributesList().size());
        clientTelemetryReporter.telemetryProvider().resource().getAttributesList().forEach(attribute -> {
            if (attribute.getKey().equals("key1")) {
                assertEquals("value1", attribute.getValue().getStringValue());
            } else {
                assertEquals("key2", attribute.getKey());
                assertEquals("value2", attribute.getValue().getStringValue());
            }
        });

        clientTelemetryReporter.updateMetricsLabels(Collections.singletonMap("key2", "valueUpdated"));
        assertEquals(2, clientTelemetryReporter.telemetryProvider().resource().getAttributesList().size());
        clientTelemetryReporter.telemetryProvider().resource().getAttributesList().forEach(attribute -> {
            if (attribute.getKey().equals("key1")) {
                assertEquals("value1", attribute.getValue().getStringValue());
            } else {
                assertEquals("key2", attribute.getKey());
                assertEquals("valueUpdated", attribute.getValue().getStringValue());
            }
        });
    }

    @Test
    public void testTelemetrySenderTimeToNextUpdate() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(0, telemetrySender.timeToNextUpdate(100));

        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertEquals(20000, telemetrySender.timeToNextUpdate(100), 200);

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertEquals(100, telemetrySender.timeToNextUpdate(100));

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        long time = telemetrySender.timeToNextUpdate(100);
        assertTrue(time > 0 && time >= 0.5 * time && time <= 1.5 * time);

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));
        assertEquals(100, telemetrySender.timeToNextUpdate(100));

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED));
        assertEquals(0, telemetrySender.timeToNextUpdate(100));

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS));
        assertEquals(Long.MAX_VALUE, telemetrySender.timeToNextUpdate(100));

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATED));
        assertEquals(Long.MAX_VALUE, telemetrySender.timeToNextUpdate(100));
    }

    @Test
    public void testCreateRequestSubscriptionNeeded() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertTrue(requestOptional.isPresent());
        assertInstanceOf(GetTelemetrySubscriptionsRequest.class, requestOptional.get().build());
        GetTelemetrySubscriptionsRequest request = (GetTelemetrySubscriptionsRequest) requestOptional.get().build();

        GetTelemetrySubscriptionsRequest expectedResult = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(Uuid.ZERO_UUID), true).build();

        assertEquals(expectedResult.data(), request.data());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS, telemetrySender.state());
    }

    @Test
    public void testCreateRequestSubscriptionNeededAfterExistingSubscription() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertTrue(requestOptional.isPresent());
        assertInstanceOf(GetTelemetrySubscriptionsRequest.class, requestOptional.get().build());
        GetTelemetrySubscriptionsRequest request = (GetTelemetrySubscriptionsRequest) requestOptional.get().build();

        GetTelemetrySubscriptionsRequest expectedResult = new GetTelemetrySubscriptionsRequest.Builder(
            new GetTelemetrySubscriptionsRequestData().setClientInstanceId(subscription.clientInstanceId()), true).build();

        assertEquals(expectedResult.data(), request.data());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS, telemetrySender.state());
    }

    @Test
    public void testCreateRequestPushNeeded() {
        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);

        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        // create request to move state to SUBSCRIPTION_IN_PROGRESS
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        telemetrySender.createRequest();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertTrue(requestOptional.isPresent());
        assertInstanceOf(PushTelemetryRequest.class, requestOptional.get().build());
        PushTelemetryRequest request = (PushTelemetryRequest) requestOptional.get().build();

        PushTelemetryRequest expectedResult = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData().setClientInstanceId(subscription.clientInstanceId())
                .setSubscriptionId(subscription.subscriptionId()), true).build();

        assertEquals(expectedResult.data(), request.data());
        assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());
    }

    @Test
    public void testCreateRequestPushNeededWithoutSubscription() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        // create request to move state to SUBSCRIPTION_IN_PROGRESS
        telemetrySender.createRequest();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertFalse(requestOptional.isPresent());
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
    }

    @Test
    public void testCreateRequestInvalidState() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());

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
    public void testCreateRequestPushNoCollector() {
        final long now = time.milliseconds();
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        // create request to move state to SUBSCRIPTION_IN_PROGRESS
        telemetrySender.createRequest();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        telemetrySender.updateSubscriptionResult(subscription, now);
        long interval = telemetrySender.timeToNextUpdate(100);
        assertTrue(interval > 0 && interval != 2000 && interval >= 0.5 * interval && interval <= 1.5 * interval);

        time.sleep(1000);
        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertFalse(requestOptional.isPresent());

        assertEquals(20000, telemetrySender.timeToNextUpdate(100));
        assertEquals(now + 1000, telemetrySender.lastRequestMs());
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testCreateRequestPushCompression(CompressionType compressionType) {
        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);

        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        ClientTelemetryReporter.ClientTelemetrySubscription subscription = new ClientTelemetryReporter.ClientTelemetrySubscription(
            uuid, 1234, 20000, Collections.singletonList(compressionType), true, null);
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());

        Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
        assertNotNull(requestOptional);
        assertTrue(requestOptional.isPresent());
        assertInstanceOf(PushTelemetryRequest.class, requestOptional.get().build());
        PushTelemetryRequest request = (PushTelemetryRequest) requestOptional.get().build();

        assertEquals(subscription.clientInstanceId(), request.data().clientInstanceId());
        assertEquals(subscription.subscriptionId(), request.data().subscriptionId());
        assertEquals(compressionType.id, request.data().compressionType());
        assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());
    }

    @Test
    public void testCreateRequestPushCompressionException() {
        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);

        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        ClientTelemetryReporter.ClientTelemetrySubscription subscription = new ClientTelemetryReporter.ClientTelemetrySubscription(
            uuid, 1234, 20000, Collections.singletonList(CompressionType.GZIP), true, null);
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());

        try (MockedStatic<ClientTelemetryUtils> mockedCompress = Mockito.mockStatic(ClientTelemetryUtils.class, new CallsRealMethods())) {
            mockedCompress.when(() -> ClientTelemetryUtils.compress(any(), any())).thenThrow(new IOException());

            Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
            assertNotNull(requestOptional);
            assertTrue(requestOptional.isPresent());
            assertInstanceOf(PushTelemetryRequest.class, requestOptional.get().build());
            PushTelemetryRequest request = (PushTelemetryRequest) requestOptional.get().build();

            assertEquals(subscription.clientInstanceId(), request.data().clientInstanceId());
            assertEquals(subscription.subscriptionId(), request.data().subscriptionId());
            // CompressionType.NONE is used when compression fails.
            assertEquals(CompressionType.NONE.id, request.data().compressionType());
            assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());
        }
    }

    @Test
    public void testCreateRequestPushCompressionFallbackToNextType() {
        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);

        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        // Set up subscription with multiple compression types: GZIP -> LZ4 -> SNAPPY
        ClientTelemetryReporter.ClientTelemetrySubscription subscription = new ClientTelemetryReporter.ClientTelemetrySubscription(
            uuid, 1234, 20000, List.of(CompressionType.GZIP, CompressionType.LZ4, CompressionType.SNAPPY), true, null);
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());

        try (MockedStatic<ClientTelemetryUtils> mockedCompress = Mockito.mockStatic(ClientTelemetryUtils.class, new CallsRealMethods())) {
            // First request: GZIP fails with NoClassDefFoundError, should use NONE for this request
            mockedCompress.when(() -> ClientTelemetryUtils.compress(any(), eq(CompressionType.GZIP))).thenThrow(new NoClassDefFoundError("GZIP not available"));

            Optional<AbstractRequest.Builder<?>> requestOptional = telemetrySender.createRequest();
            assertNotNull(requestOptional);
            assertTrue(requestOptional.isPresent());
            assertInstanceOf(PushTelemetryRequest.class, requestOptional.get().build());
            PushTelemetryRequest request = (PushTelemetryRequest) requestOptional.get().build();

            // Should fallback to NONE for this request (GZIP gets cached as unsupported)
            assertEquals(CompressionType.NONE.id, request.data().compressionType());
            assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());

            // Reset state for next request
            assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

            // Second request: LZ4 is selected (since GZIP is now cached as unsupported), LZ4 fails, should use NONE
            // Note that some libraries eg. LZ4 return KafkaException with cause as NoClassDefFoundError
            mockedCompress.when(() -> ClientTelemetryUtils.compress(any(), eq(CompressionType.LZ4))).thenThrow(new KafkaException(new NoClassDefFoundError("LZ4 not available")));

            requestOptional = telemetrySender.createRequest();
            assertNotNull(requestOptional);
            assertTrue(requestOptional.isPresent());
            assertInstanceOf(PushTelemetryRequest.class, requestOptional.get().build());
            request = (PushTelemetryRequest) requestOptional.get().build();

            // Should fallback to NONE for this request (LZ4 gets cached as unsupported)
            assertEquals(CompressionType.NONE.id, request.data().compressionType());
            assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());

            // Reset state for next request
            assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

            // Third request: SNAPPY is selected (since GZIP and LZ4 are now cached as unsupported), SNAPPY fails, should use NONE
            mockedCompress.when(() -> ClientTelemetryUtils.compress(any(), eq(CompressionType.SNAPPY))).thenThrow(new NoClassDefFoundError("SNAPPY not available"));

            requestOptional = telemetrySender.createRequest();
            assertNotNull(requestOptional);
            assertTrue(requestOptional.isPresent());
            assertInstanceOf(PushTelemetryRequest.class, requestOptional.get().build());
            request = (PushTelemetryRequest) requestOptional.get().build();

            // Should fallback to NONE for this request (SNAPPY gets cached as unsupported)
            assertEquals(CompressionType.NONE.id, request.data().compressionType());
            assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());

            // Reset state for next request
            assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

            // Fourth request: All compression types are now cached as unsupported, should use NONE directly
            requestOptional = telemetrySender.createRequest();
            assertNotNull(requestOptional);
            assertTrue(requestOptional.isPresent());
            assertInstanceOf(PushTelemetryRequest.class, requestOptional.get().build());
            request = (PushTelemetryRequest) requestOptional.get().build();

            // Should use NONE directly (no compression types are supported)
            assertEquals(CompressionType.NONE.id, request.data().compressionType());
            assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());
        }
    }

    @Test
    public void testCreateRequestPushCompressionFallbackAndTermination() {
        clientTelemetryReporter.configure(configs);
        clientTelemetryReporter.contextChange(metricsContext);

        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        // Set up subscription with ZSTD compression type
        ClientTelemetryReporter.ClientTelemetrySubscription subscription = new ClientTelemetryReporter.ClientTelemetrySubscription(
            uuid, 1234, 20000, List.of(CompressionType.ZSTD, CompressionType.LZ4), true, null);
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());

        try (MockedStatic<ClientTelemetryUtils> mockedCompress = Mockito.mockStatic(ClientTelemetryUtils.class, new CallsRealMethods())) {
            
            // === Test 1: NoClassDefFoundError fallback (recoverable) ===
            mockedCompress.when(() -> ClientTelemetryUtils.compress(any(), eq(CompressionType.ZSTD)))
                    .thenThrow(new NoClassDefFoundError("com/github/luben/zstd/BufferPool"));
            
            assertEquals(ClientTelemetryState.PUSH_NEEDED, telemetrySender.state());
            
            Optional<AbstractRequest.Builder<?>> request1 = telemetrySender.createRequest();
            assertNotNull(request1);
            assertTrue(request1.isPresent());
            assertInstanceOf(PushTelemetryRequest.class, request1.get().build());
            PushTelemetryRequest pushRequest1 = (PushTelemetryRequest) request1.get().build();
            assertEquals(CompressionType.NONE.id, pushRequest1.data().compressionType()); // Fallback to NONE
            assertEquals(ClientTelemetryState.PUSH_IN_PROGRESS, telemetrySender.state());
            
            // Reset state (simulate successful response handling)
            assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
            
            // === Test 2: OutOfMemoryError causes termination (non-recoverable Error) ===
            mockedCompress.reset();
            mockedCompress.when(() -> ClientTelemetryUtils.compress(any(), eq(CompressionType.LZ4)))
                    .thenThrow(new OutOfMemoryError("Out of memory during compression"));
            
            assertEquals(ClientTelemetryState.PUSH_NEEDED, telemetrySender.state());

            assertThrows(KafkaException.class, telemetrySender::createRequest);
            assertEquals(ClientTelemetryState.TERMINATED, telemetrySender.state());
            
            // === Test 3: After termination, no more requests ===
            Optional<AbstractRequest.Builder<?>> request3 = telemetrySender.createRequest();
            assertNotNull(request3);
            assertFalse(request3.isPresent()); // No request created
            assertEquals(ClientTelemetryState.TERMINATED, telemetrySender.state()); // State remains TERMINATED
        }
    }

    @Test
    public void testHandleResponseGetSubscriptions() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        Uuid clientInstanceId = Uuid.randomUuid();
        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData()
                .setClientInstanceId(clientInstanceId)
                .setSubscriptionId(5678)
                .setAcceptedCompressionTypes(Collections.singletonList(CompressionType.GZIP.id))
                .setPushIntervalMs(20000)
                .setRequestedMetrics(Collections.singletonList("*")));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.PUSH_NEEDED, telemetrySender.state());

        ClientTelemetryReporter.ClientTelemetrySubscription subscription = telemetrySender.subscription();
        assertNotNull(subscription);
        assertEquals(clientInstanceId, subscription.clientInstanceId());
        assertEquals(5678, subscription.subscriptionId());
        assertEquals(Collections.singletonList(CompressionType.GZIP), subscription.acceptedCompressionTypes());
        assertEquals(20000, subscription.pushIntervalMs());
        assertEquals(ClientTelemetryUtils.SELECTOR_ALL_METRICS, subscription.selector());
    }

    @Test
    public void testHandleResponseGetSubscriptionsWithoutMetrics() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
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

        ClientTelemetryReporter.ClientTelemetrySubscription subscription = telemetrySender.subscription();
        assertNotNull(subscription);
        assertEquals(clientInstanceId, subscription.clientInstanceId());
        assertEquals(5678, subscription.subscriptionId());
        assertEquals(Collections.singletonList(CompressionType.GZIP), subscription.acceptedCompressionTypes());
        assertEquals(20000, subscription.pushIntervalMs());
        assertEquals(ClientTelemetryUtils.SELECTOR_NO_METRICS, subscription.selector());
    }

    @Test
    public void testHandleResponseGetTelemetryErrorResponse() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        // throttling quota exceeded
        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(300000, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        // invalid request error
        response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.INVALID_REQUEST.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        // unsupported version error
        telemetrySender.enabled(true);
        response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        // unknown error
        telemetrySender.enabled(true);
        response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
    }

    @Test
    public void testHandleResponseSubscriptionChange() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        KafkaMetricsCollector kafkaMetricsCollector = Mockito.mock(KafkaMetricsCollector.class);
        clientTelemetryReporter.metricsCollector(kafkaMetricsCollector);
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        Uuid clientInstanceId = Uuid.randomUuid();
        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData()
                .setClientInstanceId(clientInstanceId)
                .setSubscriptionId(15678)
                .setAcceptedCompressionTypes(Collections.singletonList(CompressionType.ZSTD.id))
                .setPushIntervalMs(10000)
                .setDeltaTemporality(false) // Change delta temporality as well
                .setRequestedMetrics(Collections.singletonList("org.apache.kafka.producer")));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.PUSH_NEEDED, telemetrySender.state());

        ClientTelemetryReporter.ClientTelemetrySubscription responseSubscription = telemetrySender.subscription();
        assertNotNull(responseSubscription);
        assertEquals(clientInstanceId, responseSubscription.clientInstanceId());
        assertEquals(15678, responseSubscription.subscriptionId());
        assertEquals(Collections.singletonList(CompressionType.ZSTD), responseSubscription.acceptedCompressionTypes());
        assertEquals(10000, responseSubscription.pushIntervalMs());
        assertFalse(responseSubscription.deltaTemporality());
        assertTrue(responseSubscription.selector().test(new MetricKey("org.apache.kafka.producer")));
        assertTrue(responseSubscription.selector().test(new MetricKey("org.apache.kafka.producerabc")));
        assertTrue(responseSubscription.selector().test(new MetricKey("org.apache.kafka.producer.abc")));
        assertFalse(responseSubscription.selector().test(new MetricKey("org.apache.kafka.produce")));

        Mockito.verify(kafkaMetricsCollector, Mockito.times(1)).metricsReset();
    }

    @Test
    public void testHandleResponsePushTelemetry() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
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
    public void testHandleResponsePushTelemetryTerminating() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS));

        PushTelemetryResponse response = new PushTelemetryResponse(new PushTelemetryResponseData());

        telemetrySender.handleResponse(response);
        // The telemetry sender remains in TERMINATING_PUSH_IN_PROGRESS so that a subsequent close() finishes the job
        assertEquals(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS, telemetrySender.state());
        assertEquals(subscription.pushIntervalMs(), telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATED));
    }

    @Test
    public void testHandleResponsePushTelemetryErrorResponse() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
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
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // unsupported compression type
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.UNSUPPORTED_COMPRESSION_TYPE.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(0, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // telemetry too large
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.TELEMETRY_TOO_LARGE.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(20000, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // throttling quota exceeded
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(20000, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // invalid request error
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.INVALID_REQUEST.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // unsupported version error
        telemetrySender.enabled(true);
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // invalid record
        telemetrySender.enabled(true);
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.INVALID_RECORD.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        // unknown error
        telemetrySender.enabled(true);
        response = new PushTelemetryResponse(
            new PushTelemetryResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));

        telemetrySender.handleResponse(response);
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
    }

    @Test
    public void testClientInstanceId() throws InterruptedException {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        CountDownLatch lock = new CountDownLatch(2);

        AtomicReference<Optional<Uuid>> clientInstanceId = new AtomicReference<>();
        new Thread(() -> {
            try {
                clientInstanceId.set(telemetrySender.clientInstanceId(Duration.ofMillis(10000)));
            } finally {
                lock.countDown();
            }
        }).start();

        new Thread(() -> {
            try {
                telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
            } finally {
                lock.countDown();
            }
        }).start();

        assertTrue(lock.await(2000, TimeUnit.MILLISECONDS));
        assertNotNull(clientInstanceId.get());
        assertTrue(clientInstanceId.get().isPresent());
        assertEquals(uuid, clientInstanceId.get().get());
    }

    @Test
    public void testComputeStaggeredIntervalMs() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        assertEquals(0, telemetrySender.computeStaggeredIntervalMs(0, 0.5, 1.5));
        assertEquals(1, telemetrySender.computeStaggeredIntervalMs(1, 0.99, 1));
        long timeMs = telemetrySender.computeStaggeredIntervalMs(1000, 0.5, 1.5);
        assertTrue(timeMs >= 500 && timeMs <= 1500);
    }

    @Test
    public void testTelemetryReporterInitiateClose() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));

        clientTelemetryReporter.initiateClose();
        assertEquals(ClientTelemetryState.TERMINATING_PUSH_NEEDED, ((ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());
    }

    @Test
    public void testTelemetryReporterInitiateCloseNoSubscription() {
        clientTelemetryReporter.initiateClose();
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, ((ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());
    }

    @Test
    public void testTelemetryReporterInitiateCloseAlreadyInTerminatedStates() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED));

        clientTelemetryReporter.initiateClose();
        assertEquals(ClientTelemetryState.TERMINATING_PUSH_NEEDED, ((ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS));
        clientTelemetryReporter.initiateClose();
        assertEquals(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS, ((ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATED));
        clientTelemetryReporter.initiateClose();
        assertEquals(ClientTelemetryState.TERMINATED, ((ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter
            .telemetrySender()).state());
    }

    @Test
    public void testHandleFailedGetTelemetrySubscriptionsRequestWithRetriableException() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        KafkaException retriableException = new TimeoutException("Request timed out");
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(retriableException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
    }

    @Test
    public void testHandleFailedGetTelemetrySubscriptionsRequestWithWrappedRetriableException() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        KafkaException wrappedException = new KafkaException(new DisconnectException("Connection lost"));
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(wrappedException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
    }

    @Test
    public void testHandleFailedGetTelemetrySubscriptionsRequestWithFatalException() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        KafkaException fatalException = new AuthorizationException("Not authorized for telemetry");
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(fatalException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
    }

    @Test
    public void testHandleFailedGetTelemetrySubscriptionsRequestWithWrappedFatalException() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        KafkaException wrappedException = new KafkaException("Version check failed", 
            new UnsupportedVersionException("Broker doesn't support telemetry"));
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(wrappedException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
    }

    @Test
    public void testHandleFailedPushTelemetryRequestWithRetriableException() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        KafkaException networkException = new NetworkException("Network failure");
        telemetrySender.handleFailedPushTelemetryRequest(networkException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
    }

    @Test
    public void testHandleFailedPushTelemetryRequestWithFatalException() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_IN_PROGRESS));

        KafkaException authException = new AuthorizationException("Not authorized to push telemetry");
        telemetrySender.handleFailedPushTelemetryRequest(authException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
    }

    @Test
    public void testHandleFailedRequestWithMultipleRetriableExceptionsInChain() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        KafkaException chainedException = new TimeoutException("Outer timeout",
            new DisconnectException("Inner disconnect"));
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(chainedException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS, telemetrySender.intervalMs());
        assertTrue(telemetrySender.enabled());
    }
    
    @Test
    public void testHandleFailedRequestWithGenericKafkaException() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));

        KafkaException genericException = new KafkaException("Unknown error");
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(genericException);

        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertEquals(Integer.MAX_VALUE, telemetrySender.intervalMs());
        assertFalse(telemetrySender.enabled());
    }

    @Test
    public void testHandleFailedRequestDuringTermination() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.PUSH_NEEDED));
        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED));

        KafkaException exception = new TimeoutException("Timeout");
        telemetrySender.handleFailedPushTelemetryRequest(exception);

        assertEquals(ClientTelemetryState.TERMINATING_PUSH_NEEDED, telemetrySender.state());
        assertTrue(telemetrySender.enabled());
    }

    @Test
    public void testSequentialFailuresWithDifferentExceptionTypes() {
        ClientTelemetryReporter.DefaultClientTelemetrySender telemetrySender = (ClientTelemetryReporter.DefaultClientTelemetrySender) clientTelemetryReporter.telemetrySender();
        telemetrySender.updateSubscriptionResult(subscription, time.milliseconds());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(
            new TimeoutException("Timeout 1"));
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertTrue(telemetrySender.enabled());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(
            new DisconnectException("Disconnect"));
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertTrue(telemetrySender.enabled());

        assertTrue(telemetrySender.maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS));
        telemetrySender.handleFailedGetTelemetrySubscriptionsRequest(
            new UnsupportedVersionException("Version not supported"));
        assertEquals(ClientTelemetryState.SUBSCRIPTION_NEEDED, telemetrySender.state());
        assertFalse(telemetrySender.enabled());
    }

    @AfterEach
    public void tearDown() {
        clientTelemetryReporter.close();
    }
}
