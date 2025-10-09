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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.server.metrics.ClientMetricsTestUtils.TestClientMetricsReceiver;
import org.apache.kafka.server.metrics.ClientMetricsTestUtils.TestClientTelemetryExporter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTelemetryInterfaceCompatibilityTest {

    private ClientTelemetryPlugin clientTelemetryPlugin;

    @BeforeEach
    public void setUp() {
        clientTelemetryPlugin = new ClientTelemetryPlugin();
    }

    @Test
    public void testDeprecatedClientTelemetryReceiverInterface() throws UnknownHostException {
        // Test that the deprecated ClientTelemetryReceiver interface still works
        TestClientMetricsReceiver receiver = new TestClientMetricsReceiver();

        assertTrue(clientTelemetryPlugin.isEmpty());
        clientTelemetryPlugin.add(receiver);
        assertFalse(clientTelemetryPlugin.isEmpty());

        assertEquals(0, receiver.exportMetricsInvokedCount);
        assertTrue(receiver.metricsData.isEmpty());

        byte[] metrics = "test-metrics-deprecated".getBytes(StandardCharsets.UTF_8);
        clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), 5000);

        // Verify deprecated receiver was called
        assertEquals(1, receiver.exportMetricsInvokedCount);
        assertEquals(1, receiver.metricsData.size());
        assertEquals(ByteBuffer.wrap(metrics), receiver.metricsData.get(0));
    }

    @Test
    public void testNewClientTelemetryExporterInterface() throws UnknownHostException {
        // Test that the new ClientTelemetryExporter interface works
        TestClientTelemetryExporter exporter = new TestClientTelemetryExporter();

        assertTrue(clientTelemetryPlugin.isEmpty());
        clientTelemetryPlugin.add(exporter);
        assertFalse(clientTelemetryPlugin.isEmpty());

        assertEquals(0, exporter.exportMetricsInvokedCount);
        assertTrue(exporter.metricsData.isEmpty());
        assertTrue(exporter.pushIntervals.isEmpty());

        byte[] metrics = "test-metrics-new".getBytes(StandardCharsets.UTF_8);
        int pushIntervalMs = 10000;
        clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), pushIntervalMs);

        // Verify new exporter was called with push interval
        assertEquals(1, exporter.exportMetricsInvokedCount);
        assertEquals(1, exporter.metricsData.size());
        assertEquals(ByteBuffer.wrap(metrics), exporter.metricsData.get(0));
        assertEquals(1, exporter.pushIntervals.size());
        assertEquals(pushIntervalMs, exporter.pushIntervals.get(0));
    }

    @Test
    public void testBothInterfacesCoexist() throws UnknownHostException {
        // Test that both deprecated and new interfaces can coexist
        TestClientMetricsReceiver receiver = new TestClientMetricsReceiver();
        TestClientTelemetryExporter exporter = new TestClientTelemetryExporter();

        clientTelemetryPlugin.add(receiver);
        clientTelemetryPlugin.add(exporter);
        assertFalse(clientTelemetryPlugin.isEmpty());

        byte[] metrics = "test-metrics-both".getBytes(StandardCharsets.UTF_8);
        int pushIntervalMs = 15000;
        clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), pushIntervalMs);

        // Verify both were called
        assertEquals(1, receiver.exportMetricsInvokedCount);
        assertEquals(1, receiver.metricsData.size());
        assertEquals(ByteBuffer.wrap(metrics), receiver.metricsData.get(0));

        assertEquals(1, exporter.exportMetricsInvokedCount);
        assertEquals(1, exporter.metricsData.size());
        assertEquals(ByteBuffer.wrap(metrics), exporter.metricsData.get(0));
        assertEquals(pushIntervalMs, exporter.pushIntervals.get(0));
    }

    @Test
    public void testMultipleDeprecatedReceivers() throws UnknownHostException {
        // Test that multiple deprecated receivers can be registered
        TestClientMetricsReceiver receiver1 = new TestClientMetricsReceiver();
        TestClientMetricsReceiver receiver2 = new TestClientMetricsReceiver();

        clientTelemetryPlugin.add(receiver1);
        clientTelemetryPlugin.add(receiver2);

        byte[] metrics = "test-metrics-multiple".getBytes(StandardCharsets.UTF_8);
        clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), 5000);

        // Verify both receivers were called
        assertEquals(1, receiver1.exportMetricsInvokedCount);
        assertEquals(1, receiver2.exportMetricsInvokedCount);
        assertEquals(ByteBuffer.wrap(metrics), receiver1.metricsData.get(0));
        assertEquals(ByteBuffer.wrap(metrics), receiver2.metricsData.get(0));
    }

    @Test
    public void testMultipleNewExporters() throws UnknownHostException {
        // Test that multiple new exporters can be registered
        TestClientTelemetryExporter exporter1 = new TestClientTelemetryExporter();
        TestClientTelemetryExporter exporter2 = new TestClientTelemetryExporter();

        clientTelemetryPlugin.add(exporter1);
        clientTelemetryPlugin.add(exporter2);

        byte[] metrics = "test-metrics-multiple-new".getBytes(StandardCharsets.UTF_8);
        int pushIntervalMs = 20000;
        clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), pushIntervalMs);

        // Verify both exporters were called
        assertEquals(1, exporter1.exportMetricsInvokedCount);
        assertEquals(1, exporter2.exportMetricsInvokedCount);
        assertEquals(ByteBuffer.wrap(metrics), exporter1.metricsData.get(0));
        assertEquals(ByteBuffer.wrap(metrics), exporter2.metricsData.get(0));
        assertEquals(pushIntervalMs, exporter1.pushIntervals.get(0));
        assertEquals(pushIntervalMs, exporter2.pushIntervals.get(0));
    }

    @Test
    public void testNullAndEmptyMetricsPayload() throws UnknownHostException {
        // Test that null and empty metrics are passed through to exporters
        // (ClientMetricsManager is responsible for filtering these out before calling the plugin)
        TestClientMetricsReceiver receiver = new TestClientMetricsReceiver();
        TestClientTelemetryExporter exporter = new TestClientTelemetryExporter();

        clientTelemetryPlugin.add(receiver);
        clientTelemetryPlugin.add(exporter);

        // Export with null metrics - exporters are still called
        clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(null), true).build(), 5000);

        // Verify both receiver and exporter were called (plugin doesn't filter)
        assertEquals(1, receiver.exportMetricsInvokedCount);
        assertEquals(1, exporter.exportMetricsInvokedCount);

        // Export with empty ByteBuffer (0 bytes)
        clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.allocate(0)), true).build(), 5000);

        // Verify exporters were called again with empty buffer
        assertEquals(2, receiver.exportMetricsInvokedCount);
        assertEquals(2, exporter.exportMetricsInvokedCount);
        assertEquals(0, receiver.metricsData.get(1).remaining());
        assertEquals(0, exporter.metricsData.get(1).remaining());
    }

    @Test
    public void testPushIntervalPropagation() throws UnknownHostException {
        TestClientTelemetryExporter exporter = new TestClientTelemetryExporter();
        clientTelemetryPlugin.add(exporter);

        byte[] metrics = "test".getBytes(StandardCharsets.UTF_8);

        // Test different push intervals
        int[] pushIntervals = {1000, 5000, 10000, 30000, 60000};
        for (int pushInterval : pushIntervals) {
            clientTelemetryPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
                new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(),
                pushInterval);
        }

        assertEquals(pushIntervals.length, exporter.exportMetricsInvokedCount);
        assertEquals(pushIntervals.length, exporter.pushIntervals.size());

        for (int i = 0; i < pushIntervals.length; i++) {
            assertEquals(pushIntervals[i], exporter.pushIntervals.get(i));
        }
    }
}