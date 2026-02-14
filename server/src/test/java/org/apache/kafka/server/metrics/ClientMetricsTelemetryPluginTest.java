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

public class ClientMetricsTelemetryPluginTest {

    private TestClientMetricsReceiver telemetryReceiver;
    private ClientTelemetryExporterPlugin clientTelemetryExporterPlugin;
    private TestClientTelemetryExporter telemetryExporter;

    @BeforeEach
    public void setUp() {
        telemetryReceiver = new TestClientMetricsReceiver();
        telemetryExporter = new TestClientTelemetryExporter();
        clientTelemetryExporterPlugin = new ClientTelemetryExporterPlugin();
    }

    @Test
    public void testExportMetricsWithDeprecatedReceiver() throws UnknownHostException {
        assertTrue(clientTelemetryExporterPlugin.isEmpty());

        clientTelemetryExporterPlugin.add(telemetryReceiver);
        assertFalse(clientTelemetryExporterPlugin.isEmpty());

        assertEquals(0, telemetryReceiver.exportMetricsInvokedCount);
        assertTrue(telemetryReceiver.metricsData.isEmpty());

        byte[] metrics = "test-metrics".getBytes(StandardCharsets.UTF_8);
        clientTelemetryExporterPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), 5000);

        assertEquals(1, telemetryReceiver.exportMetricsInvokedCount);
        assertEquals(1, telemetryReceiver.metricsData.size());
        assertEquals(metrics, telemetryReceiver.metricsData.get(0).array());
    }

    @Test
    public void testExportMetricsWithNewExporter() throws UnknownHostException {
        assertTrue(clientTelemetryExporterPlugin.isEmpty());
        clientTelemetryExporterPlugin.add(telemetryExporter);
        assertFalse(clientTelemetryExporterPlugin.isEmpty());

        assertEquals(0, telemetryExporter.exportMetricsInvokedCount);
        assertTrue(telemetryExporter.metricsData.isEmpty());
        assertTrue(telemetryExporter.pushIntervals.isEmpty());

        byte[] metrics = "test-metrics-new".getBytes(StandardCharsets.UTF_8);
        int pushIntervalMs = 10000;
        clientTelemetryExporterPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), pushIntervalMs);

        assertEquals(1, telemetryExporter.exportMetricsInvokedCount);
        assertEquals(1, telemetryExporter.metricsData.size());
        assertEquals(ByteBuffer.wrap(metrics), telemetryExporter.metricsData.get(0));
        assertEquals(1, telemetryExporter.pushIntervals.size());
        assertEquals(pushIntervalMs, telemetryExporter.pushIntervals.get(0));
    }

    @Test
    public void testExportMetricsWithBothReceiverAndExporter() throws UnknownHostException {
        // Test with separate receiver and exporter objects - both should be called
        clientTelemetryExporterPlugin.add(telemetryReceiver);
        clientTelemetryExporterPlugin.add(telemetryExporter);
        assertFalse(clientTelemetryExporterPlugin.isEmpty());

        byte[] metrics = "test-metrics-both".getBytes(StandardCharsets.UTF_8);
        int pushIntervalMs = 15000;
        clientTelemetryExporterPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), pushIntervalMs);

        // Both should be called since they are separate objects
        assertEquals(1, telemetryReceiver.exportMetricsInvokedCount);
        assertEquals(1, telemetryExporter.exportMetricsInvokedCount);

        assertEquals(ByteBuffer.wrap(metrics), telemetryReceiver.metricsData.get(0));
        assertEquals(ByteBuffer.wrap(metrics), telemetryExporter.metricsData.get(0));
        assertEquals(pushIntervalMs, telemetryExporter.pushIntervals.get(0));
    }

    @Test
    public void testExportMetricsWithDualImplementation() throws UnknownHostException {
        // Test with a class that implements both interfaces
        // This mimics production behavior in DynamicBrokerConfig where pattern matching
        // ensures only the exporter is added when a single object implements both interfaces
        ClientMetricsTestUtils.TestDualImplementation dualImpl = new ClientMetricsTestUtils.TestDualImplementation();

        // In production (DynamicBrokerConfig.scala), when a reporter implements both interfaces,
        // only the exporter is added due to pattern matching that checks ClientTelemetryExporterProvider first
        clientTelemetryExporterPlugin.add(dualImpl.clientTelemetryExporter());
        assertFalse(clientTelemetryExporterPlugin.isEmpty());

        byte[] metrics = "test-metrics-dual".getBytes(StandardCharsets.UTF_8);
        int pushIntervalMs = 12000;
        clientTelemetryExporterPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), pushIntervalMs);

        // Only the exporter should be called (receiver should not be invoked)
        assertEquals(0, dualImpl.getReceiver().exportMetricsInvokedCount);
        assertEquals(1, dualImpl.getExporter().exportMetricsInvokedCount);

        assertEquals(ByteBuffer.wrap(metrics), dualImpl.getExporter().metricsData.get(0));
        assertEquals(pushIntervalMs, dualImpl.getExporter().pushIntervals.get(0));
    }
}
