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

public class ClientTelemetryTest {

    private ClientTelemetryExporterPlugin clientTelemetryExporterPlugin;

    @BeforeEach
    public void setUp() {
        clientTelemetryExporterPlugin = new ClientTelemetryExporterPlugin();
    }

    @Test
    public void testMultipleDeprecatedReceivers() throws UnknownHostException {
        // Test that multiple deprecated receivers can be registered
        TestClientMetricsReceiver receiver1 = new TestClientMetricsReceiver();
        TestClientMetricsReceiver receiver2 = new TestClientMetricsReceiver();

        clientTelemetryExporterPlugin.add(receiver1);
        clientTelemetryExporterPlugin.add(receiver2);

        byte[] metrics = "test-metrics-multiple".getBytes(StandardCharsets.UTF_8);
        clientTelemetryExporterPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
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

        clientTelemetryExporterPlugin.add(exporter1);
        clientTelemetryExporterPlugin.add(exporter2);

        byte[] metrics = "test-metrics-multiple-new".getBytes(StandardCharsets.UTF_8);
        int pushIntervalMs = 20000;
        clientTelemetryExporterPlugin.exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build(), pushIntervalMs);

        // Verify both exporters were called
        assertEquals(1, exporter1.exportMetricsInvokedCount);
        assertEquals(1, exporter2.exportMetricsInvokedCount);
        assertEquals(ByteBuffer.wrap(metrics), exporter1.metricsData.get(0));
        assertEquals(ByteBuffer.wrap(metrics), exporter2.metricsData.get(0));
        assertEquals(pushIntervalMs, exporter1.pushIntervals.get(0));
        assertEquals(pushIntervalMs, exporter2.pushIntervals.get(0));
    }
}
