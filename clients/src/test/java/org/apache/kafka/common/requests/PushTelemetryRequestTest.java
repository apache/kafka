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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.telemetry.internals.MetricKey;
import org.apache.kafka.common.telemetry.internals.SinglePointMetric;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PushTelemetryRequestTest {

    @Test
    public void testGetErrorResponse() {
        PushTelemetryRequest req = new PushTelemetryRequest(new PushTelemetryRequestData(), (short) 0);
        PushTelemetryResponse response = req.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 1), response.errorCounts());
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testMetricsDataCompression(CompressionType compressionType) throws IOException {
        List<SinglePointMetric> metrics = sampleMetrics();
        byte[] raw = Utils.toArray(ClientTelemetryUtils.compressMetrics(metrics, CompressionType.NONE));
        PushTelemetryRequest req = getPushTelemetryRequest(metrics, raw, compressionType);

        ByteBuffer receivedMetricsBuffer = req.metricsData(1024 * 1024);
        assertNotNull(receivedMetricsBuffer);
        assertTrue(receivedMetricsBuffer.capacity() > 0);
        assertArrayEquals(raw, Utils.toArray(receivedMetricsBuffer));
    }

    private PushTelemetryRequest getPushTelemetryRequest(List<SinglePointMetric> metrics, byte[] raw, CompressionType compressionType) throws IOException {
        ByteBuffer compressedData = ClientTelemetryUtils.compressMetrics(metrics, compressionType);
        if (compressionType != CompressionType.NONE) {
            assertTrue(compressedData.limit() < raw.length);
        } else {
            assertArrayEquals(Utils.toArray(compressedData), raw);
        }

        return new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setMetrics(compressedData)
                .setCompressionType(compressionType.id)).build();
    }

    private List<SinglePointMetric> sampleMetrics() {
        List<SinglePointMetric> metrics = new ArrayList<>();
        metrics.add(SinglePointMetric.sum(
            new MetricKey("metricName"), 1.0, true, Instant.now(), null, Collections.emptySet()));
        metrics.add(SinglePointMetric.sum(
            new MetricKey("metricName1"), 100.0, false, Instant.now(), Instant.now(), Collections.emptySet()));
        metrics.add(SinglePointMetric.deltaSum(
            new MetricKey("metricName2"), 1.0, true, Instant.now(), Instant.now(), Collections.emptySet()));
        metrics.add(SinglePointMetric.gauge(
            new MetricKey("metricName3"), 1.0, Instant.now(), Collections.emptySet()));
        metrics.add(SinglePointMetric.gauge(
            new MetricKey("metricName4"), Long.valueOf(100), Instant.now(), Collections.emptySet()));
        return metrics;
    }

}
