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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTelemetryUtilsTest {

    @Test
    public void testMaybeFetchErrorIntervalMs() {
        assertEquals(Optional.empty(), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.NONE.code(), -1));
        assertEquals(Optional.of(Integer.MAX_VALUE), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.INVALID_REQUEST.code(), -1));
        assertEquals(Optional.of(Integer.MAX_VALUE), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.INVALID_RECORD.code(), -1));
        assertEquals(Optional.of(0), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.UNKNOWN_SUBSCRIPTION_ID.code(), -1));
        assertEquals(Optional.of(0), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.UNSUPPORTED_COMPRESSION_TYPE.code(), -1));
        assertEquals(Optional.of(ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.TELEMETRY_TOO_LARGE.code(), -1));
        assertEquals(Optional.of(20000), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.TELEMETRY_TOO_LARGE.code(), 20000));
        assertEquals(Optional.of(ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.THROTTLING_QUOTA_EXCEEDED.code(), -1));
        assertEquals(Optional.of(20000), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.THROTTLING_QUOTA_EXCEEDED.code(), 20000));
        assertEquals(Optional.of(Integer.MAX_VALUE), ClientTelemetryUtils.maybeFetchErrorIntervalMs(Errors.UNKNOWN_SERVER_ERROR.code(), -1));
    }

    @Test
    public void testGetSelectorFromRequestedMetrics() {
        // no metrics selector
        assertEquals(ClientTelemetryUtils.SELECTOR_NO_METRICS, ClientTelemetryUtils.getSelectorFromRequestedMetrics(Collections.emptyList()));
        assertEquals(ClientTelemetryUtils.SELECTOR_NO_METRICS, ClientTelemetryUtils.getSelectorFromRequestedMetrics(null));
        // all metrics selector
        assertEquals(ClientTelemetryUtils.SELECTOR_ALL_METRICS, ClientTelemetryUtils.getSelectorFromRequestedMetrics(Collections.singletonList("*")));
        // specific metrics selector
        Predicate<? super MetricKeyable> selector = ClientTelemetryUtils.getSelectorFromRequestedMetrics(Arrays.asList("metric1", "metric2"));
        assertNotEquals(ClientTelemetryUtils.SELECTOR_NO_METRICS, selector);
        assertNotEquals(ClientTelemetryUtils.SELECTOR_ALL_METRICS, selector);
        assertTrue(selector.test(new MetricKey("metric1.test")));
        assertTrue(selector.test(new MetricKey("metric2.test")));
        assertFalse(selector.test(new MetricKey("test.metric1")));
        assertFalse(selector.test(new MetricKey("test.metric2")));
    }

    @Test
    public void testGetCompressionTypesFromAcceptedList() {
        assertEquals(0, ClientTelemetryUtils.getCompressionTypesFromAcceptedList(null).size());
        assertEquals(0, ClientTelemetryUtils.getCompressionTypesFromAcceptedList(Collections.emptyList()).size());

        List<Byte> compressionTypes = new ArrayList<>();
        compressionTypes.add(CompressionType.GZIP.id);
        compressionTypes.add(CompressionType.LZ4.id);
        compressionTypes.add(CompressionType.SNAPPY.id);
        compressionTypes.add(CompressionType.ZSTD.id);
        compressionTypes.add(CompressionType.NONE.id);
        compressionTypes.add((byte) -1);

        // should take the first compression type
        assertEquals(5, ClientTelemetryUtils.getCompressionTypesFromAcceptedList(compressionTypes).size());
    }

    @Test
    public void testValidateClientInstanceId() {
        assertThrows(IllegalArgumentException.class, () -> ClientTelemetryUtils.validateClientInstanceId(null));
        assertThrows(IllegalArgumentException.class, () -> ClientTelemetryUtils.validateClientInstanceId(Uuid.ZERO_UUID));

        Uuid uuid = Uuid.randomUuid();
        assertEquals(uuid, ClientTelemetryUtils.validateClientInstanceId(uuid));
    }

    @ParameterizedTest
    @ValueSource(ints = {300_000, Integer.MAX_VALUE - 1, Integer.MAX_VALUE})
    public void testValidateIntervalMsValid(int pushIntervalMs) {
        assertEquals(pushIntervalMs, ClientTelemetryUtils.validateIntervalMs(pushIntervalMs));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    public void testValidateIntervalMsInvalid(int pushIntervalMs) {
        assertEquals(ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS, ClientTelemetryUtils.validateIntervalMs(pushIntervalMs));
    }

    @Test
    public void testPreferredCompressionType() {
        assertEquals(CompressionType.NONE, ClientTelemetryUtils.preferredCompressionType(Collections.emptyList()));
        assertEquals(CompressionType.NONE, ClientTelemetryUtils.preferredCompressionType(null));
        assertEquals(CompressionType.NONE, ClientTelemetryUtils.preferredCompressionType(Arrays.asList(CompressionType.NONE, CompressionType.GZIP)));
        assertEquals(CompressionType.GZIP, ClientTelemetryUtils.preferredCompressionType(Arrays.asList(CompressionType.GZIP, CompressionType.NONE)));
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testCompressDecompress(CompressionType compressionType) throws IOException {
        byte[] testString = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = ClientTelemetryUtils.compress(testString, compressionType);
        assertNotNull(compressed);
        if (compressionType != CompressionType.NONE) {
            assertTrue(compressed.length < testString.length);
        } else {
            assertArrayEquals(testString, compressed);
        }

        ByteBuffer decompressed = ClientTelemetryUtils.decompress(compressed, compressionType);
        assertNotNull(decompressed);
        assertArrayEquals(testString, decompressed.array());
    }
}