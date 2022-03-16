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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ClientTelemetryUtilsTest extends BaseClientTelemetryTest {

    @Test
    public void testValidateMetricNames() {
        // empty metric names
        assertEquals(ClientTelemetryUtils.SELECTOR_NO_METRICS, ClientTelemetryUtils.validateMetricNames(Collections.emptyList()));
        assertEquals(ClientTelemetryUtils.SELECTOR_NO_METRICS, ClientTelemetryUtils.validateMetricNames(null));
    }

    @Test
    public void testValidateAcceptedCompressionTypes() {
        // invalid compression types
        assertEquals(0, ClientTelemetryUtils.validateAcceptedCompressionTypes(null).size());
        assertEquals(0, ClientTelemetryUtils.validateAcceptedCompressionTypes(Collections.emptyList()).size());

        List<Byte> compressionTypes = new ArrayList<>();
        compressionTypes.add((byte) CompressionType.GZIP.id);
        compressionTypes.add((byte) CompressionType.LZ4.id);
        compressionTypes.add((byte) CompressionType.SNAPPY.id);
        compressionTypes.add((byte) CompressionType.ZSTD.id);
        compressionTypes.add((byte) CompressionType.NONE.id);
        compressionTypes.add((byte) -1);

        // should take the first compression type
        assertEquals(5, ClientTelemetryUtils.validateAcceptedCompressionTypes(compressionTypes).size());
    }

    @Test
    public void testValidateClientInstanceId() {
        assertThrows(IllegalArgumentException.class, () -> ClientTelemetryUtils.validateClientInstanceId(null));
        Uuid uuid = Uuid.randomUuid();
        assertEquals(uuid, ClientTelemetryUtils.validateClientInstanceId(uuid));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    public void testValidatePushIntervalInvalid(int pushIntervalMs) {
        assertEquals(ClientTelemetry.DEFAULT_PUSH_INTERVAL_MS, ClientTelemetryUtils.validatePushIntervalMs(pushIntervalMs));
    }

    @ParameterizedTest
    @ValueSource(ints = {300_000, Integer.MAX_VALUE - 1, Integer.MAX_VALUE})
    public void testValidatePushIntervalValid(int pushIntervalMs) {
        assertEquals(pushIntervalMs, ClientTelemetryUtils.validatePushIntervalMs(pushIntervalMs));
    }

    @Test
    public void testPreferredCompressionType() {
        assertEquals(CompressionType.NONE, ClientTelemetryUtils.preferredCompressionType(Collections.emptyList()));
        assertEquals(CompressionType.NONE, ClientTelemetryUtils.preferredCompressionType(null));
        assertEquals(CompressionType.GZIP, ClientTelemetryUtils.preferredCompressionType(Arrays.asList(CompressionType.GZIP, CompressionType.LZ4, CompressionType.ZSTD)));
        assertEquals(CompressionType.LZ4, ClientTelemetryUtils.preferredCompressionType(Collections.singletonList(CompressionType.LZ4)));
    }

    @Test
    public void testMaybeCreateFailsIfClientIdIsNull() {
        assertThrows(NullPointerException.class, () -> ClientTelemetryUtils.create(true, newLogContext(), MOCK_TIME, null));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMaybeCreateFailsIfClientIdIsNull(boolean enableMetricsPush) {
        testMaybeCreateFailsIfParametersAreNull(enableMetricsPush, MOCK_TIME, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMaybeCreateFailsIfParametersAreNull(boolean enableMetricsPush) {
        testMaybeCreateFailsIfParametersAreNull(enableMetricsPush, null, CLIENT_ID);
    }

    private void testMaybeCreateFailsIfParametersAreNull(boolean enableMetricsPush, Time time, String clientId) {
        // maybeCreate won't (or at least it shouldn't) fail if these are both non-null
        if (time != null && clientId != null)
            return;

        // maybeCreate won't fail if we don't attempt to construct metrics in the first place
        if (!enableMetricsPush)
            return;

        Class<NullPointerException> e = NullPointerException.class;

        assertThrows(e,
            () -> ClientTelemetryUtils.create(true, newLogContext(), time, clientId),
            String.format("maybeCreate should have thrown a %s for time: %s and clientId: %s", e.getName(), time, clientId));
    }

}
