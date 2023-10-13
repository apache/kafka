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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


@Timeout(value = 40)
public class ControllerRegistrationTest {
    static <K, V> Map<K, V> doubleMap(K k1, V v1, K k2, V v2) {
        HashMap<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return Collections.unmodifiableMap(map);
    }

    private static final List<ControllerRegistration> REGISTRATIONS = Arrays.asList(
        new ControllerRegistration.Builder().
            setId(0).
            setIncarnationId(Uuid.fromString("ycRmGrOFQru7HXf6fOybZQ")).
            setZkMigrationReady(true).
            setListeners(doubleMap(
                "PLAINTEXT", new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9107),
                "SSL", new Endpoint("SSL", SecurityProtocol.SSL, "localhost", 9207))).
            setSupportedFeatures(Collections.singletonMap(MetadataVersion.FEATURE_NAME, VersionRange.of(1, 10))).
            build(),
        new ControllerRegistration.Builder().
            setId(1).
            setIncarnationId(Uuid.fromString("ubT_wuD6R3uopZ_lV76dQg")).
            setZkMigrationReady(true).
            setListeners(doubleMap(
                "PLAINTEXT", new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9108),
                "SSL", new Endpoint("SSL", SecurityProtocol.SSL, "localhost", 9208))).
            setSupportedFeatures(Collections.singletonMap(MetadataVersion.FEATURE_NAME, VersionRange.of(1, 10))).
            build(),
        new ControllerRegistration.Builder().
            setId(2).
            setIncarnationId(Uuid.fromString("muQS341gRIeNh9Ps7reDSw")).
            setZkMigrationReady(false).
            setListeners(doubleMap(
                "PLAINTEXT", new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9109),
                "SSL", new Endpoint("SSL", SecurityProtocol.SSL, "localhost", 9209))).
            setSupportedFeatures(Collections.singletonMap(MetadataVersion.FEATURE_NAME, VersionRange.of(1, 10))).
            build()
    );

    @Test
    public void testValues() {
        assertEquals(0, REGISTRATIONS.get(0).id());
        assertEquals(1, REGISTRATIONS.get(1).id());
        assertEquals(2, REGISTRATIONS.get(2).id());
    }

    @Test
    public void testEquals() {
        assertNotEquals(REGISTRATIONS.get(0), REGISTRATIONS.get(1));
        assertNotEquals(REGISTRATIONS.get(1), REGISTRATIONS.get(0));
        assertNotEquals(REGISTRATIONS.get(0), REGISTRATIONS.get(2));
        assertNotEquals(REGISTRATIONS.get(2), REGISTRATIONS.get(0));
        assertEquals(REGISTRATIONS.get(0), REGISTRATIONS.get(0));
        assertEquals(REGISTRATIONS.get(1), REGISTRATIONS.get(1));
        assertEquals(REGISTRATIONS.get(2), REGISTRATIONS.get(2));
    }

    @Test
    public void testToString() {
        assertEquals("ControllerRegistration(id=1, " +
            "incarnationId=ubT_wuD6R3uopZ_lV76dQg, " +
            "zkMigrationReady=true, " +
            "listeners=[" +
            "Endpoint(listenerName='PLAINTEXT', securityProtocol=PLAINTEXT, host='localhost', port=9108), " +
            "Endpoint(listenerName='SSL', securityProtocol=SSL, host='localhost', port=9208)]" +
            ", supportedFeatures={metadata.version: 1-10})",
            REGISTRATIONS.get(1).toString());
    }

    @Test
    public void testFromRecordAndToRecord() {
        testRoundTrip(REGISTRATIONS.get(0));
        testRoundTrip(REGISTRATIONS.get(1));
        testRoundTrip(REGISTRATIONS.get(2));
    }

    private void testRoundTrip(ControllerRegistration registration) {
        ApiMessageAndVersion messageAndVersion = registration.
            toRecord(new ImageWriterOptions.Builder().build());
        ControllerRegistration registration2 = new ControllerRegistration.Builder(
            (RegisterControllerRecord) messageAndVersion.message()).build();
        assertEquals(registration, registration2);
        ApiMessageAndVersion messageAndVersion2 = registration2.
            toRecord(new ImageWriterOptions.Builder().build());
        assertEquals(messageAndVersion, messageAndVersion2);
    }

    @Test
    public void testToNode() {
        assertEquals(Optional.empty(), REGISTRATIONS.get(0).node("NONEXISTENT"));
        assertEquals(Optional.of(new Node(0, "localhost", 9107, null)),
            REGISTRATIONS.get(0).node("PLAINTEXT"));
        assertEquals(Optional.of(new Node(0, "localhost", 9207, null)),
            REGISTRATIONS.get(0).node("SSL"));
    }
}
