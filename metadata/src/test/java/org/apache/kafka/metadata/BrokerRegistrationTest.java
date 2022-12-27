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
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Timeout(value = 40)
public class BrokerRegistrationTest {
    private static final List<BrokerRegistration> REGISTRATIONS = Arrays.asList(
        new BrokerRegistration(0, 0, Uuid.fromString("pc1GhUlBS92cGGaKXl6ipw"),
            Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9090)),
            Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 2)),
            Optional.empty(), false, false),
        new BrokerRegistration(1, 0, Uuid.fromString("3MfdxWlNSn2UDYsmDP1pYg"),
            Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9091)),
            Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 2)),
            Optional.empty(), true, false),
        new BrokerRegistration(2, 0, Uuid.fromString("eY7oaG1RREie5Kk9uy1l6g"),
            Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9092)),
            Stream.of(new SimpleEntry<>("foo", VersionRange.of((short) 2, (short) 3)),
                new SimpleEntry<>("bar", VersionRange.of((short) 1, (short) 4))).collect(
                        Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)),
            Optional.of("myrack"), false, true));

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
        assertEquals("BrokerRegistration(id=1, epoch=0, " +
            "incarnationId=3MfdxWlNSn2UDYsmDP1pYg, listeners=[Endpoint(" +
            "listenerName='INTERNAL', securityProtocol=PLAINTEXT, " +
            "host='localhost', port=9091)], supportedFeatures={foo: 1-2}, " +
            "rack=Optional.empty, fenced=true, inControlledShutdown=false, isMigratingZkBroker=false)",
            REGISTRATIONS.get(1).toString());
        assertEquals("BrokerRegistration(id=2, epoch=0, " +
            "incarnationId=eY7oaG1RREie5Kk9uy1l6g, listeners=[Endpoint(" +
            "listenerName='INTERNAL', securityProtocol=PLAINTEXT, " +
            "host='localhost', port=9092)], supportedFeatures={bar: 1-4, foo: 2-3}, " +
            "rack=Optional[myrack], fenced=false, inControlledShutdown=true, isMigratingZkBroker=false)",
            REGISTRATIONS.get(2).toString());
    }

    @Test
    public void testFromRecordAndToRecord() {
        testRoundTrip(REGISTRATIONS.get(0));
        testRoundTrip(REGISTRATIONS.get(1));
        testRoundTrip(REGISTRATIONS.get(2));
    }

    private void testRoundTrip(BrokerRegistration registration) {
        ApiMessageAndVersion messageAndVersion = registration.
            toRecord(new ImageWriterOptions.Builder().build());
        BrokerRegistration registration2 = BrokerRegistration.fromRecord(
            (RegisterBrokerRecord) messageAndVersion.message());
        assertEquals(registration, registration2);
        ApiMessageAndVersion messageAndVersion2 = registration2.
            toRecord(new ImageWriterOptions.Builder().build());
        assertEquals(messageAndVersion, messageAndVersion2);
    }

    @Test
    public void testToNode() {
        assertEquals(Optional.empty(), REGISTRATIONS.get(0).node("NONEXISTENT"));
        assertEquals(Optional.of(new Node(0, "localhost", 9090, null)),
            REGISTRATIONS.get(0).node("INTERNAL"));
        assertEquals(Optional.of(new Node(1, "localhost", 9091, null)),
            REGISTRATIONS.get(1).node("INTERNAL"));
        assertEquals(Optional.of(new Node(2, "localhost", 9092, "myrack")),
            REGISTRATIONS.get(2).node("INTERNAL"));
    }
}
