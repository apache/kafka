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

import org.apache.kafka.common.DirectoryId;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class BrokerRegistrationTest {
    private static final List<BrokerRegistration> REGISTRATIONS = Arrays.asList(
        new BrokerRegistration.Builder().
            setId(0).
            setEpoch(0).
            setIncarnationId(Uuid.fromString("pc1GhUlBS92cGGaKXl6ipw")).
            setListeners(Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9090))).
            setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 2))).
            setRack(Optional.empty()).
            setFenced(false).
            setInControlledShutdown(false).build(),
        new BrokerRegistration.Builder().
            setId(1).
            setEpoch(0).
            setIncarnationId(Uuid.fromString("3MfdxWlNSn2UDYsmDP1pYg")).
            setListeners(Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9091))).
            setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 2))).
            setRack(Optional.empty()).
            setFenced(true).
            setInControlledShutdown(false).build(),
        new BrokerRegistration.Builder().
            setId(2).
            setEpoch(0).
            setIncarnationId(Uuid.fromString("eY7oaG1RREie5Kk9uy1l6g")).
            setListeners(Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9092))).
            setSupportedFeatures(Stream.of(new SimpleEntry<>("foo", VersionRange.of((short) 2, (short) 3)),
                new SimpleEntry<>("bar", VersionRange.of((short) 1, (short) 4))).collect(
                        Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue))).
            setRack(Optional.of("myrack")).
            setFenced(false).
            setInControlledShutdown(true).build(),
        new BrokerRegistration.Builder().
            setId(3).
            setEpoch(0).
            setIncarnationId(Uuid.fromString("1t8VyWx2TCSTpUWuqj-FOw")).
            setListeners(Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
            setSupportedFeatures(Stream.of(new SimpleEntry<>("metadata.version", VersionRange.of((short) 7, (short) 7)))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue))).
            setRack(Optional.empty()).
            setFenced(false).
            setInControlledShutdown(true).
            setIsMigratingZkBroker(true).
            setDirectories(Arrays.asList(Uuid.fromString("r4HpEsMuST6nQ4rznIEJVA"))).
            build());

    @Test
    public void testValues() {
        assertEquals(0, REGISTRATIONS.get(0).id());
        assertEquals(1, REGISTRATIONS.get(1).id());
        assertEquals(2, REGISTRATIONS.get(2).id());
        assertEquals(3, REGISTRATIONS.get(3).id());
    }

    @Test
    public void testEquals() {
        assertNotEquals(REGISTRATIONS.get(0), REGISTRATIONS.get(1));
        assertNotEquals(REGISTRATIONS.get(1), REGISTRATIONS.get(0));
        assertNotEquals(REGISTRATIONS.get(0), REGISTRATIONS.get(2));
        assertNotEquals(REGISTRATIONS.get(2), REGISTRATIONS.get(0));
        assertNotEquals(REGISTRATIONS.get(3), REGISTRATIONS.get(0));
        assertNotEquals(REGISTRATIONS.get(3), REGISTRATIONS.get(1));
        assertNotEquals(REGISTRATIONS.get(3), REGISTRATIONS.get(2));
        assertEquals(REGISTRATIONS.get(0), REGISTRATIONS.get(0));
        assertEquals(REGISTRATIONS.get(1), REGISTRATIONS.get(1));
        assertEquals(REGISTRATIONS.get(2), REGISTRATIONS.get(2));
        assertEquals(REGISTRATIONS.get(3), REGISTRATIONS.get(3));
    }

    @Test
    public void testToString() {
        assertEquals("BrokerRegistration(id=1, epoch=0, " +
            "incarnationId=3MfdxWlNSn2UDYsmDP1pYg, listeners=[Endpoint(" +
            "listenerName='INTERNAL', securityProtocol=PLAINTEXT, " +
            "host='localhost', port=9091)], supportedFeatures={foo: 1-2}, " +
            "rack=Optional.empty, fenced=true, inControlledShutdown=false, isMigratingZkBroker=false, directories=[])",
            REGISTRATIONS.get(1).toString());
        assertEquals("BrokerRegistration(id=2, epoch=0, " +
            "incarnationId=eY7oaG1RREie5Kk9uy1l6g, listeners=[Endpoint(" +
            "listenerName='INTERNAL', securityProtocol=PLAINTEXT, " +
            "host='localhost', port=9092)], supportedFeatures={bar: 1-4, foo: 2-3}, " +
            "rack=Optional[myrack], fenced=false, inControlledShutdown=true, isMigratingZkBroker=false, directories=[])",
            REGISTRATIONS.get(2).toString());
        assertEquals("BrokerRegistration(id=3, epoch=0, " +
            "incarnationId=1t8VyWx2TCSTpUWuqj-FOw, listeners=[Endpoint(" +
            "listenerName='INTERNAL', securityProtocol=PLAINTEXT, " +
            "host='localhost', port=9093)], supportedFeatures={metadata.version: 7}, " +
            "rack=Optional.empty, fenced=false, inControlledShutdown=true, isMigratingZkBroker=true, " +
            "directories=[r4HpEsMuST6nQ4rznIEJVA])",
            REGISTRATIONS.get(3).toString());
    }

    @Test
    public void testFromRecordAndToRecord() {
        testRoundTrip(REGISTRATIONS.get(0));
        testRoundTrip(REGISTRATIONS.get(1));
        testRoundTrip(REGISTRATIONS.get(2));
        testRoundTrip(REGISTRATIONS.get(3));
    }

    private void testRoundTrip(BrokerRegistration registration) {
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        ApiMessageAndVersion messageAndVersion = registration.
            toRecord(options);
        BrokerRegistration registration2 = BrokerRegistration.fromRecord(
            (RegisterBrokerRecord) messageAndVersion.message());
        assertEquals(registration, registration2);
        ApiMessageAndVersion messageAndVersion2 = registration2.
            toRecord(options);
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
        assertEquals(Optional.of(new Node(3, "localhost", 9093, null)),
            REGISTRATIONS.get(3).node("INTERNAL"));
    }

    @Test
    public void testDirectoriesAreSorted() {
        BrokerRegistration registration = new BrokerRegistration.Builder().
                setId(0).
                setEpoch(0).
                setIncarnationId(Uuid.fromString("ik32HZbLTW6ulw1yyrC8jQ")).
                setListeners(Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9090))).
                setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 2))).
                setRack(Optional.empty()).
                setFenced(false).
                setInControlledShutdown(false).
                setDirectories(Arrays.asList(
                    Uuid.fromString("3MWIBL9NR4eXhtdfBVA7Bw"),
                    Uuid.fromString("SZQIVeLMQGiNi68StNSNZA"),
                    Uuid.fromString("LWZsWPBrQruOMMrnEBj7bw"),
                    Uuid.fromString("OpIJIaO6RKaOGvHlNmOEhA"),
                    Uuid.fromString("JhYia5HRTLihf2FFJVxopQ"),
                    Uuid.fromString("VNetSHnySxSbvjwKrBzpkw"))
                ).
                build();
        assertEquals(Arrays.asList(
                Uuid.fromString("3MWIBL9NR4eXhtdfBVA7Bw"),
                Uuid.fromString("JhYia5HRTLihf2FFJVxopQ"),
                Uuid.fromString("LWZsWPBrQruOMMrnEBj7bw"),
                Uuid.fromString("OpIJIaO6RKaOGvHlNmOEhA"),
                Uuid.fromString("SZQIVeLMQGiNi68StNSNZA"),
                Uuid.fromString("VNetSHnySxSbvjwKrBzpkw")
        ), registration.directories());
    }

    @Test
    void testHasOnlineDir() {
        BrokerRegistration registration = new BrokerRegistration.Builder().
                setId(0).
                setEpoch(0).
                setIncarnationId(Uuid.fromString("m6CiJvfITZeKVC6UuhlZew")).
                setListeners(Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9090))).
                setSupportedFeatures(Collections.singletonMap("foo", VersionRange.of((short) 1, (short) 2))).
                setRack(Optional.empty()).
                setFenced(false).
                setInControlledShutdown(false).
                setDirectories(Arrays.asList(
                    Uuid.fromString("dir1G6EtuR1OTdAzFw1AFQ"),
                    Uuid.fromString("dir2gwpjTvKC7sMfcLNd8g"),
                    Uuid.fromString("dir3Ir8mQ0mMxfv93RITDA")
                )).
                build();
        assertTrue(registration.hasOnlineDir(Uuid.fromString("dir1G6EtuR1OTdAzFw1AFQ")));
        assertTrue(registration.hasOnlineDir(Uuid.fromString("dir2gwpjTvKC7sMfcLNd8g")));
        assertTrue(registration.hasOnlineDir(Uuid.fromString("dir3Ir8mQ0mMxfv93RITDA")));
        assertTrue(registration.hasOnlineDir(DirectoryId.UNASSIGNED));
        assertTrue(registration.hasOnlineDir(DirectoryId.MIGRATING));
        assertFalse(registration.hasOnlineDir(Uuid.fromString("sOwN7HH7S1maxpU1WzlzXg")));
        assertFalse(registration.hasOnlineDir(DirectoryId.LOST));
    }
}
