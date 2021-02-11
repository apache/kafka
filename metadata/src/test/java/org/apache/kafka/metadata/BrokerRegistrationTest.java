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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class BrokerRegistrationTest {
    private static final List<BrokerRegistration> REGISTRATIONS = Arrays.asList(
        new BrokerRegistration(0, 0, Uuid.fromString("pc1GhUlBS92cGGaKXl6ipw"),
            Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9090)),
            Collections.singletonMap("foo", new VersionRange((short) 1, (short) 2)),
            Optional.empty(), false),
        new BrokerRegistration(1, 0, Uuid.fromString("3MfdxWlNSn2UDYsmDP1pYg"),
            Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9091)),
            Collections.singletonMap("foo", new VersionRange((short) 1, (short) 2)),
            Optional.empty(), false),
        new BrokerRegistration(2, 0, Uuid.fromString("eY7oaG1RREie5Kk9uy1l6g"),
            Arrays.asList(new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9092)),
            Collections.singletonMap("foo", new VersionRange((short) 2, (short) 3)),
            Optional.empty(), false));

    @Test
    public void testValues() {
        assertEquals(0, REGISTRATIONS.get(0).id());
        assertEquals(1, REGISTRATIONS.get(1).id());
        assertEquals(2, REGISTRATIONS.get(2).id());
    }

    @Test
    public void testEquals() {
        assertFalse(REGISTRATIONS.get(0).equals(REGISTRATIONS.get(1)));
        assertFalse(REGISTRATIONS.get(1).equals(REGISTRATIONS.get(0)));
        assertFalse(REGISTRATIONS.get(0).equals(REGISTRATIONS.get(2)));
        assertFalse(REGISTRATIONS.get(2).equals(REGISTRATIONS.get(0)));
        assertTrue(REGISTRATIONS.get(0).equals(REGISTRATIONS.get(0)));
        assertTrue(REGISTRATIONS.get(1).equals(REGISTRATIONS.get(1)));
        assertTrue(REGISTRATIONS.get(2).equals(REGISTRATIONS.get(2)));
    }

    @Test
    public void testToString() {
        assertEquals("BrokerRegistration(id=1, epoch=0, " +
            "incarnationId=3MfdxWlNSn2UDYsmDP1pYg, listeners=[Endpoint(" +
            "listenerName='INTERNAL', securityProtocol=PLAINTEXT, " +
            "host='localhost', port=9091)], supportedFeatures={foo: 1-2}, " +
            "rack=Optional.empty, fenced=false)",
            REGISTRATIONS.get(1).toString());
    }
}
