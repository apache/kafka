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
package org.apache.kafka.clients.admin;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RaftVoterEndpointTest {

    @Test
    public void testListenerNullConstructor() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new RaftVoterEndpoint(null, "example.com", 8080));
        assertEquals("Null argument not allowed.", ex.getMessage());
    }

    @Test
    public void testListenerWhitespaceConstructor() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new RaftVoterEndpoint(" CONTROLLER", "example.com", 8080));
        assertEquals("Leading or trailing whitespace is not allowed.", ex.getMessage());
    }

    @Test
    public void testListenerEmptyConstructor() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new RaftVoterEndpoint("", "example.com", 8080));
        assertEquals("Empty string is not allowed.", ex.getMessage());
    }

    @Test
    public void testListenerNotUpperCaseConstructor() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new RaftVoterEndpoint("controller", "example.com", 8080));
        assertEquals("String must be UPPERCASE.", ex.getMessage());
    }

    @Test
    public void testHostNullConstructor() {
        assertThrows(NullPointerException.class,
                () -> new RaftVoterEndpoint("CONTROLLER", null, 8080));
    }

    @Test
    public void testAccessors() {
        var endpoint = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        assertEquals("CONTROLLER", endpoint.listener());
        assertEquals("example.com", endpoint.host());
        assertEquals(8080, endpoint.port());
    }

    @Test
    public void testEquals() {
        var a = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        var b = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        assertEquals(a, b);
    }

    @Test
    public void testHashCode() {
        var a = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        var b = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testNotEqualsWithDifferentListener() {
        var listenerA = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        var listenerB = new RaftVoterEndpoint("BROKER", "example.com", 8080);
        assertNotEquals(listenerA, listenerB);
    }

    @Test
    public void testNotEqualsWithDifferentHost() {
        var hostA = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        var hostB = new RaftVoterEndpoint("CONTROLLER", "other.com", 8080);
        assertNotEquals(hostA, hostB);
    }

    @Test
    public void testNotEqualsWithDifferentPort() {
        var portA = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        var portB = new RaftVoterEndpoint("CONTROLLER", "example.com", 9092);
        assertNotEquals(portA, portB);
    }

    @Test
    public void testNotEqualsWithNull() {
        var endpoint = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        assertNotEquals(null, endpoint);
    }

    @Test
    public void testNotEqualsWithDifferentClass() {
        var endpoint = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        var string = "CONTROLLER://example.com:8080";
        assertNotEquals(string, endpoint);
    }

    @Test
    public void testToString() {
        var endpoint = new RaftVoterEndpoint("CONTROLLER", "example.com", 8080);
        assertEquals("CONTROLLER://example.com:8080", endpoint.toString());
    }

    @Test
    public void testToStringWithIpv6Host() {
        var endpoint = new RaftVoterEndpoint("CONTROLLER", "::1", 8080);
        assertEquals("CONTROLLER://[::1]:8080", endpoint.toString());
    }
}