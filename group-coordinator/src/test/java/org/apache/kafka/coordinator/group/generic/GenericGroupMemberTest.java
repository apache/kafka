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

package org.apache.kafka.coordinator.group.generic;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.coordinator.group.generic.GenericGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.generic.Protocol.EMPTY_METADATA;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GenericGroupMemberTest {

    @Test
    public void testMatchesSupportedProtocols() {
        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            Collections.singletonList(
                new Protocol("range", new byte[] {0})
            ),
            EMPTY_METADATA
        );

        assertTrue(member.matches(Collections.singletonList(
            new Protocol("range", new byte[] {0})
        )));
        assertFalse(member.matches(Collections.singletonList(
            new Protocol("range", new byte[] {1})
        )));
        assertFalse(member.matches(Collections.singletonList(
            new Protocol("roundrobin", EMPTY_METADATA)
        )));

        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", EMPTY_METADATA));
        protocols.add(new Protocol("roundrobin", EMPTY_METADATA));
        assertFalse(member.matches(protocols));
    }

    @Test
    public void testVoteForPreferredProtocol() {
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", EMPTY_METADATA));
        protocols.add(new Protocol("roundrobin", EMPTY_METADATA));

        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            protocols,
            EMPTY_ASSIGNMENT
        );

        Set<String> expectedProtocolNames = new HashSet<>();
        expectedProtocolNames.add("range");
        expectedProtocolNames.add("roundrobin");

        assertEquals("range", member.vote(expectedProtocolNames));

        expectedProtocolNames.clear();
        expectedProtocolNames.add("unknown");
        expectedProtocolNames.add("roundrobin");
        assertEquals("roundrobin", member.vote(expectedProtocolNames));
    }

    @Test
    public void testMetadata() {
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", new byte[]{0}));
        protocols.add(new Protocol("roundrobin", new byte[]{1}));

        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            protocols,
            EMPTY_ASSIGNMENT
        );

        assertArrayEquals(new byte[]{0}, member.metadata("range"));
        assertArrayEquals(new byte[]{1}, member.metadata("roundrobin"));
    }

    @Test
    public void testMetadataRaisesOnUnsupportedProtocol() {
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", new byte[]{0}));
        protocols.add(new Protocol("roundrobin", new byte[]{1}));

        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            protocols,
            EMPTY_ASSIGNMENT
        );

        assertThrows(IllegalArgumentException.class, () ->
            member.metadata("unknown")
        );
    }

    @Test
    public void testVoteRaisesOnNoSupportedProtocols() {
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", new byte[]{0}));
        protocols.add(new Protocol("roundrobin", new byte[]{1}));

        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            protocols,
            EMPTY_ASSIGNMENT
        );

        assertThrows(IllegalArgumentException.class, () ->
            member.vote(Collections.singleton("unknown"))
        );
    }

    @Test
    public void testHasValidGroupInstanceId() {
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", new byte[]{0}));
        protocols.add(new Protocol("roundrobin", new byte[]{1}));

        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            protocols,
            EMPTY_ASSIGNMENT
        );

        assertTrue(member.isStaticMember());
        assertEquals(Optional.of("group-instance-id"), member.groupInstanceId());
    }
    
    @Test
    public void testPlainProtocolSet() {
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", new byte[]{0}));
        protocols.add(new Protocol("roundrobin", new byte[]{1}));

        Set<String> expectedProtocolNames = new HashSet<>();
        expectedProtocolNames.add("range");
        expectedProtocolNames.add("roundrobin");
        
        assertEquals(expectedProtocolNames, GenericGroupMember.plainProtocolSet(protocols));
    }

    @Test
    public void testHasHeartbeatSatisfied() {
        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            Collections.singletonList(
                new Protocol("range", EMPTY_METADATA)
            ),
            EMPTY_ASSIGNMENT
        );

        assertFalse(member.hasSatisfiedHeartbeat());

        member.setHeartBeatSatisfied(true);
        assertTrue(member.hasSatisfiedHeartbeat());

        member.setIsNew(true);
        member.setHeartBeatSatisfied(false);
        assertFalse(member.hasSatisfiedHeartbeat());

        member.setIsNew(false);
        member.setAwaitingJoinFuture(new CompletableFuture<>());
        assertTrue(member.hasSatisfiedHeartbeat());

        member.setAwaitingJoinFuture(null);
        member.setAwaitingSyncFuture(new CompletableFuture<>());
        assertTrue(member.hasSatisfiedHeartbeat());
    }
}
