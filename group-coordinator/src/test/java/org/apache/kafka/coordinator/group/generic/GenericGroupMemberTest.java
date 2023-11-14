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

import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.coordinator.group.generic.GenericGroupMember.EMPTY_ASSIGNMENT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GenericGroupMemberTest {

    @Test
    public void testMatchesSupportedProtocols() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[]{0}));

        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            protocols,
            new byte[0]
        );

        JoinGroupRequestProtocolCollection collection = new JoinGroupRequestProtocolCollection();
        collection.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[] {0}));

        assertTrue(member.matches(collection));

        collection = new JoinGroupRequestProtocolCollection();
        collection.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[] {1}));

        assertFalse(member.matches(collection));

        collection = new JoinGroupRequestProtocolCollection();
        collection.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        assertFalse(member.matches(collection));

        collection = new JoinGroupRequestProtocolCollection();
        collection.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        collection.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        assertFalse(member.matches(collection));
        assertTrue(member.matches(protocols));
    }

    @Test
    public void testVoteForPreferredProtocol() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

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
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[]{0}));
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[]{1}));

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
        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            new JoinGroupRequestProtocolCollection(),
            EMPTY_ASSIGNMENT
        );

        assertThrows(IllegalArgumentException.class, () ->
            member.metadata("unknown")
        );
    }

    @Test
    public void testVoteRaisesOnNoSupportedProtocols() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[]{0}));
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[]{1}));

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
        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            new JoinGroupRequestProtocolCollection(),
            EMPTY_ASSIGNMENT
        );

        assertTrue(member.isStaticMember());
        assertEquals(Optional.of("group-instance-id"), member.groupInstanceId());
    }
    
    @Test
    public void testPlainProtocolSet() {
        JoinGroupRequestProtocolCollection protocolCollection =
            new JoinGroupRequestProtocolCollection();

        protocolCollection.add(new JoinGroupRequestProtocol()
            .setName("range").setMetadata(new byte[]{0}));
        protocolCollection.add(new JoinGroupRequestProtocol()
            .setName("roundrobin").setMetadata(new byte[]{1}));

        Set<String> expectedProtocolNames = new HashSet<>();
        expectedProtocolNames.add("range");
        expectedProtocolNames.add("roundrobin");

        assertEquals(expectedProtocolNames, GenericGroupMember.plainProtocolSet(protocolCollection));
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
            new JoinGroupRequestProtocolCollection(),
            EMPTY_ASSIGNMENT
        );

        assertFalse(member.hasSatisfiedHeartbeat());

        member.setIsNew(true);
        assertFalse(member.hasSatisfiedHeartbeat());

        member.setIsNew(false);
        member.setAwaitingJoinFuture(new CompletableFuture<>());
        assertTrue(member.hasSatisfiedHeartbeat());

        member.setAwaitingJoinFuture(null);
        member.setAwaitingSyncFuture(new CompletableFuture<>());
        assertTrue(member.hasSatisfiedHeartbeat());
    }

    @Test
    public void testDescribeNoMetadata() {
        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            new JoinGroupRequestProtocolCollection(),
            new byte[0]
        );

        DescribeGroupsResponseData.DescribedGroupMember expectedDescribedGroupMember =
            new DescribeGroupsResponseData.DescribedGroupMember()
                .setMemberId("member")
                .setGroupInstanceId("group-instance-id")
                .setClientId("client-id")
                .setClientHost("client-host")
                .setMemberAssignment(new byte[0]);

        DescribeGroupsResponseData.DescribedGroupMember describedGroupMember = member.describeNoMetadata();

        assertEquals(expectedDescribedGroupMember, describedGroupMember);
    }

    @Test
    public void testDescribe() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection(Collections.singletonList(
            new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(new byte[]{0})
        ).iterator());

        GenericGroupMember member = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            protocols,
            new byte[0]
        );

        DescribeGroupsResponseData.DescribedGroupMember expectedDescribedGroupMember =
            new DescribeGroupsResponseData.DescribedGroupMember()
                .setMemberId("member")
                .setGroupInstanceId("group-instance-id")
                .setClientId("client-id")
                .setClientHost("client-host")
                .setMemberAssignment(new byte[0])
                .setMemberMetadata(member.metadata("range"));

        DescribeGroupsResponseData.DescribedGroupMember describedGroupMember = member.describe("range");

        assertEquals(expectedDescribedGroupMember, describedGroupMember);
    }
}
