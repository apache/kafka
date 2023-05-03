package org.apache.kafka.coordinator.group.generic;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GenericGroupMemberTest {

    @Test
    public void testEquals() {
        GenericGroupMember member1 = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            Collections.singletonList(
                new Protocol("range", new byte[0])
            ),
            new byte[0]
        );

        GenericGroupMember member2 = new GenericGroupMember(
            "member",
            Optional.of("group-instance-id"),
            "client-id",
            "client-host",
            10,
            4500,
            "generic",
            Collections.singletonList(
                new Protocol("range", new byte[0])
            ),
            new byte[0]
        );;

        assertEquals(member1, member2);
    }

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
            new byte[0]
        );

        assertTrue(member.matches(Collections.singletonList(
            new Protocol("range", new byte[] {0})
        )));
        assertFalse(member.matches(Collections.singletonList(
            new Protocol("range", new byte[] {1})
        )));
        assertFalse(member.matches(Collections.singletonList(
            new Protocol("roundrobin", new byte[0])
        )));

        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", new byte[0]));
        protocols.add(new Protocol("roundrobin", new byte[0]));
        assertFalse(member.matches(protocols));
    }

    @Test
    public void testVoteForPreferredProtocol() {
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(new Protocol("range", new byte[0]));
        protocols.add(new Protocol("roundrobin", new byte[0]));

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
            new byte[0]
        );;

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
            new byte[0]
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
            new byte[0]
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
            new byte[0]
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
                new Protocol("range", new byte[0])
            ),
            new byte[0]
        );

        assertFalse(member.hasSatisfiedHeartbeat());

        member.setHeartBeatSatisfied(true);
        assertTrue(member.hasSatisfiedHeartbeat());

        member.setIsNew(true);
        member.setHeartBeatSatisfied(false);
        assertFalse(member.hasSatisfiedHeartbeat());

        member.setIsNew(false);
        member.setAwaitingJoinCallback(new CompletableFuture<>());
        assertTrue(member.hasSatisfiedHeartbeat());

        member.setAwaitingJoinCallback(null);
        member.setAwaitingSyncCallback(new CompletableFuture<>());
        assertTrue(member.hasSatisfiedHeartbeat());
    }
}
