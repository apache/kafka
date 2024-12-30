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
package org.apache.kafka.raft;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.server.common.Feature;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class VoterSetTest {
    @Test
    void testEmptyVoterSet() {
        assertEquals(VoterSet.empty(), VoterSet.fromMap(Collections.emptyMap()));
    }

    @Test
    void testVoterNode() {
        VoterSet voterSet = VoterSet.fromMap(voterMap(IntStream.of(1, 2, 3), true));
        assertEquals(
            Optional.of(new Node(1, "localhost", 9991)),
            voterSet.voterNode(1, DEFAULT_LISTENER_NAME)
        );
        assertEquals(Optional.empty(), voterSet.voterNode(1, ListenerName.normalised("MISSING")));
        assertEquals(Optional.empty(), voterSet.voterNode(4, DEFAULT_LISTENER_NAME));
    }

    @Test
    void testVoterNodes() {
        VoterSet voterSet = VoterSet.fromMap(voterMap(IntStream.of(1, 2, 3), true));

        assertEquals(
            Set.of(new Node(1, "localhost", 9991), new Node(2, "localhost", 9992)),
            voterSet.voterNodes(IntStream.of(1, 2).boxed(), DEFAULT_LISTENER_NAME)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> voterSet.voterNodes(IntStream.of(1, 2).boxed(), ListenerName.normalised("MISSING"))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> voterSet.voterNodes(IntStream.of(1, 4).boxed(), DEFAULT_LISTENER_NAME)
        );
    }

    @Test
    void testVoterIds() {
        VoterSet voterSet = VoterSet.fromMap(voterMap(IntStream.of(1, 2, 3), true));
        assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), voterSet.voterIds());
    }

    @Test
    void testAddVoter() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1, 2, 3), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertEquals(Optional.empty(), voterSet.addVoter(voterNode(1, true)));

        VoterSet.VoterNode voter4 = voterNode(4, true);
        aVoterMap.put(voter4.voterKey().id(), voter4);
        assertEquals(Optional.of(VoterSet.fromMap(new HashMap<>(aVoterMap))), voterSet.addVoter(voter4));
    }

    @Test
    void testRemoveVoter() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1, 2, 3), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertEquals(Optional.empty(), voterSet.removeVoter(ReplicaKey.of(4, ReplicaKey.NO_DIRECTORY_ID)));
        assertEquals(Optional.empty(), voterSet.removeVoter(ReplicaKey.of(4, Uuid.randomUuid())));

        VoterSet.VoterNode voter3 = aVoterMap.remove(3);
        assertEquals(
            Optional.of(VoterSet.fromMap(new HashMap<>(aVoterMap))),
            voterSet.removeVoter(voter3.voterKey())
        );
    }

    @Test
    void testUpdateVoter() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1, 2, 3), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertEquals(Optional.empty(), voterSet.updateVoter(voterNode(4, true)));
        assertFalse(voterSet.voterNodeNeedsUpdate(voterNode(4, true)));
        assertEquals(Optional.empty(), voterSet.updateVoter(voterNode(3, true)));
        assertFalse(voterSet.voterNodeNeedsUpdate(voterNode(3, true)));

        VoterSet.VoterNode voter3 = aVoterMap.get(3);
        VoterSet.VoterNode newVoter3 = VoterSet.VoterNode.of(
            voter3.voterKey(),
            Endpoints.fromInetSocketAddresses(
                Collections.singletonMap(
                    ListenerName.normalised("ABC"),
                    InetSocketAddress.createUnresolved("abc", 1234)
                )
            ),
            new SupportedVersionRange((short) 1, (short) 1)
        );
        aVoterMap.put(3, newVoter3);

        assertTrue(voterSet.voterNodeNeedsUpdate(newVoter3));
        assertEquals(
            Optional.of(VoterSet.fromMap(new HashMap<>(aVoterMap))),
            voterSet.updateVoter(newVoter3)
        );
    }


    @Test
    void testCannotRemoveToEmptyVoterSet() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        ReplicaKey voter1 = aVoterMap.get(1).voterKey();
        assertTrue(voterSet.isVoter(voter1));
        assertEquals(Optional.empty(), voterSet.removeVoter(voter1));
    }

    @Test
    void testIsVoterWithDirectoryId() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1, 2, 3), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertTrue(voterSet.isVoter(aVoterMap.get(1).voterKey()));
        assertFalse(voterSet.isVoter(ReplicaKey.of(1, Uuid.randomUuid())));
        assertFalse(voterSet.isVoter(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertFalse(
            voterSet.isVoter(ReplicaKey.of(2, aVoterMap.get(1).voterKey().directoryId().get()))
        );
        assertFalse(
            voterSet.isVoter(ReplicaKey.of(4, aVoterMap.get(1).voterKey().directoryId().get()))
        );
        assertFalse(voterSet.isVoter(ReplicaKey.of(4, ReplicaKey.NO_DIRECTORY_ID)));
    }

    @Test
    void testIsVoterWithoutDirectoryId() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1, 2, 3), false);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertTrue(voterSet.isVoter(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertTrue(voterSet.isVoter(ReplicaKey.of(1, Uuid.randomUuid())));
        assertFalse(voterSet.isVoter(ReplicaKey.of(4, Uuid.randomUuid())));
        assertFalse(voterSet.isVoter(ReplicaKey.of(4, ReplicaKey.NO_DIRECTORY_ID)));
    }

    @Test
    void testVoterNodeIsVoterWithDirectoryId() {
        VoterSet.VoterNode voterNode = voterNode(1, true);

        assertTrue(voterNode.isVoter(voterNode.voterKey()));
        assertFalse(voterNode.isVoter(ReplicaKey.of(1, Uuid.randomUuid())));
        assertFalse(voterNode.isVoter(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertFalse(voterNode.isVoter(ReplicaKey.of(2, Uuid.randomUuid())));
        assertFalse(voterNode.isVoter(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID)));
        assertFalse(voterNode.isVoter(ReplicaKey.of(2, voterNode.voterKey().directoryId().get())));
    }

    @Test
    void testVoterNodeIsVoterWithoutDirectoryId() {
        VoterSet.VoterNode voterNode = voterNode(1, false);

        assertTrue(voterNode.isVoter(voterNode.voterKey()));
        assertTrue(voterNode.isVoter(ReplicaKey.of(1, Uuid.randomUuid())));
        assertTrue(voterNode.isVoter(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertTrue(voterNode.isVoter(ReplicaKey.of(1, Uuid.randomUuid())));
        assertFalse(voterNode.isVoter(ReplicaKey.of(2, Uuid.randomUuid())));
        assertFalse(voterNode.isVoter(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID)));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testEndpoints(boolean withDirectoryId) {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1, 2, 3), withDirectoryId);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertNotEquals(Endpoints.empty(), voterSet.listeners(1));
        assertNotEquals(Endpoints.empty(), voterSet.listeners(2));
        assertNotEquals(Endpoints.empty(), voterSet.listeners(3));
        assertEquals(Endpoints.empty(), voterSet.listeners(4));
    }

    @Test
    void testIsOnlyVoterInStandalone() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertTrue(voterSet.isOnlyVoter(aVoterMap.get(1).voterKey()));
        assertFalse(voterSet.isOnlyVoter(ReplicaKey.of(1, Uuid.randomUuid())));
        assertFalse(voterSet.isOnlyVoter(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertFalse(
            voterSet.isOnlyVoter(ReplicaKey.of(4, aVoterMap.get(1).voterKey().directoryId().get()))
        );
        assertFalse(voterSet.isOnlyVoter(ReplicaKey.of(4, ReplicaKey.NO_DIRECTORY_ID)));
    }

    @Test
    void testIsOnlyVoterInNotStandalone() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(IntStream.of(1, 2), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(aVoterMap));

        assertFalse(voterSet.isOnlyVoter(aVoterMap.get(1).voterKey()));
        assertFalse(voterSet.isOnlyVoter(ReplicaKey.of(1, Uuid.randomUuid())));
        assertFalse(voterSet.isOnlyVoter(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertFalse(
            voterSet.isOnlyVoter(ReplicaKey.of(2, aVoterMap.get(1).voterKey().directoryId().get()))
        );
        assertFalse(
            voterSet.isOnlyVoter(ReplicaKey.of(4, aVoterMap.get(1).voterKey().directoryId().get()))
        );
        assertFalse(voterSet.isOnlyVoter(ReplicaKey.of(4, ReplicaKey.NO_DIRECTORY_ID)));
    }

    @Test
    void testRecordRoundTrip() {
        VoterSet voterSet = VoterSet.fromMap(voterMap(IntStream.of(1, 2, 3), true));

        assertEquals(voterSet, VoterSet.fromVotersRecord(voterSet.toVotersRecord((short) 0)));
    }

    @Test
    void testOverlappingMajority() {
        Map<Integer, VoterSet.VoterNode> startingVoterMap = voterMap(IntStream.of(1, 2, 3), true);
        VoterSet startingVoterSet = voterSet(startingVoterMap);

        VoterSet biggerVoterSet = startingVoterSet
            .addVoter(voterNode(4, true))
            .get();
        assertMajorities(true, startingVoterSet, biggerVoterSet);

        VoterSet smallerVoterSet = startingVoterSet
            .removeVoter(startingVoterMap.get(1).voterKey())
            .get();
        assertMajorities(true, startingVoterSet, smallerVoterSet);

        VoterSet replacedVoterSet = startingVoterSet
            .removeVoter(startingVoterMap.get(1).voterKey())
            .get()
            .addVoter(voterNode(1, true))
            .get();
        assertMajorities(true, startingVoterSet, replacedVoterSet);
    }

    @Test
    void testNonoverlappingMajority() {
        Map<Integer, VoterSet.VoterNode> startingVoterMap = voterMap(IntStream.of(1, 2, 3, 4, 5), true);
        VoterSet startingVoterSet = voterSet(startingVoterMap);

        // Two additions don't have an overlapping majority
        VoterSet biggerVoterSet = startingVoterSet
            .addVoter(voterNode(6, true))
            .get()
            .addVoter(voterNode(7, true))
            .get();
        assertMajorities(false, startingVoterSet, biggerVoterSet);

        // Two removals don't have an overlapping majority
        VoterSet smallerVoterSet = startingVoterSet
            .removeVoter(startingVoterMap.get(1).voterKey())
            .get()
            .removeVoter(startingVoterMap.get(2).voterKey())
            .get();
        assertMajorities(false, startingVoterSet, smallerVoterSet);

        // Two replacements don't have an overlapping majority
        VoterSet replacedVoterSet = startingVoterSet
            .removeVoter(startingVoterMap.get(1).voterKey())
            .get()
            .addVoter(voterNode(1, true))
            .get()
            .removeVoter(startingVoterMap.get(2).voterKey())
            .get()
            .addVoter(voterNode(2, true))
            .get();
        assertMajorities(false, startingVoterSet, replacedVoterSet);
    }

    private void assertMajorities(boolean overlap, VoterSet a, VoterSet b) {
        assertEquals(
            overlap,
            a.hasOverlappingMajority(b),
            String.format("a = %s, b = %s", a, b)
        );
        assertEquals(
            overlap,
            b.hasOverlappingMajority(a),
            String.format("b = %s, a = %s", b, a)
        );
    }

    public static final ListenerName DEFAULT_LISTENER_NAME = ListenerName.normalised("LISTENER");

    public static Map<Integer, VoterSet.VoterNode> voterMap(
        IntStream replicas,
        boolean withDirectoryId
    ) {
        return replicas
            .boxed()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> voterNode(id, withDirectoryId)
                )
            );
    }

    public static Map<Integer, VoterSet.VoterNode> voterMap(Stream<ReplicaKey> replicas) {
        return replicas
            .collect(Collectors.toMap(ReplicaKey::id, VoterSetTest::voterNode));
    }

    public static VoterSet.VoterNode voterNode(int id, boolean withDirectoryId) {
        return voterNode(
            ReplicaKey.of(
                id,
                withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID
            )
        );
    }

    public static VoterSet.VoterNode voterNode(ReplicaKey replicaKey) {
        return new VoterSet.VoterNode(
            replicaKey,
            Endpoints.fromInetSocketAddresses(
                Collections.singletonMap(
                    DEFAULT_LISTENER_NAME,
                    InetSocketAddress.createUnresolved(
                        "localhost",
                        9990 + replicaKey.id()
                    )
                )
            ),
            Feature.KRAFT_VERSION.supportedVersionRange()
        );
    }

    public static VoterSet voterSet(Map<Integer, VoterSet.VoterNode> voters) {
        return VoterSet.fromMap(voters);
    }

    public static VoterSet voterSet(Stream<ReplicaKey> voterKeys) {
        return voterSet(voterMap(voterKeys));
    }
}
