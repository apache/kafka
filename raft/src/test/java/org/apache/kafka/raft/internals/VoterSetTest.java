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
package org.apache.kafka.raft.internals;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final public class VoterSetTest {
    @Test
    void testEmptyVoterSet() {
        assertThrows(IllegalArgumentException.class, () -> new VoterSet(Collections.emptyMap()));
    }

    @Test
    void testVoterAddress() {
        VoterSet voterSet = new VoterSet(voterMap(Arrays.asList(1, 2, 3)));
        assertEquals(Optional.of(new InetSocketAddress("replica-1", 1234)), voterSet.voterAddress(1, "LISTENER"));
        assertEquals(Optional.empty(), voterSet.voterAddress(1, "MISSING"));
        assertEquals(Optional.empty(), voterSet.voterAddress(4, "LISTENER"));
    }

    @Test
    void testVoterIds() {
        VoterSet voterSet = new VoterSet(voterMap(Arrays.asList(1, 2, 3)));
        assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), voterSet.voterIds());
    }

    @Test
    void testAddVoter() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(Arrays.asList(1, 2, 3));
        VoterSet voterSet = new VoterSet(new HashMap<>(aVoterMap));

        assertEquals(Optional.empty(), voterSet.addVoter(voterNode(1)));

        VoterSet.VoterNode voter4 = voterNode(4);
        aVoterMap.put(voter4.voterKey().id(), voter4);
        assertEquals(Optional.of(new VoterSet(new HashMap<>(aVoterMap))), voterSet.addVoter(voter4));
    }

    @Test
    void testRemoveVoter() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(Arrays.asList(1, 2, 3));
        VoterSet voterSet = new VoterSet(new HashMap<>(aVoterMap));

        assertEquals(Optional.empty(), voterSet.removeVoter(VoterSet.VoterKey.of(4, Optional.empty())));
        assertEquals(Optional.empty(), voterSet.removeVoter(VoterSet.VoterKey.of(4, Optional.of(Uuid.randomUuid()))));

        VoterSet.VoterNode voter3 = aVoterMap.remove(3);
        assertEquals(
            Optional.of(new VoterSet(new HashMap<>(aVoterMap))),
            voterSet.removeVoter(voter3.voterKey())
        );
    }

    @Test
    void testRecordRoundTrip() {
        VoterSet voterSet = new VoterSet(voterMap(Arrays.asList(1, 2, 3)));

        assertEquals(voterSet, VoterSet.fromVotersRecord(voterSet.toVotersRecord((short) 0)));
    }

    @Test
    void testOverlappingMajority() {
        Map<Integer, VoterSet.VoterNode> startingVoterMap = voterMap(Arrays.asList(1, 2, 3));
        VoterSet startingVoterSet = voterSet(startingVoterMap);

        VoterSet biggerVoterSet = startingVoterSet
            .addVoter(voterNode(4))
            .get();
        assertMajorities(true, startingVoterSet, biggerVoterSet);

        VoterSet smallerVoterSet = startingVoterSet
            .removeVoter(startingVoterMap.get(1).voterKey())
            .get();
        assertMajorities(true, startingVoterSet, smallerVoterSet);

        VoterSet replacedVoterSet = startingVoterSet
            .removeVoter(startingVoterMap.get(1).voterKey())
            .get()
            .addVoter(voterNode(1))
            .get();
        assertMajorities(true, startingVoterSet, replacedVoterSet);
    }

    @Test
    void testNonoverlappingMajority() {
        Map<Integer, VoterSet.VoterNode> startingVoterMap = voterMap(Arrays.asList(1, 2, 3, 4, 5));
        VoterSet startingVoterSet = voterSet(startingVoterMap);

        // Two additions don't have an overlapping majority
        VoterSet biggerVoterSet = startingVoterSet
            .addVoter(voterNode(6))
            .get()
            .addVoter(voterNode(7))
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
            .addVoter(voterNode(1))
            .get()
            .removeVoter(startingVoterMap.get(2).voterKey())
            .get()
            .addVoter(voterNode(2))
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

    public static Map<Integer, VoterSet.VoterNode> voterMap(List<Integer> replicas) {
        return replicas
            .stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    VoterSetTest::voterNode
                )
            );
    }

    static VoterSet.VoterNode voterNode(int id) {
        return new VoterSet.VoterNode(
            VoterSet.VoterKey.of(id, Optional.of(Uuid.randomUuid())),
            Collections.singletonMap(
                "LISTENER",
                InetSocketAddress.createUnresolved(String.format("replica-%d", id), 1234)
            ),
            new SupportedVersionRange((short) 0, (short) 0)
        );
    }

    public static VoterSet voterSet(Map<Integer, VoterSet.VoterNode> voters) {
        return new VoterSet(voters);
    }
}
