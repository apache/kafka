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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
        VoterSet voterSet = new VoterSet(voterMap(Arrays.asList(1, 2, 3), true));
        assertEquals(Optional.of(new InetSocketAddress("replica-1", 1234)), voterSet.voterAddress(1, "LISTENER"));
        assertEquals(Optional.empty(), voterSet.voterAddress(1, "MISSING"));
        assertEquals(Optional.empty(), voterSet.voterAddress(4, "LISTENER"));
    }

    @Test
    void testVoterIds() {
        VoterSet voterSet = new VoterSet(voterMap(Arrays.asList(1, 2, 3), true));
        assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), voterSet.voterIds());
    }

    @Test
    void testAddVoter() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(Arrays.asList(1, 2, 3), true);
        VoterSet voterSet = new VoterSet(new HashMap<>(aVoterMap));

        assertEquals(Optional.empty(), voterSet.addVoter(voterNode(1, true)));

        VoterSet.VoterNode voter4 = voterNode(4, true);
        aVoterMap.put(voter4.id(), voter4);
        assertEquals(Optional.of(new VoterSet(new HashMap<>(aVoterMap))), voterSet.addVoter(voter4));
    }

    @Test
    void testRemoveVoter() {
        Map<Integer, VoterSet.VoterNode> aVoterMap = voterMap(Arrays.asList(1, 2, 3), true);
        VoterSet voterSet = new VoterSet(new HashMap<>(aVoterMap));

        assertEquals(Optional.empty(), voterSet.removeVoter(4, Optional.empty()));
        assertEquals(Optional.empty(), voterSet.removeVoter(4, Optional.of(Uuid.randomUuid())));

        VoterSet.VoterNode voter3 = aVoterMap.remove(3);
        assertEquals(
            Optional.of(new VoterSet(new HashMap<>(aVoterMap))),
            voterSet.removeVoter(voter3.id(), voter3.uuid())
        );
    }

    @Test
    void testRecordRoundTrip() {
        VoterSet voterSet = new VoterSet(voterMap(Arrays.asList(1, 2, 3), true));

        assertEquals(voterSet, VoterSet.fromVotersRecord(voterSet.toVotersRecord((short) 0)));
    }

    public static Map<Integer, VoterSet.VoterNode> voterMap(
        Collection<Integer> replicas,
        boolean withUuid
    ) {
        return replicas
            .stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> VoterSetTest.voterNode(id, withUuid)
                )
            );
    }

    static VoterSet.VoterNode voterNode(int id, boolean withUuid) {
        return new VoterSet.VoterNode(
            id,
            withUuid ? Optional.of(Uuid.randomUuid()) : Optional.empty(),
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
