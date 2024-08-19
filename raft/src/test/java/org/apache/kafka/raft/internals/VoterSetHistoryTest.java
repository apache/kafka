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

import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.raft.VoterSetTest;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class VoterSetHistoryTest {
    @Test
    void testStaticVoterSet() {
        VoterSet staticVoterSet = VoterSet.fromMap(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        VoterSetHistory votersHistory = new VoterSetHistory(staticVoterSet);

        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(0));
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(100));
        assertEquals(staticVoterSet, votersHistory.lastValue());

        // Should be a no-op
        votersHistory.truncateNewEntries(100);
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(0));
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(100));
        assertEquals(staticVoterSet, votersHistory.lastValue());

        // Should be a no-op
        votersHistory.truncateOldEntries(100);
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(0));
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(100));
        assertEquals(staticVoterSet, votersHistory.lastValue());
    }

    @Test
    void TestNoStaticVoterSet() {
        VoterSetHistory votersHistory = new VoterSetHistory(VoterSet.empty());

        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(0));
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(100));
        assertEquals(VoterSet.empty(), votersHistory.lastValue());
    }

    @Test
    void testAddAt() {
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2, 3), true);
        VoterSet staticVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        VoterSetHistory votersHistory = new VoterSetHistory(staticVoterSet);

        assertThrows(
            IllegalArgumentException.class,
            () -> votersHistory.addAt(-2, VoterSet.fromMap(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true)))
        );
        assertEquals(staticVoterSet, votersHistory.lastValue());

        voterMap.put(4, VoterSetTest.voterNode(4, true));
        VoterSet addedVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(100, addedVoterSet);

        assertEquals(addedVoterSet, votersHistory.lastValue());
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(99));
        assertEquals(Optional.of(addedVoterSet), votersHistory.valueAtOrBefore(100));

        voterMap.remove(4);
        VoterSet removedVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(200, removedVoterSet);

        assertEquals(removedVoterSet, votersHistory.lastValue());
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(99));
        assertEquals(Optional.of(addedVoterSet), votersHistory.valueAtOrBefore(199));
        assertEquals(Optional.of(removedVoterSet), votersHistory.valueAtOrBefore(200));
    }

    @Test
    void testBootstrapAddAt() {
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2, 3), true);
        VoterSet bootstrapVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        VoterSetHistory votersHistory = new VoterSetHistory(VoterSet.empty());

        votersHistory.addAt(-1, bootstrapVoterSet);
        assertEquals(bootstrapVoterSet, votersHistory.lastValue());
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(-2));
        assertEquals(Optional.of(bootstrapVoterSet), votersHistory.valueAtOrBefore(-1));

        voterMap.put(4, VoterSetTest.voterNode(4, true));
        VoterSet addedVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(100, addedVoterSet);

        assertEquals(addedVoterSet, votersHistory.lastValue());
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(-2));
        assertEquals(Optional.of(bootstrapVoterSet), votersHistory.valueAtOrBefore(99));
        assertEquals(Optional.of(addedVoterSet), votersHistory.valueAtOrBefore(100));

        voterMap.remove(4);
        VoterSet removedVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(200, removedVoterSet);

        assertEquals(removedVoterSet, votersHistory.lastValue());
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(-2));
        assertEquals(Optional.of(bootstrapVoterSet), votersHistory.valueAtOrBefore(99));
        assertEquals(Optional.of(addedVoterSet), votersHistory.valueAtOrBefore(199));
        assertEquals(Optional.of(removedVoterSet), votersHistory.valueAtOrBefore(200));
    }

    @Test
    void testAddAtNonOverlapping() {
        VoterSetHistory votersHistory = new VoterSetHistory(VoterSet.empty());

        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2, 3), true);
        VoterSet voterSet = VoterSet.fromMap(new HashMap<>(voterMap));

        // Add a starting voter to the history
        votersHistory.addAt(100, voterSet);

        // Remove voter so that it doesn't overlap
        VoterSet nonoverlappingRemovedSet = voterSet
            .removeVoter(voterMap.get(1).voterKey()).get()
            .removeVoter(voterMap.get(2).voterKey()).get();

        assertThrows(
            IllegalArgumentException.class,
            () -> votersHistory.addAt(200, nonoverlappingRemovedSet)
        );
        assertEquals(voterSet, votersHistory.lastValue());


        // Add voters so that it doesn't overlap
        VoterSet nonoverlappingAddSet = voterSet
            .addVoter(VoterSetTest.voterNode(4, true)).get()
            .addVoter(VoterSetTest.voterNode(5, true)).get();

        assertThrows(
            IllegalArgumentException.class,
            () -> votersHistory.addAt(200, nonoverlappingAddSet)
        );
        assertEquals(voterSet, votersHistory.lastValue());
    }

    @Test
    void testNonoverlappingFromStaticVoterSet() {
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2, 3), true);
        VoterSet staticVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        VoterSetHistory votersHistory = new VoterSetHistory(VoterSet.empty());

        // Remove voter so that it doesn't overlap
        VoterSet nonoverlappingRemovedSet = staticVoterSet
            .removeVoter(voterMap.get(1).voterKey()).get()
            .removeVoter(voterMap.get(2).voterKey()).get();

        votersHistory.addAt(100, nonoverlappingRemovedSet);
        assertEquals(nonoverlappingRemovedSet, votersHistory.lastValue());
    }

    @Test
    void testTruncateTo() {
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2, 3), true);
        VoterSet staticVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        VoterSetHistory votersHistory = new VoterSetHistory(staticVoterSet);

        // Add voter 4 to the voter set and voter set history
        voterMap.put(4, VoterSetTest.voterNode(4, true));
        VoterSet voterSet1234 = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(100, voterSet1234);

        // Add voter 5 to the voter set and voter set history
        voterMap.put(5, VoterSetTest.voterNode(5, true));
        VoterSet voterSet12345 = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(200, voterSet12345);

        votersHistory.truncateNewEntries(201);
        assertEquals(voterSet12345, votersHistory.lastValue());
        votersHistory.truncateNewEntries(200);
        assertEquals(voterSet1234, votersHistory.lastValue());
        votersHistory.truncateNewEntries(101);
        assertEquals(voterSet1234, votersHistory.lastValue());
        votersHistory.truncateNewEntries(100);
        assertEquals(staticVoterSet, votersHistory.lastValue());
    }

    @Test
    void testTrimPrefixTo() {
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2, 3), true);
        VoterSet staticVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        VoterSetHistory votersHistory = new VoterSetHistory(staticVoterSet);

        // Add voter 4 to the voter set and voter set history
        voterMap.put(4, VoterSetTest.voterNode(4, true));
        VoterSet voterSet1234 = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(100, voterSet1234);

        // Add voter 5 to the voter set and voter set history
        voterMap.put(5, VoterSetTest.voterNode(5, true));
        VoterSet voterSet12345 = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(200, voterSet12345);

        votersHistory.truncateOldEntries(99);
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(99));
        assertEquals(Optional.of(voterSet1234), votersHistory.valueAtOrBefore(100));

        votersHistory.truncateOldEntries(100);
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(99));
        assertEquals(Optional.of(voterSet1234), votersHistory.valueAtOrBefore(100));

        votersHistory.truncateOldEntries(101);
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(99));
        assertEquals(Optional.of(voterSet1234), votersHistory.valueAtOrBefore(100));

        votersHistory.truncateOldEntries(200);
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(199));
        assertEquals(Optional.of(voterSet12345), votersHistory.valueAtOrBefore(200));
    }

    @Test
    void testClear() {
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(IntStream.of(1, 2, 3), true);
        VoterSet staticVoterSet = VoterSet.fromMap(new HashMap<>(voterMap));
        VoterSetHistory votersHistory = new VoterSetHistory(staticVoterSet);

        // Add voter 4 to the voter set and voter set history
        voterMap.put(4, VoterSetTest.voterNode(4, true));
        VoterSet voterSet1234 = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(100, voterSet1234);

        // Add voter 5 to the voter set and voter set history
        voterMap.put(5, VoterSetTest.voterNode(5, true));
        VoterSet voterSet12345 = VoterSet.fromMap(new HashMap<>(voterMap));
        votersHistory.addAt(200, voterSet12345);

        votersHistory.clear();

        assertEquals(staticVoterSet, votersHistory.lastValue());
    }
}
