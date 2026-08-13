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

package org.apache.kafka.controller;

import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.raft.VoterSetTestUtil;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RaftClientVotersSupplierTest {
    private static VoterSet voterSet(int... ids) {
        return VoterSetTestUtil.voterSet(VoterSetTestUtil.voterMap(IntStream.of(ids), true));
    }

    @Test
    public void testVoterIds() {
        RaftClient<?> raftClient = Mockito.mock(RaftClient.class);
        Mockito.when(raftClient.latestVoterSet()).thenReturn(voterSet(0, 1, 2));

        assertEquals(Set.of(0, 1, 2), new RaftClientVotersSupplier(raftClient).get());
    }

    @Test
    public void testVoterIdsAreReadOnEveryCall() {
        RaftClient<?> raftClient = Mockito.mock(RaftClient.class);
        Mockito.when(raftClient.latestVoterSet()).thenReturn(voterSet(0, 1, 2));
        RaftClientVotersSupplier votersSupplier = new RaftClientVotersSupplier(raftClient);

        assertEquals(Set.of(0, 1, 2), votersSupplier.get());

        // The voter set changes when the cluster supports dynamic quorums, and the latest voter set
        // is used even if the VotersRecord which removed 2 and added 3 has not been committed yet.
        Mockito.when(raftClient.latestVoterSet()).thenReturn(voterSet(0, 1, 3));

        assertEquals(Set.of(0, 1, 3), votersSupplier.get());
    }
}
