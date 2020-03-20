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
package org.apache.kafka.streams.processor.internals.assignment;

import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.buildClientRankingsByTask;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.RankedClient;
import org.apache.kafka.streams.processor.internals.Task;
import org.easymock.EasyMock;
import org.junit.Test;

public class HighAvailabilityTaskAssignorTest {
    private static final long ACCEPTABLE_RECOVERY_LAG = 100L;

    private final TaskId task0_0 = new TaskId(0, 0);

    private final UUID uuid1 = UUID.randomUUID();
    private final UUID uuid2 = UUID.randomUUID();
    private final UUID uuid3 = UUID.randomUUID();

    @Test
    public void shouldRankPreviousClientAboveEquallyCaughtUpClient() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(Task.LATEST_OFFSET);
        expect(client2.lagFor(task0_0)).andReturn(0L);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid1, Task.LATEST_OFFSET),
            new RankedClient<>(uuid2, 0L)
        );

        replay(client1, client2);

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, ACCEPTABLE_RECOVERY_LAG);

        final SortedSet<RankedClient<UUID>> clientRanking = statefulTasksToRankedCandidates.get(task0_0);

        EasyMock.verify(client1, client2);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankTaskWithUnknownOffsetSumBelowCaughtUpClientAndClientWithLargeLag() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);
        final ClientState client3 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(UNKNOWN_OFFSET_SUM);
        expect(client2.lagFor(task0_0)).andReturn(50L);
        expect(client3.lagFor(task0_0)).andReturn(500L);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid2, 0L),
            new RankedClient<>(uuid1, 1L),
            new RankedClient<>(uuid3, 500L)
        );

        replay(client1, client2, client3);

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2),
            mkEntry(uuid3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, ACCEPTABLE_RECOVERY_LAG);

        final SortedSet<RankedClient<UUID>> clientRanking = statefulTasksToRankedCandidates.get(task0_0);

        EasyMock.verify(client1, client2, client3);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankAllClientsWithinAcceptableRecoveryLagWithRank0() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(100L);
        expect(client2.lagFor(task0_0)).andReturn(0L);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid1, 0L),
            new RankedClient<>(uuid2, 0L)
        );

        replay(client1, client2);

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, ACCEPTABLE_RECOVERY_LAG);

        EasyMock.verify(client1, client2);
        assertThat(statefulTasksToRankedCandidates.get(task0_0), equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankNotCaughtUpClientsAccordingToLag() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);
        final ClientState client3 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(900L);
        expect(client2.lagFor(task0_0)).andReturn(800L);
        expect(client3.lagFor(task0_0)).andReturn(500L);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid3, 500L),
            new RankedClient<>(uuid2, 800L),
            new RankedClient<>(uuid1, 900L)
        );

        replay(client1, client2, client3);

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2),
            mkEntry(uuid3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, ACCEPTABLE_RECOVERY_LAG);

        EasyMock.verify(client1, client2, client3);
        assertThat(statefulTasksToRankedCandidates.get(task0_0), equalTo(expectedClientRanking));
    }
}
