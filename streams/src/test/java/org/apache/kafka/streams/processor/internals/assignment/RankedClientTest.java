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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.RankedClient.buildClientRankingsByTask;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.easymock.EasyMock;
import org.junit.Test;

public class RankedClientTest {

    private static final long ACCEPTABLE_RECOVERY_LAG = 100L;

    private ClientState client1 = EasyMock.createNiceMock(ClientState.class);
    private ClientState client2 = EasyMock.createNiceMock(ClientState.class);
    private ClientState client3 = EasyMock.createNiceMock(ClientState.class);
    
    @Test
    public void shouldRankPreviousClientAboveEquallyCaughtUpClient() {
        expect(client1.lagFor(TASK_0_0)).andReturn(Task.LATEST_OFFSET);
        expect(client2.lagFor(TASK_0_0)).andReturn(0L);
        replay(client1, client2);

        final SortedSet<RankedClient> expectedClientRanking = mkSortedSet(
            new RankedClient(UUID_1, Task.LATEST_OFFSET),
            new RankedClient(UUID_2, 0L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(TASK_0_0), states, ACCEPTABLE_RECOVERY_LAG);

        final SortedSet<RankedClient> clientRanking = statefulTasksToRankedCandidates.get(TASK_0_0);

        EasyMock.verify(client1, client2);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankTaskWithUnknownOffsetSumBelowCaughtUpClientAndClientWithLargeLag() {
        expect(client1.lagFor(TASK_0_0)).andReturn(UNKNOWN_OFFSET_SUM);
        expect(client2.lagFor(TASK_0_0)).andReturn(50L);
        expect(client3.lagFor(TASK_0_0)).andReturn(500L);
        replay(client1, client2, client3);

        final SortedSet<RankedClient> expectedClientRanking = mkSortedSet(
            new RankedClient(UUID_2, 0L),
            new RankedClient(UUID_1, 1L),
            new RankedClient(UUID_3, 500L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2),
            mkEntry(UUID_3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(TASK_0_0), states, ACCEPTABLE_RECOVERY_LAG);

        final SortedSet<RankedClient> clientRanking = statefulTasksToRankedCandidates.get(TASK_0_0);

        EasyMock.verify(client1, client2, client3);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankAllClientsWithinAcceptableRecoveryLagWithRank0() {
        expect(client1.lagFor(TASK_0_0)).andReturn(100L);
        expect(client2.lagFor(TASK_0_0)).andReturn(0L);
        replay(client1, client2);

        final SortedSet<RankedClient> expectedClientRanking = mkSortedSet(
            new RankedClient(UUID_1, 0L),
            new RankedClient(UUID_2, 0L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(TASK_0_0), states, ACCEPTABLE_RECOVERY_LAG);

        EasyMock.verify(client1, client2);
        assertThat(statefulTasksToRankedCandidates.get(TASK_0_0), equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankNotCaughtUpClientsAccordingToLag() {
        expect(client1.lagFor(TASK_0_0)).andReturn(900L);
        expect(client2.lagFor(TASK_0_0)).andReturn(800L);
        expect(client3.lagFor(TASK_0_0)).andReturn(500L);
        replay(client1, client2, client3);

        final SortedSet<RankedClient> expectedClientRanking = mkSortedSet(
            new RankedClient(UUID_3, 500L),
            new RankedClient(UUID_2, 800L),
            new RankedClient(UUID_1, 900L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2),
            mkEntry(UUID_3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(TASK_0_0), states, ACCEPTABLE_RECOVERY_LAG);

        EasyMock.verify(client1, client2, client3);
        assertThat(statefulTasksToRankedCandidates.get(TASK_0_0), equalTo(expectedClientRanking));
    }

    @Test
    public void shouldReturnEmptyClientRankingsWithNoStatefulTasks() {
        final Map<UUID, ClientState> states = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );

        assertTrue(buildClientRankingsByTask(emptySet(), states, ACCEPTABLE_RECOVERY_LAG).isEmpty());
    }
}
