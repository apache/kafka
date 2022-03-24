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

package org.apache.kafka.metadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class PartitionRegistrationTest {
    @Test
    public void testElectionWasClean() {
        assertTrue(PartitionRegistration.electionWasClean(1, new int[]{1, 2}));
        assertFalse(PartitionRegistration.electionWasClean(1, new int[]{0, 2}));
        assertFalse(PartitionRegistration.electionWasClean(1, new int[]{}));
        assertTrue(PartitionRegistration.electionWasClean(3, new int[]{1, 2, 3, 4, 5, 6}));
    }

    @Test
    public void testPartitionControlInfoMergeAndDiff() {
        PartitionRegistration a = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{1, 2}, Replicas.NONE, Replicas.NONE, 1, LeaderRecoveryState.RECOVERED, 0, 0);
        PartitionRegistration b = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{3}, Replicas.NONE, Replicas.NONE, 3, LeaderRecoveryState.RECOVERED, 1, 1);
        PartitionRegistration c = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{1}, Replicas.NONE, Replicas.NONE, 1, LeaderRecoveryState.RECOVERED, 0, 1);
        assertEquals(b, a.merge(new PartitionChangeRecord().
            setLeader(3).setIsr(Arrays.asList(3))));
        assertEquals("isr: [1, 2] -> [3], leader: 1 -> 3, leaderEpoch: 0 -> 1, partitionEpoch: 0 -> 1",
            b.diff(a));
        assertEquals("isr: [1, 2] -> [1], partitionEpoch: 0 -> 1",
            c.diff(a));
    }

    @Test
    public void testRecordRoundTrip() {
        PartitionRegistration registrationA = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{1, 2}, new int[]{1}, Replicas.NONE, 1, LeaderRecoveryState.RECOVERED, 0, 0);
        Uuid topicId = Uuid.fromString("OGdAI5nxT_m-ds3rJMqPLA");
        int partitionId = 4;
        ApiMessageAndVersion record = registrationA.toRecord(topicId, partitionId);
        PartitionRegistration registrationB =
            new PartitionRegistration((PartitionRecord) record.message());
        assertEquals(registrationA, registrationB);
    }

    @Test
    public void testToLeaderAndIsrPartitionState() {
        PartitionRegistration a = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{1, 2}, Replicas.NONE, Replicas.NONE, 1, LeaderRecoveryState.RECOVERED, 123, 456);
        PartitionRegistration b = new PartitionRegistration(
            new int[]{2, 3, 4}, new int[]{2, 3, 4}, Replicas.NONE, Replicas.NONE, 2, LeaderRecoveryState.RECOVERED, 234, 567);
        assertEquals(new LeaderAndIsrPartitionState().
                setTopicName("foo").
                setPartitionIndex(1).
                setControllerEpoch(-1).
                setLeader(1).
                setLeaderEpoch(123).
                setIsr(Arrays.asList(1, 2)).
                setZkVersion(456).
                setReplicas(Arrays.asList(1, 2, 3)).
                setAddingReplicas(Collections.emptyList()).
                setRemovingReplicas(Collections.emptyList()).
                setIsNew(true).toString(),
            a.toLeaderAndIsrPartitionState(new TopicPartition("foo", 1), true).toString());
        assertEquals(new LeaderAndIsrPartitionState().
                setTopicName("bar").
                setPartitionIndex(0).
                setControllerEpoch(-1).
                setLeader(2).
                setLeaderEpoch(234).
                setIsr(Arrays.asList(2, 3, 4)).
                setZkVersion(567).
                setReplicas(Arrays.asList(2, 3, 4)).
                setAddingReplicas(Collections.emptyList()).
                setRemovingReplicas(Collections.emptyList()).
                setIsNew(false).toString(),
            b.toLeaderAndIsrPartitionState(new TopicPartition("bar", 0), false).toString());
    }

    @Test
    public void testMergePartitionChangeRecordWithReassignmentData() {
        PartitionRegistration partition0 = new PartitionRegistration(new int[] {1, 2, 3},
            new int[] {1, 2, 3}, Replicas.NONE, Replicas.NONE, 1, LeaderRecoveryState.RECOVERED, 100, 200);
        PartitionRegistration partition1 = partition0.merge(new PartitionChangeRecord().
            setRemovingReplicas(Collections.singletonList(3)).
            setAddingReplicas(Collections.singletonList(4)).
            setReplicas(Arrays.asList(1, 2, 3, 4)));
        assertEquals(new PartitionRegistration(new int[] {1, 2, 3, 4},
            new int[] {1, 2, 3}, new int[] {3}, new int[] {4}, 1, LeaderRecoveryState.RECOVERED, 100, 201), partition1);
        PartitionRegistration partition2 = partition1.merge(new PartitionChangeRecord().
            setIsr(Arrays.asList(1, 2, 4)).
            setRemovingReplicas(Collections.emptyList()).
            setAddingReplicas(Collections.emptyList()).
            setReplicas(Arrays.asList(1, 2, 4)));
        assertEquals(new PartitionRegistration(new int[] {1, 2, 4},
            new int[] {1, 2, 4}, Replicas.NONE, Replicas.NONE, 1, LeaderRecoveryState.RECOVERED, 100, 202), partition2);
        assertFalse(partition2.isReassigning());
    }
}
