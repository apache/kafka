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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;


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
        PartitionRegistration a = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setIsr(new int[]{1, 2}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(0).build();
        PartitionRegistration b = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setIsr(new int[]{3}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(1).build();
        PartitionRegistration c = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setIsr(new int[]{1}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(1).build();
        assertEquals(b, a.merge(new PartitionChangeRecord().
            setLeader(3).setIsr(Arrays.asList(3))));
        assertEquals("isr: [1, 2] -> [3], leader: 1 -> 3, leaderEpoch: 0 -> 1, partitionEpoch: 0 -> 1",
            b.diff(a));
        assertEquals("isr: [1, 2] -> [1], partitionEpoch: 0 -> 1",
            c.diff(a));
    }

    @Test
    public void testRecordRoundTrip() {
        PartitionRegistration registrationA = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setIsr(new int[]{1, 2}).setRemovingReplicas(new int[]{1}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(0).build();
        Uuid topicId = Uuid.fromString("OGdAI5nxT_m-ds3rJMqPLA");
        int partitionId = 4;
        ApiMessageAndVersion record = registrationA.toRecord(topicId, partitionId);
        PartitionRegistration registrationB =
            new PartitionRegistration((PartitionRecord) record.message());
        assertEquals(registrationA, registrationB);
    }

    @Test
    public void testToLeaderAndIsrPartitionState() {
        PartitionRegistration a = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setIsr(new int[]{1, 2}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(123).setPartitionEpoch(456).build();
        PartitionRegistration b = new PartitionRegistration.Builder().
            setReplicas(new int[]{2, 3, 4}).setIsr(new int[]{2, 3, 4}).setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(234).setPartitionEpoch(567).build();
        assertEquals(new LeaderAndIsrPartitionState().
                setTopicName("foo").
                setPartitionIndex(1).
                setControllerEpoch(-1).
                setLeader(1).
                setLeaderEpoch(123).
                setIsr(Arrays.asList(1, 2)).
                setPartitionEpoch(456).
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
                setPartitionEpoch(567).
                setReplicas(Arrays.asList(2, 3, 4)).
                setAddingReplicas(Collections.emptyList()).
                setRemovingReplicas(Collections.emptyList()).
                setIsNew(false).toString(),
            b.toLeaderAndIsrPartitionState(new TopicPartition("bar", 0), false).toString());
    }

    @Test
    public void testMergePartitionChangeRecordWithReassignmentData() {
        PartitionRegistration partition0 = new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).
            setIsr(new int[] {1, 2, 3}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionRegistration partition1 = partition0.merge(new PartitionChangeRecord().
            setRemovingReplicas(Collections.singletonList(3)).
            setAddingReplicas(Collections.singletonList(4)).
            setReplicas(Arrays.asList(1, 2, 3, 4)));
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4}).
            setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {3}).setAddingReplicas(new int[] {4}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(201).build(), partition1);
        PartitionRegistration partition2 = partition1.merge(new PartitionChangeRecord().
            setIsr(Arrays.asList(1, 2, 4)).
            setRemovingReplicas(Collections.emptyList()).
            setAddingReplicas(Collections.emptyList()).
            setReplicas(Arrays.asList(1, 2, 4)));
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 4}).
            setIsr(new int[] {1, 2, 4}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(202).build(), partition2);
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingReplicas() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> builder.build());
        assertEquals("You must set replicas.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingIsr() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0});
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> builder.build());
        assertEquals("You must set isr.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingLeader() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0});
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> builder.build());
        assertEquals("You must set leader.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingLeaderRecoveryState() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0}).
            setLeader(0);
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> builder.build());
        assertEquals("You must set leader recovery state.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingLeaderEpoch() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED);
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> builder.build());
        assertEquals("You must set leader epoch.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingPartitionEpoch() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0);
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> builder.build());
        assertEquals("You must set partition epoch.", exception.getMessage());
    }

    @Test
    public void testBuilderSuccess() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0, 1}).
            setIsr(new int[]{0, 1}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{1}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0).
            setPartitionEpoch(0);
        PartitionRegistration partitionRegistration = builder.build();
        assertEquals(Replicas.toList(new int[]{0, 1}), Replicas.toList(partitionRegistration.replicas));
        assertEquals(Replicas.toList(new int[]{0, 1}), Replicas.toList(partitionRegistration.isr));
        assertEquals(Replicas.toList(new int[]{0}), Replicas.toList(partitionRegistration.removingReplicas));
        assertEquals(Replicas.toList(new int[]{1}), Replicas.toList(partitionRegistration.addingReplicas));
        assertEquals(0, partitionRegistration.leader);
        assertEquals(LeaderRecoveryState.RECOVERED, partitionRegistration.leaderRecoveryState);
        assertEquals(0, partitionRegistration.leaderEpoch);
        assertEquals(0, partitionRegistration.partitionEpoch);
    }

    @Test
    public void testBuilderSetsDefaultAddingAndRemovingReplicas() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0, 1}).
            setIsr(new int[]{0, 1}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0).
            setPartitionEpoch(0);
        PartitionRegistration partitionRegistration = builder.build();
        assertEquals(Replicas.toList(Replicas.NONE), Replicas.toList(partitionRegistration.removingReplicas));
        assertEquals(Replicas.toList(Replicas.NONE), Replicas.toList(partitionRegistration.addingReplicas));
    }

    @Property
    public void testConsistentEqualsAndHashCode(
        @ForAll("uniqueSamples") PartitionRegistration a,
        @ForAll("uniqueSamples") PartitionRegistration b
    ) {
        if (a.equals(b)) {
            assertEquals(a.hashCode(), b.hashCode());
        }

        if (a.hashCode() != b.hashCode()) {
            assertNotEquals(a, b);
        }
    }

    @Provide
    Arbitrary<PartitionRegistration> uniqueSamples() {
        return Arbitraries.of(
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(101).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(201).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERING).setLeaderEpoch(100).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {4, 5, 6}).setAddingReplicas(new int[] {1, 2, 3}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {1, 2, 3}).setAddingReplicas(new int[] {4, 5, 6}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {1, 2, 3}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setAddingReplicas(new int[] {4, 5, 6}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build()
        );
    }

}
