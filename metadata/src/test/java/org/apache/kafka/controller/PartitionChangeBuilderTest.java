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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.controller.PartitionChangeBuilder.BestLeader;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_CHANGE_RECORD;
import static org.apache.kafka.controller.PartitionChangeBuilder.changeRecordIsNoOp;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class PartitionChangeBuilderTest {
    @Test
    public void testChangeRecordIsNoOp() {
        assertTrue(changeRecordIsNoOp(new PartitionChangeRecord()));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().setLeader(1)));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
            setIsr(Arrays.asList(1, 2, 3))));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
            setRemovingReplicas(Arrays.asList(1))));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
            setAddingReplicas(Arrays.asList(4))));
    }

    private final static PartitionRegistration FOO = new PartitionRegistration(
        new int[] {2, 1, 3}, new int[] {2, 1, 3}, Replicas.NONE, Replicas.NONE,
        1, 100, 200);

    private final static Uuid FOO_ID = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

    private static PartitionChangeBuilder createFooBuilder(boolean allowUnclean) {
        return new PartitionChangeBuilder(FOO, FOO_ID, 0, r -> r != 3, () -> allowUnclean);
    }

    private final static PartitionRegistration BAR = new PartitionRegistration(
        new int[] {1, 2, 3, 4}, new int[] {1, 2, 3}, new int[] {1}, new int[] {4},
        1, 100, 200);

    private final static Uuid BAR_ID = Uuid.fromString("LKfUsCBnQKekvL9O5dY9nw");

    private static PartitionChangeBuilder createBarBuilder(boolean allowUnclean) {
        return new PartitionChangeBuilder(BAR, BAR_ID, 0, r -> r != 3, () -> allowUnclean);
    }

    private static void assertBestLeaderEquals(PartitionChangeBuilder builder,
                                               int expectedNode,
                                               boolean expectedUnclean) {
        BestLeader bestLeader = builder.new BestLeader();
        assertEquals(expectedNode, bestLeader.node);
        assertEquals(expectedUnclean, bestLeader.unclean);
    }

    @Test
    public void testBestLeader() {
        assertBestLeaderEquals(createFooBuilder(false), 2, false);
        assertBestLeaderEquals(createFooBuilder(true), 2, false);
        assertBestLeaderEquals(createFooBuilder(false).
                setTargetIsr(Arrays.asList(1, 3)), 1, false);
        assertBestLeaderEquals(createFooBuilder(true).
            setTargetIsr(Arrays.asList(1, 3)), 1, false);
        assertBestLeaderEquals(createFooBuilder(false).
            setTargetIsr(Arrays.asList(3)), NO_LEADER, false);
        assertBestLeaderEquals(createFooBuilder(true).
            setTargetIsr(Arrays.asList(3)), 2, true);
        assertBestLeaderEquals(createFooBuilder(true).
                setTargetIsr(Arrays.asList(4)).setTargetReplicas(Arrays.asList(2, 1, 3, 4)),
            4, false);
    }

    @Test
    public void testShouldTryElection() {
        assertFalse(createFooBuilder(false).shouldTryElection());
        assertTrue(createFooBuilder(false).setAlwaysElectPreferredIfPossible(true).
            shouldTryElection());
        assertTrue(createFooBuilder(false).setTargetIsr(Arrays.asList(2, 3)).
            shouldTryElection());
        assertFalse(createFooBuilder(false).setTargetIsr(Arrays.asList(2, 1)).
            shouldTryElection());
    }

    private static void testTriggerLeaderEpochBumpIfNeededLeader(PartitionChangeBuilder builder,
                                                                 PartitionChangeRecord record,
                                                                 int expectedLeader) {
        builder.triggerLeaderEpochBumpIfNeeded(record);
        assertEquals(expectedLeader, record.leader());
    }

    @Test
    public void testTriggerLeaderEpochBumpIfNeeded() {
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(false),
            new PartitionChangeRecord(), NO_LEADER_CHANGE);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(false).
            setTargetIsr(Arrays.asList(2, 1)), new PartitionChangeRecord(), 1);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(false).
            setTargetIsr(Arrays.asList(2, 1, 3, 4)), new PartitionChangeRecord(),
            NO_LEADER_CHANGE);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(false).
            setTargetReplicas(Arrays.asList(2, 1, 3, 4)), new PartitionChangeRecord(),
            NO_LEADER_CHANGE);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(false).
            setTargetReplicas(Arrays.asList(2, 1, 3, 4)),
            new PartitionChangeRecord().setLeader(2), 2);
    }

    @Test
    public void testNoChange() {
        assertEquals(Optional.empty(), createFooBuilder(false).build());
        assertEquals(Optional.empty(), createFooBuilder(true).build());
        assertEquals(Optional.empty(), createBarBuilder(false).build());
        assertEquals(Optional.empty(), createBarBuilder(true).build());
    }

    @Test
    public void testIsrChangeAndLeaderBump() {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
            setTopicId(FOO_ID).
            setPartitionId(0).
            setIsr(Arrays.asList(2, 1)).
            setLeader(1), PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder(false).setTargetIsr(Arrays.asList(2, 1)).build());
    }

    @Test
    public void testIsrChangeAndLeaderChange() {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setIsr(Arrays.asList(2, 3)).
                setLeader(2), PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder(false).setTargetIsr(Arrays.asList(2, 3)).build());
    }

    @Test
    public void testReassignmentRearrangesReplicas() {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(3, 2, 1)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder(false).setTargetReplicas(Arrays.asList(3, 2, 1)).build());
    }

    @Test
    public void testIsrEnlargementCompletesReassignment() {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(BAR_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(2, 3, 4)).
                setIsr(Arrays.asList(2, 3, 4)).
                setLeader(2).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()),
                PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createBarBuilder(false).setTargetIsr(Arrays.asList(1, 2, 3, 4)).build());
    }

    @Test
    public void testRevertReassignment() {
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(BAR);
        assertEquals(Arrays.asList(1, 2, 3), revert.replicas());
        assertEquals(Arrays.asList(1, 2, 3), revert.isr());
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(BAR_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(1, 2, 3)).
                setLeader(1).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()),
                PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createBarBuilder(false).
                setTargetReplicas(revert.replicas()).
                setTargetIsr(revert.isr()).
                setTargetRemoving(Collections.emptyList()).
                setTargetAdding(Collections.emptyList()).
                build());
    }

    @Test
    public void testRemovingReplicaReassignment() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Replicas.toList(FOO.replicas), Arrays.asList(1, 2));
        assertEquals(Collections.singletonList(3), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(1, 2, 3), replicas.merged());
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(1, 2)).
                setIsr(Arrays.asList(2, 1)).
                setLeader(1),
                PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder(false).
                setTargetReplicas(replicas.merged()).
                setTargetRemoving(replicas.removing()).
                build());
    }

    @Test
    public void testAddingReplicaReassignment() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Replicas.toList(FOO.replicas), Arrays.asList(1, 2, 3, 4));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Collections.singletonList(4), replicas.adding());
        assertEquals(Arrays.asList(1, 2, 3, 4), replicas.merged());
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(1, 2, 3, 4)).
                setAddingReplicas(Collections.singletonList(4)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder(false).
                setTargetReplicas(replicas.merged()).
                setTargetAdding(replicas.adding()).
                build());
    }
}
