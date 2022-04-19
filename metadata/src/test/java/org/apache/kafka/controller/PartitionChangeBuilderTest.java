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
import org.apache.kafka.controller.PartitionChangeBuilder.ElectionResult;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_CHANGE_RECORD;
import static org.apache.kafka.controller.PartitionChangeBuilder.Election;
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
        1, LeaderRecoveryState.RECOVERED, 100, 200);

    private final static Uuid FOO_ID = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

    private static PartitionChangeBuilder createFooBuilder() {
        return new PartitionChangeBuilder(FOO, FOO_ID, 0, r -> r != 3, true);
    }

    private final static PartitionRegistration BAR = new PartitionRegistration(
        new int[] {1, 2, 3, 4}, new int[] {1, 2, 3}, new int[] {1}, new int[] {4},
        1, LeaderRecoveryState.RECOVERED, 100, 200);

    private final static Uuid BAR_ID = Uuid.fromString("LKfUsCBnQKekvL9O5dY9nw");

    private static PartitionChangeBuilder createBarBuilder() {
        return new PartitionChangeBuilder(BAR, BAR_ID, 0, r -> r != 3, true);
    }

    private final static PartitionRegistration BAZ = new PartitionRegistration(
        new int[] {2, 1, 3}, new int[] {1, 3}, Replicas.NONE, Replicas.NONE,
        3, LeaderRecoveryState.RECOVERED, 100, 200);

    private final static Uuid BAZ_ID = Uuid.fromString("wQzt5gkSTwuQNXZF5gIw7A");

    private static PartitionChangeBuilder createBazBuilder() {
        return new PartitionChangeBuilder(BAZ, BAZ_ID, 0, __ -> true, true);
    }

    private final static PartitionRegistration OFFLINE = new PartitionRegistration(
        new int[] {2, 1, 3}, new int[] {3}, Replicas.NONE, Replicas.NONE,
        -1, LeaderRecoveryState.RECOVERED, 100, 200);

    private final static Uuid OFFLINE_ID = Uuid.fromString("LKfUsCBnQKekvL9O5dY9nw");

    private static PartitionChangeBuilder createOfflineBuilder() {
        return new PartitionChangeBuilder(OFFLINE, OFFLINE_ID, 0, r -> r == 1, true);
    }

    private static void assertElectLeaderEquals(PartitionChangeBuilder builder,
                                               int expectedNode,
                                               boolean expectedUnclean) {
        ElectionResult electionResult = builder.electLeader();
        assertEquals(expectedNode, electionResult.node);
        assertEquals(expectedUnclean, electionResult.unclean);
    }

    @Test
    public void testElectLeader() {
        assertElectLeaderEquals(createFooBuilder().setElection(Election.PREFERRED), 2, false);
        assertElectLeaderEquals(createFooBuilder(), 1, false);
        assertElectLeaderEquals(createFooBuilder().setElection(Election.UNCLEAN), 1, false);
        assertElectLeaderEquals(createFooBuilder().setTargetIsr(Arrays.asList(1, 3)), 1, false);
        assertElectLeaderEquals(createFooBuilder().setElection(Election.UNCLEAN).setTargetIsr(Arrays.asList(1, 3)), 1, false);
        assertElectLeaderEquals(createFooBuilder().setTargetIsr(Arrays.asList(3)), NO_LEADER, false);
        assertElectLeaderEquals(createFooBuilder().setElection(Election.UNCLEAN).setTargetIsr(Arrays.asList(3)), 2, true);
        assertElectLeaderEquals(
            createFooBuilder().setElection(Election.UNCLEAN).setTargetIsr(Arrays.asList(4)).setTargetReplicas(Arrays.asList(2, 1, 3, 4)),
            4,
            false
        );

        assertElectLeaderEquals(createBazBuilder().setElection(Election.PREFERRED), 3, false);
        assertElectLeaderEquals(createBazBuilder(), 3, false);
        assertElectLeaderEquals(createBazBuilder().setElection(Election.UNCLEAN), 3, false);
    }

    private static void testTriggerLeaderEpochBumpIfNeededLeader(PartitionChangeBuilder builder,
                                                                 PartitionChangeRecord record,
                                                                 int expectedLeader) {
        builder.triggerLeaderEpochBumpIfNeeded(record);
        assertEquals(expectedLeader, record.leader());
    }

    @Test
    public void testTriggerLeaderEpochBumpIfNeeded() {
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(),
            new PartitionChangeRecord(), NO_LEADER_CHANGE);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder().
            setTargetIsr(Arrays.asList(2, 1)), new PartitionChangeRecord(), 1);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder().
            setTargetIsr(Arrays.asList(2, 1, 3, 4)), new PartitionChangeRecord(),
            NO_LEADER_CHANGE);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder().
            setTargetReplicas(Arrays.asList(2, 1, 3, 4)), new PartitionChangeRecord(),
            NO_LEADER_CHANGE);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder().
            setTargetReplicas(Arrays.asList(2, 1, 3, 4)),
            new PartitionChangeRecord().setLeader(2), 2);
    }

    @Test
    public void testNoChange() {
        assertEquals(Optional.empty(), createFooBuilder().build());
        assertEquals(Optional.empty(), createFooBuilder().setElection(Election.UNCLEAN).build());
        assertEquals(Optional.empty(), createBarBuilder().build());
        assertEquals(Optional.empty(), createBarBuilder().setElection(Election.UNCLEAN).build());
        assertEquals(Optional.empty(), createBazBuilder().setElection(Election.PREFERRED).build());
    }

    @Test
    public void testIsrChangeAndLeaderBump() {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
            setTopicId(FOO_ID).
            setPartitionId(0).
            setIsr(Arrays.asList(2, 1)).
            setLeader(1), PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder().setTargetIsr(Arrays.asList(2, 1)).build());
    }

    @Test
    public void testIsrChangeAndLeaderChange() {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setIsr(Arrays.asList(2, 3)).
                setLeader(2), PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder().setTargetIsr(Arrays.asList(2, 3)).build());
    }

    @Test
    public void testReassignmentRearrangesReplicas() {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(3, 2, 1)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion())),
            createFooBuilder().setTargetReplicas(Arrays.asList(3, 2, 1)).build());
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
            createBarBuilder().setTargetIsr(Arrays.asList(1, 2, 3, 4)).build());
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
            createBarBuilder().
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
            createFooBuilder().
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
            createFooBuilder().
                setTargetReplicas(replicas.merged()).
                setTargetAdding(replicas.adding()).
                build());
    }

    @Test
    public void testUncleanLeaderElection() {
        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(
            new PartitionChangeRecord()
                .setTopicId(FOO_ID)
                .setPartitionId(0)
                .setIsr(Arrays.asList(2))
                .setLeader(2)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value()),
            PARTITION_CHANGE_RECORD.highestSupportedVersion()
        );
        assertEquals(
            Optional.of(expectedRecord),
            createFooBuilder().setElection(Election.UNCLEAN).setTargetIsr(Arrays.asList(3)).build()
        );

        expectedRecord = new ApiMessageAndVersion(
            new PartitionChangeRecord()
                .setTopicId(OFFLINE_ID)
                .setPartitionId(0)
                .setIsr(Arrays.asList(1))
                .setLeader(1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value()),
            PARTITION_CHANGE_RECORD.highestSupportedVersion()
        );
        assertEquals(
            Optional.of(expectedRecord),
            createOfflineBuilder().setElection(Election.UNCLEAN).build()
        );
        assertEquals(
            Optional.of(expectedRecord),
            createOfflineBuilder().setElection(Election.UNCLEAN).setTargetIsr(Arrays.asList(2)).build()
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testChangeInLeadershipDoesNotChangeRecoveryState(boolean isLeaderRecoverySupported) {
        final byte noChange = (byte) -1;
        int leaderId = 1;
        LeaderRecoveryState recoveryState = LeaderRecoveryState.RECOVERING;
        PartitionRegistration registration = new PartitionRegistration(
            new int[] {leaderId, leaderId + 1, leaderId + 2},
            new int[] {leaderId},
            Replicas.NONE,
            Replicas.NONE,
            leaderId,
            recoveryState,
            100,
            200
        );

        // Change the partition so that there is no leader
        PartitionChangeBuilder offlineBuilder = new PartitionChangeBuilder(
            registration,
            FOO_ID,
            0,
            brokerId -> false,
            isLeaderRecoverySupported
        );
        // Set the target ISR to empty to indicate that the last leader is offline
        offlineBuilder.setTargetIsr(Collections.emptyList());

        // The partition should stay as recovering
        PartitionChangeRecord changeRecord = (PartitionChangeRecord) offlineBuilder
            .build()
            .get()
            .message();
        assertEquals(noChange, changeRecord.leaderRecoveryState());
        assertEquals(NO_LEADER, changeRecord.leader());

        registration = registration.merge(changeRecord);

        assertEquals(NO_LEADER, registration.leader);
        assertEquals(leaderId, registration.isr[0]);
        assertEquals(recoveryState, registration.leaderRecoveryState);

        // Bring the leader back online
        PartitionChangeBuilder onlineBuilder = new PartitionChangeBuilder(
            registration,
            FOO_ID,
            0,
            brokerId -> true,
            isLeaderRecoverySupported
        );

        // The only broker in the ISR is elected leader and stays in the recovering
        changeRecord = (PartitionChangeRecord) onlineBuilder.build().get().message();
        assertEquals(noChange, changeRecord.leaderRecoveryState());

        registration = registration.merge(changeRecord);

        assertEquals(leaderId, registration.leader);
        assertEquals(leaderId, registration.isr[0]);
        assertEquals(recoveryState, registration.leaderRecoveryState);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUncleanSetsLeaderRecoveringState(boolean isLeaderRecoverySupported) {
        final byte noChange = (byte) -1;
        int leaderId = 1;
        PartitionRegistration registration = new PartitionRegistration(
            new int[] {leaderId, leaderId + 1, leaderId + 2},
            new int[] {leaderId + 1, leaderId + 2},
            Replicas.NONE,
            Replicas.NONE,
            NO_LEADER,
            LeaderRecoveryState.RECOVERED,
            100,
            200
        );

        // Change the partition using unclean leader election
        PartitionChangeBuilder onlineBuilder = new PartitionChangeBuilder(
            registration,
            FOO_ID,
            0,
            brokerId -> brokerId == leaderId,
            isLeaderRecoverySupported
        ).setElection(Election.UNCLEAN);
        

        // The partition should stay as recovering
        PartitionChangeRecord changeRecord = (PartitionChangeRecord) onlineBuilder
            .build()
            .get()
            .message();

        byte expectedRecoveryChange = noChange;
        if (isLeaderRecoverySupported) {
            expectedRecoveryChange = LeaderRecoveryState.RECOVERING.value();
        }

        assertEquals(expectedRecoveryChange, changeRecord.leaderRecoveryState());
        assertEquals(leaderId, changeRecord.leader());
        assertEquals(1, changeRecord.isr().size());
        assertEquals(leaderId, changeRecord.isr().get(0));

        registration = registration.merge(changeRecord);

        LeaderRecoveryState expectedRecovery = LeaderRecoveryState.RECOVERED;
        if (isLeaderRecoverySupported) {
            expectedRecovery = LeaderRecoveryState.RECOVERING;
        }

        assertEquals(leaderId, registration.leader);
        assertEquals(leaderId, registration.isr[0]);
        assertEquals(expectedRecovery, registration.leaderRecoveryState);
    }
}
