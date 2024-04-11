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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.controller.PartitionChangeBuilder.ElectionResult;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.metadata.placement.DefaultDirProvider;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.controller.PartitionChangeBuilder.Election;
import static org.apache.kafka.controller.PartitionChangeBuilder.changeRecordIsNoOp;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;
import static org.apache.kafka.metadata.placement.PartitionAssignmentTest.partitionAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;


@Timeout(value = 40)
public class PartitionChangeBuilderTest {
    private static final DefaultDirProvider DEFAULT_DIR_PROVIDER = brokerId -> DirectoryId.UNASSIGNED;

    private static Stream<Arguments> partitionChangeRecordVersions() {
        return IntStream.range(PartitionChangeRecord.LOWEST_SUPPORTED_VERSION, PartitionChangeRecord.HIGHEST_SUPPORTED_VERSION + 1).mapToObj(version -> Arguments.of((short) version));
    }

    @Test
    public void testChangeRecordIsNoOp() {
        /* If the next few checks fail please update them based on the latest schema and make sure
         * to update changeRecordIsNoOp to take into account the new schema or tagged fields.
         */
        // Check that the supported versions haven't changed
        assertEquals(2, PartitionChangeRecord.HIGHEST_SUPPORTED_VERSION);
        assertEquals(0, PartitionChangeRecord.LOWEST_SUPPORTED_VERSION);
        // For the latest version check that the number of tagged fields hasn't changed
        TaggedFields taggedFields = (TaggedFields) PartitionChangeRecord.SCHEMA_0.get(2).def.type;
        assertEquals(6, taggedFields.numFields());

        assertTrue(changeRecordIsNoOp(new PartitionChangeRecord()));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().setLeader(1)));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
            setIsr(Arrays.asList(1, 2, 3))));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
            setRemovingReplicas(Arrays.asList(1))));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
            setAddingReplicas(Arrays.asList(4))));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
                setEligibleLeaderReplicas(Arrays.asList(5))));
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().
                setLastKnownElr(Arrays.asList(6))));
        assertFalse(
            changeRecordIsNoOp(
                new PartitionChangeRecord()
                  .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
            )
        );
        assertFalse(changeRecordIsNoOp(new PartitionChangeRecord().setDirectories(Arrays.asList(
                Uuid.fromString("5JwD0VNXRV2Wr9CCON38Tw"),
                Uuid.fromString("zpL1bRzTQXmmgdxlLHOWuw"),
                Uuid.fromString("6iGUpAkHQXC6bY0FTcPRDw")
        ))));
    }

    private static final PartitionRegistration FOO = new PartitionRegistration.Builder().
        setReplicas(new int[] {2, 1, 3}).
        setDirectories(new Uuid[]{
            Uuid.fromString("dpdvA5AZSWySmnPFTnu5Kw"),
            Uuid.fromString("V60B3cglScq3Xk8BX1NxAQ"),
            DirectoryId.UNASSIGNED,
        }).
        setIsr(new int[] {2, 1, 3}).
        setLeader(1).
        setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
        setLeaderEpoch(100).
        setPartitionEpoch(200).
        build();

    private final static Uuid FOO_ID = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

    private static MetadataVersion metadataVersionForPartitionChangeRecordVersion(short version) {
        switch (version) {
            case (short) 0:
                return MetadataVersion.IBP_3_7_IV0;
            case (short) 1:
                return MetadataVersion.IBP_3_7_IV2;
            case (short) 2:
                return MetadataVersion.IBP_3_8_IV0;
            default:
                throw new RuntimeException("Unknown PartitionChangeRecord version " + version);
        }
    }

    private static PartitionChangeBuilder createFooBuilder(MetadataVersion metadataVersion) {
        return new PartitionChangeBuilder(FOO,
                FOO_ID,
                0,
                r -> r != 3,
                metadataVersion,
                2)
                .setDefaultDirProvider(DEFAULT_DIR_PROVIDER);
    }

    private static PartitionChangeBuilder createFooBuilder(short version) {
        return new PartitionChangeBuilder(FOO,
                FOO_ID,
                0,
                r -> r != 3,
                metadataVersionForPartitionChangeRecordVersion(version), 
                2).
                setEligibleLeaderReplicasEnabled(isElrEnabled(version)).
                setDefaultDirProvider(DEFAULT_DIR_PROVIDER);
    }

    private static final PartitionRegistration BAR = new PartitionRegistration.Builder().
        setReplicas(new int[] {1, 2, 3, 4}).
        setDirectories(new Uuid[] {
            DirectoryId.UNASSIGNED,
            Uuid.fromString("X5FnAcIgTheWgTMzeO5WHw"),
            Uuid.fromString("GtrcdoSOTm2vFMGFeZq0eg"),
            Uuid.fromString("YcOqPw5ARmeKr1y9W3AkFw"),
        }).
        setIsr(new int[] {1, 2, 3}).
        setRemovingReplicas(new int[] {1}).
        setAddingReplicas(new int[] {4}).
        setLeader(1).
        setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
        setLeaderEpoch(100).
        setPartitionEpoch(200).
        build();

    private final static Uuid BAR_ID = Uuid.fromString("LKfUsCBnQKekvL9O5dY9nw");

    private static boolean isElrEnabled(short partitionChangeRecordVersion) {
        return partitionChangeRecordVersion >= 2;
    }

    private static PartitionChangeBuilder createBarBuilder(short version) {
        return new PartitionChangeBuilder(BAR,
                BAR_ID,
                0,
                r -> r != 3,
                metadataVersionForPartitionChangeRecordVersion(version),
                2).
                setEligibleLeaderReplicasEnabled(isElrEnabled(version)).
                setDefaultDirProvider(DEFAULT_DIR_PROVIDER);
    }

    private static final PartitionRegistration BAZ = new PartitionRegistration.Builder().
        setReplicas(new int[] {2, 1, 3}).
        setDirectories(new Uuid[] {
            Uuid.fromString("ywnfFpTBTbOsFdZ6uAdOmw"),
            Uuid.fromString("Th0x70ecRbWvZNNV33jyRA"),
            Uuid.fromString("j216tuSoQsC9JFd1Z5ZP6w"),
        }).
        setIsr(new int[] {1, 3}).
        setLeader(3).
        setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
        setLeaderEpoch(100).
        setPartitionEpoch(200).
        build();

    private final static Uuid BAZ_ID = Uuid.fromString("wQzt5gkSTwuQNXZF5gIw7A");

    private static PartitionChangeBuilder createBazBuilder(short version) {
        return new PartitionChangeBuilder(BAZ,
                BAZ_ID,
                0,
                __ -> true,
                metadataVersionForPartitionChangeRecordVersion(version),
                2).
                setEligibleLeaderReplicasEnabled(isElrEnabled(version)).
                setDefaultDirProvider(DEFAULT_DIR_PROVIDER);
    }

    private static final PartitionRegistration OFFLINE_WITHOUT_ELR = new PartitionRegistration.Builder().
        setReplicas(new int[] {2, 1, 3}).
        setDirectories(new Uuid[]{
           Uuid.fromString("iYGgiDV5Sb2EtH6hbgYnCA"),
           Uuid.fromString("XI2t4qAUSkGlLZSKeEVf8g"),
           Uuid.fromString("eqRW24kIRlitzQFzmovE0Q")
        }).
        setIsr(new int[] {3}).
        setLeader(-1).
        setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
        setLeaderEpoch(100).
        setPartitionEpoch(200).
        build();

    private static final PartitionRegistration OFFLINE_WITH_ELR = new PartitionRegistration.Builder().
            setReplicas(new int[] {2, 1, 3}).
            setDirectories(new Uuid[]{
                    Uuid.fromString("CQEqt7trRrmqyNxUT1CY0g"),
                    Uuid.fromString("59Mb9smoSsC0bGUP2FYV8A"),
                    Uuid.fromString("LBTmsCVJREqJuIEtwqxRDg")
            }).
            setElr(new int[] {3}).
            setIsr(new int[] {}).
            setLastKnownElr(new int[] {2}).
            setLeader(-1).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(100).
            setPartitionEpoch(200).
            build();

    private final static Uuid OFFLINE_ID = Uuid.fromString("LKfUsCBnQKekvL9O5dY9nw");

    private static PartitionChangeBuilder createOfflineBuilder(short partitionChangeRecordVersion) {
        MetadataVersion metadataVersion =
            metadataVersionForPartitionChangeRecordVersion(partitionChangeRecordVersion);
        if (metadataVersion.isElrSupported()) {
            return new PartitionChangeBuilder(OFFLINE_WITH_ELR, OFFLINE_ID, 0, r -> r == 1,
                    metadataVersion, 2).
                     setEligibleLeaderReplicasEnabled(true).
                     setDefaultDirProvider(DEFAULT_DIR_PROVIDER);
        } else {
            return new PartitionChangeBuilder(OFFLINE_WITHOUT_ELR, OFFLINE_ID, 0, r -> r == 1,
                    metadataVersion, 2).
                     setEligibleLeaderReplicasEnabled(false).
                     setDefaultDirProvider(DEFAULT_DIR_PROVIDER);
        }
    }

    private static void assertElectLeaderEquals(PartitionChangeBuilder builder,
                                               int expectedNode,
                                               boolean expectedUnclean) {
        ElectionResult electionResult = builder.electLeader();
        assertEquals(expectedNode, electionResult.node);
        assertEquals(expectedUnclean, electionResult.unclean);
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testElectLeader(short version) {
        assertElectLeaderEquals(createFooBuilder(version).setElection(Election.PREFERRED), 2, false);
        assertElectLeaderEquals(createFooBuilder(version), 1, false);
        assertElectLeaderEquals(createFooBuilder(version).setElection(Election.UNCLEAN), 1, false);
        assertElectLeaderEquals(createFooBuilder(version)
            .setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(1, 3))), 1, false);
        assertElectLeaderEquals(createFooBuilder(version).setElection(Election.UNCLEAN)
            .setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(1, 3))), 1, false);
        assertElectLeaderEquals(createFooBuilder(version)
            .setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(3))), NO_LEADER, false);
        assertElectLeaderEquals(createFooBuilder(version).setElection(Election.UNCLEAN).
            setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(3))), 2, true);
        assertElectLeaderEquals(
            createFooBuilder(version).setElection(Election.UNCLEAN)
                .setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(4))).setTargetReplicas(Arrays.asList(2, 1, 3, 4)),
            4,
            false
        );

        assertElectLeaderEquals(createBazBuilder(version).setElection(Election.PREFERRED), 3, false);
        assertElectLeaderEquals(createBazBuilder(version), 3, false);
        assertElectLeaderEquals(createBazBuilder(version).setElection(Election.UNCLEAN), 3, false);
    }

    private static void testTriggerLeaderEpochBumpIfNeededLeader(PartitionChangeBuilder builder,
                                                                 PartitionChangeRecord record,
                                                                 int expectedLeader) {
        builder.triggerLeaderEpochBumpIfNeeded(record);
        assertEquals(expectedLeader, record.leader());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testTriggerLeaderEpochBumpIfNeeded(short version) {
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(version),
            new PartitionChangeRecord(), NO_LEADER_CHANGE);
        // Shrinking the ISR doesn't increase the leader epoch
        testTriggerLeaderEpochBumpIfNeededLeader(
            createFooBuilder(version).setTargetIsrWithBrokerStates(
                AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1))
            ),
            new PartitionChangeRecord(),
            NO_LEADER_CHANGE
        );
        // Expanding the ISR doesn't increase the leader epoch
        testTriggerLeaderEpochBumpIfNeededLeader(
            createFooBuilder(version).setTargetIsrWithBrokerStates(
                AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1, 3, 4))
            ),
            new PartitionChangeRecord(),
            NO_LEADER_CHANGE
        );
        // Expanding the ISR during migration doesn't increase leader epoch
        testTriggerLeaderEpochBumpIfNeededLeader(
            createFooBuilder(version)
                .setTargetIsrWithBrokerStates(
                    AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1, 3, 4)))
                .setZkMigrationEnabled(true),
            new PartitionChangeRecord(),
            NO_LEADER_CHANGE
        );
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(version).
            setTargetReplicas(Arrays.asList(2, 1, 3, 4)), new PartitionChangeRecord(),
            NO_LEADER_CHANGE);
        testTriggerLeaderEpochBumpIfNeededLeader(createFooBuilder(version).
            setTargetReplicas(Arrays.asList(2, 1, 3, 4)),
            new PartitionChangeRecord().setLeader(2), 2);

        // Check that the leader epoch is bump if the ISR shrinks and isSkipLeaderEpochBumpSupported is not supported.
        // See KAFKA-15021 for details.
        testTriggerLeaderEpochBumpIfNeededLeader(
            new PartitionChangeBuilder(FOO, FOO_ID, 0, r -> r != 3, MetadataVersion.IBP_3_5_IV2, 2)
                .setTargetIsrWithBrokerStates(
                    AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1))
                ),
            new PartitionChangeRecord(),
            1
        );
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testLeaderEpochBumpZkMigration(short version) {
        // KAFKA-15109: Shrinking the ISR while in ZK migration mode requires a leader epoch bump
        testTriggerLeaderEpochBumpIfNeededLeader(
            createFooBuilder(version)
                .setTargetIsrWithBrokerStates(
                    AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1)))
                .setZkMigrationEnabled(true),
            new PartitionChangeRecord(),
            1
        );

        testTriggerLeaderEpochBumpIfNeededLeader(
            createFooBuilder(version)
                .setTargetIsrWithBrokerStates(
                    AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1)))
                .setZkMigrationEnabled(false),
            new PartitionChangeRecord(),
            NO_LEADER_CHANGE
        );

        // For older MV, always expect the epoch to increase regardless of ZK migration
        testTriggerLeaderEpochBumpIfNeededLeader(
            createFooBuilder(MetadataVersion.IBP_3_5_IV2)
                .setTargetIsrWithBrokerStates(
                    AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1)))
                .setZkMigrationEnabled(true),
            new PartitionChangeRecord(),
            1
        );

        testTriggerLeaderEpochBumpIfNeededLeader(
            createFooBuilder(MetadataVersion.IBP_3_5_IV2)
                .setTargetIsrWithBrokerStates(
                    AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1)))
                .setZkMigrationEnabled(false),
            new PartitionChangeRecord(),
            1
        );
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testNoChange(short version) {
        assertEquals(Optional.empty(), createFooBuilder(version).build());
        assertEquals(Optional.empty(), createFooBuilder(version).setElection(Election.UNCLEAN).build());
        assertEquals(Optional.empty(), createBarBuilder(version).build());
        assertEquals(Optional.empty(), createBarBuilder(version).setElection(Election.UNCLEAN).build());
        assertEquals(Optional.empty(), createBazBuilder(version).setElection(Election.PREFERRED).build());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testIsrChangeDoesntBumpLeaderEpoch(short version) {
        // Changing the ISR should not cause the leader epoch to increase
        assertEquals(
            // Expected
            Optional.of(
                new ApiMessageAndVersion(
                    new PartitionChangeRecord()
                      .setTopicId(FOO_ID)
                      .setPartitionId(0)
                      .setIsr(Arrays.asList(2, 1)),
                    version
                )
            ),
            // Actual
            createFooBuilder(version)
              .setTargetIsrWithBrokerStates(
                  AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 1))
              )
              .build()
        );
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testIsrChangeAndLeaderChange(short version) {
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setIsr(Arrays.asList(2, 3)).
                setLeader(2), version)),
            createFooBuilder(version).setTargetIsrWithBrokerStates(AlterPartitionRequest.
                newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2, 3))).build());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testReassignmentRearrangesReplicas(short version) {
        PartitionChangeRecord expectedRecord = new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(3, 2, 1));
        if (version >= 1) {
            Map<Integer, Uuid> dirs = DirectoryId.createAssignmentMap(FOO.replicas, FOO.directories);
            expectedRecord.setDirectories(Arrays.asList(dirs.get(3), dirs.get(2), dirs.get(1)));
        }
        assertEquals(Optional.of(new ApiMessageAndVersion(expectedRecord, version)),
            createFooBuilder(version).setTargetReplicas(Arrays.asList(3, 2, 1)).build());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testIsrEnlargementCompletesReassignment(short version) {
        PartitionChangeRecord expectedRecord = new PartitionChangeRecord().
                setTopicId(BAR_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(2, 3, 4)).
                setIsr(Arrays.asList(2, 3, 4)).
                setLeader(2).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList());
        if (version >= 1) {
            Map<Integer, Uuid> dirs = DirectoryId.createAssignmentMap(BAR.replicas, BAR.directories);
            expectedRecord.setDirectories(Arrays.asList(dirs.get(2), dirs.get(3), dirs.get(4)));
        }
        assertEquals(Optional.of(new ApiMessageAndVersion(expectedRecord, version)),
            createBarBuilder(version).setTargetIsrWithBrokerStates(AlterPartitionRequest.
                newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(1, 2, 3, 4))).build());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testRevertReassignment(short version) {
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(BAR);
        assertEquals(Arrays.asList(1, 2, 3), revert.replicas());
        assertEquals(Arrays.asList(1, 2, 3), revert.isr());
        PartitionChangeRecord expectedRecord = new PartitionChangeRecord().
                setTopicId(BAR_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(1, 2, 3)).
                setLeader(1).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList());
        if (version >= 1) {
            Map<Integer, Uuid> dirs = DirectoryId.createAssignmentMap(BAR.replicas, BAR.directories);
            expectedRecord.setDirectories(Arrays.asList(dirs.get(1), dirs.get(2), dirs.get(3)));
        }
        assertEquals(Optional.of(new ApiMessageAndVersion(expectedRecord, version)),
            createBarBuilder(version).
                setTargetReplicas(revert.replicas()).
                setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(revert.isr())).
                setTargetRemoving(Collections.emptyList()).
                setTargetAdding(Collections.emptyList()).
                build());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testRemovingReplicaReassignment(short version) {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            partitionAssignment(Replicas.toList(FOO.replicas)), partitionAssignment(Arrays.asList(1, 2)));
        assertEquals(Collections.singletonList(3), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(1, 2, 3), replicas.replicas());
        PartitionChangeRecord expectedRecord = new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(1, 2)).
                setIsr(Arrays.asList(2, 1)).
                setLeader(1);
        if (version >= 1) {
            Map<Integer, Uuid> dirs = DirectoryId.createAssignmentMap(FOO.replicas, FOO.directories);
            expectedRecord.setDirectories(Arrays.asList(dirs.get(1), dirs.get(2)));
        }
        assertEquals(Optional.of(new ApiMessageAndVersion(expectedRecord, version)),
            createFooBuilder(version).
                setTargetReplicas(replicas.replicas()).
                setTargetRemoving(replicas.removing()).
                build());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testAddingReplicaReassignment(short version) {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            partitionAssignment(Replicas.toList(FOO.replicas)), partitionAssignment(Arrays.asList(1, 2, 3, 4)));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Collections.singletonList(4), replicas.adding());
        assertEquals(Arrays.asList(1, 2, 3, 4), replicas.replicas());
        PartitionChangeRecord expectedRecord = new PartitionChangeRecord().
                setTopicId(FOO_ID).
                setPartitionId(0).
                setReplicas(Arrays.asList(1, 2, 3, 4)).
                setAddingReplicas(Collections.singletonList(4));
        if (version >= 1) {
            Map<Integer, Uuid> dirs = DirectoryId.createAssignmentMap(FOO.replicas, FOO.directories);
            expectedRecord.setDirectories(Arrays.asList(dirs.get(1), dirs.get(2), dirs.get(3), DirectoryId.UNASSIGNED));
        }
        assertEquals(Optional.of(new ApiMessageAndVersion(expectedRecord, version)),
            createFooBuilder(version).
                setTargetReplicas(replicas.replicas()).
                setTargetAdding(replicas.adding()).
                build());
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testUncleanLeaderElection(short version) {
        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(
            new PartitionChangeRecord()
                .setTopicId(FOO_ID)
                .setPartitionId(0)
                .setIsr(Arrays.asList(2))
                .setLeader(2)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value()),
            version
        );
        assertEquals(
            Optional.of(expectedRecord),
            createFooBuilder(version).setElection(Election.UNCLEAN)
                .setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(3))).build()
        );

        PartitionChangeRecord record = new PartitionChangeRecord()
            .setTopicId(OFFLINE_ID)
            .setPartitionId(0)
            .setIsr(Arrays.asList(1))
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value());

        if (version >= 2) {
            // The test partition has ELR, so unclean election will clear these fiedls.
            record.setEligibleLeaderReplicas(Collections.emptyList())
                .setLastKnownElr(Collections.emptyList());
        }

        expectedRecord = new ApiMessageAndVersion(record, version);
        assertEquals(
            Optional.of(expectedRecord),
            createOfflineBuilder(version).setElection(Election.UNCLEAN).build()
        );

        assertEquals(
            Optional.of(expectedRecord),
            createOfflineBuilder(version).setElection(Election.UNCLEAN)
                .setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(2))).build()
        );
    }

    private static Stream<Arguments> leaderRecoveryAndZkMigrationParams() {
        return Stream.of(
                arguments(true, true),
                arguments(true, false),
                arguments(false, true),
                arguments(false, false)
        );
    }

    @ParameterizedTest
    @MethodSource("leaderRecoveryAndZkMigrationParams")
    public void testChangeInLeadershipDoesNotChangeRecoveryState(boolean isLeaderRecoverySupported, boolean zkMigrationsEnabled) {
        final byte noChange = (byte) -1;
        int leaderId = 1;
        LeaderRecoveryState recoveryState = LeaderRecoveryState.RECOVERING;
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {leaderId, leaderId + 1, leaderId + 2}).
            setDirectories(new Uuid[] {
                    Uuid.fromString("1sF6XXLkSN2LtDums7CJ8Q"),
                    Uuid.fromString("iaBBVsoHQR6NDKXwliKMqw"),
                    Uuid.fromString("sHaBwjdrR2S3bL4E1RKC8Q")
            }).
            setIsr(new int[] {leaderId}).
            setLeader(leaderId).
            setLeaderRecoveryState(recoveryState).
            setLeaderEpoch(100).
            setPartitionEpoch(200).
            build();

        MetadataVersion metadataVersion = leaderRecoveryMetadataVersion(isLeaderRecoverySupported);

        // Change the partition so that there is no leader
        PartitionChangeBuilder offlineBuilder = new PartitionChangeBuilder(
            registration,
            FOO_ID,
            0,
            brokerId -> false,
            metadataVersion,
            2
        );
        offlineBuilder.setZkMigrationEnabled(zkMigrationsEnabled);
        // Set the target ISR to empty to indicate that the last leader is offline
        offlineBuilder.setTargetIsrWithBrokerStates(Collections.emptyList());

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
            metadataVersion,
            2
        );
        onlineBuilder.setZkMigrationEnabled(zkMigrationsEnabled);

        // The only broker in the ISR is elected leader and stays in the recovering
        changeRecord = (PartitionChangeRecord) onlineBuilder.build().get().message();
        assertEquals(noChange, changeRecord.leaderRecoveryState());

        registration = registration.merge(changeRecord);

        assertEquals(leaderId, registration.leader);
        assertEquals(leaderId, registration.isr[0]);
        assertEquals(recoveryState, registration.leaderRecoveryState);
    }

    @ParameterizedTest
    @MethodSource("leaderRecoveryAndZkMigrationParams")
    void testUncleanSetsLeaderRecoveringState(boolean isLeaderRecoverySupported, boolean zkMigrationsEnabled) {
        final byte noChange = (byte) -1;
        int leaderId = 1;
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {leaderId, leaderId + 1, leaderId + 2}).
            setDirectories(new Uuid[] {
                Uuid.fromString("uYpxts0pS4K4bk5XOoXB4g"),
                Uuid.fromString("kS6fHEqwRYucduWkmvsevw"),
                Uuid.fromString("De9RqRThQRGjKg3i3yzUxA")
            }).
            setIsr(new int[] {leaderId + 1, leaderId + 2}).
            setLeader(NO_LEADER).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(100).
            setPartitionEpoch(200).
            build();

        MetadataVersion metadataVersion = leaderRecoveryMetadataVersion(isLeaderRecoverySupported);

        // Change the partition using unclean leader election
        PartitionChangeBuilder onlineBuilder = new PartitionChangeBuilder(
            registration,
            FOO_ID,
            0,
            brokerId -> brokerId == leaderId,
            metadataVersion,
            2
        ).setElection(Election.UNCLEAN);
        onlineBuilder.setZkMigrationEnabled(zkMigrationsEnabled);
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

    @Test
    public void testStoppedLeaderIsDemotedAfterReassignmentCompletesEvenIfNoNewEligibleLeaders() {
        // Set up PartitionRegistration as if there's an ongoing reassignment from [0, 1] to [2, 3]
        int[] replicas = new int[] {2, 3, 0, 1};
        Uuid[] directories = {
                Uuid.fromString("XCBQClkBSZyphD87QUXzDA"),
                Uuid.fromString("Or2Rp9tTQOSVuy12hsfmTA"),
                Uuid.fromString("pThsodMNSwGvljTfc1RNVQ"),
                Uuid.fromString("d8CGoNJmS5mJdF20tc8P7g")
        };
        // The ISR starts off with the old replicas
        int[] isr = new int[] {0, 1};
        // We're removing [0, 1]
        int[] removingReplicas = new int[] {0, 1};
        // And adding [2, 3]
        int[] addingReplicas = new int[] {2, 3};
        // The leader is 0, one of the replicas we're removing
        int leader = 0;
        LeaderRecoveryState leaderRecoveryState = LeaderRecoveryState.RECOVERED;
        int leaderEpoch = 0;
        int partitionEpoch = 0;
        PartitionRegistration part = new PartitionRegistration.Builder().
            setReplicas(replicas).
            setDirectories(directories).
            setIsr(isr).
            setRemovingReplicas(removingReplicas).
            setAddingReplicas(addingReplicas).
            setLeader(leader).
            setLeaderRecoveryState(leaderRecoveryState).
            setLeaderEpoch(leaderEpoch).
            setPartitionEpoch(partitionEpoch).
            build();

        Uuid topicId = Uuid.randomUuid();
        // Always return false for valid leader. This is so none of the new replicas are valid leaders. This is so we
        // test what happens when the previous leader is a replica being "stopped" ie removed from the replicas list
        // and none of the adding replicas can be a leader. We want to make sure we do not leave the previous replica
        // being stopped as leader.
        IntPredicate isValidLeader = l -> false;

        PartitionChangeBuilder partitionChangeBuilder = new PartitionChangeBuilder(
            part,
            topicId,
            0,
            isValidLeader,
            leaderRecoveryMetadataVersion(false),
            2
        );

        // Before we build the new PartitionChangeBuilder, confirm the current leader is 0.
        assertEquals(0, part.leader);
        // The important part is that the new leader is NO_LEADER.
        assertEquals(Optional.of(new ApiMessageAndVersion(new PartitionChangeRecord().
                setTopicId(topicId).
                setPartitionId(0).
                setReplicas(Arrays.asList(2, 3)).
                setIsr(Arrays.asList(2, 3)).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()).
                setLeader(NO_LEADER),
                (short) 0)),
            partitionChangeBuilder.setTargetIsr(Arrays.asList(0, 1, 2, 3)).
                build());
    }

    private MetadataVersion leaderRecoveryMetadataVersion(boolean isSupported) {
        if (isSupported) {
            return MetadataVersion.IBP_3_2_IV0;
        } else {
            return MetadataVersion.IBP_3_1_IV0;
        }
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testEligibleLeaderReplicas_IsrShrinkBelowMinISR(short version) {
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3, 4})
            .setDirectories(new Uuid[] {
                Uuid.fromString("NeQeLdHhSXi4tQGaFcszKA"),
                Uuid.fromString("LsVrQZ73RSSuEWA8hhqQhg"),
                Uuid.fromString("0IaY4zXKRR6sROgE8yHfnw"),
                Uuid.fromString("1WxphfLCSZqMHKK4JMppuw")
            })
            .setIsr(new int[] {1, 2, 3, 4})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");
        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> r != 3,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setEligibleLeaderReplicasEnabled(isElrEnabled(version))
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
            .setUseLastKnownLeaderInBalancedRecovery(false);

        // Update ISR to {1, 2}
        builder.setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(1, 2)));

        PartitionChangeRecord record = new PartitionChangeRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setIsr(Arrays.asList(1, 2))
            .setLeader(-2)
            .setLeaderRecoveryState(LeaderRecoveryState.NO_CHANGE);
        if (version >= 2) {
            record.setEligibleLeaderReplicas(Arrays.asList(3, 4));
        }
        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(record, version);
        assertEquals(Optional.of(expectedRecord), builder.build());
        partition = partition.merge((PartitionChangeRecord) builder.build().get().message());
        if (version >= 2) {
            assertTrue(Arrays.equals(new int[]{3, 4}, partition.elr), partition.toString());
            assertTrue(Arrays.equals(new int[]{}, partition.lastKnownElr), partition.toString());
        } else {
            assertEquals(0, partition.elr.length);
            assertEquals(0, partition.lastKnownElr.length);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testEligibleLeaderReplicas_IsrExpandAboveMinISR(short version) {
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3, 4})
            .setDirectories(new Uuid[]{
                Uuid.fromString("CWgRKBKkToGn1HKzNb2qqQ"),
                Uuid.fromString("SCnk7zfSQSmlKqvV702d3A"),
                Uuid.fromString("9tO0QHlJRhimjKfH8m9d8A"),
                Uuid.fromString("JaaqVOxNT2OGVNCCIFA2JQ")
            })
            .setIsr(new int[] {1, 2})
            .setElr(new int[] {3})
            .setLastKnownElr(new int[] {4})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");
        // Min ISR is 3.
        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> r != 3,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setEligibleLeaderReplicasEnabled(isElrEnabled(version))
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
            .setUseLastKnownLeaderInBalancedRecovery(false);

        builder.setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(1, 2, 3)));
        PartitionChangeRecord record = new PartitionChangeRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setIsr(Arrays.asList(1, 2, 3))
            .setLeader(-2)
            .setLeaderRecoveryState(LeaderRecoveryState.NO_CHANGE);

        // Both versions will set the elr and lastKnownElr as empty list.
        record.setEligibleLeaderReplicas(Collections.emptyList())
            .setLastKnownElr(Collections.emptyList());
        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(record, version);
        assertEquals(Optional.of(expectedRecord), builder.build());
        partition = partition.merge((PartitionChangeRecord) builder.build().get().message());
        assertEquals(0, partition.elr.length);
        assertEquals(0, partition.lastKnownElr.length);
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testEligibleLeaderReplicas_IsrAddNewMemberNotInELR(short version) {
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3, 4})
            .setDirectories(new Uuid[]{
                Uuid.fromString("gPcIwlldQXikdUB3F4GB6w"),
                Uuid.fromString("gFs7V8mKR66z8e5qwtjIMA"),
                Uuid.fromString("zKHU2fwrRkuypqTgITl46g"),
                Uuid.fromString("zEgmBBh8QJGqbBIvzvH7JA")
            })
            .setIsr(new int[] {1})
            .setElr(new int[] {3})
            .setLastKnownElr(new int[] {2})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");
        // Min ISR is 3.
        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> r != 3,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setEligibleLeaderReplicasEnabled(isElrEnabled(version))
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
            .setUseLastKnownLeaderInBalancedRecovery(false);

        builder.setTargetIsrWithBrokerStates(AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(Arrays.asList(1, 4)));
        PartitionChangeRecord record = new PartitionChangeRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setIsr(Arrays.asList(1, 4))
            .setLeader(-2)
            .setLeaderRecoveryState(LeaderRecoveryState.NO_CHANGE);
        if (version < 2) {
            record.setEligibleLeaderReplicas(Collections.emptyList());
            record.setLastKnownElr(Collections.emptyList());
        }
        // No change is expected to ELR/LastKnownElr.
        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(record, version);
        assertEquals(Optional.of(expectedRecord), builder.build());
        partition = partition.merge((PartitionChangeRecord) builder.build().get().message());
        if (version >= 2) {
            assertTrue(Arrays.equals(new int[]{3}, partition.elr), partition.toString());
            assertTrue(Arrays.equals(new int[]{2}, partition.lastKnownElr), partition.toString());
        } else {
            assertTrue(Arrays.equals(new int[]{}, partition.elr), partition.toString());
            assertTrue(Arrays.equals(new int[]{}, partition.lastKnownElr), partition.toString());
        }
    }

    @ParameterizedTest
    @MethodSource("partitionChangeRecordVersions")
    public void testEligibleLeaderReplicas_RemoveUncleanShutdownReplicasFromElr(short version) {
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3, 4})
            .setDirectories(new Uuid[] {
                Uuid.fromString("keB9ssIPRlibyVJT5FcBVA"),
                Uuid.fromString("FhezfoReTSmHoKxi8wOIOg"),
                Uuid.fromString("QHtFxu8LShm6RiyAP6PxYg"),
                Uuid.fromString("tUJOMtvMQkGga30ydluvbQ")
            })
            .setIsr(new int[] {1})
            .setElr(new int[] {2, 3})
            .setLastKnownElr(new int[] {})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");
        // Min ISR is 3.
        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> r != 3,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setEligibleLeaderReplicasEnabled(isElrEnabled(version))
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
            .setUseLastKnownLeaderInBalancedRecovery(false);

        builder.setUncleanShutdownReplicas(Arrays.asList(3));

        PartitionChangeRecord record = new PartitionChangeRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setLeader(-2)
            .setLeaderRecoveryState(LeaderRecoveryState.NO_CHANGE);
        if (version >= 2) {
            record.setEligibleLeaderReplicas(Arrays.asList(2))
                .setLastKnownElr(Arrays.asList(3));
        } else {
            record.setEligibleLeaderReplicas(Collections.emptyList());
        }
        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(record, version);
        assertEquals(Optional.of(expectedRecord), builder.build());
        partition = partition.merge((PartitionChangeRecord) builder.build().get().message());
        if (version >= 2) {
            assertTrue(Arrays.equals(new int[]{2}, partition.elr), partition.toString());
            assertTrue(Arrays.equals(new int[]{3}, partition.lastKnownElr), partition.toString());
        } else {
            assertTrue(Arrays.equals(new int[]{}, partition.elr), partition.toString());
            assertTrue(Arrays.equals(new int[]{}, partition.lastKnownElr), partition.toString());
        }
    }

    @Test
    public void testKeepsDirectoriesAfterReassignment() {
        PartitionRegistration registration = new PartitionRegistration.Builder().
                setReplicas(new int[]{2, 1, 3}).
                setDirectories(new Uuid[]{
                        Uuid.fromString("v1PVrX6uS5m8CByXlLfmWg"),
                        Uuid.fromString("iU2znv45Q9yQkOpkTSy3jA"),
                        Uuid.fromString("fM5NKyWTQHqEihjIkUl99Q")
                }).
                setIsr(new int[]{2, 1, 3}).
                setLeader(1).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(100).
                setPartitionEpoch(200).
                build();
        Optional<ApiMessageAndVersion> built = new PartitionChangeBuilder(registration, FOO_ID,
                0, r -> true, MetadataVersion.IBP_3_7_IV2, 2).
                setTargetReplicas(Arrays.asList(3, 1, 5, 4)).
                setDirectory(5, Uuid.fromString("RNJ5oFjjSSWMMFRwqdCfJg")).
                setDefaultDirProvider(DEFAULT_DIR_PROVIDER).
                build();
        Optional<ApiMessageAndVersion> expected = Optional.of(new ApiMessageAndVersion(
                new PartitionChangeRecord().
                        setTopicId(FOO_ID).
                        setPartitionId(0).
                        setLeader(1).
                        setReplicas(Arrays.asList(3, 1, 5, 4)).
                        setDirectories(Arrays.asList(
                                Uuid.fromString("fM5NKyWTQHqEihjIkUl99Q"),
                                Uuid.fromString("iU2znv45Q9yQkOpkTSy3jA"),
                                Uuid.fromString("RNJ5oFjjSSWMMFRwqdCfJg"),
                                DirectoryId.UNASSIGNED
                        )),
                (short) 1
        ));
        assertEquals(expected, built);
    }

    @Test
    public void testUpdateDirectories() {
        PartitionRegistration registration = new PartitionRegistration.Builder().
                setReplicas(new int[]{2, 1, 3}).
                setDirectories(new Uuid[]{
                        Uuid.fromString("S1zMYZczRjWmucidLqGA5g"),
                        Uuid.fromString("9eRNXTvFTsWUJObvW51V5A"),
                        Uuid.fromString("UpePYVBgRAi3c4ujQrf3Kg")
                }).
                setIsr(new int[]{2, 1, 3}).
                setLeader(2).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(100).
                setPartitionEpoch(200).
                build();
        Optional<ApiMessageAndVersion> built = new PartitionChangeBuilder(registration, FOO_ID,
                0, r -> true, MetadataVersion.latestTesting(), 2).
                setDirectory(3, Uuid.fromString("pN1VKs9zRzK4APflpegAVg")).
                setDirectory(1, DirectoryId.LOST).
                setDefaultDirProvider(DEFAULT_DIR_PROVIDER).
                build();
        Optional<ApiMessageAndVersion> expected = Optional.of(new ApiMessageAndVersion(
                new PartitionChangeRecord().
                        setTopicId(FOO_ID).
                        setPartitionId(0).
                        setDirectories(Arrays.asList(
                                Uuid.fromString("S1zMYZczRjWmucidLqGA5g"),
                                DirectoryId.LOST,
                                Uuid.fromString("pN1VKs9zRzK4APflpegAVg")
                        )),
                (short) 2
        ));
        assertEquals(expected, built);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEligibleLeaderReplicas_ElrCanBeElected(boolean lastKnownLeaderEnabled) {
        short version = 2;
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3, 4})
            .setDirectories(DirectoryId.migratingArray(4))
            .setIsr(new int[] {1})
            .setElr(new int[] {3})
            .setLastKnownElr(lastKnownLeaderEnabled ? new int[] {} : new int[] {2})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

        // Make replica 1 offline.
        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> r != 1,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setEligibleLeaderReplicasEnabled(isElrEnabled(version))
            .setUseLastKnownLeaderInBalancedRecovery(lastKnownLeaderEnabled)
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER);

        builder.setTargetIsr(Collections.emptyList());

        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(
            new PartitionChangeRecord()
                .setTopicId(topicId)
                .setPartitionId(0)
                .setIsr(Arrays.asList(3))
                .setEligibleLeaderReplicas(Arrays.asList(1))
                .setLeader(3)
                .setLeaderRecoveryState(LeaderRecoveryState.NO_CHANGE),
            version
        );
        assertEquals(Optional.of(expectedRecord), builder.build());
        partition = partition.merge((PartitionChangeRecord) builder.build().get().message());
        assertTrue(Arrays.equals(new int[]{1}, partition.elr), partition.toString());
        assertTrue(Arrays.equals(lastKnownLeaderEnabled ? new int[]{} : new int[]{2}, partition.lastKnownElr), partition.toString());
        assertTrue(Arrays.equals(new int[]{3}, partition.isr), partition.toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEligibleLeaderReplicas_IsrCanShrinkToZero(boolean lastKnownLeaderEnabled) {
        short version = 2;
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3, 4})
            .setDirectories(new Uuid[]{
                    Uuid.fromString("MrTKKPEpRv66ZpWv4V7EBQ"),
                    Uuid.fromString("CkvgdEcWTVmdhfNuJXL0xA"),
                    Uuid.fromString("4a2coMsPRkSCsiTVWSksSw"),
                    Uuid.fromString("tmPdVjzASZ2ZqiS0cVJvtQ")
            })
            .setIsr(new int[] {1, 2, 3, 4})
            .setElr(new int[] {})
            .setLastKnownElr(new int[] {})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

        // Mark all the replicas offline.
        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> false,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setEligibleLeaderReplicasEnabled(true)
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
            .setUseLastKnownLeaderInBalancedRecovery(lastKnownLeaderEnabled);

        builder.setTargetIsr(Collections.emptyList());

        PartitionChangeRecord record = new PartitionChangeRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setIsr(Collections.emptyList())
            .setLeader(-1)
            .setLeaderRecoveryState(LeaderRecoveryState.NO_CHANGE)
            .setEligibleLeaderReplicas(Arrays.asList(1, 2, 3, 4));

        if (lastKnownLeaderEnabled) {
            record.setLastKnownElr(Arrays.asList(1));
        }

        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(record, version);
        assertEquals(Optional.of(expectedRecord), builder.build());
        partition = partition.merge((PartitionChangeRecord) builder.build().get().message());
        assertTrue(Arrays.equals(new int[]{1, 2, 3, 4}, partition.elr), partition.toString());
        if (lastKnownLeaderEnabled) {
            assertTrue(Arrays.equals(new int[]{1}, partition.lastKnownElr), partition.toString());
            builder = new PartitionChangeBuilder(partition, topicId, 0, r -> false,
                    metadataVersionForPartitionChangeRecordVersion(version), 3)
                .setElection(Election.PREFERRED)
                .setEligibleLeaderReplicasEnabled(true)
                .setUncleanShutdownReplicas(Arrays.asList(2))
                .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
                .setUseLastKnownLeaderInBalancedRecovery(lastKnownLeaderEnabled);
            PartitionChangeRecord changeRecord = (PartitionChangeRecord) builder.build().get().message();
            assertTrue(changeRecord.lastKnownElr() == null, changeRecord.toString());
        } else {
            assertTrue(Arrays.equals(new int[]{}, partition.lastKnownElr), partition.toString());
        }
        assertTrue(Arrays.equals(new int[]{}, partition.isr), partition.toString());
    }

    @Test
    public void testEligibleLeaderReplicas_ElectLastKnownLeader() {
        short version = 2;
        PartitionRegistration partition = new PartitionRegistration.Builder()
                .setReplicas(new int[] {1, 2, 3, 4})
                .setDirectories(DirectoryId.migratingArray(4))
                .setIsr(new int[] {})
                .setElr(new int[] {})
                .setLastKnownElr(new int[] {1})
                .setLeader(-1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                .setLeaderEpoch(100)
                .setPartitionEpoch(200)
                .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> true,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setUseLastKnownLeaderInBalancedRecovery(true)
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
            .setEligibleLeaderReplicasEnabled(true);

        builder.setTargetIsr(Collections.emptyList());

        ApiMessageAndVersion expectedRecord = new ApiMessageAndVersion(
            new PartitionChangeRecord()
                .setTopicId(topicId)
                .setPartitionId(0)
                .setIsr(Arrays.asList(1))
                .setLeader(1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value())
                .setLastKnownElr(Collections.emptyList()),
            version
        );
        assertEquals(Optional.of(expectedRecord), builder.build());
        partition = partition.merge((PartitionChangeRecord) builder.build().get().message());
        assertTrue(Arrays.equals(new int[]{}, partition.elr), partition.toString());
        assertTrue(Arrays.equals(new int[]{}, partition.lastKnownElr), partition.toString());
        assertTrue(Arrays.equals(new int[]{1}, partition.isr), partition.toString());
    }

    @Test
    public void testEligibleLeaderReplicas_ElectLastKnownLeaderShouldFail() {
        short version = 2;
        PartitionRegistration partition = new PartitionRegistration.Builder()
                .setReplicas(new int[] {1, 2, 3, 4})
                .setDirectories(new Uuid[]{
                        Uuid.fromString("zANDdMukTEqefOvHpmniMg"),
                        Uuid.fromString("Ui2Eq8rbRiuW7m7uiPTRyg"),
                        Uuid.fromString("MhgJOZrrTsKNcGM0XKK4aA"),
                        Uuid.fromString("Y25PaCAmRfyGIKxAThhBAw")
                })
                .setIsr(new int[] {})
                .setElr(new int[] {3})
                .setLastKnownElr(new int[] {1})
                .setLeader(-1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                .setLeaderEpoch(100)
                .setPartitionEpoch(200)
                .build();
        Uuid topicId = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

        PartitionChangeBuilder builder = new PartitionChangeBuilder(partition, topicId, 0, r -> r != 3,
                metadataVersionForPartitionChangeRecordVersion(version), 3)
            .setElection(Election.PREFERRED)
            .setEligibleLeaderReplicasEnabled(true)
            .setDefaultDirProvider(DEFAULT_DIR_PROVIDER)
            .setUseLastKnownLeaderInBalancedRecovery(true);

        builder.setTargetIsr(Collections.emptyList());

        // No change to the partition.
        assertEquals(Optional.empty(), builder.build());
    }
}
