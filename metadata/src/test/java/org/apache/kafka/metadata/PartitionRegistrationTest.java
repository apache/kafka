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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.UnwritableMetadataException;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        PartitionRegistration a = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setDirectories(DirectoryId.unassignedArray(3)).
            setIsr(new int[]{1, 2}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(0).build();
        PartitionRegistration b = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setDirectories(DirectoryId.unassignedArray(3)).
            setIsr(new int[]{3}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(1).build();
        PartitionRegistration c = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).setDirectories(DirectoryId.unassignedArray(3)).
            setIsr(new int[]{1}).setLastKnownElr(new int[]{3}).setElr(new int[]{2}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(1).build();
        assertEquals(b, a.merge(new PartitionChangeRecord().
            setLeader(3).setIsr(Collections.singletonList(3))));
        assertEquals("isr: [1, 2] -> [3], leader: 1 -> 3, leaderEpoch: 0 -> 1, partitionEpoch: 0 -> 1",
            b.diff(a));
        assertEquals("isr: [1, 2] -> [1], elr: [] -> [2], lastKnownElr: [] -> [3], partitionEpoch: 0 -> 1",
            c.diff(a));
    }

    @Test
    public void testRecordRoundTrip() {
        PartitionRegistration registrationA = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).
            setDirectories(DirectoryId.migratingArray(3)).
            setIsr(new int[]{1, 2}).setRemovingReplicas(new int[]{1}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(0).setPartitionEpoch(0).build();
        Uuid topicId = Uuid.fromString("OGdAI5nxT_m-ds3rJMqPLA");
        int partitionId = 4;
        ApiMessageAndVersion record = registrationA.toRecord(topicId, partitionId, new ImageWriterOptions.Builder().
                setMetadataVersion(MetadataVersion.IBP_3_7_IV0).build()); // highest MV for PartitionRecord v0
        PartitionRegistration registrationB =
            new PartitionRegistration((PartitionRecord) record.message());
        assertEquals(registrationA, registrationB);
    }

    @Test
    public void testToLeaderAndIsrPartitionState() {
        PartitionRegistration a = new PartitionRegistration.Builder().
            setReplicas(new int[]{1, 2, 3}).
                setDirectories(new Uuid[]{
                    Uuid.fromString("NSmkU0ieQuy2IHN59Ce0Bw"),
                    Uuid.fromString("Y8N9gnSKSLKKFCioX2laGA"),
                    Uuid.fromString("Oi7nvb8KQPyaGEqr4JtCRw")
                }).
            setIsr(new int[]{1, 2}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(123).setPartitionEpoch(456).build();
        PartitionRegistration b = new PartitionRegistration.Builder().
            setReplicas(new int[]{2, 3, 4}).
                setDirectories(new Uuid[]{
                    Uuid.fromString("tAn3q03aQAWEYkNajXm3lA"),
                    Uuid.fromString("zgj8rqatTmWMyWBsRZyiVg"),
                    Uuid.fromString("bAAlGAz1TN2doZjtWlvhRQ")
                }).
            setIsr(new int[]{2, 3, 4}).setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(234).setPartitionEpoch(567).build();
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
        Uuid dir1 = Uuid.fromString("FbRuu7CeQtq5YFreEzg16g");
        Uuid dir2 = Uuid.fromString("4rtHTelWSSStAFMODOg3cQ");
        Uuid dir3 = Uuid.fromString("Id1WXzHURROilVxZWJNZlw");
        PartitionRegistration partition0 = new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).
            setDirectories(new Uuid[]{dir1, dir2, dir3}).
            setIsr(new int[] {1, 2, 3}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionRegistration partition1 = partition0.merge(new PartitionChangeRecord().
            setRemovingReplicas(Collections.singletonList(3)).
            setAddingReplicas(Collections.singletonList(4)).
            setReplicas(Arrays.asList(1, 2, 3, 4)).
            setDirectories(Arrays.asList(dir1, dir2, dir3, DirectoryId.UNASSIGNED)));
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4}).
            setDirectories(new Uuid[]{dir1, dir2, dir3, DirectoryId.UNASSIGNED}).
            setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {3}).setAddingReplicas(new int[] {4}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(201).build(), partition1);
        PartitionRegistration partition2 = partition1.merge(new PartitionChangeRecord().
            setIsr(Arrays.asList(1, 2, 4)).
            setRemovingReplicas(Collections.emptyList()).
            setAddingReplicas(Collections.emptyList()).
            setReplicas(Arrays.asList(1, 2, 4)).
            setDirectories(Arrays.asList(dir1, dir2, DirectoryId.UNASSIGNED)));
        assertEquals(new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 4}).
            setDirectories(new Uuid[]{dir1, dir2, DirectoryId.UNASSIGNED}).
            setIsr(new int[] {1, 2, 4}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(202).build(), partition2);
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingReplicas() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder();
        IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("You must set replicas.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingIsr() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).setDirectories(new Uuid[]{DirectoryId.UNASSIGNED});
        IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("You must set isr.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingLeader() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setDirectories(new Uuid[]{DirectoryId.LOST}).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0});

        IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("You must set leader.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingLeaderRecoveryState() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setDirectories(new Uuid[]{DirectoryId.MIGRATING}).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0}).
            setLeader(0);
        IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("You must set leader recovery state.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingLeaderEpoch() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setDirectories(new Uuid[]{Uuid.fromString("OP4I696sRmCPanlNidxJYw")}).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED);
        IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("You must set leader epoch.", exception.getMessage());
    }

    @Test
    public void testBuilderThrowsIllegalStateExceptionWhenMissingPartitionEpoch() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0}).
            setDirectories(DirectoryId.migratingArray(1)).
            setIsr(new int[]{0}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{0}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0);
        IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("You must set partition epoch.", exception.getMessage());
    }

    @Test
    public void testBuilderSuccess() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0, 1, 2}).
            setDirectories(DirectoryId.unassignedArray(3)).
            setIsr(new int[]{0, 1}).
            setElr(new int[]{2}).
            setLastKnownElr(new int[]{0, 1, 2}).
            setRemovingReplicas(new int[]{0}).
            setAddingReplicas(new int[]{1}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0).
            setPartitionEpoch(0);
        PartitionRegistration partitionRegistration = builder.build();
        assertEquals(Replicas.toList(new int[]{0, 1, 2}), Replicas.toList(partitionRegistration.replicas));
        assertEquals(Replicas.toList(new int[]{0, 1}), Replicas.toList(partitionRegistration.isr));
        assertEquals(Replicas.toList(new int[]{2}), Replicas.toList(partitionRegistration.elr));
        assertEquals(Replicas.toList(new int[]{0, 1, 2}), Replicas.toList(partitionRegistration.lastKnownElr));
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
            setDirectories(DirectoryId.migratingArray(2)).
            setIsr(new int[]{0, 1}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0).
            setPartitionEpoch(0);
        PartitionRegistration partitionRegistration = builder.build();
        assertEquals(Replicas.toList(Replicas.NONE), Replicas.toList(partitionRegistration.removingReplicas));
        assertEquals(Replicas.toList(Replicas.NONE), Replicas.toList(partitionRegistration.addingReplicas));
    }

    private static Stream<Arguments> metadataVersionsForTestPartitionRegistration() {
        return Stream.of(
            MetadataVersion.IBP_3_7_IV1,
            MetadataVersion.IBP_3_7_IV2,
            MetadataVersion.IBP_4_0_IV1
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("metadataVersionsForTestPartitionRegistration")
    public void testPartitionRegistrationToRecord(MetadataVersion metadataVersion) {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0, 1, 2, 3, 4}).
            setDirectories(new Uuid[]{
                DirectoryId.UNASSIGNED,
                Uuid.fromString("KBJBm9GVRAG9Ffe25odmmg"),
                DirectoryId.LOST,
                Uuid.fromString("7DZNT5qBS7yFF7VMMHS7kw"),
                Uuid.fromString("cJGPUZsMSEqbidOLYLOIXg")
            }).
            setIsr(new int[]{0, 1}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0).
            setPartitionEpoch(0).
            setElr(new int[]{2, 3}).
            setLastKnownElr(new int[]{4});
        PartitionRegistration partitionRegistration = builder.build();
        Uuid topicID = Uuid.randomUuid();
        PartitionRecord expectRecord = new PartitionRecord().
            setTopicId(topicID).
            setPartitionId(0).
            setReplicas(Arrays.asList(0, 1, 2, 3, 4)).
            setIsr(Arrays.asList(0, 1)).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()).
            setLeaderEpoch(0).
            setPartitionEpoch(0);
        if (metadataVersion.isElrSupported()) {
            expectRecord.
                setEligibleLeaderReplicas(Arrays.asList(2, 3)).
                setLastKnownElr(Collections.singletonList(4));
        }
        if (metadataVersion.isDirectoryAssignmentSupported()) {
            expectRecord.setDirectories(Arrays.asList(
                    DirectoryId.UNASSIGNED,
                    Uuid.fromString("KBJBm9GVRAG9Ffe25odmmg"),
                    DirectoryId.LOST,
                    Uuid.fromString("7DZNT5qBS7yFF7VMMHS7kw"),
                    Uuid.fromString("cJGPUZsMSEqbidOLYLOIXg")
            ));
        }
        List<UnwritableMetadataException> exceptions = new ArrayList<>();
        ImageWriterOptions options = new ImageWriterOptions.Builder().
                setMetadataVersion(metadataVersion).
                setLossHandler(exceptions::add).
                build();
        assertEquals(new ApiMessageAndVersion(expectRecord, metadataVersion.partitionRecordVersion()),
            partitionRegistration.toRecord(topicID, 0, options));
        if (!metadataVersion.isDirectoryAssignmentSupported()) {
            assertTrue(exceptions.stream().
                    anyMatch(e -> e.getMessage().contains("the directory assignment state of one or more replicas")));
        }
        assertEquals(Replicas.toList(Replicas.NONE), Replicas.toList(partitionRegistration.addingReplicas));
    }

    @Test
    public void testPartitionRegistrationToRecord_ElrShouldBeNullIfEmpty() {
        PartitionRegistration.Builder builder = new PartitionRegistration.Builder().
            setReplicas(new int[]{0, 1, 2, 3, 4}).
            setDirectories(DirectoryId.migratingArray(5)).
            setIsr(new int[]{0, 1}).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0).
            setPartitionEpoch(0);
        PartitionRegistration partitionRegistration = builder.build();
        Uuid topicID = Uuid.randomUuid();
        PartitionRecord expectRecord = new PartitionRecord().
            setTopicId(topicID).
            setPartitionId(0).
            setReplicas(Arrays.asList(0, 1, 2, 3, 4)).
            setIsr(Arrays.asList(0, 1)).
            setLeader(0).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()).
            setLeaderEpoch(0).
            setDirectories(Arrays.asList(DirectoryId.migratingArray(5))).
            setPartitionEpoch(0);
        List<UnwritableMetadataException> exceptions = new ArrayList<>();
        ImageWriterOptions options = new ImageWriterOptions.Builder().
            setMetadataVersion(MetadataVersion.IBP_4_0_IV1).
            setLossHandler(exceptions::add).
            build();
        assertEquals(new ApiMessageAndVersion(expectRecord, (short) 2), partitionRegistration.toRecord(topicID, 0, options));
        assertEquals(Replicas.toList(Replicas.NONE), Replicas.toList(partitionRegistration.addingReplicas));
        assertTrue(exceptions.isEmpty());
    }

    @Property
    public void testConsistentEqualsAndHashCode(
        @ForAll("uniqueSamples") PartitionRegistration a,
        @ForAll("uniqueSamples") PartitionRegistration b
    ) {
        if (a.equals(b)) {
            assertEquals(a.hashCode(), b.hashCode(), "a=" + a + "\nb=" + b);
        }

        if (a.hashCode() != b.hashCode()) {
            assertNotEquals(a, b, "a=" + a + "\nb=" + b);
        }
    }

    @Provide
    Arbitrary<PartitionRegistration> uniqueSamples() {
        return Arbitraries.of(
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setDirectories(new Uuid[]{Uuid.fromString("HyTsxr8hT6Gq5heZMA2Bug"), Uuid.fromString("ePwTiSgFRvaKRBaUX3EcZQ"), Uuid.fromString("F3zwSDR1QWGKNNLMowVoYg")}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).setElr(new int[] {1, 2, 3}).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setDirectories(new Uuid[]{Uuid.fromString("94alcrMLQ6GOV8EHfAxJnA"), Uuid.fromString("LlD2QCA5RpalzKwPsUTGpw"), Uuid.fromString("Ahfjx9j5SIKpmz48pTLFRg")}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(101).setPartitionEpoch(200).setLastKnownElr(new int[] {1, 2}).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setDirectories(new Uuid[]{Uuid.fromString("KcXLjTpYSPGjM20DjHd5rA"), Uuid.fromString("NXiBSMNHSvWqvz3qM8a6Vg"), Uuid.fromString("yWinzh1DRD25nHuXUxLfBQ")}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(201).setElr(new int[] {1, 2}).setLastKnownElr(new int[] {1, 2}).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1, 2, 3}).
                setDirectories(new Uuid[]{Uuid.fromString("9bDLWtoRRaKUToKixl3NUg"), Uuid.fromString("nLJMwhSUTEOU7DEI0U2GOw"), Uuid.fromString("ULAltTBAQlG2peJh9DZZrw")}).
                setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).setLastKnownElr(new int[] {1, 2}).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).setIsr(new int[] {1}).
                setDirectories(new Uuid[]{Uuid.fromString("kWM0QcMoRg6BHc7sdVsjZg"), Uuid.fromString("84F4VbPGTRWewKhlCYctbQ"), Uuid.fromString("W505iUM0S6a5Ds83d1WjcQ")}).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERING).setLeaderEpoch(100).setPartitionEpoch(200).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {4, 5, 6}).setAddingReplicas(new int[] {1, 2, 3}).
                setDirectories(DirectoryId.unassignedArray(6)).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).setElr(new int[] {1, 2, 3}).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {1, 2, 3}).setAddingReplicas(new int[] {4, 5, 6}).
                setDirectories(DirectoryId.migratingArray(6)).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).setLastKnownElr(new int[] {1, 2}).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setRemovingReplicas(new int[] {1, 3}).
                setDirectories(DirectoryId.unassignedArray(6)).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).setElr(new int[] {1, 2, 3}).build(),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4, 5, 6}).setIsr(new int[] {1, 2, 3}).setAddingReplicas(new int[] {4, 5, 6}).
                setDirectories(DirectoryId.migratingArray(6)).
                setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).setElr(new int[] {2, 3}).setLastKnownElr(new int[] {1, 2}).build()
        );

    }

    @Test
    public void testDirectories() {
        PartitionRegistration partitionRegistration = new PartitionRegistration.Builder().
                setReplicas(new int[] {3, 2, 1}).
                setDirectories(new Uuid[]{
                        Uuid.fromString("FbRuu7CeQtq5YFreEzg16g"),
                        Uuid.fromString("4rtHTelWSSStAFMODOg3cQ"),
                        Uuid.fromString("Id1WXzHURROilVxZWJNZlw")
                }).
                setIsr(new int[] {1, 2, 3}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(100).setPartitionEpoch(200).build();
        assertEquals(Uuid.fromString("Id1WXzHURROilVxZWJNZlw"), partitionRegistration.directory(1));
        assertEquals(Uuid.fromString("4rtHTelWSSStAFMODOg3cQ"), partitionRegistration.directory(2));
        assertEquals(Uuid.fromString("FbRuu7CeQtq5YFreEzg16g"), partitionRegistration.directory(3));
        assertThrows(IllegalArgumentException.class, () -> partitionRegistration.directory(4));
    }

    @Test
    public void testMigratingRecordDirectories() {
        PartitionRecord record = new PartitionRecord().
                setTopicId(Uuid.fromString("ONlQ7DDzQtGESsG499UDQg")).
                setPartitionId(0).
                setReplicas(Arrays.asList(0, 1)).
                setIsr(Arrays.asList(0, 1)).
                setLeader(0).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()).
                setLeaderEpoch(0).
                setPartitionEpoch(0);
        PartitionRegistration registration = new PartitionRegistration(record);
        assertArrayEquals(new Uuid[]{DirectoryId.MIGRATING, DirectoryId.MIGRATING}, registration.directories);
    }
}
