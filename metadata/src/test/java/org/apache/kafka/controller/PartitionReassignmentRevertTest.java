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

import java.util.Arrays;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class PartitionReassignmentRevertTest {
    @Test
    public void testNoneAddedOrRemoved() {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {3, 2, 1}).setIsr(new int[] {3, 2}).
            setDirectories(new Uuid[]{
                    Uuid.fromString("Qln01zZAQMKzFTRCw22Y4w"),
                    Uuid.fromString("jjUcnIL2TxWEGMZ1mHvkPA"),
                    Uuid.fromString("JSZNFA0uQFmH1N75hQxWug")
            }).
            setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3, 2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeRemoving() {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {3, 2, 1}).setIsr(new int[] {3, 2}).
            setDirectories(new Uuid[]{
                    Uuid.fromString("WG58zlb5RR6TqdI81SCXeA"),
                    Uuid.fromString("izoB1H6TQdOExQ4XNMNXeQ"),
                    Uuid.fromString("TluNaJDjRemuy17sO7dDKg")
            }).
            setRemovingReplicas(new int[]{2, 1}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3, 2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeAdding() {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {4, 5, 3, 2, 1}).setIsr(new int[] {4, 5, 2}).
            setDirectories(new Uuid[]{
                    Uuid.fromString("gR9P3R9NQ5GhtewattwuZw"),
                    Uuid.fromString("vzgieGUjSr6vPvl3ZAWQcg"),
                    Uuid.fromString("8UWF5CQfQDqSyzcChmbrgw"),
                    Uuid.fromString("X3J9b4K5TumAM5a3YOKk2w"),
                    Uuid.fromString("LjZGhHfFRSCSCQw42dLlNA")
            }).
            setAddingReplicas(new int[]{4, 5}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeRemovingAndAdding() {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {4, 5, 3, 2, 1}).setIsr(new int[] {4, 5, 2}).
            setDirectories(new Uuid[]{
                Uuid.fromString("IHR5DKGdQju05pbDpwfdbA"),
                Uuid.fromString("9zsVmGReTDOAyuPEtp58Cw"),
                Uuid.fromString("bsUouEfRSLi50Pj3nqke2A"),
                Uuid.fromString("8l9R5BMcQZGbICOXPmxZNw"),
                Uuid.fromString("3n5Gwv8jRMiIFMgoTxVCdA")
            }).
            setRemovingReplicas(new int[]{2}).setAddingReplicas(new int[]{4, 5}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testIsrSpecialCase() {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {4, 5, 3, 2, 1}).setIsr(new int[] {4, 5}).
            setDirectories(new Uuid[]{
                    Uuid.fromString("1oXnuHL6T8y7yMtEP4FSdg"),
                    Uuid.fromString("3ddu6izxT2aCpQrA3C2bjw"),
                    Uuid.fromString("WvCpqmaZTaSbifd43Vl2Xg"),
                    Uuid.fromString("n0cmj6NgTaahRMwa75FnRA"),
                    Uuid.fromString("S2mDcyiAQAe92ZrlyodaDw")
            }).
            setRemovingReplicas(new int[]{2}).setAddingReplicas(new int[]{4, 5}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3), revert.isr());
        assertTrue(revert.unclean());
    }
}
