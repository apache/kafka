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

import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class PartitionReassignmentRevertTest {
    @Test
    public void testNoneAddedOrRemoved() throws Exception {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {3, 2, 1}).setIsr(new int[] {3, 2}).
                setRemovingReplicas(Replicas.NONE).setAddingReplicas(Replicas.NONE).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3, 2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeRemoving() throws Exception {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {3, 2, 1}).setIsr(new int[] {3, 2}).
            setRemovingReplicas(new int[]{2, 1}).setAddingReplicas(Replicas.NONE).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3, 2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeAdding() throws Exception {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {4, 5, 3, 2, 1}).setIsr(new int[] {4, 5, 2}).
            setRemovingReplicas(Replicas.NONE).setAddingReplicas(new int[]{4, 5}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeRemovingAndAdding() throws Exception {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {4, 5, 3, 2, 1}).setIsr(new int[] {4, 5, 2}).
            setRemovingReplicas(new int[]{2}).setAddingReplicas(new int[]{4, 5}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testIsrSpecialCase() throws Exception {
        PartitionRegistration registration = new PartitionRegistration.Builder().
            setReplicas(new int[] {4, 5, 3, 2, 1}).setIsr(new int[] {4, 5}).
            setRemovingReplicas(new int[]{2}).setAddingReplicas(new int[]{4, 5}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(100).setPartitionEpoch(200).build();
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3), revert.isr());
        assertTrue(revert.unclean());
    }
}
