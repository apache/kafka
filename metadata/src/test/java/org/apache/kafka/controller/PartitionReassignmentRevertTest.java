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
    public void testNoneAddedOrRemoved() {
        PartitionRegistration registration = new PartitionRegistration(
            new int[] {3, 2, 1}, new int[] {3, 2},
                Replicas.NONE, Replicas.NONE, 3, LeaderRecoveryState.RECOVERED, 100, 200);
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3, 2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeRemoving() {
        PartitionRegistration registration = new PartitionRegistration(
            new int[] {3, 2, 1}, new int[] {3, 2},
            new int[] {2, 1}, Replicas.NONE, 3, LeaderRecoveryState.RECOVERED, 100, 200);
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3, 2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeAdding() {
        PartitionRegistration registration = new PartitionRegistration(
            new int[] {4, 5, 3, 2, 1}, new int[] {4, 5, 2},
            Replicas.NONE, new int[] {4, 5}, 3, LeaderRecoveryState.RECOVERED, 100, 200);
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testSomeRemovingAndAdding() {
        PartitionRegistration registration = new PartitionRegistration(
            new int[] {4, 5, 3, 2, 1}, new int[] {4, 5, 2},
            new int[] {2}, new int[] {4, 5}, 3, LeaderRecoveryState.RECOVERED, 100, 200);
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(2), revert.isr());
        assertFalse(revert.unclean());
    }

    @Test
    public void testIsrSpecialCase() {
        PartitionRegistration registration = new PartitionRegistration(
            new int[] {4, 5, 3, 2, 1}, new int[] {4, 5},
            new int[] {2}, new int[] {4, 5}, 3, LeaderRecoveryState.RECOVERED, 100, 200);
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(registration);
        assertEquals(Arrays.asList(3, 2, 1), revert.replicas());
        assertEquals(Arrays.asList(3), revert.isr());
        assertTrue(revert.unclean());
    }
}
