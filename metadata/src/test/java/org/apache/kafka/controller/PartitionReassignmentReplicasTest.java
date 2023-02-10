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
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(40)
public class PartitionReassignmentReplicasTest {
    @Test
    public void testNoneAddedOrRemoved() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Arrays.asList(3, 2, 1), Arrays.asList(3, 2, 1));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(3, 2, 1), replicas.merged());
    }

    @Test
    public void testAdditions() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Arrays.asList(3, 2, 1), Arrays.asList(3, 6, 2, 1, 5));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Arrays.asList(5, 6), replicas.adding());
        assertEquals(Arrays.asList(3, 6, 2, 1, 5), replicas.merged());
    }

    @Test
    public void testRemovals() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Arrays.asList(3, 2, 1, 0), Arrays.asList(3, 1));
        assertEquals(Arrays.asList(0, 2), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(3, 1, 0, 2), replicas.merged());
    }

    @Test
    public void testAdditionsAndRemovals() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Arrays.asList(3, 2, 1, 0), Arrays.asList(7, 3, 1, 9));
        assertEquals(Arrays.asList(0, 2), replicas.removing());
        assertEquals(Arrays.asList(7, 9), replicas.adding());
        assertEquals(Arrays.asList(7, 3, 1, 9, 0, 2), replicas.merged());
    }

    @Test
    public void testRearrangement() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Arrays.asList(3, 2, 1, 0), Arrays.asList(0, 1, 3, 2));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(0, 1, 3, 2), replicas.merged());
    }
}
