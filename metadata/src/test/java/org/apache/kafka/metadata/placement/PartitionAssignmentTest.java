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

package org.apache.kafka.metadata.placement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class PartitionAssignmentTest {
    public static PartitionAssignment partitionAssignment(List<Integer> replicas) {
        return new PartitionAssignment(replicas, __ -> DirectoryId.MIGRATING);
    }

    @Test
    public void testPartitionAssignmentReplicas() {
        List<Integer> replicas = Arrays.asList(0, 1, 2);
        assertEquals(replicas, partitionAssignment(replicas).replicas());
    }

    @Test
    public void testConsistentEqualsAndHashCode() {
        List<PartitionAssignment> partitionAssignments = Arrays.asList(
            partitionAssignment(
                Arrays.asList(0, 1, 2)
            ),
            partitionAssignment(
                Arrays.asList(1, 2, 0)
            )
        );

        for (int i = 0; i < partitionAssignments.size(); i++) {
            for (int j = 0; j < partitionAssignments.size(); j++) {
                if (i == j) {
                    assertEquals(partitionAssignments.get(i), partitionAssignments.get(j));
                    assertEquals(partitionAssignments.get(i), partitionAssignment(partitionAssignments.get(i).replicas()));
                    assertEquals(partitionAssignments.get(i).hashCode(), partitionAssignments.get(j).hashCode());
                } else {
                    assertNotEquals(partitionAssignments.get(i), partitionAssignments.get(j));
                    assertNotEquals(partitionAssignments.get(i).hashCode(), partitionAssignments.get(j).hashCode());
                }
            }
        }
    }

    @Test
    public void testToString() {
        List<Integer> replicas = Arrays.asList(0, 1, 2);
        List<Uuid> directories = Arrays.asList(
                Uuid.fromString("65WMNfybQpCDVulYOxMCTw"),
                Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw"),
                Uuid.fromString("wFtTi4FxTlOhhHytfxv7fQ")
        );
        PartitionAssignment partitionAssignment = new PartitionAssignment(replicas, directories::get);
        assertEquals("PartitionAssignment(replicas=[0, 1, 2], " +
                "directories=[65WMNfybQpCDVulYOxMCTw, VkZ5AkuESPGkMc2OxpKUjw, wFtTi4FxTlOhhHytfxv7fQ])", partitionAssignment.toString());
    }
}
