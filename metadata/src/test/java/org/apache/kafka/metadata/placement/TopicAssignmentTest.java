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

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.kafka.metadata.placement.PartitionAssignmentTest.partitionAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TopicAssignmentTest {

    @Test
    public void testTopicAssignmentReplicas() {
        List<Integer> replicasP0 = List.of(0, 1, 2);
        List<Integer> replicasP1 = List.of(1, 2, 0);
        List<PartitionAssignment> partitionAssignments = List.of(
            partitionAssignment(replicasP0),
            partitionAssignment(replicasP1)
        );
        assertEquals(partitionAssignments, new TopicAssignment(partitionAssignments).assignments());
    }

    @Test
    public void testConsistentEqualsAndHashCode() {
        List<TopicAssignment> topicAssignments = List.of(
            new TopicAssignment(
                List.of(
                    partitionAssignment(
                        List.of(0, 1, 2)
                    )
                )
            ),
            new TopicAssignment(
                List.of(
                    partitionAssignment(
                        List.of(1, 2, 0)
                    )
                )
            )
        );

        for (int i = 0; i < topicAssignments.size(); i++) {
            for (int j = 0; j < topicAssignments.size(); j++) {
                if (i == j) {
                    assertEquals(topicAssignments.get(i), topicAssignments.get(j));
                    assertEquals(topicAssignments.get(i), new TopicAssignment(topicAssignments.get(i).assignments()));
                    assertEquals(topicAssignments.get(i).hashCode(), topicAssignments.get(j).hashCode());
                } else {
                    assertNotEquals(topicAssignments.get(i), topicAssignments.get(j));
                    assertNotEquals(topicAssignments.get(i).hashCode(), topicAssignments.get(j).hashCode());
                }
            }
        }
    }

    @Test
    public void testToString() {
        List<Integer> replicas = List.of(0, 1, 2);
        List<Uuid> directories = List.of(
                Uuid.fromString("v56qeYzNRrqNtXsxzcReog"),
                Uuid.fromString("MvUIAsOiRlSePeiBHdZrSQ"),
                Uuid.fromString("jUqCchHtTHqMxeVv4dw1RA")
        );
        List<PartitionAssignment> partitionAssignments = List.of(
            new PartitionAssignment(replicas, directories::get)
        );
        TopicAssignment topicAssignment = new TopicAssignment(partitionAssignments);
        assertEquals("TopicAssignment[assignments=[PartitionAssignment(replicas=[0, 1, 2], " +
                "directories=[v56qeYzNRrqNtXsxzcReog, MvUIAsOiRlSePeiBHdZrSQ, jUqCchHtTHqMxeVv4dw1RA])]]", topicAssignment.toString());
    }
}
