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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionGrouperTest {

    private final List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0])
    );

    private final Cluster metadata = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        infos,
        Collections.emptySet(),
        Collections.emptySet());

    @Test
    public void shouldComputeGroupingForTwoGroups() {
        final PartitionGrouper grouper = new PartitionGrouper();
        final Map<TaskId, Set<TopicPartition>> expectedPartitionsForTask = new HashMap<>();
        final Map<Subtopology, Set<String>> topicGroups = new HashMap<>();

        topicGroups.put(SUBTOPOLOGY_0, Set.of("topic1"));
        expectedPartitionsForTask.put(new TaskId(SUBTOPOLOGY_0.nodeGroupId, 0, SUBTOPOLOGY_0.namedTopology), Set.of(new TopicPartition("topic1", 0)));
        expectedPartitionsForTask.put(new TaskId(SUBTOPOLOGY_0.nodeGroupId, 1, SUBTOPOLOGY_0.namedTopology), Set.of(new TopicPartition("topic1", 1)));
        expectedPartitionsForTask.put(new TaskId(SUBTOPOLOGY_0.nodeGroupId, 2, SUBTOPOLOGY_0.namedTopology), Set.of(new TopicPartition("topic1", 2)));

        topicGroups.put(SUBTOPOLOGY_1, Set.of("topic2"));
        expectedPartitionsForTask.put(new TaskId(SUBTOPOLOGY_1.nodeGroupId, 0, SUBTOPOLOGY_1.namedTopology), Set.of(new TopicPartition("topic2", 0)));
        expectedPartitionsForTask.put(new TaskId(SUBTOPOLOGY_1.nodeGroupId, 1, SUBTOPOLOGY_1.namedTopology), Set.of(new TopicPartition("topic2", 1)));

        assertEquals(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
    }

    @Test
    public void shouldComputeGroupingForSingleGroupWithMultipleTopics() {
        final PartitionGrouper grouper = new PartitionGrouper();
        final Map<TaskId, Set<TopicPartition>> expectedPartitionsForTask = new HashMap<>();
        final Map<Subtopology, Set<String>> topicGroups = new HashMap<>();

        topicGroups.put(SUBTOPOLOGY_0, Set.of("topic1", "topic2"));
        expectedPartitionsForTask.put(
            new TaskId(SUBTOPOLOGY_0.nodeGroupId, 0, SUBTOPOLOGY_0.namedTopology),
            Set.of(new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)));
        expectedPartitionsForTask.put(
            new TaskId(SUBTOPOLOGY_0.nodeGroupId, 1,  SUBTOPOLOGY_0.namedTopology),
            Set.of(new TopicPartition("topic1", 1), new TopicPartition("topic2", 1)));
        expectedPartitionsForTask.put(
                new TaskId(SUBTOPOLOGY_0.nodeGroupId, 2,  SUBTOPOLOGY_0.namedTopology),
            Set.of(new TopicPartition("topic1", 2)));

        assertEquals(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
    }

    @Test
    public void shouldNotCreateAnyTasksBecauseOneTopicHasUnknownPartitions() {
        final PartitionGrouper grouper = new PartitionGrouper();
        final Map<Subtopology, Set<String>> topicGroups = new HashMap<>();
    
        topicGroups.put(SUBTOPOLOGY_0, Set.of("topic1", "unknownTopic", "topic2"));
        assertThrows(RuntimeException.class, () -> grouper.partitionGroups(topicGroups, metadata));
    }
}
