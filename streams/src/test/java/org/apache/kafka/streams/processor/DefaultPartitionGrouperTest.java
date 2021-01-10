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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@SuppressWarnings("deprecation")
public class DefaultPartitionGrouperTest {

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
        Collections.<String>emptySet(),
        Collections.<String>emptySet());

    @Test
    public void shouldComputeGroupingForTwoGroups() {
        final PartitionGrouper grouper = new DefaultPartitionGrouper();
        final Map<TaskId, Set<TopicPartition>> expectedPartitionsForTask = new HashMap<>();
        final Map<Integer, Set<String>> topicGroups = new HashMap<>();

        int topicGroupId = 0;

        topicGroups.put(topicGroupId, mkSet("topic1"));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic1", 0)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic1", 1)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 2), mkSet(new TopicPartition("topic1", 2)));

        topicGroups.put(++topicGroupId, mkSet("topic2"));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic2", 0)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic2", 1)));

        assertEquals(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
    }

    @Test
    public void shouldComputeGroupingForSingleGroupWithMultipleTopics() {
        final PartitionGrouper grouper = new DefaultPartitionGrouper();
        final Map<TaskId, Set<TopicPartition>> expectedPartitionsForTask = new HashMap<>();
        final Map<Integer, Set<String>> topicGroups = new HashMap<>();

        final int topicGroupId = 0;

        topicGroups.put(topicGroupId, mkSet("topic1", "topic2"));
        expectedPartitionsForTask.put(
            new TaskId(topicGroupId, 0),
            mkSet(new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)));
        expectedPartitionsForTask.put(
            new TaskId(topicGroupId, 1),
            mkSet(new TopicPartition("topic1", 1), new TopicPartition("topic2", 1)));
        expectedPartitionsForTask.put(
            new TaskId(topicGroupId, 2),
            mkSet(new TopicPartition("topic1", 2)));

        assertEquals(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
    }

    @Test
    public void shouldNotCreateAnyTasksBecauseOneTopicHasUnknownPartitions() {
        final PartitionGrouper grouper = new DefaultPartitionGrouper();
        final Map<Integer, Set<String>> topicGroups = new HashMap<>();
    
        final int topicGroupId = 0;
    
        topicGroups.put(topicGroupId, mkSet("topic1", "unknownTopic", "topic2"));
        assertThrows(RuntimeException.class, () -> grouper.partitionGroups(topicGroups, metadata));
    }
}
