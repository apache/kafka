/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.kafka.common.utils.Utils.mkSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DefaultPartitionGrouperTest {

    private List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0])
    );

    private Cluster metadata = new Cluster(Collections.singletonList(Node.noNode()), infos, Collections.<String>emptySet());

    @Test
    public void testGrouping() {
        PartitionGrouper grouper = new DefaultPartitionGrouper();
        int topicGroupId;
        Map<TaskId, Set<TopicPartition>> expectedPartitionsForTask;
        Map<Integer, Set<String>> topicGroups;

        topicGroups = new HashMap<>();
        topicGroups.put(0, mkSet("topic1"));
        topicGroups.put(1, mkSet("topic2"));

        expectedPartitionsForTask = new HashMap<>();
        topicGroupId = 0;
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic1", 0)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic1", 1)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 2), mkSet(new TopicPartition("topic1", 2)));
        topicGroupId++;
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic2", 0)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic2", 1)));

        assertEquals(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));

        topicGroups = new HashMap<>();
        topicGroups.put(0, mkSet("topic1", "topic2"));

        expectedPartitionsForTask = new HashMap<>();
        topicGroupId = 0;
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic1", 1), new TopicPartition("topic2", 1)));
        expectedPartitionsForTask.put(new TaskId(topicGroupId, 2), mkSet(new TopicPartition("topic1", 2)));

        assertEquals(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
    }
}
