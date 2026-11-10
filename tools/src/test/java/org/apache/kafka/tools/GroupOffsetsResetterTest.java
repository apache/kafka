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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupOffsetsResetterTest {

    private static final Node NODE = new Node(0, "localhost", 9092);

    @Test
    public void testFilterNoneLeaderPartitionsWithDeletedTopic() {
        try (MockAdminClient adminClient = new MockAdminClient(List.of(NODE), NODE)) {
            // "alive" exists and has a leader. "deleted" is never added, so describeTopics
            // fails its future with UnknownTopicOrPartitionException, just like a deleted topic.
            adminClient.addTopic(false, "alive",
                List.of(new TopicPartitionInfo(0, NODE, List.of(NODE), List.of(NODE))), Map.of());
            GroupOffsetsResetter resetter = newResetter(adminClient);
            TopicPartition alive = new TopicPartition("alive", 0);
            TopicPartition deleted = new TopicPartition("deleted", 0);

            List<TopicPartition> result = resetter.filterNoneLeaderPartitions(List.of(alive, deleted));
            assertEquals(List.of(deleted), result);
        }
    }

    private static GroupOffsetsResetter newResetter(MockAdminClient adminClient) {
        return new GroupOffsetsResetter(adminClient, new OptionParser(),
            new GroupOffsetsResetter.GroupOffsetsResetterOptions(List.of(), List.of(), List.of(), List.of(), 5000L));
    }
}
