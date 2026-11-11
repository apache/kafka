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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdminClientTestUtilsTest {

    private static final String TOPIC = "test-topic";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);

    @Test
    void testCurrentLeader() throws Exception {
        Admin admin = adminWithPartitions(new TopicPartitionInfo(0, new Node(1, "", 0), List.of(), List.of()));

        assertEquals(Optional.of(1), AdminClientTestUtils.currentLeader(admin, TOPIC_PARTITION));
    }

    @Test
    void testCurrentLeaderWithNullLeader() throws Exception {
        Admin admin = adminWithPartitions(new TopicPartitionInfo(0, null, List.of(), List.of()));

        assertEquals(Optional.empty(), AdminClientTestUtils.currentLeader(admin, TOPIC_PARTITION));
    }

    @Test
    void testCurrentLeaderWithNoNodeLeader() throws Exception {
        Admin admin = adminWithPartitions(new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of()));

        assertEquals(Optional.empty(), AdminClientTestUtils.currentLeader(admin, TOPIC_PARTITION));
    }

    @Test
    void testCurrentLeaderWithUnknownPartition() throws Exception {
        Admin admin = adminWithPartitions(new TopicPartitionInfo(1, new Node(1, "", 0), List.of(), List.of()));

        assertEquals(Optional.empty(), AdminClientTestUtils.currentLeader(admin, TOPIC_PARTITION));
    }

    private static Admin adminWithPartitions(TopicPartitionInfo... partitions) {
        Admin admin = mock(Admin.class);
        when(admin.describeTopics(anyCollection())).thenReturn(AdminClientTestUtils.describeTopicsResult(TOPIC,
                new TopicDescription(TOPIC, false, List.of(partitions))));
        return admin;
    }
}
