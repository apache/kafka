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

package org.apache.kafka.common.test;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("resource")
public class AdminUtilsTest {

    @Test
    void testFetchOrWaitForLeader() throws Exception {
        String topic = "test-topic";
        int partition = 0;
        Admin admin = mock(Admin.class);
        when(admin.describeTopics(anyCollection())).thenAnswer(new Answer<Object>() {
            boolean called = false;

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                if (called) {
                    return resultWithLeader(new Node(0, "", 0));
                } else {
                    called = true;
                    return resultWithLeader(new Node(1, "", 0));
                }
            }

            DescribeTopicsResult resultWithLeader(Node leader) {
                TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(partition, leader, List.of(), List.of());
                return AdminClientTestUtils.describeTopicsResult(topic,
                        new TopicDescription(topic, false, List.of(topicPartitionInfo)));
            }
        });

        int result = AdminUtils.fetchOrWaitForLeader(admin, topic, partition, 1000);

        assertEquals(1, result);
    }

    @Test
    void testFetchOrWaitForLeaderTimesOut() throws Exception {
        String topic = "test-topic";
        int partition = 0;
        Node leader = null;
        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(partition, leader, List.of(), List.of());
        DescribeTopicsResult describeResult = AdminClientTestUtils.describeTopicsResult(topic,
                new TopicDescription(topic, false, List.of(topicPartitionInfo)));
        Admin admin = mock(Admin.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);

        assertThrows(AssertionError.class, () ->
                        AdminUtils.fetchOrWaitForLeader(admin, topic, partition, 1),
                "Timing out after 1 ms since a leader was not elected for partition test-topic-0");
    }
}
