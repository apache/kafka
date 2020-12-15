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
package org.apache.kafka.common.requests;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirCollection;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class AlterReplicaLogDirsRequestTest {

    @Test
    public void testErrorResponse() {
        AlterReplicaLogDirsRequestData data = new AlterReplicaLogDirsRequestData()
                .setDirs(new AlterReplicaLogDirCollection(
                        singletonList(new AlterReplicaLogDir()
                                .setPath("/data0")
                                .setTopics(new AlterReplicaLogDirTopicCollection(
                                        singletonList(new AlterReplicaLogDirTopic()
                                                .setName("topic")
                                                .setPartitions(asList(0, 1, 2))).iterator()))).iterator()));
        AlterReplicaLogDirsResponse errorResponse = new AlterReplicaLogDirsRequest.Builder(data).build()
                .getErrorResponse(123, new LogDirNotFoundException("/data0"));
        assertEquals(1, errorResponse.data().results().size());
        AlterReplicaLogDirTopicResult topicResponse = errorResponse.data().results().get(0);
        assertEquals("topic", topicResponse.topicName());
        assertEquals(3, topicResponse.partitions().size());
        for (int i = 0; i < 3; i++) {
            assertEquals(i, topicResponse.partitions().get(i).partitionIndex());
            assertEquals(Errors.LOG_DIR_NOT_FOUND.code(), topicResponse.partitions().get(i).errorCode());
        }
    }

    @Test
    public void testPartitionDir() {
        AlterReplicaLogDirsRequestData data = new AlterReplicaLogDirsRequestData()
                .setDirs(new AlterReplicaLogDirCollection(
                        asList(new AlterReplicaLogDir()
                                .setPath("/data0")
                                .setTopics(new AlterReplicaLogDirTopicCollection(
                                        asList(new AlterReplicaLogDirTopic()
                                                .setName("topic")
                                                .setPartitions(asList(0, 1)),
                                               new AlterReplicaLogDirTopic()
                                                .setName("topic2")
                                                .setPartitions(asList(7))).iterator())),
                                new AlterReplicaLogDir()
                                        .setPath("/data1")
                                        .setTopics(new AlterReplicaLogDirTopicCollection(
                                                asList(new AlterReplicaLogDirTopic()
                                                        .setName("topic3")
                                                        .setPartitions(asList(12))).iterator()))).iterator()));
        AlterReplicaLogDirsRequest request = new AlterReplicaLogDirsRequest.Builder(data).build();
        Map<TopicPartition, String> expect = new HashMap<>();
        expect.put(new TopicPartition("topic", 0), "/data0");
        expect.put(new TopicPartition("topic", 1), "/data0");
        expect.put(new TopicPartition("topic2", 7), "/data0");
        expect.put(new TopicPartition("topic3", 12), "/data1");
        assertEquals(expect, request.partitionDirs());
    }
}
