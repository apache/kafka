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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.server.common.TopicIdPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssignmentsHelperTest {

    private static final Uuid TOPIC_1 = Uuid.fromString("tJU174w8QiSXLimIdtUKKg");
    private static final Uuid TOPIC_2 = Uuid.fromString("3vHa6oVIRKOmm0VYhGzTJQ");
    private static final Uuid DIR_1 = Uuid.fromString("Nm6KAvyxQNS63HyB4yRsTQ");
    private static final Uuid DIR_2 = Uuid.fromString("l3Rv0JxcRLCQ6rLoGbYUgQ");
    private static final Uuid DIR_3 = Uuid.fromString("ILABYpv3SKOBjqws4SR8Ww");

    @Test
    public void testBuildRequestData() {
        Map<TopicIdPartition, Uuid> assignment = new HashMap<TopicIdPartition, Uuid>() {{
                put(new TopicIdPartition(TOPIC_1, 1), DIR_1);
                put(new TopicIdPartition(TOPIC_1, 2), DIR_2);
                put(new TopicIdPartition(TOPIC_1, 3), DIR_3);
                put(new TopicIdPartition(TOPIC_1, 4), DIR_1);
                put(new TopicIdPartition(TOPIC_2, 5), DIR_2);
            }};
        AssignReplicasToDirsRequestData built = AssignmentsHelper.buildRequestData(8, 100L, assignment);
        AssignReplicasToDirsRequestData expected = new AssignReplicasToDirsRequestData()
                .setBrokerId(8)
                .setBrokerEpoch(100L)
                .setDirectories(Arrays.asList(
                        new AssignReplicasToDirsRequestData.DirectoryData()
                                .setId(DIR_2)
                                .setTopics(Arrays.asList(
                                        new AssignReplicasToDirsRequestData.TopicData()
                                                .setTopicId(TOPIC_1)
                                                .setPartitions(Collections.singletonList(
                                                        new AssignReplicasToDirsRequestData.PartitionData()
                                                                .setPartitionIndex(2)
                                                )),
                                        new AssignReplicasToDirsRequestData.TopicData()
                                                .setTopicId(TOPIC_2)
                                                .setPartitions(Collections.singletonList(
                                                        new AssignReplicasToDirsRequestData.PartitionData()
                                                                .setPartitionIndex(5)
                                                ))
                                )),
                        new AssignReplicasToDirsRequestData.DirectoryData()
                                .setId(DIR_3)
                                .setTopics(Collections.singletonList(
                                        new AssignReplicasToDirsRequestData.TopicData()
                                                .setTopicId(TOPIC_1)
                                                .setPartitions(Collections.singletonList(
                                                        new AssignReplicasToDirsRequestData.PartitionData()
                                                                .setPartitionIndex(3)
                                                ))
                                )),
                        new AssignReplicasToDirsRequestData.DirectoryData()
                                .setId(DIR_1)
                                .setTopics(Collections.singletonList(
                                        new AssignReplicasToDirsRequestData.TopicData()
                                                .setTopicId(TOPIC_1)
                                                .setPartitions(Arrays.asList(
                                                        new AssignReplicasToDirsRequestData.PartitionData()
                                                                .setPartitionIndex(4),
                                                        new AssignReplicasToDirsRequestData.PartitionData()
                                                                .setPartitionIndex(1)
                                                ))
                                ))
                ));
        assertEquals(AssignmentsHelper.normalize(expected), AssignmentsHelper.normalize(built));
    }
}
