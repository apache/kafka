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

import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;


public class ControlRequestTest {
    @Test
    public void testLeaderAndIsrRequestNormalization() {
        Set<TopicPartition> tps = generateRandomTopicPartitions(10, 10);
        Map<TopicPartition, LeaderAndIsrRequest.PartitionState> partitionStates = new HashMap<>();
        for (TopicPartition tp: tps) {
            partitionStates.put(tp, new LeaderAndIsrRequest.PartitionState(0, 0, 0,
                    Collections.emptyList(), 0, Collections.emptyList(), false));
        }
        LeaderAndIsrRequest.Builder builder = new LeaderAndIsrRequest.Builder((short) 2, 0, 0, 0,
                partitionStates, Collections.emptySet());

        Assert.assertTrue(builder.build((short) 2).size() <  builder.build((short) 1).size());
    }

    @Test
    public void testUpdateMetadataRequestNormalization() {
        Set<TopicPartition> tps = generateRandomTopicPartitions(10, 10);
        Map<TopicPartition, UpdateMetadataRequest.PartitionState> partitionStates = new HashMap<>();
        for (TopicPartition tp: tps) {
            partitionStates.put(tp, new UpdateMetadataRequest.PartitionState(0, 0, 0,
                    Collections.emptyList(), 0, Collections.emptyList(), Collections.emptyList()));
        }
        UpdateMetadataRequest.Builder builder = new UpdateMetadataRequest.Builder((short) 5, 0, 0, 0,
                partitionStates, Collections.emptySet());

        Assert.assertTrue(builder.build((short) 5).size() <  builder.build((short) 4).size());
    }

    @Test
    public void testStopReplicaRequestNormalization() {
        Set<TopicPartition> tps = generateRandomTopicPartitions(10, 10);
        Map<TopicPartition, UpdateMetadataRequest.PartitionState> partitionStates = new HashMap<>();
        for (TopicPartition tp: tps) {
            partitionStates.put(tp, new UpdateMetadataRequest.PartitionState(0, 0, 0,
                    Collections.emptyList(), 0, Collections.emptyList(), Collections.emptyList()));
        }
        StopReplicaRequest.Builder builder = new StopReplicaRequest.Builder((short) 5, 0, 0, 0, false, tps);

        Assert.assertTrue(builder.build((short) 1).size() <  builder.build((short) 0).size());
    }

    private Set<TopicPartition> generateRandomTopicPartitions(int numTopic, int numPartitionPerTopic) {
        Set<TopicPartition> tps = new HashSet<>();
        Random r = new Random();
        for (int i = 0; i < numTopic; i++) {
            byte[] array = new byte[32];
            r.nextBytes(array);
            String topic = new String(array);
            for (int j = 0; j < numPartitionPerTopic; j++) {
                tps.add(new TopicPartition(topic, j));
            }
        }
        return tps;
    }

}
